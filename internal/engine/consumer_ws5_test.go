package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	transformernats "github.com/NurfitraPujo/cdc-pipeline/internal/transformer/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// fakeTransportFailingTransformer always fails with an error wrapping
// transformernats.ErrTransportFailure, simulating a sustained daya-core
// outage without any real NATS dependency -- WS-5 item 1/2's contract is
// "whatever wraps ErrTransportFailure must never isolate/DLQ," and this lets
// that contract be tested at the engine layer independent of the real NATS
// transport tests in internal/transformer/nats.
type fakeTransportFailingTransformer struct {
	calls int32
}

func (f *fakeTransportFailingTransformer) Name() string { return "fake/transport-failing" }
func (f *fakeTransportFailingTransformer) Transform(_ context.Context, _ *protocol.Message) (*protocol.Message, bool, error) {
	return nil, false, f.err()
}
func (f *fakeTransportFailingTransformer) TransformBatch(_ context.Context, _ []protocol.Message) ([]protocol.Message, error) {
	atomic.AddInt32(&f.calls, 1)
	return nil, f.err()
}
func (f *fakeTransportFailingTransformer) err() error {
	return fmt.Errorf("%w: NATS request failed: %w", transformernats.ErrTransportFailure, errors.New("nats: no responders available for request"))
}

var _ transformer.BatchTransformer = (*fakeTransportFailingTransformer)(nil)

// fakeApplicationFailingTransformer always fails with a plain application
// error (not wrapping ErrTransportFailure) -- the "doesn't use the feature"
// control: this must still isolate/DLQ exactly as before WS-5.
type fakeApplicationFailingTransformer struct{}

func (f *fakeApplicationFailingTransformer) Name() string { return "fake/application-failing" }
func (f *fakeApplicationFailingTransformer) Transform(_ context.Context, _ *protocol.Message) (*protocol.Message, bool, error) {
	return nil, false, errors.New("record rejected: malformed payload")
}
func (f *fakeApplicationFailingTransformer) TransformBatch(_ context.Context, _ []protocol.Message) ([]protocol.Message, error) {
	return nil, errors.New("record rejected: malformed payload")
}

var _ transformer.BatchTransformer = (*fakeApplicationFailingTransformer)(nil)

// fakeChanSubscriber is a minimal stream.Subscriber over a plain channel,
// used by the tests below in place of gomock's MockSubscriber so a fixed
// sequence of wmMsgs can be fed to Consumer.Run without per-call
// expectations.
type fakeChanSubscriber struct {
	ch chan *message.Message
}

func newFakeChanSubscriber(msgs ...*message.Message) *fakeChanSubscriber {
	ch := make(chan *message.Message, len(msgs)+1)
	for _, m := range msgs {
		ch <- m
	}
	return &fakeChanSubscriber{ch: ch}
}

func (f *fakeChanSubscriber) Subscribe(_ context.Context, _ string) (<-chan *message.Message, error) {
	return f.ch, nil
}
func (f *fakeChanSubscriber) Close() error { return nil }

func makeBatchMsg(uuid, table string) *message.Message {
	batch := protocol.MessageBatch{{SourceID: "s1", Table: table, Op: protocol.OpInsert, Payload: []byte(`{"id":1}`), LSN: 1}}
	data, _ := batch.MarshalMsg(nil)
	return message.NewMessage(uuid, data)
}

// TestHandleSinkError_TransportFailure_NeverIsolatesOrDLQs pins WS-5 item 1:
// an error wrapping transformernats.ErrTransportFailure must never cross
// the MaxRetries threshold into isolation/DLQ, however many times it
// recurs -- daya-core being down must always be retried, never routed to
// DLQ. Drives the real flushWithFilter -> processMessages ->
// handleSinkError path, not handleSinkError called directly.
func TestHandleSinkError_TransportFailure_NeverIsolatesOrDLQs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	// DLQ publish must NEVER be called for a transport failure, however
	// many retries pile up -- this is the crux of the test, expressed as a
	// negative expectation gomock will fail the test on if violated.
	mockPub.EXPECT().Publish("cdc_pipeline_p1_dlq", gomock.Any()).Times(0)

	retryCfg := protocol.RetryConfig{
		MaxRetries:      1, // deliberately tiny so a bug would isolate almost immediately
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
		EnableDLQ:       true,
	}

	tf := &fakeTransportFailingTransformer{}
	transformers := []ConfiguredTransformer{{Transformer: tf, OperationTypes: []protocol.OperationType{protocol.OpInsert}}}

	// Redeliver the SAME uuid repeatedly (mirroring dlq_test.go's
	// TestConsumer_DLQ), well past MaxRetries:1 -- this is what makes the
	// test load-bearing: without the transportErr guard, entry.count > 1
	// would isolate on the 2nd-3rd delivery and this test would fail on the
	// Times(0) DLQ expectation.
	redelivered := makeBatchMsg("u1", "orders")
	sub := newFakeChanSubscriber(redelivered, redelivered, redelivered, redelivered, redelivered, redelivered)
	close(sub.ch)

	c := NewConsumer("p1", "sink1", sub, mockPub, mockSink, transformers, mockKV, 1, 100*time.Millisecond, retryCfg, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.Run(ctx, "topic1")
	require.NoError(t, err)

	assert.GreaterOrEqual(t, atomic.LoadInt32(&tf.calls), int32(6), "transformer must have been invoked for every batch (never short-circuited into DLQ)")
	// mockPub's Times(0) expectation on DLQ Publish is verified by ctrl.Finish() above.
}

// TestHandleSinkError_ApplicationFailure_StillIsolatesAndDLQs is the
// control: a plain application error (not wrapping ErrTransportFailure)
// must still isolate/DLQ exactly as it did before WS-5 -- the "doesn't use
// the feature" configuration.
func TestHandleSinkError_ApplicationFailure_StillIsolatesAndDLQs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	// Must eventually DLQ -- at least one Publish to the DLQ topic. Also
	// permit RecordAck publishes on the acks topic (isolatePoisonBatch's own
	// terminal-ack bookkeeping), which is orthogonal to what this test pins.
	mockPub.EXPECT().Publish("cdc_pipeline_p1_dlq", gomock.Any()).Return(nil).MinTimes(1)
	mockPub.EXPECT().Publish("cdc_pipeline_p1_acks", gomock.Any()).Return(nil).AnyTimes()

	retryCfg := protocol.RetryConfig{
		MaxRetries:      1,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
		EnableDLQ:       true,
	}

	transformers := []ConfiguredTransformer{{Transformer: &fakeApplicationFailingTransformer{}, OperationTypes: []protocol.OperationType{protocol.OpInsert}}}

	// Redeliver the SAME uuid repeatedly (as dlq_test.go's TestConsumer_DLQ
	// does) to simulate JetStream redelivering a Nacked message -- retries
	// only accumulate against a UUID that recurs, and MaxRetries:1 needs
	// entry.count > 1 to isolate.
	redelivered := makeBatchMsg("v1", "orders")
	sub := newFakeChanSubscriber(redelivered, redelivered, redelivered)
	close(sub.ch)

	c := NewConsumer("p1", "sink1", sub, mockPub, mockSink, transformers, mockKV, 1, 100*time.Millisecond, retryCfg, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.Run(ctx, "topic1")
	require.NoError(t, err)
}

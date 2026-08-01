package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newRecordAckMsg(t *testing.T, sinkID string, lsns []uint64) *message.Message {
	t.Helper()
	ra := protocol.RecordAck{PipelineID: "p1", SourceID: "s1", SinkID: sinkID, LSNs: lsns, Timestamp: time.Now()}
	payload, err := ra.MarshalMsg(nil)
	require.NoError(t, err)
	envelope := protocol.Message{Op: protocol.OpRecordAck, SinkID: sinkID, Payload: payload, Timestamp: time.Now()}
	envData, err := envelope.MarshalMsg(nil)
	require.NoError(t, err)
	return message.NewMessage("m-"+sinkID, envData)
}

// Test 14: Producer forwards RecordAck losslessly, in order, and acks the
// NATS message only after the forward onto ackChan succeeds; no default:
// drop path exists any more (asserted by filling ackChan and observing the
// producer block rather than lose the ack).
func TestProducer_ForwardsRecordAck_Lossless_Blocking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrc := mocks.NewMockSource(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	cfg := protocol.PipelineConfig{ID: "p1", Sources: []string{"s1"}, Sinks: []string{"sinkA"}}
	srcCfg := protocol.SourceConfig{ID: "s1"}

	srcMsgChan := make(chan []protocol.Message)
	ackChan := make(chan source.SourceAck) // unbuffered: exercises the blocking send
	ackMsgChan := make(chan *message.Message)

	mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any(), []string{"sinkA"}).Return((<-chan []protocol.Message)(srcMsgChan), (chan<- source.SourceAck)(ackChan), nil)
	mockSrc.EXPECT().Stop().Return(nil).AnyTimes() // HIGH-2: Run defers source.Stop()
	mockSub.EXPECT().Subscribe(gomock.Any(), protocol.AcksTopic("p1")).Return(ackMsgChan, nil)

	p := NewProducer("p1", "nats://localhost:4222", cfg, mockSrc, mockPub, mockSub, mockKV, srcCfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		_, err := p.Run(ctx, srcCfg, protocol.Checkpoint{})
		errChan <- err
	}()

	// --- Blocking behavior: nothing reads ackChan yet. Publish one RecordAck
	// and assert the NATS message is NOT acked while the producer blocks on
	// the channel send (proves there is no default: drop path).
	blockMsg := newRecordAckMsg(t, "sinkA", []uint64{1})
	ackMsgChan <- blockMsg

	select {
	case <-blockMsg.Acked():
		t.Fatal("message was acked before ackChan send completed; blocking-send invariant violated")
	case <-time.After(150 * time.Millisecond):
		// expected: still blocked
	}

	// Now drain it — this unblocks the producer and lets the ack land.
	select {
	case got := <-ackChan:
		assert.Equal(t, source.SourceAck{SinkID: "sinkA", LSNs: []uint64{1}}, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forwarded SourceAck")
	}

	select {
	case <-blockMsg.Acked():
	case <-time.After(time.Second):
		t.Fatal("NATS message was never acked after the channel send completed")
	}

	// --- Lossless, in-order forwarding of N RecordAcks.
	const n = 5
	sent := make([]*message.Message, n)
	results := make(chan source.SourceAck, n)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			select {
			case got := <-ackChan:
				results <- got
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		sent[i] = newRecordAckMsg(t, "sinkA", []uint64{uint64(100 + i)})
		ackMsgChan <- sent[i]
	}

	<-done
	close(results)

	i := 0
	for got := range results {
		assert.Equal(t, source.SourceAck{SinkID: "sinkA", LSNs: []uint64{uint64(100 + i)}}, got, "ack %d out of order or corrupted", i)
		i++
	}
	assert.Equal(t, n, i, "expected %d SourceAcks, got %d", n, i)

	for i, m := range sent {
		select {
		case <-m.Acked():
		case <-time.After(time.Second):
			t.Fatalf("sent message %d was never acked", i)
		}
	}

	cancel()
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatal("producer did not exit after context cancel")
	}
}

// Test 15: Producer skips the IngressLSN checkpoint write for OpSnapshot and
// LSN-0 messages, and writes it only for the qualifying insert.
func TestProducer_SkipsCheckpointForSnapshotAndZeroLSN(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrc := mocks.NewMockSource(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	cfg := protocol.PipelineConfig{ID: "p1", Sources: []string{"s1"}, Sinks: []string{"sinkA"}}
	srcCfg := protocol.SourceConfig{ID: "s1"}

	srcMsgChan := make(chan []protocol.Message, 1)
	ackChan := make(chan source.SourceAck, 1)
	ackMsgChan := make(chan *message.Message)

	mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return((<-chan []protocol.Message)(srcMsgChan), (chan<- source.SourceAck)(ackChan), nil)
	mockSrc.EXPECT().Stop().Return(nil).AnyTimes() // HIGH-2: Run defers source.Stop()
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return(ackMsgChan, nil)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	p := NewProducer("p1", "nats://localhost:4222", cfg, mockSrc, mockPub, mockSub, mockKV, srcCfg)

	// The only Put call we expect is the ingress checkpoint for the insert.
	insertCheckpointKey := protocol.IngressCheckpointKey("p1", "s1", protocol.TableRef{Schema: "public", Table: "t_insert"})
	var putCalls []string
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(func(key string, data []byte) (uint64, error) {
		putCalls = append(putCalls, key)
		return 1, nil
	}).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		_, err := p.Run(ctx, srcCfg, protocol.Checkpoint{})
		errChan <- err
	}()

	srcMsgChan <- []protocol.Message{
		{SourceID: "s1", Table: "t_snapshot", Op: protocol.OpSnapshot, LSN: 0},
		{SourceID: "s1", Table: "t_insert", Op: protocol.OpInsert, LSN: 42},
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-errChan

	assert.Contains(t, putCalls, insertCheckpointKey)
	for _, key := range putCalls {
		assert.NotEqual(t, protocol.IngressCheckpointKey("p1", "s1", protocol.TableRef{Schema: "public", Table: "t_snapshot"}), key, "snapshot message must not get an ingress checkpoint write")
	}
}

// fakePublisher lets us fail/succeed Publish calls deterministically and
// records what was published (overall, and split by topic so DLQ vs
// AcksTopic traffic can be told apart), used by tests 16 / 16b and the
// terminal-decision defect tests below. flush()/isolatePoisonBatch call it
// synchronously and single-threaded in these tests, so no locking is needed.
type fakePublisher struct {
	failCount        int
	published        []*message.Message
	publishedByTopic map[string][]*message.Message
}

func newFakePublisher(failCount int) *fakePublisher {
	return &fakePublisher{failCount: failCount, publishedByTopic: make(map[string][]*message.Message)}
}

func (f *fakePublisher) Publish(topic string, messages ...*message.Message) error {
	if f.failCount > 0 {
		f.failCount--
		return errors.New("publish failed")
	}
	f.published = append(f.published, messages...)
	f.publishedByTopic[topic] = append(f.publishedByTopic[topic], messages...)
	return nil
}

func (f *fakePublisher) Close() error { return nil }

// decodeRecordAck unmarshals a raw AcksTopic wire message into its
// protocol.RecordAck payload, asserting the envelope shape along the way.
func decodeRecordAck(t *testing.T, wmMsg *message.Message) protocol.RecordAck {
	t.Helper()
	var envelope protocol.Message
	_, err := envelope.UnmarshalMsg(wmMsg.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.OpRecordAck, envelope.Op)

	var recordAck protocol.RecordAck
	_, err = recordAck.UnmarshalMsg(envelope.Payload)
	require.NoError(t, err)
	return recordAck
}

// Test 16: Consumer emits exactly one RecordAck per flush containing the
// exact LSN set (excluding OpSnapshot / LSN-0), and an exhausted ack-publish
// failure Nacks the wmMsgs without acking anything.
func TestConsumer_EmitsOneRecordAckPerFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	batch := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpSnapshot, LSN: 0},
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10},
		{SourceID: "s1", Table: "t1", Op: protocol.OpUpdate, LSN: 20},
	}
	wmMsg := message.NewMessage("wm-1", nil)
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(nil)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	c.flush(context.Background(), batch, []*message.Message{wmMsg})

	require.Len(t, pub.published, 1, "expected exactly one RecordAck publish per flush")
	var got protocol.Message
	_, err := got.UnmarshalMsg(pub.published[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, protocol.OpRecordAck, got.Op)
	assert.Equal(t, "sinkA", got.SinkID)

	var recordAck protocol.RecordAck
	_, err = recordAck.UnmarshalMsg(got.Payload)
	require.NoError(t, err)
	assert.Equal(t, []uint64{10, 20}, recordAck.LSNs, "snapshot/zero-LSN messages must be excluded")
	assert.Equal(t, "sinkA", recordAck.SinkID)

	select {
	case <-wmMsg.Acked():
	default:
		t.Fatal("wmMsg should be acked after a successful RecordAck publish")
	}
}

func TestConsumer_RecordAckPublishFailure_NacksWmMsgs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	// Fail every attempt (> recordAckMaxAttempts) so publish is exhausted.
	pub := newFakePublisher(recordAckMaxAttempts + 5)

	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	batch := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10},
	}
	wmMsg := message.NewMessage("wm-1", nil)
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c.flush(ctx, batch, []*message.Message{wmMsg})

	assert.Empty(t, pub.published, "no successful publish should have been recorded")

	select {
	case <-wmMsg.Acked():
		t.Fatal("wmMsg must NOT be acked when RecordAck publish is exhausted")
	default:
	}
	select {
	case <-wmMsg.Nacked():
	default:
		t.Fatal("wmMsg must be Nacked when RecordAck publish is exhausted, so JetStream redelivers")
	}
}

// Test 16b: publish-before-ack ordering. blockingPublisher's Publish call
// blocks until the test releases it, simulating "fault injected after
// BatchUpload succeeds but before the wmMsg ack" — i.e. a window where the
// RecordAck publish is still pending. While Publish is blocked, the wmMsg
// must not be acked yet; only once Publish returns (the RecordAck is on the
// wire) may the wmMsg ack happen. This proves there is no interleaving where
// the wmMsg is acked while the publish is still in flight.
func TestConsumer_PublishBeforeAckOrdering(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	release := make(chan struct{})
	pub := &blockingPublisher{release: release, entered: make(chan struct{})}

	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	batch := []protocol.Message{{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10}}
	wmMsg := message.NewMessage("wm-1", nil)
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(nil)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	flushDone := make(chan struct{})
	go func() {
		c.flush(context.Background(), batch, []*message.Message{wmMsg})
		close(flushDone)
	}()

	// Wait until Publish has been entered (BatchUpload succeeded, publish in
	// flight), then assert the wmMsg is NOT yet acked.
	select {
	case <-pub.entered:
	case <-time.After(time.Second):
		t.Fatal("flush never reached the RecordAck publish call")
	}

	select {
	case <-wmMsg.Acked():
		t.Fatal("wmMsg was acked while the RecordAck publish was still pending")
	case <-time.After(150 * time.Millisecond):
		// expected: publish still pending, wmMsg not yet acked
	}

	close(release) // let Publish return

	select {
	case <-wmMsg.Acked():
	case <-time.After(time.Second):
		t.Fatal("wmMsg was never acked after the RecordAck publish completed")
	}

	<-flushDone
}

type blockingPublisher struct {
	entered chan struct{}
	release chan struct{}
	once    bool
}

func (p *blockingPublisher) Publish(topic string, messages ...*message.Message) error {
	if !p.once {
		p.once = true
		close(p.entered)
	}
	<-p.release
	return nil
}

func (p *blockingPublisher) Close() error { return nil }

// dropAllTransformer is a transformer.Transformer that filters out every
// message it sees (keep=false), simulating a config that intentionally
// drops rows.
type dropAllTransformer struct{}

func (dropAllTransformer) Name() string { return "drop-all" }
func (dropAllTransformer) Transform(ctx context.Context, msg *protocol.Message) (*protocol.Message, bool, error) {
	return nil, false, nil
}

// dropByLSNTransformer drops only messages whose LSN is in drop.
type dropByLSNTransformer struct {
	drop map[uint64]bool
}

func (d dropByLSNTransformer) Name() string { return "drop-by-lsn" }
func (d dropByLSNTransformer) Transform(ctx context.Context, msg *protocol.Message) (*protocol.Message, bool, error) {
	if d.drop[msg.LSN] {
		return nil, false, nil
	}
	return msg, true, nil
}

// Defect 1/2 regression: a transformer that drops the ENTIRE batch must
// still publish a RecordAck covering the original batch's LSNs before the
// wmMsg is acked. Without this, a fully-filtered batch acks its wmMsg while
// the source's AckManager waits forever for LSNs nobody will ever confirm.
func TestConsumer_DropEverything_EmitsRecordAckForOriginalBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl) // no BatchUpload expectation: must not be called
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	transformers := []ConfiguredTransformer{
		{Transformer: dropAllTransformer{}, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}
	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, transformers, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	batch := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10, UUID: "1"},
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 20, UUID: "2"},
	}
	wmMsg := message.NewMessage("wm-1", nil)

	c.flush(context.Background(), batch, []*message.Message{wmMsg})

	require.Len(t, pub.published, 1, "expected exactly one RecordAck publish even though every row was filtered")
	recordAck := decodeRecordAck(t, pub.published[0])
	assert.ElementsMatch(t, []uint64{10, 20}, recordAck.LSNs, "RecordAck must cover the original batch's LSNs, not the (empty) uploaded set")

	select {
	case <-wmMsg.Acked():
	default:
		t.Fatal("wmMsg should be acked once the RecordAck for the filtered batch is published")
	}
}

// Defect 1/2 regression (flushWithFilter variant), so the two flush helpers
// don't drift.
func TestConsumer_FlushWithFilter_DropEverything_EmitsRecordAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	transformers := []ConfiguredTransformer{
		{Transformer: dropAllTransformer{}, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}
	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, transformers, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	batch := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 30, UUID: "1"},
	}
	wmMsg := message.NewMessage("wm-1", nil)

	c.flushWithFilter(context.Background(), batch, []*message.Message{wmMsg}, nil)

	require.Len(t, pub.published, 1)
	recordAck := decodeRecordAck(t, pub.published[0])
	assert.Equal(t, []uint64{30}, recordAck.LSNs)

	select {
	case <-wmMsg.Acked():
	default:
		t.Fatal("wmMsg should be acked once the RecordAck for the filtered batch is published")
	}
}

// Defect 2 regression: a PARTIAL drop must produce a RecordAck covering
// uploaded ∪ dropped, not just the uploaded subset.
func TestConsumer_PartialDrop_RecordAckCoversUploadedUnionDropped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	transformers := []ConfiguredTransformer{
		{Transformer: dropByLSNTransformer{drop: map[uint64]bool{20: true}}, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}
	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, transformers, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	batch := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10, UUID: "1"},
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 20, UUID: "2"}, // dropped by transformer
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 30, UUID: "3"},
	}
	wmMsg := message.NewMessage("wm-1", nil)

	var uploaded []protocol.Message
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, msgs []protocol.Message) error {
		uploaded = msgs
		return nil
	})
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	c.flush(context.Background(), batch, []*message.Message{wmMsg})

	// Sink only saw the two surviving rows...
	gotUploadedLSNs := make([]uint64, 0, len(uploaded))
	for _, m := range uploaded {
		gotUploadedLSNs = append(gotUploadedLSNs, m.LSN)
	}
	assert.ElementsMatch(t, []uint64{10, 30}, gotUploadedLSNs)

	// ...but the RecordAck must cover the dropped LSN too.
	require.Len(t, pub.published, 1)
	recordAck := decodeRecordAck(t, pub.published[0])
	assert.ElementsMatch(t, []uint64{10, 20, 30}, recordAck.LSNs, "RecordAck must cover uploaded ∪ transformer-dropped LSNs")
}

// Defect 3 regression: a durable write that succeeds in isolation mode
// (poison-batch path) must still publish a RecordAck before acking. This is
// reachable via a moderately long transient sink outage (MaxRetries
// exceeded), not an optional feature.
func TestConsumer_IsolationMode_EmitsRecordAckBeforeAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3, EnableDLQ: false}, nil, nil)

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 77, UUID: "1"},
	}
	batchData, err := protocol.MessageBatch(msgs).MarshalMsg(nil)
	require.NoError(t, err)
	wmMsg := message.NewMessage("wm-1", batchData)

	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(nil)

	c.isolatePoisonBatch(context.Background(), []*message.Message{wmMsg})

	require.Len(t, pub.published, 1, "isolation-mode durable write must still publish a RecordAck")
	recordAck := decodeRecordAck(t, pub.published[0])
	assert.Equal(t, []uint64{77}, recordAck.LSNs)

	select {
	case <-wmMsg.Acked():
	default:
		t.Fatal("wmMsg should be acked after isolation-mode BatchUpload success + RecordAck publish")
	}
}

// Defect 4 regression: routing a message to the DLQ is itself a terminal
// durability decision (the row will never be written by anyone) and must
// still produce a RecordAck before the wmMsg is acked.
func TestConsumer_DLQRoute_EmitsRecordAckBeforeAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSink := mocks.NewMockSink(ctrl) // BatchUpload never reached on this path
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	failingTf := &mockTransformer{name: "always-fails", transformErr: errors.New("boom")}
	transformers := []ConfiguredTransformer{
		{Transformer: failingTf, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}
	c := NewConsumer("p1", "sinkA", nil, pub, mockSink, transformers, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3, EnableDLQ: true}, nil, nil)

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 88, UUID: "1"},
	}
	batchData, err := protocol.MessageBatch(msgs).MarshalMsg(nil)
	require.NoError(t, err)
	wmMsg := message.NewMessage("wm-1", batchData)

	// processMessages fails (transformer error) -> routeToDLQWithAck path.
	c.isolatePoisonBatch(context.Background(), []*message.Message{wmMsg})

	dlqTopic := protocol.DLQTopic("p1")
	require.Len(t, pub.publishedByTopic[dlqTopic], 1, "message should have been routed to the DLQ")

	ackTopic := protocol.AcksTopic("p1")
	require.Len(t, pub.publishedByTopic[ackTopic], 1, "DLQ routing must still publish a RecordAck for the LSN")
	recordAck := decodeRecordAck(t, pub.publishedByTopic[ackTopic][0])
	assert.Equal(t, []uint64{88}, recordAck.LSNs)

	select {
	case <-wmMsg.Acked():
	default:
		t.Fatal("wmMsg should be acked after DLQ routing + RecordAck publish")
	}
}

// Legacy-ack rolling-deploy fix: an old-shape ack (Op=="ack", no SinkID)
// must be forwarded as a SourceAck for EVERY sink in p.config.Sinks, since
// pre-WI-5 there was no multi-sink gating and a SourceAck{SinkID:""} could
// never satisfy AckManager.required, permanently freezing the watermark
// during a rolling deploy.
func TestProducer_LegacyAck_ConfirmsAllRequiredSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrc := mocks.NewMockSource(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	cfg := protocol.PipelineConfig{ID: "p1", Sources: []string{"s1"}, Sinks: []string{"sinkA", "sinkB"}}
	srcCfg := protocol.SourceConfig{ID: "s1"}

	srcMsgChan := make(chan []protocol.Message)
	ackChan := make(chan source.SourceAck, 2)
	ackMsgChan := make(chan *message.Message)

	mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return((<-chan []protocol.Message)(srcMsgChan), (chan<- source.SourceAck)(ackChan), nil)
	mockSrc.EXPECT().Stop().Return(nil).AnyTimes() // HIGH-2: Run defers source.Stop()
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return(ackMsgChan, nil)

	p := NewProducer("p1", "nats://localhost:4222", cfg, mockSrc, mockPub, mockSub, mockKV, srcCfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		_, err := p.Run(ctx, srcCfg, protocol.Checkpoint{})
		errChan <- err
	}()

	legacy := protocol.Message{Op: "ack", SourceID: "s1", Table: "t1", LSN: 55}
	legacyData, err := legacy.MarshalMsg(nil)
	require.NoError(t, err)
	ackMsgChan <- message.NewMessage("legacy-1", legacyData)

	got := make([]source.SourceAck, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case a := <-ackChan:
			got = append(got, a)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for SourceAck %d/2", i+1)
		}
	}

	assert.ElementsMatch(t, []source.SourceAck{
		{SinkID: "sinkA", LSNs: []uint64{55}},
		{SinkID: "sinkB", LSNs: []uint64{55}},
	}, got)

	cancel()
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatal("producer did not exit after context cancel")
	}
}

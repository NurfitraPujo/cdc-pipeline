package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	stream_nats "github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	transformernats "github.com/NurfitraPujo/cdc-pipeline/internal/transformer/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
)

// recordingSink is a minimal sink.Sink that records every BatchUpload call,
// used so this test can observe exactly when (and with what) data actually
// lands, rather than inferring it from Consumer.Run's return value.
type recordingSink struct {
	mu    sync.Mutex
	calls int
	last  []protocol.Message
}

func (s *recordingSink) Name() string { return "recording-fake" }
func (s *recordingSink) BatchUpload(_ context.Context, msgs []protocol.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.last = append([]protocol.Message(nil), msgs...)
	return nil
}
func (s *recordingSink) ApplySchema(_ context.Context, _ protocol.Message) error { return nil }
func (s *recordingSink) Stop() error                                            { return nil }
func (s *recordingSink) getCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}
func (s *recordingSink) getLast() []protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// TestConsumer_DayaCoreRollingRestart_RecoversWithoutIntervention is the
// end-to-end acceptance test for the new hard requirement: the transform
// path must survive a daya-core rolling restart. Unlike the breaker/
// classification unit tests, this drives the REAL NATS transport for both
// the ingest topic (real JetStream subscriber/publisher, real Nack/
// redelivery) and the transform RPC (a real responder process this test
// starts and stops, not a fake), because a breaker unit test would pass
// while an end-to-end recovery path stayed broken -- exactly the gap this
// effort has repeatedly found by insisting on the real path.
//
// Sequence exercised, matching the acceptance criteria verbatim:
//  1. No responder is subscribed on the transform subject (daya-core mid
//     rolling-restart) -- requests fail with nats.ErrNoResponders,
//     classified as a transport failure, and the breaker opens after its
//     3-request probe rather than paying the per-request timeout on every
//     subsequent attempt.
//  2. The consumer Nacks and backs off. No DLQ publish happens (asserted as
//     a Times(0) gomock expectation -- the test fails loudly if this is
//     ever violated) and no record reaches the sink.
//  3. A responder appears (the restart completes) with no code changing on
//     the consumer side and no process restart.
//  4. Processing resumes automatically: the SAME record that failed during
//     the outage is the one that lands in the sink, and a RecordAck is
//     published -- proving the replication-slot-advancing path resumes too,
//     not just the sink write.
func TestConsumer_DayaCoreRollingRestart_RecoversWithoutIntervention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-NATS rolling-restart integration test in short mode")
	}
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Skipping integration test due to Docker/Testcontainers panic: %v", r)
		}
	}()

	ctx := context.Background()
	natsC, err := tcnats.Run(ctx,
		"nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	if err != nil {
		t.Skipf("Docker/Testcontainers not available: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	require.NoError(t, err)

	const pipelineID = "rr1"
	const sinkID = "sink1"
	ingestTopic := "cdc_pipeline_" + pipelineID + "_ingest"
	transformSubject := "daya.transform.ws5.rolling-restart"

	ingestPub, err := stream_nats.NewNatsPublisher(natsURL)
	require.NoError(t, err)
	defer func() { _ = ingestPub.Close() }()

	consumerSub, err := stream_nats.NewNatsSubscriber(natsURL, "rr-durable", ingestTopic, 10, 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = consumerSub.Close() }()

	tfRaw, err := transformernats.NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   natsURL,
		"subject":    transformSubject,
		"timeout_ms": 300.0,
		"tables":     []interface{}{"orders"},
	})
	require.NoError(t, err)
	tf, ok := tfRaw.(*transformernats.NatsProtoTransformer)
	require.True(t, ok)
	defer func() { _ = tf.Close() }()

	transformers := []ConfiguredTransformer{{Transformer: tf, OperationTypes: []protocol.OperationType{protocol.OpInsert}}}

	fakeSink := &recordingSink{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPub := mocks.NewMockPublisher(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	// The crux of the acceptance criterion: whatever happens during the
	// outage, nothing may reach the DLQ.
	mockPub.EXPECT().Publish("cdc_pipeline_rr1_dlq", gomock.Any()).Times(0)

	ackReceived := make(chan struct{}, 1)
	mockPub.EXPECT().Publish("cdc_pipeline_rr1_acks", gomock.Any()).DoAndReturn(
		func(_ string, _ ...*message.Message) error {
			select {
			case ackReceived <- struct{}{}:
			default:
			}
			return nil
		},
	).AnyTimes()

	retryCfg := protocol.RetryConfig{
		MaxRetries:      1, // deliberately tiny: a bug that lets a transport failure isolate would trip this almost immediately
		InitialInterval: 200 * time.Millisecond,
		MaxInterval:     2 * time.Second,
		EnableDLQ:       true,
	}

	c := NewConsumer(pipelineID, sinkID, consumerSub, mockPub, fakeSink, transformers, mockKV, 1, 50*time.Millisecond, retryCfg, nil, nil)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- c.Run(runCtx, ingestTopic) }()

	// Publish ONE record while no responder is subscribed on the transform
	// subject -- "daya-core mid rolling-restart."
	batch := protocol.MessageBatch{{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, TableSchema: "public", Payload: []byte(`{"id":1}`), LSN: 42, UUID: "rr-record-1"}}
	data, err := batch.MarshalMsg(nil)
	require.NoError(t, err)
	require.NoError(t, ingestPub.Publish(ingestTopic, message.NewMessage("rr-record-1", data)))

	// Let several transform attempts fail against the down responder --
	// enough to trip the breaker (3 requests, 100% failure) and prove it
	// stays fast-failing rather than retrying a live network call every
	// time (each attempt against timeout_ms:300 -- if the breaker weren't
	// working, 3s would allow at most ~10 attempts; observing the sink is
	// still empty after this window is the behavioural half of that proof,
	// the timing assertions in protobuf_ws5_circuit_test.go cover the
	// microsecond-level half).
	time.Sleep(3 * time.Second)
	assert.Equal(t, 0, fakeSink.getCalls(), "no data should reach the sink while daya-core is unreachable")

	// "The rolling restart completes": a responder becomes available again,
	// with nothing on the consumer side restarted or reconfigured.
	respConn, err := go_nats.Connect(natsURL)
	require.NoError(t, err)
	defer respConn.Close()
	respSub, err := respConn.Subscribe(transformSubject, func(msg *go_nats.Msg) {
		resp := &cdctransformv1.TransformResponse{Results: []*cdctransformv1.TransformRecordResult{{Success: true, Keep: true}}}
		respData, mErr := proto.Marshal(resp)
		if mErr != nil {
			return
		}
		_ = msg.Respond(respData)
	})
	require.NoError(t, err)
	defer func() { _ = respSub.Unsubscribe() }()

	// No restart of the consumer, no code intervention: the breaker's own
	// half-open probe (after its cooldown) must pick this up on its own and
	// resume processing.
	require.Eventually(t, func() bool {
		return fakeSink.getCalls() > 0
	}, 40*time.Second, 200*time.Millisecond, "processing must resume automatically once daya-core is reachable again, without a restart")

	select {
	case <-ackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("expected a RecordAck publish once the batch landed -- the replication-slot-advancing path must resume too, not just the sink write")
	}

	got := fakeSink.getLast()
	require.Len(t, got, 1)
	assert.Equal(t, "rr-record-1", got[0].UUID, "the SAME record that failed during the outage must be the one that lands -- nothing lost, nothing substituted")

	cancel()
	select {
	case <-runErrCh:
	case <-time.After(5 * time.Second):
	}
}

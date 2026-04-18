package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ChaosSink fails if the payload contains "poison"
type ChaosSink struct {
	SuccessCount int
	FailCount    int
}

func (s *ChaosSink) ApplySchema(ctx context.Context, m protocol.Message) error {
	return nil
}
func (s *ChaosSink) BatchUpload(ctx context.Context, batch []protocol.Message) error {
	for _, m := range batch {
		if string(m.Payload) == `{"val":"poison"}` {
			s.FailCount++
			return fmt.Errorf("POISON PILL DETECTED")
		}
	}
	s.SuccessCount += len(batch)
	return nil
}
func (s *ChaosSink) Name() string { return "chaos" }
func (s *ChaosSink) Stop() error  { return nil }

func TestE2E_DLQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1. Setup NATS
	env := Setup(t)
	defer env.Close()

	pipelineID := "p_dlq"
	topic := "cdc_pipeline_p_dlq_main"
	dlqTopic := protocol.DLQTopic(pipelineID)

	// 2. Setup Subscriber for DLQ to verify arrival
	dlqSub, err := nats.NewNatsSubscriber(env.NatsURL, "dlq-verifier", dlqTopic, 100, 1*time.Second)
	require.NoError(t, err)
	defer dlqSub.Close()
	dlqChan, err := dlqSub.Subscribe(ctx, dlqTopic)
	require.NoError(t, err)

	// 3. Setup Publisher
	pub, err := nats.NewNatsPublisher(env.NatsURL)
	require.NoError(t, err)
	defer pub.Close()

	// 4. Setup Consumer with ChaosSink
	retryCfg := protocol.RetryConfig{
		MaxRetries:      0,
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     100 * time.Millisecond,
		EnableDLQ:       true,
	}

	sub, err := nats.NewNatsSubscriber(env.NatsURL, "worker-1", topic, 100, 1*time.Second)
	require.NoError(t, err)
	defer sub.Close()

	sink := &ChaosSink{}
	cons := engine.NewConsumer(pipelineID, "sink1", sub, pub, sink, nil, env.GetKV(), 10, 100*time.Millisecond, retryCfg, nil, nil)

	go func() {
		_ = cons.Run(ctx, topic)
	}()

	// 5. Publish 3 messages: [Good, Bad, Good]
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: "insert", Payload: []byte(`{"val":"good1"}`), UUID: "u1"},
		{SourceID: "s1", Table: "t1", Op: "insert", Payload: []byte(`{"val":"poison"}`), UUID: "u2"},
		{SourceID: "s1", Table: "t1", Op: "insert", Payload: []byte(`{"val":"good2"}`), UUID: "u3"},
	}

	// We must publish them as a single batch to simulate the bulk failure
	batchData, _ := protocol.MessageBatch(msgs).MarshalMsg(nil)
	wmMsg := message.NewMessage("batch-1", batchData)
	err = pub.Publish(topic, wmMsg)
	require.NoError(t, err)

	// 6. Verify DLQ
	select {
	case dlqMsg := <-dlqChan:
		t.Logf("Received message in DLQ: %s", dlqMsg.UUID)
		var dlqBatch []protocol.Message
		_, err := protocol.UnmarshalMessageBatch(dlqMsg.Payload, &dlqBatch)
		require.NoError(t, err)
		// Watermill might deliver the whole batch if it was published as a batch.
		// Our consumer routes the whole wmMsg to DLQ.
		assert.Equal(t, "batch-1", dlqMsg.UUID)
		dlqMsg.Ack()
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for poison pill in DLQ")
	}

	// 7. Verify Sink Counts
	// In this simple test, if one wmMsg fails, we isolate it.
	// Since all 3 protocol.Messages were in ONE wmMsg, the isolation mode
	// will try to upload that ONE wmMsg individually, fail, and route to DLQ.
	// So SuccessCount will be 0 if they are all in one batch.
	// If we want to test partial success, we should publish them as separate Watermill messages.

	t.Logf("Test Complete. Sink Success: %d, Sink Failures: %d", sink.SuccessCount, sink.FailCount)
}

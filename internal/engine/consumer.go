package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/fitrapujo/daya-data-pipeline/internal/protocol"
	"github.com/fitrapujo/daya-data-pipeline/internal/sink"
	"github.com/fitrapujo/daya-data-pipeline/internal/stream"
	"github.com/nats-io/nats.go"
)

type Consumer struct {
	subscriber stream.Subscriber
	sink       sink.Sink
	kv         nats.KeyValue
	pipelineID string
	batchSize  int
	batchWait  time.Duration
}

func NewConsumer(pipelineID string, sub stream.Subscriber, snk sink.Sink, kv nats.KeyValue, batchSize int, batchWait time.Duration) *Consumer {
	return &Consumer{
		pipelineID: pipelineID,
		subscriber: sub,
		sink:       snk,
		kv:         kv,
		batchSize:  batchSize,
		batchWait:  batchWait,
	}
}

func (c *Consumer) Run(ctx context.Context, topic string) error {
	msgChan, err := c.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS: %w", err)
	}

	var batch []protocol.Message
	var wmMsgs []*message.Message
	timer := time.NewTimer(c.batchWait)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// 1. Sink-side Deduplication & Batch Upload
		// Sink implementation is responsible for idempotency
		if err := c.sink.BatchUpload(ctx, batch); err != nil {
			// NACK all messages in the batch for redelivery
			for _, m := range wmMsgs {
				m.Nack()
			}
			return fmt.Errorf("failed to upload batch to sink: %w", err)
		}

		// 2. ACK all messages on success
		for _, m := range wmMsgs {
			m.Ack()
		}

		// 3. Update Egress Checkpoint in NATS KV (for the last message in batch)
		last := batch[len(batch)-1]
		checkpoint := protocol.Checkpoint{
			EgressLSN: last.LSN,
			LastPK:    last.PK,
			Status:    "ACTIVE",
			UpdatedAt: time.Now(),
		}
		cpData, _ := checkpoint.MarshalMsg(nil)
		key := fmt.Sprintf("pipelines.%s.sources.%s.tables.%s.egress_checkpoint", c.pipelineID, last.SourceID, last.Table)
		if _, err := c.kv.Put(key, cpData); err != nil {
			// Checkpoint update failure shouldn't trigger NACK if upload was successful, 
			// but we should log it.
			fmt.Printf("Warning: Failed to update egress checkpoint: %v\n", err)
		}

		// Reset batch
		batch = nil
		wmMsgs = nil
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(c.batchWait)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		case wmMsg, ok := <-msgChan:
			if !ok {
				return nil
			}

			// 1. Deserialize from MessagePack
			var m protocol.Message
			if _, err := m.UnmarshalMsg(wmMsg.Payload); err != nil {
				fmt.Printf("Error: Failed to unmarshal MessagePack payload: %v\n", err)
				wmMsg.Nack() // Or Ack and DLQ?
				continue
			}

			batch = append(batch, m)
			wmMsgs = append(wmMsgs, wmMsg)

			if len(batch) >= c.batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

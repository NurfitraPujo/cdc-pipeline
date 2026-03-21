package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

type Consumer struct {
	subscriber stream.Subscriber
	sink       sink.Sink
	kv         nats.KeyValue
	pipelineID string
	batchSize  int
	batchWait  time.Duration
	targetLSN  uint64
	mu         sync.RWMutex
	isDraining bool
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

		if err := c.sink.BatchUpload(ctx, batch); err != nil {
			for _, m := range wmMsgs {
				m.Nack()
			}
			return fmt.Errorf("failed to upload batch to sink: %w", err)
		}

		for _, m := range wmMsgs {
			m.Ack()
		}

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
			log.Printf("Warning: Failed to update egress checkpoint: %v", err)
		}

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
			if c.checkDrained(0) {
				return nil
			}
		case wmMsg, ok := <-msgChan:
			if !ok {
				return nil
			}

			var m protocol.Message
			if _, err := m.UnmarshalMsg(wmMsg.Payload); err != nil {
				log.Printf("Error: Failed to unmarshal MessagePack payload: %v", err)
				wmMsg.Nack()
				continue
			}

			batch = append(batch, m)
			wmMsgs = append(wmMsgs, wmMsg)

			if len(batch) >= c.batchSize {
				if err := flush(); err != nil {
					return err
				}
				if c.checkDrained(m.LSN) {
					return nil
				}
			}
		}
	}
}

func (c *Consumer) Drain(targetLSN uint64) {
	c.mu.Lock()
	c.targetLSN = targetLSN
	c.isDraining = true
	c.mu.Unlock()
}

func (c *Consumer) checkDrained(currentLSN uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.isDraining {
		return false
	}
	if currentLSN >= c.targetLSN {
		return true
	}
	return false
}

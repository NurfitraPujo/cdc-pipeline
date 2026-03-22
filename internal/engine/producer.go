package engine

import (
	"context"
	"fmt"
	"log"
	"sync"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

type Producer struct {
	source       source.Source
	publisher    stream.Publisher
	kv           nats.KeyValue
	pipelineID   string
	cancelSource context.CancelFunc
	mu           sync.Mutex
}

func NewProducer(pipelineID string, src source.Source, pub stream.Publisher, kv nats.KeyValue) *Producer {
	return &Producer{
		pipelineID: pipelineID,
		source:     src,
		publisher:  pub,
		kv:         kv,
	}
}

func (p *Producer) Run(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (uint64, error) {
	sourceCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancelSource = cancel
	p.mu.Unlock()

	msgChan, ackChan, err := p.source.Start(sourceCtx, srcConfig, checkpoint)
	if err != nil {
		return 0, fmt.Errorf("failed to start source: %w", err)
	}

	var lastLSN uint64
	for {
		select {
		case <-ctx.Done():
			return lastLSN, ctx.Err()
		case msgs, ok := <-msgChan:
			if !ok {
				return lastLSN, nil
			}

			// Marshal the entire batch
			batch := protocol.MessageBatch(msgs)
			payload, err := batch.MarshalMsg(nil)
			if err != nil {
				return lastLSN, fmt.Errorf("failed to marshal message batch: %w", err)
			}

			// Use a pipeline-wide ingest topic for batches
			topic := fmt.Sprintf("daya.pipeline.%s.ingest", p.pipelineID)
			wmMsg := message.NewMessage(watermill.NewUUID(), payload)
			
			if err := p.publisher.Publish(topic, wmMsg); err != nil {
				return lastLSN, fmt.Errorf("failed to publish message batch: %w", err)
			}

			// Update last LSN and checkpoint based on the last message in the batch
			if len(msgs) > 0 {
				lastM := msgs[len(msgs)-1]
				lastLSN = lastM.LSN
				checkpoint.IngressLSN = lastM.LSN
				checkpoint.LastPK = lastM.PK
				checkpoint.Status = "ACTIVE"
				
				cpData, _ := checkpoint.MarshalMsg(nil)
				key := fmt.Sprintf("pipelines.%s.sources.%s.tables.%s.ingress_checkpoint", p.pipelineID, lastM.SourceID, lastM.Table)
				if _, err := p.kv.Put(key, cpData); err != nil {
					log.Printf("Warning: Failed to update ingress checkpoint: %v", err)
				}
			}

			// Signal acknowledgment back to the source handler
			select {
			case ackChan <- struct{}{}:
			case <-ctx.Done():
				return lastLSN, ctx.Err()
			}
		}
	}
}

func (p *Producer) Drain() {
	p.mu.Lock()
	if p.cancelSource != nil {
		p.cancelSource()
	}
	p.mu.Unlock()
}

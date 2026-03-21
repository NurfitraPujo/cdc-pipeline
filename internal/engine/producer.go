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

	msgChan, err := p.source.Start(sourceCtx, srcConfig, checkpoint)
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

			for _, m := range msgs {
				payload, err := m.MarshalMsg(nil)
				if err != nil {
					return lastLSN, fmt.Errorf("failed to marshal message: %w", err)
				}

				uuid := fmt.Sprintf("%s:%s:%d:%s", m.SourceID, m.Table, m.LSN, m.PK)
				wmMsg := message.NewMessage(watermill.NewUUID(), payload)
				wmMsg.UUID = uuid

				topic := fmt.Sprintf("daya.pipeline.%s.source.%s.table.%s", p.pipelineID, m.SourceID, m.Table)
				if err := p.publisher.Publish(topic, wmMsg); err != nil {
					return lastLSN, fmt.Errorf("failed to publish message: %w", err)
				}

				lastLSN = m.LSN
				checkpoint.IngressLSN = m.LSN
				checkpoint.LastPK = m.PK
				checkpoint.Status = "ACTIVE"
				
				cpData, _ := checkpoint.MarshalMsg(nil)
				key := fmt.Sprintf("pipelines.%s.sources.%s.tables.%s.ingress_checkpoint", p.pipelineID, m.SourceID, m.Table)
				if _, err := p.kv.Put(key, cpData); err != nil {
					log.Printf("Warning: Failed to update ingress checkpoint: %v", err)
				}
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

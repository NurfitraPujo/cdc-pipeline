package engine

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream"
	"github.com/nats-io/nats.go"
)

type Producer struct {
	source    source.Source
	publisher stream.Publisher
	kv        nats.KeyValue
	pipelineID string
}

func NewProducer(pipelineID string, src source.Source, pub stream.Publisher, kv nats.KeyValue) *Producer {
	return &Producer{
		pipelineID: pipelineID,
		source:     src,
		publisher:  pub,
		kv:         kv,
	}
}

func (p *Producer) Run(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) error {
	msgChan, err := p.source.Start(ctx, srcConfig, checkpoint)
	if err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msgs, ok := <-msgChan:
			if !ok {
				return nil
			}

			for _, m := range msgs {
				// 1. Serialize to MessagePack
				payload, err := m.MarshalMsg(nil)
				if err != nil {
					return fmt.Errorf("failed to marshal message: %w", err)
				}

				// 2. Wrap in watermill.Message
				// UUID is mapped to Source:Table:LSN:PK for deduplication
				uuid := fmt.Sprintf("%s:%s:%d:%s", m.SourceID, m.Table, m.LSN, m.PK)
				wmMsg := message.NewMessage(watermill.NewUUID(), payload) // Use watermill.NewUUID() or custom
				wmMsg.UUID = uuid // Explicitly set UUID for TrackMsgId in NATS

				// 3. Publish to NATS
				topic := fmt.Sprintf("daya.pipeline.%s.source.%s.table.%s", p.pipelineID, m.SourceID, m.Table)
				if err := p.publisher.Publish(topic, wmMsg); err != nil {
					return fmt.Errorf("failed to publish message: %w", err)
				}

				// 4. Update Ingress Checkpoint in NATS KV
				checkpoint.IngressLSN = m.LSN
				checkpoint.LastPK = m.PK
				checkpoint.Status = "ACTIVE" // Or keep current
				
				cpData, _ := checkpoint.MarshalMsg(nil)
				key := fmt.Sprintf("pipelines.%s.sources.%s.tables.%s.ingress_checkpoint", p.pipelineID, m.SourceID, m.Table)
				if _, err := p.kv.Put(key, cpData); err != nil {
					return fmt.Errorf("failed to update checkpoint: %w", err)
				}
			}
		}
	}
}

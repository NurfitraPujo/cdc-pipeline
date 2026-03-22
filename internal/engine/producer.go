package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/sony/gobreaker"
)

type Producer struct {
	pipelineID   string
	source       source.Source
	publisher    stream.Publisher
	kv           nats.KeyValue
	mu           sync.Mutex
	cancelSource context.CancelFunc
	cb           *gobreaker.CircuitBreaker
}

func NewProducer(pipelineID string, src source.Source, pub stream.Publisher, kv nats.KeyValue) *Producer {
	settings := gobreaker.Settings{
		Name:        "nats-publisher",
		MaxRequests: 3,
		Interval:    5 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Printf("Circuit Breaker [%s] changed from %s to %s", name, from, to)
		},
	}

	return &Producer{
		pipelineID: pipelineID,
		source:     src,
		publisher:  pub,
		kv:         kv,
		cb:         gobreaker.NewCircuitBreaker(settings),
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

		retryPublish:
			// Wrap NATS publishing in a circuit breaker
			_, err := p.cb.Execute(func() (interface{}, error) {
				// Marshal the entire batch
				batch := protocol.MessageBatch(msgs)
				payload, err := batch.MarshalMsg(nil)
				if err != nil {
					return nil, err
				}

				topic := fmt.Sprintf("daya.pipeline.%s.ingest", p.pipelineID)
				wmMsg := message.NewMessage(watermill.NewUUID(), payload)
				
				if err := p.publisher.Publish(topic, wmMsg); err != nil {
					return nil, err
				}
				return nil, nil
			})

			if err != nil {
				log.Printf("NATS Publish blocked by circuit breaker: %v", err)
				
				// Update table stats to show CIRCUIT_OPEN status
				if len(msgs) > 0 {
					m := msgs[0]
					stKey := protocol.TableStatsKey(p.pipelineID, m.SourceID, m.Table)
					if entry, err := p.kv.Get(stKey); err == nil {
						var st protocol.TableStats
						if err := json.Unmarshal(entry.Value(), &st); err == nil {
							st.Status = "CIRCUIT_OPEN"
							stData, _ := st.MarshalMsg(nil)
							if _, err := p.kv.Put(stKey, stData); err != nil {
								log.Printf("Warning: Failed to update stats: %v", err)
							}
						}
					}
				}

				// Wait before retrying the SAME batch
				select {
				case <-time.After(2 * time.Second):
					goto retryPublish
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				}
			}

			// Success! Update checkpoints
			if len(msgs) > 0 {
				latestByTable := make(map[string]protocol.Message)
				for _, m := range msgs {
					latestByTable[m.SourceID+"."+m.Table] = m
					if m.LSN > lastLSN {
						lastLSN = m.LSN
					}
				}

				for _, m := range latestByTable {
					cp := protocol.Checkpoint{
						IngressLSN: m.LSN,
						LastPK:     m.PK,
						Status:     "ACTIVE",
						UpdatedAt:  time.Now(),
					}
					cpData, _ := cp.MarshalMsg(nil)
					key := protocol.IngressCheckpointKey(p.pipelineID, m.SourceID, m.Table)
					if _, err := p.kv.Put(key, cpData); err != nil {
						return lastLSN, fmt.Errorf("failed to update ingress checkpoint: %w", err)
					}
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

func (p *Producer) Drain() error {
	p.mu.Lock()
	if p.cancelSource != nil {
		p.cancelSource()
	}
	p.mu.Unlock()
	return nil
}

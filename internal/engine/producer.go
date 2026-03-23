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
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/metrics"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/sony/gobreaker"
)

type Producer struct {
	pipelineID   string
	config       protocol.PipelineConfig
	source       source.Source
	publisher    stream.Publisher
	kv           nats.KeyValue
	mu           sync.Mutex
	cancelSource context.CancelFunc
	cb           *gobreaker.CircuitBreaker
}

func NewProducer(pipelineID string, cfg protocol.PipelineConfig, src source.Source, pub stream.Publisher, kv nats.KeyValue) *Producer {
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
			
			// Prometheus
			stateVal := 0.0 // Closed
			if to == gobreaker.StateOpen {
				stateVal = 1.0
			} else if to == gobreaker.StateHalfOpen {
				stateVal = 2.0
			}
			metrics.CircuitBreakerState.WithLabelValues(pipelineID).Set(stateVal)
		},
	}

	return &Producer{
		pipelineID:   pipelineID,
		config:       cfg,
		source:       src,
		publisher:    pub,
		kv:           kv,
		cb:           gobreaker.NewCircuitBreaker(settings),
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
				marker := protocol.Message{
					Op:        "drain_marker",
					Timestamp: time.Now(),
				}
				batch := protocol.MessageBatch{marker}
				payload, _ := batch.MarshalMsg(nil)
				topic := fmt.Sprintf("daya.pipeline.%s.ingest", p.pipelineID)
				wmMsg := message.NewMessage(watermill.NewUUID(), payload)
				if err := p.publisher.Publish(topic, wmMsg); err != nil {
					log.Printf("Error sending drain marker: %v", err)
				}
				return lastLSN, nil
			}

		retryPublish:
			// 1. Process Discovery
			for _, m := range msgs {
				if m.Op == "schema_change" && m.Schema != nil {
					p.handleDiscovery(m)
				}
				if m.LSN > lastLSN {
					lastLSN = m.LSN
				}
			}

			// Wrap NATS publishing in a circuit breaker
			_, err := p.cb.Execute(func() (interface{}, error) {
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
				if len(msgs) > 0 {
					m := msgs[0]
					stKey := protocol.TableStatsKey(p.pipelineID, m.SourceID, m.Table)
					if entry, err := p.kv.Get(stKey); err == nil {
						var st protocol.TableStats
						if err := json.Unmarshal(entry.Value(), &st); err == nil {
							st.Status = "CIRCUIT_OPEN"
							stData, err := st.MarshalMsg(nil)
							if err == nil {
								if _, err := p.kv.Put(stKey, stData); err != nil {
									log.Printf("Error updating circuit breaker status: %v", err)
								}
							}
						}
					}
				}

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
						log.Printf("Error updating ingress checkpoint: %v", err)
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

func (p *Producer) handleDiscovery(m protocol.Message) {
	isNew := true
	for _, t := range p.config.Tables {
		if t == m.Schema.Table {
			isNew = false
			break
		}
	}

	if isNew {
		log.Printf("New table discovered via CDC: %s.%s", m.Schema.Schema, m.Schema.Table)
		p.config.Tables = append(p.config.Tables, m.Schema.Table)
		
		data, _ := json.Marshal(p.config)
		if _, err := p.kv.Put(protocol.PipelineConfigKey(p.pipelineID), data); err != nil {
			log.Printf("Error updating pipeline config for discovery: %v", err)
		}
	}

	metaKey := fmt.Sprintf("daya.pipeline.%s.sources.%s.tables.%s.metadata", p.pipelineID, m.SourceID, m.Table)
	metaData, err := json.Marshal(m.Schema)
	if err == nil {
		if _, err := p.kv.Put(metaKey, metaData); err != nil {
			log.Printf("Error updating table metadata: %v", err)
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

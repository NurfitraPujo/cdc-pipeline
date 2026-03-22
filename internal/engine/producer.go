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

	// Discovery state
	snapshotMu    sync.RWMutex
	snapshotting  map[string]uint64 // Table -> SnapshotLSN
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
		snapshotting: make(map[string]uint64),
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
			// 1. Process Discovery & Filtering
			filteredMsgs := make([]protocol.Message, 0, len(msgs))
			for _, m := range msgs {
				// Handle Discovery
				if m.Op == "schema_change" && m.Schema != nil {
					p.handleDiscovery(m)
				}

				// LSN Cut-over Filtering
				p.snapshotMu.RLock()
				snapshotLSN, isSnapshotting := p.snapshotting[m.Table]
				p.snapshotMu.RUnlock()

				if isSnapshotting {
					if m.Op != "snapshot" && m.LSN < snapshotLSN {
						// Discard old CDC data for table currently being snapshotted
						continue
					}
					// If snapshot is complete (m.Op == "snapshot" and IsLast?), we'd clear snapshotting map.
					// For now, let's assume simple LSN cut-over.
				}

				filteredMsgs = append(filteredMsgs, m)
			}

			if len(filteredMsgs) == 0 {
				// Signal ACK even if we discarded everything to unblock source
				select {
				case ackChan <- struct{}{}:
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				}
				continue
			}

			// Wrap NATS publishing in a circuit breaker
			_, err := p.cb.Execute(func() (interface{}, error) {
				batch := protocol.MessageBatch(filteredMsgs)
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
				
				// Optional: Update table stats to show CIRCUIT_OPEN status
				if len(filteredMsgs) > 0 {
					m := filteredMsgs[0]
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

				select {
				case <-time.After(2 * time.Second):
					goto retryPublish
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				}
			}

			// Success! Update checkpoints
			if len(filteredMsgs) > 0 {
				latestByTable := make(map[string]protocol.Message)
				for _, m := range filteredMsgs {
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

func (p *Producer) handleDiscovery(m protocol.Message) {
	// Check if table is new to this pipeline
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
		
		// 1. Mark as snapshotting with current LSN
		p.snapshotMu.Lock()
		p.snapshotting[m.Schema.Table] = m.LSN
		p.snapshotMu.Unlock()

		// 2. Update PipelineConfig in NATS KV to trigger reload and snapshot
		// NOTE: In a granular worker model, we might not want a full reload here.
		// But based on our "Self-Expanding Loop" design, we do.
		data, _ := json.Marshal(p.config)
		if _, err := p.kv.Put(protocol.PipelineConfigKey(p.pipelineID), data); err != nil {
			log.Printf("Error updating pipeline config for discovery: %v", err)
		}
	}

	// Update TableMetadata in KV
	metaKey := fmt.Sprintf("daya.pipeline.%s.sources.%s.tables.%s.metadata", p.pipelineID, m.SourceID, m.Table)
	metaData, _ := json.Marshal(m.Schema)
	if _, err := p.kv.Put(metaKey, metaData); err != nil {
		log.Printf("Error updating table metadata: %v", err)
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

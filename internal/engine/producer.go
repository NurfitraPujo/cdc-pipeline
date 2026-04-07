package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
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
			log.Info().Str("breaker", name).Str("from", from.String()).Str("to", to.String()).Msg("Circuit Breaker changed state")

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
		pipelineID: pipelineID,
		config:     cfg,
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
	maxPublishRetries := 10
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
				payload, err := batch.MarshalMsg(nil)
				if err != nil {
					return lastLSN, fmt.Errorf("failed to marshal drain marker: %w", err)
				}
				topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
				wmMsg := message.NewMessage(watermill.NewUUID(), payload)
				if err := p.publisher.Publish(topic, wmMsg); err != nil {
					return lastLSN, fmt.Errorf("failed to publish drain marker: %w", err)
				}
				return lastLSN, nil
			}

			// 1. Process Discovery
			for _, m := range msgs {
				if m.Op == "schema_change" && m.Schema != nil {
					p.handleDiscovery(m)
				}
				if m.LSN > lastLSN {
					lastLSN = m.LSN
				}
			}

			// Wrap NATS publishing in a circuit breaker with retry limit
			var publishErr error
			for attempt := 0; attempt < maxPublishRetries; attempt++ {
				_, publishErr = p.cb.Execute(func() (interface{}, error) {
					batch := protocol.MessageBatch(msgs)
					payload, err := batch.MarshalMsg(nil)
					if err != nil {
						return nil, err
					}

					topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
					wmMsg := message.NewMessage(watermill.NewUUID(), payload)

					if err := p.publisher.Publish(topic, wmMsg); err != nil {
						return nil, err
					}
					return nil, nil
				})

				if publishErr == nil {
					break
				}

				// Update circuit breaker status on first failure
				if attempt == 0 && len(msgs) > 0 {
					m := msgs[0]
					stKey := protocol.TableStatsKey(p.pipelineID, m.SourceID, m.Table)
					if entry, err := p.kv.Get(stKey); err == nil {
						var st protocol.TableStats
						if err := json.Unmarshal(entry.Value(), &st); err == nil {
							st.Status = "CIRCUIT_OPEN"
							stData, err := st.MarshalMsg(nil)
							if err == nil {
								if _, err := p.kv.Put(stKey, stData); err != nil {
									log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating circuit breaker status")
								}
							}
						}
					}
				}

				select {
				case <-time.After(2 * time.Second):
					// Continue to next retry
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				}
			}

			if publishErr != nil {
				return lastLSN, fmt.Errorf("failed to publish after %d attempts: %w", maxPublishRetries, publishErr)
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
					cpData, err := cp.MarshalMsg(nil)
					if err != nil {
						log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error marshaling checkpoint")
						continue
					}
					key := protocol.IngressCheckpointKey(p.pipelineID, m.SourceID, m.Table)
					if _, err := p.kv.Put(key, cpData); err != nil {
						log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating ingress checkpoint")
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
		log.Info().Str("pipeline_id", p.pipelineID).Str("schema", m.Schema.Schema).Str("table", m.Schema.Table).Msg("New table discovered via CDC")
		p.config.Tables = append(p.config.Tables, m.Schema.Table)

		data, err := json.Marshal(p.config)
		if err != nil {
			log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error marshaling pipeline config for discovery")
		} else if _, err := p.kv.Put(protocol.PipelineConfigKey(p.pipelineID), data); err != nil {
			log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating pipeline config for discovery")
		}
	}

	metaKey := fmt.Sprintf("cdc.pipeline.%s.sources.%s.tables.%s.metadata", p.pipelineID, m.SourceID, m.Table)
	metaData, err := json.Marshal(m.Schema)
	if err == nil {
		if _, err := p.kv.Put(metaKey, metaData); err != nil {
			log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating table metadata")
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

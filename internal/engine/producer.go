package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
)

type tableEvolution struct {
	status        string
	revision      uint64
	correlationID string
	cachedSchema  map[string]string
	lastCheckAt   time.Time
}

type Producer struct {
	pipelineID   string
	config       protocol.PipelineConfig
	source       source.Source
	publisher    stream.Publisher
	kv           nats.KeyValue
	mu           sync.RWMutex
	cancelSource context.CancelFunc
	cb           *gobreaker.CircuitBreaker

	sourceConfig       protocol.SourceConfig
	snapshotMu         sync.Mutex
	snapshotInProgress map[string]bool
	snapshotDoneChan   chan string

	muEvo     sync.RWMutex
	evoStates map[string]*tableEvolution // table name -> evolution state
}

func NewProducer(pipelineID string, cfg protocol.PipelineConfig, src source.Source, pub stream.Publisher, kv nats.KeyValue) *Producer {
	settings := gobreaker.Settings{
		Name:        "nats-publisher-" + pipelineID,
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
			switch to {
			case gobreaker.StateOpen:
				stateVal = 1.0
			case gobreaker.StateHalfOpen:
				stateVal = 2.0
			}
			metrics.CircuitBreakerState.WithLabelValues(pipelineID).Set(stateVal)
		},
	}

	return &Producer{
		pipelineID:         pipelineID,
		config:             cfg,
		source:             src,
		publisher:          pub,
		kv:                 kv,
		cb:                 gobreaker.NewCircuitBreaker(settings),
		snapshotInProgress: make(map[string]bool),
		snapshotDoneChan:   make(chan string, 10),
		evoStates:          make(map[string]*tableEvolution),
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

			// 1. Process Discovery & Schema Evolution
			discoveredTables := make([]protocol.Message, 0, 10)
			mainBatch := make(protocol.MessageBatch, 0, len(msgs))

			for _, m := range msgs {
				if m.Op == "schema_change" && m.Schema != nil {
					discoveredTables = append(discoveredTables, m)
				}
				if m.LSN > lastLSN {
					lastLSN = m.LSN
				}

				// Schema Evolution Check
				p.muEvo.RLock()
				state, exists := p.evoStates[m.Table]
				isFrozen := exists && state.status == "FROZEN"
				p.muEvo.RUnlock()

				if isFrozen {
					if err := p.bufferToJetStream(m.Table, m); err != nil {
						log.Error().Err(err).Str("table", m.Table).Msg("Failed to buffer message")
					}
					continue
				}

				if diff, changed := p.detectSchemaChange(m); changed {
					log.Info().Str("table", m.Table).Msg("Schema change detected, freezing table and emitting OpSchemaChange")
					// Emit OpSchemaChange
					scm := protocol.Message{
						SourceID:      m.SourceID,
						Table:         m.Table,
						Op:            protocol.OpSchemaChange,
						LSN:           m.LSN,
						Timestamp:     time.Now(),
						Diff:          diff,
						CorrelationID: diff.CorrelationID,
					}

					// Publish OpSchemaChange to ingest topic
					scBatch := protocol.MessageBatch{scm}
					scPayload, _ := scBatch.MarshalMsg(nil)
					topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
					wmMsg := message.NewMessage(watermill.NewUUID(), scPayload)
					if err := p.publisher.Publish(topic, wmMsg); err != nil {
						log.Error().Err(err).Msg("Failed to publish OpSchemaChange")
					}

					// Buffer the current message
					if err := p.bufferToJetStream(m.Table, m); err != nil {
						log.Error().Err(err).Str("table", m.Table).Msg("Failed to buffer message")
					}
					continue
				}

				mainBatch = append(mainBatch, m)
			}

			if len(mainBatch) == 0 {
				if len(discoveredTables) > 0 {
					for _, t := range discoveredTables {
						p.handleDiscovery(ctx, t)
					}
				}
				// Signal acknowledgment back to the source handler
				select {
				case ackChan <- struct{}{}:
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				}
				continue
			}

			// Wrap NATS publishing in a circuit breaker with retry limit
			var publishErr error
			for attempt := 0; attempt < maxPublishRetries; attempt++ {
				_, publishErr = p.cb.Execute(func() (interface{}, error) {
					payload, err := mainBatch.MarshalMsg(nil)
					if err != nil {
						log.Error().Err(err).Msg("Failed to marshal batch payload")
						return nil, err
					}

					topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
					wmMsg := message.NewMessage(watermill.NewUUID(), payload)

					if err := p.publisher.Publish(topic, wmMsg); err != nil {
						return nil, err
					}

					log.Debug().Any("data", mainBatch).Msg("Published messages to NATS")
					return nil, nil
				})

				if publishErr == nil {
					break
				}

				// Update circuit breaker status on first failure
				if attempt == 0 && len(mainBatch) > 0 {
					m := mainBatch[0]
					stKey := protocol.ProducerTableStatsKey(p.pipelineID, m.SourceID, m.Table)
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
			if len(mainBatch) > 0 {
				latestByTable := make(map[string]protocol.Message)
				for _, m := range mainBatch {
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

			if len(discoveredTables) > 0 {
				for _, t := range discoveredTables {
					p.handleDiscovery(ctx, t)
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

func (p *Producer) detectSchemaChange(msg protocol.Message) (*protocol.SchemaDiff, bool) {
	p.muEvo.Lock()
	defer p.muEvo.Unlock()

	state, ok := p.evoStates[msg.Table]
	if !ok {
		// Try to load from KV first
		key := protocol.SchemaEvolutionKey(p.pipelineID, msg.Table)
		entry, err := p.kv.Get(key)
		if err == nil {
			var st tableEvolution
			if err := json.Unmarshal(entry.Value(), &st); err == nil {
				st.revision = entry.Revision()
				p.evoStates[msg.Table] = &st
				state = &st
				ok = true
			}
		}
	}

	if !ok {
		// Initialize with current columns
		cols := make(map[string]string)
		for k := range msg.Data {
			cols[k] = "unknown" // We don't know the type from Data easily, but we can track names
		}
		state = &tableEvolution{
			status:       "ACTIVE",
			cachedSchema: cols,
			lastCheckAt:  time.Now(),
		}
		p.evoStates[msg.Table] = state
	}

	if state.status == "FROZEN" {
		return nil, false // Already frozen, wait for resolution
	}

	added := make(map[string]string)
	for k := range msg.Data {
		if _, exists := state.cachedSchema[k]; !exists {
			added[k] = "unknown"
		}
	}

	if len(added) > 0 {
		diff := &protocol.SchemaDiff{
			Table:         msg.Table,
			Timestamp:     time.Now(),
			Source:        msg.SourceID,
			Added:         added,
			CorrelationID: uuid.New().String(),
		}

		// Transition to FROZEN
		state.status = "FROZEN"
		state.correlationID = diff.CorrelationID
		for k, v := range added {
			state.cachedSchema[k] = v
		}

		// Persist state to KV
		p.persistEvoState(msg.Table, state)

		return diff, true
	}

	return nil, false
}

func (p *Producer) persistEvoState(table string, state *tableEvolution) {
	key := protocol.SchemaEvolutionKey(p.pipelineID, table)
	data, _ := json.Marshal(state)
	var err error
	var rev uint64
	if state.revision == 0 {
		rev, err = p.kv.Put(key, data)
	} else {
		rev, err = p.kv.Update(key, data, state.revision)
	}

	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to persist evolution state")
	} else {
		state.revision = rev
	}
}

func (p *Producer) bufferToJetStream(table string, msg protocol.Message) error {
	topic := fmt.Sprintf("cdc_pipeline_%s_buffer_%s", p.pipelineID, table)
	batch := protocol.MessageBatch{msg}
	payload, err := batch.MarshalMsg(nil)
	if err != nil {
		return err
	}

	wmMsg := message.NewMessage(watermill.NewUUID(), payload)
	return p.publisher.Publish(topic, wmMsg)
}

func (p *Producer) handleDiscovery(ctx context.Context, m protocol.Message) {
	// HACK: Ignore tables created by the snapshot engine to prevent discovery feedback loop
	if strings.HasPrefix(m.Schema.Table, "cdc_snapshot_") {
		return
	}

	isNew := true
	p.mu.RLock()
	for _, t := range p.config.Tables {
		if t == m.Schema.Table {
			isNew = false
			break
		}
	}
	p.mu.RUnlock()

	log.Debug().Str("table", m.Schema.Table).Bool("is_new", isNew).Array("config_tables", func() *zerolog.Array {
		arr := zerolog.Arr()
		for _, t := range p.config.Tables {
			arr.Str(t)
		}
		return arr
	}()).Msg("Table discovery check")

	if isNew {
		log.Info().Str("pipeline_id", p.pipelineID).Str("schema", m.Schema.Schema).Str("table", m.Schema.Table).Msg("New table discovered via CDC")
		p.mu.Lock()
		p.config.Tables = append(p.config.Tables, m.Schema.Table)
		p.mu.Unlock()

		// DO NOT write back to KV. The source's job is to discover and report.
		// The ConfigManager is responsible for persisting config changes.
		// For now, we just update the metadata for observability.
		metaKey := fmt.Sprintf("cdc.pipeline.%s.sources.%s.tables.%s.metadata", p.pipelineID, m.SourceID, m.Table)
		metaData, err := json.Marshal(m.Schema)
		if err == nil {
			if _, err := p.kv.Put(metaKey, metaData); err != nil {
				log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating table metadata")
			}
		}

		if err := p.source.RestartWithNewTables(ctx, []string{m.Schema.Table}); err != nil {
			log.Error().Err(err).Str("pipeline_id", p.pipelineID).Str("table", m.Schema.Table).Msg("Error restarting source with new table")
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

func (p *Producer) SetSourceConfig(cfg protocol.SourceConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sourceConfig = cfg
}

func (p *Producer) SetDynamicTablesChan(ch <-chan []string) {
	go func() {
		for tables := range ch {
			p.handleDynamicTables(tables)
		}
	}()
}

func (p *Producer) handleDynamicTables(newTables []string) {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()

	for _, tableName := range newTables {
		tableKey := fmt.Sprintf("public.%s", tableName)

		stateKey := protocol.TableStateKey(p.pipelineID, p.sourceConfig.ID, tableName)
		entry, err := p.kv.Get(stateKey)
		state := ""
		if err == nil {
			state = string(entry.Value())
		}

		switch state {
		case protocol.TableStateCDC:
			log.Info().Str("pipeline_id", p.pipelineID).Str("table", tableName).Msg("Table already in CDC state, skipping snapshot")
			continue
		case protocol.TableStateFailed:
			log.Error().Str("pipeline_id", p.pipelineID).Str("table", tableName).Msg("Table in failed state, requires manual intervention")
			continue
		default:
			if p.snapshotInProgress[tableKey] {
				log.Info().Str("pipeline_id", p.pipelineID).Str("table", tableName).Msg("Snapshot already in progress")
				continue
			}
		}

		log.Info().Str("pipeline_id", p.pipelineID).Str("table", tableName).Str("state", state).Msg("Starting dynamic table addition")

		p.snapshotInProgress[tableKey] = true

		go func(tbl string, key string) {
			defer func() {
				p.snapshotMu.Lock()
				delete(p.snapshotInProgress, key)
				p.snapshotMu.Unlock()
			}()

			if err := p.addTableToPublication(tbl); err != nil {
				log.Error().Err(err).Str("table", tbl).Msg("Failed to add table to publication")
				p.setTableState(tbl, protocol.TableStateFailed)
				return
			}

			if err := p.snapshotNewTable(tbl); err != nil {
				log.Error().Err(err).Str("table", tbl).Msg("Failed to snapshot new table")
				p.setTableState(tbl, protocol.TableStateFailed)
				return
			}

			p.setTableState(tbl, protocol.TableStateCDC)
			select {
			case p.snapshotDoneChan <- tbl:
			default:
			}
		}(tableName, tableKey)
	}
}

func (p *Producer) addTableToPublication(tableName string) error {
	if p.source == nil {
		return fmt.Errorf("source is nil")
	}
	alterSrc, ok := p.source.(interface {
		AlterPublication(ctx context.Context, tableName string) error
	})
	if !ok {
		return fmt.Errorf("source does not support AlterPublication")
	}
	return alterSrc.AlterPublication(context.Background(), tableName)
}

func (p *Producer) snapshotNewTable(tableName string) error {
	log.Info().Str("table", tableName).Msg("Snapshotting new table (no-op in basic implementation)")
	return nil
}

func (p *Producer) setTableState(tableName, state string) {
	stateKey := protocol.TableStateKey(p.pipelineID, p.sourceConfig.ID, tableName)
	data := []byte(state)
	if _, err := p.kv.Put(stateKey, data); err != nil {
		log.Error().Err(err).Str("table", tableName).Str("state", state).Msg("Failed to set table state")
	}
}

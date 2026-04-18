package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream"
	stream_nats "github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
)

type tableEvolution struct {
	Status         string            `json:"status"`
	Revision       uint64            `json:"revision"`
	CorrelationID  string            `json:"correlation_id"`
	CachedSchema   map[string]string `json:"cached_schema"`
	LastCheckAt    time.Time         `json:"last_check_at"`
	ChangesThisMin int               `json:"changes_this_min"`
	LastChangeAt   time.Time         `json:"last_change_at"`
}

type Producer struct {
	pipelineID   string
	natsURL      string // NEW: for buffer draining
	config       protocol.PipelineConfig
	source       source.Source
	publisher    stream.Publisher
	subscriber   stream.Subscriber // NEW: for schema acks
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

	muTableStates sync.RWMutex
	tableStates   map[string]string // table name -> snapshot state (Snapshotting, Draining, CDC)
}

func NewProducer(pipelineID, natsURL string, cfg protocol.PipelineConfig, src source.Source, pub stream.Publisher, sub stream.Subscriber, kv nats.KeyValue, srcConfig protocol.SourceConfig) *Producer {
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
		natsURL:            natsURL,
		config:             cfg,
		source:             src,
		publisher:          pub,
		subscriber:         sub,
		kv:                 kv,
		cb:                 gobreaker.NewCircuitBreaker(settings),
		snapshotInProgress: make(map[string]bool),
		snapshotDoneChan:   make(chan string, 10),
		evoStates:          make(map[string]*tableEvolution),
		tableStates:        make(map[string]string),
		sourceConfig:       srcConfig,
	}
}

func (p *Producer) Run(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (uint64, error) {
	p.mu.Lock()
	p.sourceConfig = srcConfig
	p.mu.Unlock()

	sourceCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancelSource = cancel
	p.mu.Unlock()

	// 0. Recovery: Check KV for frozen tables and restore state
	p.recoverEvoStates(ctx)

	msgChan, ackChan, err := p.source.Start(sourceCtx, srcConfig, checkpoint)
	if err != nil {
		return 0, fmt.Errorf("failed to start source: %w", err)
	}

	// Subscribe to acks topic
	ackTopic := protocol.AcksTopic(p.pipelineID)
	ackMsgChan, err := p.subscriber.Subscribe(ctx, ackTopic)
	if err != nil {
		log.Error().Err(err).Msg("Failed to subscribe to schema acks topic")
	}

	var lastLSN uint64
	maxPublishRetries := 10

	log.Debug().Msg("start receiving source updates")
	for {
		select {
		case <-ctx.Done():
			return lastLSN, ctx.Err()
		case ackMsg, ok := <-ackMsgChan:
			if !ok {
				log.Warn().Msg("Schema acks channel closed")
				continue
			}
			var ack protocol.Message
			if _, err := ack.UnmarshalMsg(ackMsg.Payload); err != nil {
				log.Error().Err(err).Msg("Failed to unmarshal ack from consumer")
				ackMsg.Nack()
				continue
			}

			if ack.Op == protocol.OpSchemaChangeAck {
				p.handleSchemaAck(ctx, ack)
				ackMsg.Ack()
			} else if ack.Op == "ack" {
				// Record ack: Propagate to source handler
				select {
				case ackChan <- struct{}{}:
				case <-ctx.Done():
					return lastLSN, nil
				default:
					// No one waiting, fine
				}
				ackMsg.Ack()
			}

		case msgs, ok := <-msgChan:
			log.Debug().Any("data", msgs).Msg("Receiving data from source")

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
			tableToBuffer := make(map[string]protocol.MessageBatch)

			for _, m := range msgs {
				if m.Op == "schema_change" && m.Schema != nil {
					discoveredTables = append(discoveredTables, m)
				}
				if m.LSN > lastLSN {
					lastLSN = m.LSN
				}

				// 1. Snapshot/Draining State Check
				p.muTableStates.RLock()
				tblState := p.tableStates[m.Table]
				isSnapshotting := tblState == protocol.TableStateSnapshotting || tblState == protocol.TableStateDraining
				p.muTableStates.RUnlock()

				// 2. Schema Evolution Check
				p.muEvo.RLock()
				state, exists := p.evoStates[m.Table]
				// Buffer if FROZEN or currently DRAINING
				isEvoFrozen := exists && (state.Status == protocol.SchemaStatusFrozen || state.Status == protocol.SchemaStatusDraining)
				p.muEvo.RUnlock()

				if isSnapshotting || isEvoFrozen {
					tableToBuffer[m.Table] = append(tableToBuffer[m.Table], m)
					continue
				}

				if m.Op == "insert" || m.Op == "update" || m.Op == "snapshot" {
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

						// Publish OpSchemaChange to ingest topic with retries
						scBatch := protocol.MessageBatch{scm}
						scPayload, _ := scBatch.MarshalMsg(nil)
						topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
						wmMsg := message.NewMessage(watermill.NewUUID(), scPayload)

						if err := p.publishWithRetry(ctx, topic, wmMsg, maxPublishRetries); err != nil {
							return lastLSN, fmt.Errorf("failed to publish OpSchemaChange for %s: %w", m.Table, err)
						}

						// Current message and all subsequent for this table must be buffered
						tableToBuffer[m.Table] = append(tableToBuffer[m.Table], m)
						continue
					}
				}

				mainBatch = append(mainBatch, m)
			}

			// 2. Publish Buffered Batches
			for table, batch := range tableToBuffer {
				if err := p.publishBufferBatch(ctx, table, batch, maxPublishRetries); err != nil {
					return lastLSN, fmt.Errorf("failed to buffer messages for %s: %w", table, err)
				}
			}

			// 3. Publish Main Ingest Batch
			if len(mainBatch) > 0 {
				topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
				payload, err := mainBatch.MarshalMsg(nil)
				if err != nil {
					return lastLSN, fmt.Errorf("failed to marshal main batch: %w", err)
				}
				wmMsg := message.NewMessage(watermill.NewUUID(), payload)

				if err := p.publishWithRetry(ctx, topic, wmMsg, maxPublishRetries); err != nil {
					return lastLSN, fmt.Errorf("failed to publish main batch: %w", err)
				}

				// Success! Update checkpoints
				latestByTable := make(map[string]protocol.Message)
				for _, m := range mainBatch {
					if m.LSN > lastLSN {
						lastLSN = m.LSN
					}
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
					if err == nil {
						key := protocol.IngressCheckpointKey(p.pipelineID, m.SourceID, m.Table)
						if _, err := p.kv.Put(key, cpData); err != nil {
							log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating ingress checkpoint")
						}
					}
				}
			}

			// Discovery handling
			if len(discoveredTables) > 0 {
				for _, t := range discoveredTables {
					p.handleDiscovery(ctx, t)
				}
			}
		}
	}
}

func (p *Producer) publishWithRetry(ctx context.Context, topic string, msg *message.Message, maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, lastErr = p.cb.Execute(func() (interface{}, error) {
			return nil, p.publisher.Publish(topic, msg)
		})

		if lastErr == nil {
			return nil
		}

		log.Warn().Err(lastErr).Str("topic", topic).Int("attempt", attempt+1).Msg("Publish failed, retrying...")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
	return fmt.Errorf("exhausted retries for topic %s: %w", topic, lastErr)
}

func (p *Producer) publishBufferBatch(ctx context.Context, table string, batch protocol.MessageBatch, maxRetries int) error {
	topic := fmt.Sprintf("cdc_pipeline_%s_buffer_%s", p.pipelineID, table)
	payload, err := batch.MarshalMsg(nil)
	if err != nil {
		return err
	}
	wmMsg := message.NewMessage(watermill.NewUUID(), payload)
	return p.publishWithRetry(ctx, topic, wmMsg, maxRetries)
}

func (p *Producer) recoverEvoStates(ctx context.Context) {
	// Simple recovery: find all evolution keys in KV
	p.mu.RLock()
	tables := p.config.Tables
	sid := p.sourceConfig.ID
	p.mu.RUnlock()

	for _, table := range tables {
		// 1. Evolution Recovery
		evoKey := protocol.SchemaEvolutionKey(p.pipelineID, table)
		entry, err := p.kv.Get(evoKey)
		if err == nil {
			var st tableEvolution
			if err := json.Unmarshal(entry.Value(), &st); err == nil {
				st.Revision = entry.Revision()
				p.muEvo.Lock()
				p.evoStates[table] = &st
				p.muEvo.Unlock()

				if st.Status == protocol.SchemaStatusFrozen || st.Status == protocol.SchemaStatusDraining {
					log.Info().Str("table", table).Str("status", st.Status).Msg("Recovered table evolution state")
					if st.Status == protocol.SchemaStatusDraining {
						go p.flushBuffer(ctx, table)
					}
				}
			}
		}

		// 2. Snapshot Recovery
		stateKey := protocol.TableStateKey(p.pipelineID, sid, table)
		entry, err = p.kv.Get(stateKey)
		state := ""
		if err == nil {
			state = string(entry.Value())
			p.muTableStates.Lock()
			p.tableStates[table] = state
			p.muTableStates.Unlock()

			if state == protocol.TableStateSnapshotting || state == protocol.TableStateDraining {
				log.Info().Str("table", table).Str("state", state).Msg("Recovered table snapshot state")
				if state == protocol.TableStateDraining {
					go p.flushBuffer(ctx, table)
				}
			}
		}

		// 3. Trigger snapshot if missing checkpoint AND not in Snapshotting/CDC/Draining
		cpKey := protocol.IngressCheckpointKey(p.pipelineID, sid, table)
		_, cpErr := p.kv.Get(cpKey)
		if cpErr != nil {
			// If we are starting from LSN 0, go-pq-cdc handles initial snapshot for configured tables.
			// We only trigger handleDynamicTables for truly dynamic ones or if we are already in a transition state.
			if state != protocol.TableStateSnapshotting && state != protocol.TableStateCDC && state != protocol.TableStateDraining {
				// ONLY trigger if not in initial list OR if we specifically want to re-snapshot
				log.Info().Str("table", table).Msg("Missing ingress checkpoint, table will be snapshotted by source or discovery")
				// go p.handleDynamicTables(sid, []string{table}) // Avoid triggering restart for initial tables
			}
		}
	}
}

func (p *Producer) handleSchemaAck(ctx context.Context, ack protocol.Message) {
	p.muEvo.Lock()
	state, ok := p.evoStates[ack.Table]
	if !ok || state.Status != protocol.SchemaStatusFrozen {
		p.muEvo.Unlock()
		return
	}

	if state.CorrelationID != ack.CorrelationID {
		log.Warn().Str("expected", state.CorrelationID).Str("got", ack.CorrelationID).Msg("CorrelationID mismatch on schema ack")
		p.muEvo.Unlock()
		return
	}

	// Ack matches! Transition to DRAINING and start background flush
	log.Info().Str("table", ack.Table).Msg("Schema ack received, draining buffer")
	state.Status = protocol.SchemaStatusDraining

	// Flush buffer in background
	go p.flushBuffer(ctx, ack.Table)

	p.persistEvoState(ack.Table, state)
	p.muEvo.Unlock()
}

func (p *Producer) flushBuffer(ctx context.Context, table string) {
	topic := fmt.Sprintf("cdc_pipeline_%s_buffer_%s", p.pipelineID, table)
	// Create a temporary subscriber to drain the buffer
	sub, err := stream_nats.NewNatsSubscriber(p.natsURL, "drainer-"+uuid.New().String(), topic, 100, 30*time.Second)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to create subscriber to drain buffer")
		return
	}
	defer sub.Close()

	msgChan, err := sub.Subscribe(ctx, topic)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to subscribe to buffer topic")
		return
	}

	mainTopic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
	// Drain everything currently in the buffer.
	// We use a short timeout to detect when the buffer is truly empty.
	for {
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case m, ok := <-msgChan:
			timer.Stop()
			if !ok {
				goto finished
			}
			log.Info().Str("table", table).Msg("Republishing buffered message")
			if err := p.publisher.Publish(mainTopic, m); err != nil {
				log.Error().Err(err).Msg("Failed to republish buffered message")
				m.Nack()
				return
			}
			m.Ack()
		case <-timer.C:
			// No messages for 1 second, assume buffer is empty
			goto finished
		}
	}

finished:
	// 1. Transition evolution state back to STABLE
	p.muEvo.Lock()
	evoState, ok := p.evoStates[table]
	if ok && evoState.Status == protocol.SchemaStatusDraining {
		log.Info().Str("table", table).Msg("Buffer flush complete for evolution, table is now ACTIVE")
		evoState.Status = protocol.SchemaStatusStable
		evoState.CorrelationID = ""
		p.persistEvoState(table, evoState)
	}
	p.muEvo.Unlock()

	// 2. Transition snapshot state back to CDC
	p.muTableStates.Lock()
	if p.tableStates[table] == protocol.TableStateDraining {
		log.Info().Str("table", table).Msg("Buffer flush complete for snapshot, table is now CDC")
		p.muTableStates.Unlock()
		p.setTableState(p.sourceConfig.ID, table, protocol.TableStateCDC)
	} else {
		p.muTableStates.Unlock()
	}
}

func (p *Producer) detectSchemaChange(m protocol.Message) (*protocol.SchemaDiff, bool) {
	// HACK: Ignore tables created by the snapshot engine
	if strings.HasPrefix(m.Table, "cdc_snapshot_") {
		return nil, false
	}

	p.muEvo.Lock()
	defer p.muEvo.Unlock()

	state, ok := p.evoStates[m.Table]
	if !ok {
		// Initialize with current columns
		cols := make(map[string]string)
		for k := range m.Data {
			cols[k] = "unknown"
		}
		state = &tableEvolution{
			Status:       protocol.SchemaStatusStable,
			CachedSchema: cols,
			LastCheckAt:  time.Now(),
		}
		p.evoStates[m.Table] = state
		return nil, false
	}

	if state.Status == protocol.SchemaStatusFrozen || state.Status == protocol.SchemaStatusDraining || state.Status == protocol.SchemaStatusSuspended {
		return nil, false
	}

	added := make(map[string]string)
	for k := range m.Data {
		if _, exists := state.CachedSchema[k]; !exists {
			added[k] = "unknown"
		}
	}

	if len(added) > 0 {
		return p.performSchemaEvolution(m.Table, m.SourceID, added)
	}

	return nil, false
}

func (p *Producer) performSchemaEvolution(tableName, sourceID string, added map[string]string) (*protocol.SchemaDiff, bool) {
	state, ok := p.evoStates[tableName]
	if !ok {
		return nil, false
	}

	// Circuit Breaker logic
	now := time.Now()
	if now.Sub(state.LastChangeAt) > time.Minute {
		state.ChangesThisMin = 0
		state.LastChangeAt = now
	}
	state.ChangesThisMin++

	if state.ChangesThisMin > 5 {
		log.Warn().Str("table", tableName).Msg("Schema change limit exceeded, SUSPENDING table evolution")
		state.Status = protocol.SchemaStatusSuspended
		p.persistEvoState(tableName, state)
		return nil, false
	}

	diff := &protocol.SchemaDiff{
		Table:         tableName,
		Timestamp:     time.Now(),
		Source:        sourceID,
		Added:         added,
		CorrelationID: uuid.New().String(),
	}

	// Transition to FROZEN
	state.Status = protocol.SchemaStatusFrozen
	state.CorrelationID = diff.CorrelationID
	for k, v := range added {
		state.CachedSchema[k] = v
	}

	// Persist state to KV
	p.persistEvoState(tableName, state)

	return diff, true
}

func (p *Producer) emitSchemaChange(ctx context.Context, sourceID, table string, lsn uint64, diff *protocol.SchemaDiff) error {
	scm := protocol.Message{
		SourceID:      sourceID,
		Table:         table,
		Op:            protocol.OpSchemaChange,
		LSN:           lsn,
		Timestamp:     time.Now(),
		Diff:          diff,
		CorrelationID: diff.CorrelationID,
	}

	scBatch := protocol.MessageBatch{scm}
	scPayload, _ := scBatch.MarshalMsg(nil)
	topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
	wmMsg := message.NewMessage(watermill.NewUUID(), scPayload)

	return p.publishWithRetry(ctx, topic, wmMsg, 10)
}

func (p *Producer) persistEvoState(table string, state *tableEvolution) {
	key := protocol.SchemaEvolutionKey(p.pipelineID, table)
	data, _ := json.Marshal(state)
	var err error
	var rev uint64
	if state.Revision == 0 {
		rev, err = p.kv.Put(key, data)
	} else {
		rev, err = p.kv.Update(key, data, state.Revision)
	}

	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to persist evolution state")
	} else {
		state.Revision = rev
	}
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

	if isNew {
		log.Info().Str("pipeline_id", p.pipelineID).Str("schema", m.Schema.Schema).Str("table", m.Schema.Table).Msg("New table discovered via CDC, starting dynamic addition")
		p.mu.Lock()
		p.config.Tables = append(p.config.Tables, m.Schema.Table)
		p.mu.Unlock()

		// 1. Update table metadata in KV
		metaKey := fmt.Sprintf("cdc.pipeline.%s.sources.%s.tables.%s.metadata", p.pipelineID, m.SourceID, m.Table)
		metaData, err := json.Marshal(m.Schema)
		if err == nil {
			if _, err := p.kv.Put(metaKey, metaData); err != nil {
				log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Error updating table metadata")
			}
		}

		// 2. Persist updated pipeline config so other workers/Manager see it
		pipeData, _ := json.Marshal(p.config)
		if _, err := p.kv.Put(protocol.PipelineConfigKey(p.pipelineID), pipeData); err != nil {
			log.Error().Err(err).Str("pipeline_id", p.pipelineID).Msg("Failed to persist updated pipeline config after discovery")
		}

		// 3. Trigger dynamic table addition flow - Manager will restart us, and recoverEvoStates will handle it
		// go p.handleDynamicTables(m.SourceID, []string{m.Schema.Table})
	} else {
		// ALWAYS warm the schema evolution cache to prevent freeze on first data message
		p.muEvo.Lock()
		state, exists := p.evoStates[m.Schema.Table]
		if !exists {
			log.Info().Str("table", m.Schema.Table).Msg("Warming evolution cache for table")
			cols := make(map[string]string)
			for k, v := range m.Schema.Columns {
				cols[k] = v
			}
			p.evoStates[m.Schema.Table] = &tableEvolution{
				Status:       protocol.SchemaStatusStable,
				CachedSchema: cols,
				LastCheckAt:  time.Now(),
			}
		} else {
			// Table exists, check for schema change
			added := make(map[string]string)
			for k, v := range m.Schema.Columns {
				if _, ok := state.CachedSchema[k]; !ok {
					added[k] = v
				}
			}

			if len(added) > 0 {
				if diff, changed := p.performSchemaEvolution(m.Schema.Table, m.SourceID, added); changed {
					log.Info().Str("table", m.Schema.Table).Int("new_cols", len(added)).Msg("Schema change detected via discovery, freezing table")
					p.muEvo.Unlock()
					if err := p.emitSchemaChange(ctx, m.SourceID, m.Schema.Table, m.LSN, diff); err != nil {
						log.Error().Err(err).Str("table", m.Schema.Table).Msg("Failed to publish OpSchemaChange from discovery")
					}
					p.muEvo.Lock()
				}
			}
		}
		p.muEvo.Unlock()
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
			p.mu.RLock()
			sid := p.sourceConfig.ID
			p.mu.RUnlock()
			p.handleDynamicTables(sid, tables)
		}
	}()
}

func (p *Producer) handleDynamicTables(sourceID string, newTables []string) {
	log.Debug().Str("source_id", sourceID).Strs("table_names", newTables).Msg("Handling new dynamic tables")
	for _, tableName := range newTables {
		tableKey := fmt.Sprintf("public.%s", tableName)

		p.snapshotMu.Lock()
		if p.snapshotInProgress[tableKey] {
			log.Info().Str("pipeline_id", p.pipelineID).Str("table", tableName).Msg("Snapshot already in progress")
			p.snapshotMu.Unlock()
			continue
		}

		stateKey := protocol.TableStateKey(p.pipelineID, sourceID, tableName)
		entry, err := p.kv.Get(stateKey)
		state := ""
		if err == nil {
			state = string(entry.Value())
		}

		if state == protocol.TableStateCDC {
			log.Info().Str("pipeline_id", p.pipelineID).Str("table", tableName).Msg("Table already in CDC state, skipping snapshot")
			p.snapshotMu.Unlock()
			continue
		}

		log.Info().Str("pipeline_id", p.pipelineID).Str("table", tableName).Str("state", state).Msg("Starting dynamic table addition")
		p.snapshotInProgress[tableKey] = true
		p.snapshotMu.Unlock()

		go func(tbl string, key string) {
			defer func() {
				p.snapshotMu.Lock()
				delete(p.snapshotInProgress, key)
				p.snapshotMu.Unlock()
			}()

			if err := p.addTableToPublication(tbl); err != nil {
				log.Error().Err(err).Str("table", tbl).Msg("Failed to add table to publication")
				p.setTableState(sourceID, tbl, protocol.TableStateFailed)
				return
			}

			// Transition to Snapshotting
			p.setTableState(sourceID, tbl, protocol.TableStateSnapshotting)

			if err := p.performChunkedSnapshot(sourceID, tbl); err != nil {
				log.Error().Err(err).Str("table", tbl).Msg("Failed to snapshot new table")
				p.setTableState(sourceID, tbl, protocol.TableStateFailed)
				return
			}

			// Transition to Draining
			p.setTableState(sourceID, tbl, protocol.TableStateDraining)

			// Flush buffer
			p.flushBuffer(context.Background(), tbl)

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

func (p *Producer) setTableState(sourceID, tableName, state string) {
	stateKey := protocol.TableStateKey(p.pipelineID, sourceID, tableName)
	data := []byte(state)
	if _, err := p.kv.Put(stateKey, data); err != nil {
		log.Error().Err(err).Str("table", tableName).Str("state", state).Msg("Failed to set table state")
	}
	p.muTableStates.Lock()
	p.tableStates[tableName] = state
	p.muTableStates.Unlock()
}

func (p *Producer) performChunkedSnapshot(sourceID, tableName string) error {
	p.mu.RLock()
	cfg := p.sourceConfig
	p.mu.RUnlock()

	// 1. Decrypt Password
	pass := ""
	if cfg.PassEncrypted != "" {
		key := crypto.GetEncryptionKey()
		if key != nil {
			var err error
			pass, err = crypto.Decrypt(cfg.PassEncrypted, key)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to decrypt source password, using as-is")
				pass = cfg.PassEncrypted
			}
		} else {
			pass = cfg.PassEncrypted
		}
	}

	// 2. Construct DSN
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, pass, cfg.Host, cfg.Port, cfg.Database)

	// 3. Open DB
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open source db: %w", err)
	}
	defer db.Close()

	// 4. Determine Primary Key
	pkCols, err := p.getPrimaryKey(db, tableName)
	if err != nil {
		return fmt.Errorf("failed to get primary key: %w", err)
	}
	if len(pkCols) == 0 {
		return fmt.Errorf("table %s has no primary key, chunked snapshot not supported", tableName)
	}
	pkStr := strings.Join(pkCols, ", ")

	// 5. Paginated SELECT *
	chunkSize := 1000
	if cfg.SnapshotChunkSize > 0 {
		chunkSize = cfg.SnapshotChunkSize
	}

	offset := 0
	for {
		query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d OFFSET %d", tableName, pkStr, chunkSize, offset)
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("snapshot query failed: %w", err)
		}

		cols, _ := rows.Columns()
		count := 0
		batch := make(protocol.MessageBatch, 0, chunkSize)

		for rows.Next() {
			count++
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			if err := rows.Scan(columnPointers...); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row: %w", err)
			}

			data := make(map[string]interface{})
			pkData := make(map[string]interface{})
			for i, colName := range cols {
				val := columns[i]
				if b, ok := val.([]byte); ok {
					val = string(b)
				}
				data[colName] = val

				// Check if this column is part of the PK
				for _, pkCol := range pkCols {
					if colName == pkCol {
						pkData[colName] = val
					}
				}
			}

			pkJSON, _ := json.Marshal(pkData)
			batch = append(batch, protocol.Message{
				SourceID:  sourceID,
				Table:     tableName,
				Op:        "snapshot",
				Timestamp: time.Now(),
				Data:      data,
				PK:        string(pkJSON),
			})
		}
		rows.Close()

		if len(batch) > 0 {
			payload, err := batch.MarshalMsg(nil)
			if err != nil {
				return fmt.Errorf("failed to marshal snapshot batch: %w", err)
			}
			topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
			wmMsg := message.NewMessage(watermill.NewUUID(), payload)

			if err := p.publishWithRetry(context.Background(), topic, wmMsg, 10); err != nil {
				return fmt.Errorf("failed to publish snapshot batch: %w", err)
			}
		}

		if count < chunkSize {
			break
		}
		offset += count
	}

	log.Info().Str("table", tableName).Int("total_rows", offset).Msg("Snapshot complete")
	return nil
}

func (p *Producer) getPrimaryKey(db *sql.DB, tableName string) ([]string, error) {
	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_name = $1
		ORDER BY kcu.ordinal_position;
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkCols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pkCols = append(pkCols, col)
	}
	return pkCols, nil
}

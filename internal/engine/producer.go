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
	stream_nats "github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
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
}

func NewProducer(pipelineID, natsURL string, cfg protocol.PipelineConfig, src source.Source, pub stream.Publisher, sub stream.Subscriber, kv nats.KeyValue) *Producer {
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
	}
}

func (p *Producer) Run(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (uint64, error) {
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
				log.Error().Err(err).Msg("Failed to unmarshal schema ack")
				ackMsg.Nack()
				continue
			}
			if ack.Op == protocol.OpSchemaChangeAck {
				p.handleSchemaAck(ctx, ack)
			}
			ackMsg.Ack()

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

			// Heavy Debug Log: show every table in the batch
			tablesInBatch := make(map[string]int)
			for _, m := range msgs {
				tablesInBatch[m.Table]++
			}
			log.Debug().
				Str("pipeline_id", p.pipelineID).
				Int("total_count", len(msgs)).
				Interface("tables", tablesInBatch).
				Msg("Producer: Processing batch from msgChan")

			// Debug Log
			if len(msgs) > 0 {
				log.Debug().
					Str("pipeline_id", p.pipelineID).
					Int("count", len(msgs)).
					Str("first_op", msgs[0].Op).
					Str("last_op", msgs[len(msgs)-1].Op).
					Msg("Producer received message batch from source")
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

				// Schema Evolution Check
				p.muEvo.RLock()
				state, exists := p.evoStates[m.Table]
				// Buffer if FROZEN or currently DRAINING
				shouldBuffer := exists && (state.Status == protocol.SchemaStatusFrozen || state.Status == protocol.SchemaStatusDraining)
				p.muEvo.RUnlock()

				if shouldBuffer {
					tableToBuffer[m.Table] = append(tableToBuffer[m.Table], m)
					continue
				}

				if diff, changed := p.detectSchemaChange(m); changed {
					log.Info().Str("table", m.Table).Msg("Schema change detected, freezing table and emitting OpSchemaChange")
					if err := p.emitSchemaChange(ctx, m.SourceID, m.Table, m.LSN, diff); err != nil {
						return lastLSN, fmt.Errorf("failed to publish OpSchemaChange for %s: %w", m.Table, err)
					}

					// Current message and all subsequent for this table must be buffered
					tableToBuffer[m.Table] = append(tableToBuffer[m.Table], m)
					continue
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

			// Signal acknowledgment back to the source handler
			select {
			case ackChan <- struct{}{}:
			case <-ctx.Done():
				return lastLSN, ctx.Err()
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
	p.mu.RUnlock()

	for _, table := range tables {
		key := protocol.SchemaEvolutionKey(p.pipelineID, table)
		entry, err := p.kv.Get(key)
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
	sub, err := stream_nats.NewNatsSubscriber(p.natsURL, "drainer-"+uuid.New().String(), 100, 30*time.Second)
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
	mainLoop:
	for {
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case m, ok := <-msgChan:
			timer.Stop()
			if !ok {
				break mainLoop
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
			break mainLoop
		}
	}

	// Transition back to STABLE
	p.muEvo.Lock()
	defer p.muEvo.Unlock()

	state, ok := p.evoStates[table]
	if ok && state.Status == protocol.SchemaStatusDraining {
		// One final non-blocking drain to catch messages buffered just before the status change
		for {
			select {
			case m, ok := <-msgChan:
				if !ok {
					goto finalized
				}
				log.Info().Str("table", table).Msg("Republishing final buffered message")
				if err := p.publisher.Publish(mainTopic, m); err != nil {
					log.Error().Err(err).Msg("Failed to republish final buffered message")
					m.Nack()
					return
				}
				m.Ack()
			default:
				goto finalized
			}
		}

	finalized:
		log.Info().Str("table", table).Msg("Buffer flush complete, table is now ACTIVE")
		state.Status = protocol.SchemaStatusStable
		state.CorrelationID = ""
		p.persistEvoState(table, state)
	}
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

func (p *Producer) performSchemaEvolution(table, sourceID string, added map[string]string) (*protocol.SchemaDiff, bool) {
	// Assumption: muEvo is held by caller
	state, ok := p.evoStates[table]
	if !ok {
		return nil, false
	}

	if state.Status == protocol.SchemaStatusFrozen || state.Status == protocol.SchemaStatusDraining || state.Status == protocol.SchemaStatusSuspended {
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
		log.Warn().Str("table", table).Msg("Schema change limit exceeded, SUSPENDING table evolution")
		state.Status = protocol.SchemaStatusSuspended
		p.persistEvoState(table, state)
		return nil, false
	}

	diff := &protocol.SchemaDiff{
		Table:         table,
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
	p.persistEvoState(table, state)

	return diff, true
}

func (p *Producer) detectSchemaChange(msg protocol.Message) (*protocol.SchemaDiff, bool) {
	// HACK: Ignore tables created by the snapshot engine
	if strings.HasPrefix(msg.Table, "cdc_snapshot_") {
		return nil, false
	}

	// Ignore discovery messages themselves to prevent initializing cache with empty Data
	if msg.Op == protocol.OpSchemaChange {
		return nil, false
	}

	p.muEvo.Lock()
	defer p.muEvo.Unlock()

	state, ok := p.evoStates[msg.Table]
	if !ok {
		// Initialize with current columns
		cols := make(map[string]string)
		for k := range msg.Data {
			cols[k] = "unknown"
		}
		state = &tableEvolution{
			Status:       protocol.SchemaStatusStable,
			CachedSchema: cols,
			LastCheckAt:  time.Now(),
		}
		p.evoStates[msg.Table] = state
		return nil, false // Return early to avoid false-positive evolution detection on initialization
	}

	added := make(map[string]string)
	msgKeys := make([]string, 0, len(msg.Data))
	for k := range msg.Data {
		msgKeys = append(msgKeys, k)
		if _, exists := state.CachedSchema[k]; !exists {
			added[k] = "unknown"
		}
	}

	cachedKeys := make([]string, 0, len(state.CachedSchema))
	for k := range state.CachedSchema {
		cachedKeys = append(cachedKeys, k)
	}

	log.Debug().
		Str("table", msg.Table).
		Strs("msg_keys", msgKeys).
		Strs("cached_keys", cachedKeys).
		Int("added_count", len(added)).
		Msg("detectSchemaChange: Comparing keys")

	if len(added) > 0 {
		return p.performSchemaEvolution(msg.Table, msg.SourceID, added)
	}

	return nil, false
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
		// If revision mismatch, we might need to reload, but for now we just log
	} else {
		state.Revision = rev
	}
}

func (p *Producer) handleDiscovery(ctx context.Context, m protocol.Message) {
	// HACK: Ignore tables created by the snapshot engine to prevent discovery feedback loop
	if strings.HasPrefix(m.Schema.Table, "cdc_snapshot_") {
		return
	}

	// ALWAYS warm the schema evolution cache to prevent freeze on first data message
	// This covers both newly discovered tables and known tables after a worker restart (cold cache)
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
				log.Info().Str("table", m.Schema.Table).Int("new_cols", len(added)).Msg("Schema change detected via discovery, freezing table and emitting OpSchemaChange")
				p.muEvo.Unlock()
				if err := p.emitSchemaChange(ctx, m.SourceID, m.Schema.Table, m.LSN, diff); err != nil {
					log.Error().Err(err).Str("table", m.Schema.Table).Msg("Failed to publish OpSchemaChange from discovery")
				}
				p.muEvo.Lock()
			}
		}
	}
	p.muEvo.Unlock()

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

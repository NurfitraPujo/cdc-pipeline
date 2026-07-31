package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
)

type tableEvolution struct {
	Status            string            `json:"status"`
	Revision          uint64            `json:"revision"`
	CorrelationID     string            `json:"correlation_id"`
	CachedSchema      map[string]string `json:"cached_schema"`
	LastCheckAt       time.Time         `json:"last_check_at"`
	ChangesThisMin    int               `json:"changes_this_min"`
	LastChangeAt      time.Time         `json:"last_change_at"`
	AcknowledgedSinks map[string]bool   `json:"acknowledged_sinks"`
}

const (
	publisherCircuitCoolDown = 10 * time.Second
	evoStatePersistAttempts  = 5
	evoStatePersistBackoff   = 50 * time.Millisecond
	evoStatePersistMaxDelay  = 200 * time.Millisecond

	// bufferDrainPendingCheckInterval is how often drainBufferedUntilIdle
	// polls JetStream's NumPending while the buffer channel is quiet. It is
	// purely a poll cadence, not a correctness deadline: drain completion is
	// decided by NumPending==0 (server-side truth), not by elapsed time (WI-9
	// replaces the old 1s-idle-timeout heuristic, which could declare a
	// buffer empty while messages were still in JetStream redelivery flight).
	bufferDrainPendingCheckInterval = 200 * time.Millisecond
	// bufferDrainPendingCheckTimeout bounds each individual NumPending call so
	// a NATS outage during drain surfaces as a logged, retried failure rather
	// than blocking forever (plan §7 Risk 5) — this matters most for the
	// final verification call, which runs under the muTableStates write lock.
	bufferDrainPendingCheckTimeout = 3 * time.Second
)

var errPublishRetriesExhausted = errors.New("publisher retries exhausted")

type producerCircuitBreaker interface {
	Execute(func() (interface{}, error)) (interface{}, error)
	IsOpen() bool
}

type gobreakerCircuitBreaker struct {
	breaker *gobreaker.CircuitBreaker
}

func (b *gobreakerCircuitBreaker) Execute(request func() (interface{}, error)) (interface{}, error) {
	return b.breaker.Execute(request)
}

func (b *gobreakerCircuitBreaker) IsOpen() bool {
	return b.breaker.State() == gobreaker.StateOpen
}

type Producer struct {
	pipelineID      string
	natsURL         string // NEW: for buffer draining
	config          protocol.PipelineConfig
	source          source.Source
	publisher       stream.Publisher
	subscriber      stream.Subscriber // NEW: for schema acks
	kv              nats.KeyValue
	mu              sync.RWMutex
	cancelSource    context.CancelFunc
	cb              producerCircuitBreaker
	circuitCoolDown time.Duration

	sourceConfig       protocol.SourceConfig
	snapshotMu         sync.Mutex
	snapshotInProgress map[string]bool
	snapshotDoneChan   chan string

	muEvo     sync.RWMutex
	evoStates map[string]*tableEvolution // table name -> evolution state

	muTableStates sync.RWMutex
	tableStates   map[string]string // table name -> snapshot state (Snapshotting, Draining, CDC, Error)

	// bufferDrainMu/bufferDraining guard against two concurrent flushBuffer
	// goroutines racing on the same table's (stable, non-UUID) drainer
	// durable name.
	bufferDrainMu  sync.Mutex
	bufferDraining map[string]bool
}

func NewProducer(pipelineID, natsURL string, cfg protocol.PipelineConfig, src source.Source, pub stream.Publisher, sub stream.Subscriber, kv nats.KeyValue, srcConfig protocol.SourceConfig) *Producer {
	settings := gobreaker.Settings{
		Name:        "nats-publisher-" + pipelineID,
		MaxRequests: 3,
		Interval:    5 * time.Second,
		Timeout:     publisherCircuitCoolDown,
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
		cb:                 &gobreakerCircuitBreaker{breaker: gobreaker.NewCircuitBreaker(settings)},
		circuitCoolDown:    publisherCircuitCoolDown,
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
	defer cancel()
	p.mu.Lock()
	p.cancelSource = cancel
	p.mu.Unlock()

	// 0. Recovery: Check KV for frozen tables and restore state
	p.recoverEvoStates(ctx)

	// ackers is the set of sink IDs whose confirmation is required before the
	// source's AckManager may advance the watermark past an LSN. It must match,
	// string-for-string, the SinkID a consumer puts on RecordAck (see
	// Consumer.publishRecordAck / c.sinkID, sourced from the same
	// p.config.Sinks list via factory.go) — a mismatch here can never be
	// satisfied and permanently freezes the replication slot.
	p.mu.RLock()
	ackers := append([]string(nil), p.config.Sinks...)
	p.mu.RUnlock()
	msgChan, ackChan, err := p.source.Start(sourceCtx, srcConfig, checkpoint, ackers)
	if err != nil {
		return 0, fmt.Errorf("failed to start source: %w", err)
	}
	// HIGH-2: Run's defer cancel() above only SIGNALS the source's
	// goroutines (coordinator, slot-lag probe, msgChan-cleanup) to wind
	// down; nothing previously awaited them. recoverProducer calls Run
	// again on errPublishRetriesExhausted (and other error returns), so
	// without an explicit Stop() here the next Run's source.Start() could
	// begin while the prior session's goroutines are still live, racing
	// s.db/s.dsn and leaking the prior *sql.DB/connector. Stop() cancels
	// (idempotent with the deferred cancel above), awaits runWg, and
	// closes the source's resources exactly once per session.
	defer func() {
		if stopErr := p.source.Stop(); stopErr != nil {
			log.Warn().Err(stopErr).Msg("failed to stop source cleanly after Run")
		}
	}()

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
			} else if ack.Op == protocol.OpRecordAck {
				var recordAck protocol.RecordAck
				if _, err := recordAck.UnmarshalMsg(ack.Payload); err != nil {
					log.Error().Err(err).Msg("Failed to unmarshal RecordAck payload")
					ackMsg.Nack()
					continue
				}
				// Blocking, ctx-guarded send: the source's ack coordinator is
				// the only thing that can advance the replication slot, so a
				// dropped ack here permanently freezes it. Ack the NATS
				// message only after the forward succeeds.
				select {
				case ackChan <- source.SourceAck{SinkID: recordAck.SinkID, LSNs: recordAck.LSNs}:
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				}
				ackMsg.Ack()
			} else if ack.Op == "ack" {
				// Legacy single-LSN shape, accepted during rollout (plan §6):
				// producer and consumers deploy together in one binary, but a
				// rolling deploy shares the durable, so an older (pre-WI-5)
				// consumer may still emit this shape for a window. That
				// consumer never set a SinkID (it didn't exist yet), so
				// forwarding it as-is would produce SourceAck{SinkID: ""},
				// which can never satisfy AckManager.required and would pin
				// the watermark until every worker restarts onto the new
				// binary. Pre-WI-5 there was no multi-sink gating at all, so
				// deliberately treat a legacy ack as confirming ALL required
				// sinks — that restores the old (weaker) semantics for the
				// mixed-version window rather than inventing a new guarantee,
				// and is strictly better than freezing the slot. This branch
				// is deleted one release later along with the rest of the
				// legacy-ack path (plan §6 step 3).
				var lsns []uint64
				if ack.LSN > 0 {
					lsns = []uint64{ack.LSN}
				}
				if len(lsns) > 0 {
					p.mu.RLock()
					sinks := append([]string(nil), p.config.Sinks...)
					p.mu.RUnlock()
					for _, sinkID := range sinks {
						select {
						case ackChan <- source.SourceAck{SinkID: sinkID, LSNs: lsns}:
						case <-ctx.Done():
							return lastLSN, ctx.Err()
						}
					}
				}
				ackMsg.Ack()
			}

		case msgs, ok := <-msgChan:
			log.Debug().Any("data", msgs).Msg("Receiving data from source")

			if !ok {
				marker := protocol.Message{
					Op:        protocol.OpDrainMarker,
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
				if m.Op == protocol.OpSchemaChange && m.Schema != nil {
					discoveredTables = append(discoveredTables, m)
				}
				if m.LSN > lastLSN {
					lastLSN = m.LSN
				}

				// 1. Snapshot/Draining State Check
				p.muTableStates.RLock()
				tblState := p.tableStates[m.Table]
				isSnapshotting := tblState == protocol.TableStateSnapshotting ||
					tblState == protocol.TableStateDraining ||
					tblState == protocol.TableStateError
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

				if m.Op == protocol.OpInsert || m.Op == protocol.OpUpdate || m.Op == protocol.OpSnapshot {
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
					// Snapshot rows are checkpointed by chunk, not by LSN (Critical
					// 11's second half): their LSN is meaningless/zero and must not
					// poison the ingress checkpoint used as a resume floor. WI-9:
					// this must also never become a drain target, so update lastLSN
					// only after this check, not before it.
					if m.Op == protocol.OpSnapshot || m.LSN == 0 {
						continue
					}
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
	if maxRetries <= 0 {
		return fmt.Errorf("%w for topic %s: max retries must be positive", errPublishRetriesExhausted, topic)
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; {
		if p.cb.IsOpen() {
			log.Warn().Str("topic", topic).Dur("cool_down", p.circuitCoolDown).Msg("Publisher circuit breaker is open; waiting before retry")
			if err := waitForRetry(ctx, p.circuitCoolDown); err != nil {
				return fmt.Errorf("waiting for publisher circuit breaker cool-down: %w", err)
			}
			continue
		}

		attempt++
		_, lastErr = p.cb.Execute(func() (interface{}, error) {
			return nil, p.publisher.Publish(topic, msg)
		})
		if lastErr == nil {
			return nil
		}

		log.Warn().Err(lastErr).Str("topic", topic).Int("attempt", attempt).Int("max_attempts", maxRetries).Msg("Publish failed, retrying")

		// An Execute failure can open the breaker. Loop immediately so the open-state
		// branch waits for the full cool-down without consuming another retry.
		if p.cb.IsOpen() {
			continue
		}

		backoff := time.Duration(attempt-1) * 100 * time.Millisecond
		if err := waitForRetry(ctx, backoff); err != nil {
			return fmt.Errorf("waiting to retry publish to topic %s: %w", topic, err)
		}
	}

	return fmt.Errorf("%w for topic %s after %d attempts: %w", errPublishRetriesExhausted, topic, maxRetries, lastErr)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *Producer) publishBufferBatch(ctx context.Context, table string, batch protocol.MessageBatch, maxRetries int) error {
	payload, err := batch.MarshalMsg(nil)
	if err != nil {
		return fmt.Errorf("marshalling routed batch for table %s: %w", table, err)
	}
	wmMsg := message.NewMessage(watermill.NewUUID(), payload)

	// Hold the read lock through the durable publish. The Drain -> CDC transition
	// takes the write lock, so it either observes this buffer publish and drains it,
	// or flips first and causes this batch to be routed directly to the main stream.
	p.muEvo.RLock()
	evoState, hasEvoState := p.evoStates[table]
	isEvolutionPaused := hasEvoState && (evoState.Status == protocol.SchemaStatusFrozen || evoState.Status == protocol.SchemaStatusDraining)
	p.muEvo.RUnlock()

	p.muTableStates.RLock()
	defer p.muTableStates.RUnlock()
	tableState := p.tableStates[table]

	shouldBuffer := tableState == protocol.TableStateSnapshotting ||
		tableState == protocol.TableStateDraining ||
		tableState == protocol.TableStateError ||
		isEvolutionPaused

	topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.pipelineID)
	if shouldBuffer {
		topic = fmt.Sprintf("cdc_pipeline_%s_buffer_%s", p.pipelineID, table)
	}

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
				if st.AcknowledgedSinks == nil {
					st.AcknowledgedSinks = make(map[string]bool)
				}
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

	state.AcknowledgedSinks[ack.SinkID] = true
	p.persistEvoState(ack.Table, state)

	if len(state.AcknowledgedSinks) >= len(p.config.Sinks) {
		log.Info().Str("table", ack.Table).Msg("All sinks acknowledged, draining buffer")
		state.Status = protocol.SchemaStatusDraining
		p.persistEvoState(ack.Table, state)
		go p.flushBuffer(ctx, ack.Table)
	}

	p.muEvo.Unlock()
}

// sanitizeDurableComponent makes s safe to embed in a JetStream durable
// consumer name, which forbids '.', whitespace, and '>'/'*' wildcards. Table
// names are frequently schema-qualified (e.g. "public.orders"), which would
// otherwise produce an invalid durable name.
func sanitizeDurableComponent(s string) string {
	return strings.NewReplacer(".", "_", " ", "_", ">", "_", "*", "_").Replace(s)
}

func (p *Producer) flushBuffer(ctx context.Context, table string) {
	// Guard against concurrent flushBuffer calls for the same table (it is
	// launched via `go` from three call sites: recoverEvoStates x2,
	// handleSchemaAck, handleDynamicTables). Two concurrent drains would
	// share the stable durable name below, and one completing and deleting
	// the JetStream consumer out from under the other is worse than the
	// UUID-suffixed-name behavior it replaced. The tableStates/evoStates
	// Draining checks narrow this but do not fully close it (state is read,
	// not reserved), so reserve explicitly here.
	p.bufferDrainMu.Lock()
	if p.bufferDraining == nil {
		p.bufferDraining = make(map[string]bool)
	}
	if p.bufferDraining[table] {
		p.bufferDrainMu.Unlock()
		log.Warn().Str("table", table).Msg("Buffer drain already in progress for this table, skipping duplicate flushBuffer call")
		return
	}
	p.bufferDraining[table] = true
	p.bufferDrainMu.Unlock()
	defer func() {
		p.bufferDrainMu.Lock()
		delete(p.bufferDraining, table)
		p.bufferDrainMu.Unlock()
	}()

	topic := fmt.Sprintf("cdc_pipeline_%s_buffer_%s", p.pipelineID, table)
	// Stable durable name (not UUID-suffixed): if this drain is interrupted
	// (process restart, NATS blip) a subsequent flushBuffer for the same
	// pipeline+table binds to the SAME durable consumer, so JetStream
	// resumes redelivery of whatever was still pending instead of a fresh
	// UUID-named consumer silently stranding those buffered messages behind
	// an abandoned durable (plan 01a WI-9). Table names are sanitized since
	// a schema-qualified name (e.g. "public.orders") would otherwise embed a
	// "." illegal in a JetStream durable name.
	durableName := fmt.Sprintf("drainer-%s-%s", p.pipelineID, sanitizeDurableComponent(table))
	sub, err := stream_nats.NewNatsSubscriber(p.natsURL, durableName, topic, 100, 30*time.Second)
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
	if _, err := p.drainBufferedUntilIdle(ctx, table, msgChan, mainTopic, sub); err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to drain buffered messages")
		return
	}

	// 1. Transition evolution state back to STABLE. A snapshot drain remains in
	// Draining while this write occurs, so concurrent table events still route to
	// the buffer and are covered by the locked final verification below.
	p.muEvo.Lock()
	evoState, ok := p.evoStates[table]
	if ok && evoState.Status == protocol.SchemaStatusDraining {
		log.Info().Str("table", table).Msg("Buffer flush complete for evolution, table is now ACTIVE")
		evoState.Status = protocol.SchemaStatusStable
		evoState.CorrelationID = ""
		p.persistEvoState(table, evoState)
	}
	p.muEvo.Unlock()

	// 2. Transition snapshot state back to CDC. The (potentially long-running,
	// retrying) buffer verification runs UNLOCKED — transitionTableToCDC only
	// takes the write lock for one short, bounded final recheck plus the
	// state flip itself, never for the whole poll loop (a sustained JetStream
	// outage must not be able to deadlock the producer's main publish path,
	// which takes the read lock per message). Incoming routed publishes hold
	// the corresponding read lock through publish, so once transitionTableToCDC
	// acquires the write lock for its final recheck, no more can land in the
	// buffer until it releases — closing the race where a CDC event could
	// otherwise land after drain exit.
	p.mu.RLock()
	sourceID := p.sourceConfig.ID
	p.mu.RUnlock()

	transitioned, err := p.transitionTableToCDC(ctx, sourceID, table, func() (bool, error) {
		return p.drainBufferedUntilIdle(ctx, table, msgChan, mainTopic, sub)
	}, sub)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to complete snapshot buffer drain")
		return
	}
	if transitioned {
		log.Info().Str("table", table).Msg("Buffer flush complete for snapshot, table is now CDC")
		// The drain fully completed (NumPending==0 observed under the write
		// lock in transitionTableToCDC) and the durable name is stable across
		// runs, so nothing will ever resume this consumer again. Delete it
		// rather than leaking one durable JetStream consumer definition per
		// evolution/drain cycle for this table.
		delCtx, cancel := context.WithTimeout(context.Background(), bufferDrainPendingCheckTimeout)
		if err := sub.DeleteConsumer(delCtx); err != nil {
			log.Warn().Err(err).Str("table", table).Msg("Failed to delete drainer JetStream consumer after completed drain")
		}
		cancel()
	}
}

// drainBufferedUntilIdle republishes buffered messages to mainTopic until the
// buffer's JetStream consumer backlog is provably empty. It no longer trusts
// a client-side idle timeout to mean "empty" (plan 01a WI-9): JetStream
// redelivery lag after a NATS restart or under load can easily exceed any
// reasonable fixed idle window, and treating that as "done" strands buffered
// rows behind the table's flip to CDC — silent data loss. Instead it polls
// pc.PendingCount (server-side truth) while msgChan is quiet, and only
// returns true once NumPending==0. pc may be nil (subscriber without
// PendingCounter support); in that degenerate case the function can only
// keep waiting on msgChan/ctx, since there is no other durable line at that
// point.
func (p *Producer) drainBufferedUntilIdle(ctx context.Context, table string, msgChan <-chan *message.Message, mainTopic string, pc stream.PendingCounter) (bool, error) {
	ticker := time.NewTicker(bufferDrainPendingCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case m, ok := <-msgChan:
			if !ok {
				return true, nil
			}

			log.Info().Str("table", table).Msg("Republishing buffered message")
			if err := p.publisher.Publish(mainTopic, m); err != nil {
				m.Nack()
				return false, fmt.Errorf("republishing buffered message for table %s: %w", table, err)
			}
			m.Ack()
		case <-ticker.C:
			if pc == nil {
				continue
			}
			// Bounded so a NATS outage during drain surfaces as a retried
			// failure rather than blocking this call (and, for the final
			// verification call, the muTableStates write lock) forever.
			pendCtx, cancel := context.WithTimeout(ctx, bufferDrainPendingCheckTimeout)
			pending, err := pc.PendingCount(pendCtx)
			cancel()
			if err != nil {
				log.Warn().Err(err).Str("table", table).Msg("drainBufferedUntilIdle: failed to query pending count, will retry")
				continue
			}
			if pending == 0 {
				return true, nil
			}
		}
	}
}

// transitionTableToCDC flips table from Draining to CDC once its buffer is
// verified empty. verifyEmpty (typically drainBufferedUntilIdle) runs
// UNLOCKED and may retry for as long as its ctx allows — it must NOT run
// under muTableStates, or a sustained JetStream outage would hold that lock
// indefinitely and deadlock the producer's main publish path (which takes
// muTableStates.RLock() per message). Once verifyEmpty reports empty, the
// write lock is taken only for a single, tightly bounded recheck via pc
// (closing the race window between the unlocked verify and acquiring the
// lock: a message could have landed in the buffer in that window) plus the
// state flip itself — bounded by bufferDrainPendingCheckTimeout, not an
// unbounded retry loop, so a NATS outage during exactly this recheck can
// only stall the flip for a few seconds, not deadlock the producer.
func (p *Producer) transitionTableToCDC(ctx context.Context, sourceID, table string, verifyEmpty func() (bool, error), pc stream.PendingCounter) (bool, error) {
	p.muTableStates.RLock()
	draining := p.tableStates[table] == protocol.TableStateDraining
	p.muTableStates.RUnlock()
	if !draining {
		return false, nil
	}

	empty, err := verifyEmpty()
	if err != nil {
		return false, fmt.Errorf("verifying final buffer state for table %s: %w", table, err)
	}
	if !empty {
		return false, nil
	}

	p.muTableStates.Lock()
	defer p.muTableStates.Unlock()

	if p.tableStates[table] != protocol.TableStateDraining {
		// Raced with something else (e.g. an error path) that already moved
		// this table out of Draining while we verified unlocked above.
		return false, nil
	}

	if pc != nil {
		checkCtx, cancel := context.WithTimeout(ctx, bufferDrainPendingCheckTimeout)
		pending, pcErr := pc.PendingCount(checkCtx)
		cancel()
		if pcErr != nil {
			return false, fmt.Errorf("final pending recheck for table %s: %w", table, pcErr)
		}
		if pending != 0 {
			// Something landed in the buffer during the unlocked verify ->
			// lock-acquisition window. Do not flip; the caller's drain loop
			// has already exited, so this table will only be retried on the
			// next flushBuffer trigger (schema ack / recovery / dynamic-table
			// addition) rather than looping here under the write lock.
			return false, nil
		}
	}

	p.tableStates[table] = protocol.TableStateCDC

	stateKey := protocol.TableStateKey(p.pipelineID, sourceID, table)
	if _, err := p.kv.Put(stateKey, []byte(protocol.TableStateCDC)); err != nil {
		return true, fmt.Errorf("persisting CDC table state for %s: %w", table, err)
	}

	return true, nil
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
			Status:            protocol.SchemaStatusStable,
			CachedSchema:      cols,
			LastCheckAt:       time.Now(),
			AcknowledgedSinks: make(map[string]bool),
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
	state.AcknowledgedSinks = make(map[string]bool)
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

func (p *Producer) persistEvoState(table string, state *tableEvolution) error {
	key := protocol.SchemaEvolutionKey(p.pipelineID, table)
	revision := state.Revision
	var lastErr error

	for attempt := 1; attempt <= evoStatePersistAttempts; attempt++ {
		state.Revision = revision
		data, err := json.Marshal(state)
		if err != nil {
			lastErr = fmt.Errorf("marshalling evolution state: %w", err)
			break
		}

		var newRevision uint64
		if revision == 0 {
			newRevision, err = p.kv.Put(key, data)
		} else {
			newRevision, err = p.kv.Update(key, data, revision)
		}
		if err == nil {
			state.Revision = newRevision
			return nil
		}

		lastErr = err
		log.Warn().Err(err).Str("table", table).Int("attempt", attempt).Int("max_attempts", evoStatePersistAttempts).Msg("Failed to persist evolution state; retrying")
		if attempt == evoStatePersistAttempts {
			break
		}

		// Refresh the fencing token after a CAS conflict. Retrying a stale revision
		// unchanged can never converge when another writer has advanced the key.
		entry, getErr := p.kv.Get(key)
		if getErr != nil {
			log.Warn().Err(getErr).Str("table", table).Msg("Failed to refresh evolution state revision")
		} else {
			revision = entry.Revision()
		}

		delay := time.Duration(attempt) * evoStatePersistBackoff
		if delay > evoStatePersistMaxDelay {
			delay = evoStatePersistMaxDelay
		}
		time.Sleep(delay)
	}

	persistErr := fmt.Errorf("persisting evolution state for table %s after %d attempts: %w", table, evoStatePersistAttempts, lastErr)
	log.Error().Err(persistErr).Str("table", table).Msg("Evolution state persistence exhausted; pausing table CDC ingestion")
	p.pauseTableCDC(table)
	return persistErr
}

func (p *Producer) pauseTableCDC(table string) {
	p.muTableStates.Lock()
	p.tableStates[table] = protocol.TableStateError
	p.muTableStates.Unlock()

	p.mu.RLock()
	sourceID := p.sourceConfig.ID
	p.mu.RUnlock()
	if sourceID == "" || p.kv == nil {
		return
	}

	stateKey := protocol.TableStateKey(p.pipelineID, sourceID, table)
	if _, err := p.kv.Put(stateKey, []byte(protocol.TableStateError)); err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to persist paused table state")
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

// SetDynamicTablesChan spawns a goroutine that consumes dynamic-table signals
// off ch until ctx is cancelled. It never blocks forever on a channel that is
// never closed: it selects on ctx.Done() alongside the channel receive.
//
// The caller must track the goroutine on wg so it isn't leaked, but that wg
// must NOT be the Pipeline's main WaitGroup: this goroutine's only exit is
// ctx cancellation, and the pipeline's graceful Drain() path deliberately
// does not cancel ctx (Producer.Drain only cancels cancelSource). Wiring this
// into the main wg would make Finished() hang until something else cancels
// the context. Callers should use a separate auxiliary WaitGroup that is
// waited on only after ctx is cancelled (see Pipeline.Shutdown).
func (p *Producer) SetDynamicTablesChan(ctx context.Context, wg *sync.WaitGroup, ch <-chan []string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case tables, ok := <-ch:
				if !ok {
					return
				}
				p.mu.RLock()
				sid := p.sourceConfig.ID
				p.mu.RUnlock()
				p.handleDynamicTables(ctx, sid, tables)
			}
		}
	}()
}

func (p *Producer) handleDynamicTables(ctx context.Context, sourceID string, newTables []string) {
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

			// Flush buffer. Uses the pipeline-lifetime ctx threaded in from
			// SetDynamicTablesChan (not context.Background()): a Background
			// ctx here meant a sustained JetStream/NATS outage during drain
			// could never be cancelled by pipeline shutdown, wedging this
			// goroutine (and, before the transitionTableToCDC fix above, the
			// producer's write lock) forever.
			p.flushBuffer(ctx, tbl)

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

	// Construct DSN
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.PassEncrypted, cfg.Host, cfg.Port, cfg.Database)

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

	cpKey := protocol.IngressCheckpointKey(p.pipelineID, sourceID, tableName)
	var lastPKValues []interface{}

	entry, err := p.kv.Get(cpKey)
	if err == nil {
		var cp protocol.Checkpoint
		if _, err := cp.UnmarshalMsg(entry.Value()); err == nil {
			if cp.Status == "Snapshotting" && cp.LastPK != "" {
				var lastPKData map[string]interface{}
				if err := json.Unmarshal([]byte(cp.LastPK), &lastPKData); err == nil {
					lastPKValues = make([]interface{}, len(pkCols))
					valid := true
					for i, col := range pkCols {
						val, ok := lastPKData[col]
						if !ok {
							valid = false
							break
						}
						lastPKValues[i] = val
					}
					if !valid {
						lastPKValues = nil
					} else {
						log.Info().Str("table", tableName).Str("last_pk", cp.LastPK).Msg("Resuming snapshot from last PK checkpoint")
					}
				}
			}
		}
	}

	totalRows := 0
	for {
		var query string
		var args []interface{}
		if len(lastPKValues) > 0 {
			if len(pkCols) == 1 {
				query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT %d", tableName, pkCols[0], pkStr, chunkSize)
				args = append(args, lastPKValues[0])
			} else {
				placeholders := make([]string, len(pkCols))
				for i := range pkCols {
					placeholders[i] = fmt.Sprintf("$%d", i+1)
				}
				query = fmt.Sprintf("SELECT * FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT %d", tableName, pkStr, strings.Join(placeholders, ", "), pkStr, chunkSize)
				args = lastPKValues
			}
		} else {
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d", tableName, pkStr, chunkSize)
		}

		rows, err := db.Query(query, args...)
		if err != nil {
			return fmt.Errorf("snapshot query failed: %w", err)
		}

		cols, _ := rows.Columns()
		count := 0
		batch := make(protocol.MessageBatch, 0, chunkSize)
		var lastRowPK map[string]interface{}

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
			lastRowPK = pkData

			pkJSON, _ := json.Marshal(pkData)
			batch = append(batch, protocol.Message{
				SourceID:  sourceID,
				Table:     tableName,
				Op:        protocol.OpSnapshot,
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

			// Persist progress to NATS KV
			lastMsg := batch[len(batch)-1]
			cp := protocol.Checkpoint{
				IngressLSN: 0,
				LastPK:     lastMsg.PK,
				Status:     "Snapshotting",
				UpdatedAt:  time.Now(),
			}
			cpData, err := cp.MarshalMsg(nil)
			if err == nil {
				if _, err := p.kv.Put(cpKey, cpData); err != nil {
					log.Error().Err(err).Str("table", tableName).Msg("Failed to persist snapshot checkpoint")
				}
			}
		}

		if count > 0 && lastRowPK != nil {
			lastPKValues = make([]interface{}, len(pkCols))
			for i, col := range pkCols {
				lastPKValues[i] = lastRowPK[col]
			}
		}

		totalRows += count
		if count < chunkSize {
			break
		}
	}

	// Finalize status to ACTIVE
	cp := protocol.Checkpoint{
		IngressLSN: 0,
		LastPK:     "",
		Status:     "ACTIVE",
		UpdatedAt:  time.Now(),
	}
	if cpData, err := cp.MarshalMsg(nil); err == nil {
		_, _ = p.kv.Put(cpKey, cpData)
	}

	log.Info().Str("table", tableName).Int("total_rows", totalRows).Msg("Snapshot complete")
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

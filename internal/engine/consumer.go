package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/sink"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const (
	retryCleanupInterval = 5 * time.Minute

	// recordAckMaxAttempts/recordAckRetryBackoff bound the RecordAck publish
	// retry before the flush gives up and Nacks the batch for JetStream
	// redelivery (plan §3.WI-5: "publish failure must not proceed silently").
	recordAckMaxAttempts  = 3
	recordAckRetryBackoff = 100 * time.Millisecond
)

type ConfiguredTransformer struct {
	Transformer    transformer.Transformer
	OperationTypes []protocol.OperationType
}

type Consumer struct {
	pipelineID       string
	sinkID           string
	subscriber       stream.Subscriber
	publisher        stream.Publisher // for DLQ
	sink             sink.Sink
	transformers     []ConfiguredTransformer
	transformerNames []string // NEW: pre-computed names for audit trail
	kv               nats.KeyValue
	batchSize        int
	batchWait        time.Duration
	retryConfig      protocol.RetryConfig
	retries          map[string]retryEntry // UUID -> retry info with timestamp
	retryMu          sync.Mutex
	stats            map[string]*protocol.TableStats
	statsMu          sync.Mutex
	lastCleanupTime  time.Time

	// Hooks for debug sink capture (nil for regular consumers)
	preTransformHook  sink.PreTransformHook
	postTransformHook sink.PostTransformHook

	// Drain control
	mu         sync.RWMutex
	isDraining bool
	targetLSN  uint64
}

type retryEntry struct {
	count     int
	lastRetry time.Time
}

func NewConsumer(pipelineID, sinkID string, sub stream.Subscriber, pub stream.Publisher, snk sink.Sink, transformers []ConfiguredTransformer, kv nats.KeyValue, batchSize int, batchWait time.Duration, retry protocol.RetryConfig, preHook sink.PreTransformHook, postHook sink.PostTransformHook) *Consumer {
	names := make([]string, len(transformers))
	for i, t := range transformers {
		names[i] = t.Transformer.Name()
	}

	return &Consumer{
		pipelineID:        pipelineID,
		sinkID:            sinkID,
		subscriber:        sub,
		publisher:         pub,
		sink:              snk,
		transformers:      transformers,
		transformerNames:  names,
		kv:                kv,
		batchSize:         batchSize,
		batchWait:         batchWait,
		retryConfig:       retry,
		retries:           make(map[string]retryEntry),
		stats:             make(map[string]*protocol.TableStats),
		lastCleanupTime:   time.Now(),
		preTransformHook:  preHook,
		postTransformHook: postHook,
	}
}

func (c *Consumer) LoadStats(sourceID string, tables []string) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	for _, table := range tables {
		key := protocol.TableStatsKey(c.pipelineID, sourceID, c.sinkID, table)
		entry, err := c.kv.Get(key)
		if err == nil {
			var st protocol.TableStats
			if err := json.Unmarshal(entry.Value(), &st); err == nil {
				c.stats[sourceID+"."+table] = &st
			}
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, msgs []protocol.Message) ([]protocol.Message, error) {
	var correlationIDs []string
	if c.preTransformHook != nil {
		correlationIDs = c.preTransformHook(ctx, c.pipelineID, c.transformerNames, msgs)
	}

	if len(c.transformers) == 0 {
		if c.postTransformHook != nil {
			c.postTransformHook(ctx, c.pipelineID, correlationIDs, c.transformerNames, msgs, msgs, nil)
		}
		return msgs, nil
	}

	processed := msgs
	filteredIndices := make([]int, 0)

	for _, t := range c.transformers {
		if len(t.OperationTypes) == 0 {
			continue
		}

		matchingIndices := make([]int, 0, len(processed))
		for i, m := range processed {
			skip := true
			for _, opType := range t.OperationTypes {
				if m.Op == opType {
					skip = false
					break
				}
			}
			if !skip {
				matchingIndices = append(matchingIndices, i)
			}
		}

		if len(matchingIndices) == 0 {
			continue
		}

		matchingMsgs := make([]protocol.Message, len(matchingIndices))
		matchingUUIDs := make(map[string]bool, len(matchingMsgs))
		for j, idx := range matchingIndices {
			matchingMsgs[j] = processed[idx]
			matchingUUIDs[matchingMsgs[j].UUID] = true
		}

		if bt, ok := t.Transformer.(transformer.BatchTransformer); ok {
			transformed, err := bt.TransformBatch(ctx, matchingMsgs)
			if err != nil {
				return nil, fmt.Errorf("batch transformer %s failed: %w", t.Transformer.Name(), err)
			}
			transformedByUUID := make(map[string]protocol.Message, len(transformed))
			for _, tm := range transformed {
				transformedByUUID[tm.UUID] = tm
			}
			newProcessed := make([]protocol.Message, 0, len(processed))
			for i, m := range processed {
				if matchingUUIDs[m.UUID] {
					if tm, ok := transformedByUUID[m.UUID]; ok {
						newProcessed = append(newProcessed, tm)
					} else {
						filteredIndices = append(filteredIndices, i)
					}
				} else {
					newProcessed = append(newProcessed, m)
				}
			}
			processed = newProcessed
		} else {
			droppedInThisStep := make(map[int]bool)
			for j, idx := range matchingIndices {
				current := &matchingMsgs[j]
				keep := true
				var err error
				current, keep, err = t.Transformer.Transform(ctx, current)
				if err != nil {
					return nil, fmt.Errorf("transformer %s failed: %w", t.Transformer.Name(), err)
				}
				if !keep || current == nil {
					filteredIndices = append(filteredIndices, idx)
					droppedInThisStep[idx] = true
				} else {
					matchingMsgs[j] = *current
					processed[idx] = *current
				}
			}
			newProcessed := make([]protocol.Message, 0, len(processed)-len(droppedInThisStep))
			for i, m := range processed {
				if !droppedInThisStep[i] {
					newProcessed = append(newProcessed, m)
				}
			}
			processed = newProcessed
		}
	}

	if c.postTransformHook != nil {
		c.postTransformHook(ctx, c.pipelineID, correlationIDs, c.transformerNames, msgs, processed, filteredIndices)
	}

	return processed, nil
}

func (c *Consumer) Run(ctx context.Context, topic string) error {
	msgChan, err := c.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS: %w", err)
	}

	var batch []protocol.Message
	var wmMsgs []*message.Message
	var schemaWMMsgs []*message.Message // T0-2: separate slice for schema-change wrappers
	timer := time.NewTimer(c.batchWait)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				c.flush(ctx, batch, wmMsgs)
			}
			// T0-2: Nack any unprocessed schema-change messages on shutdown
			for _, sm := range schemaWMMsgs {
				sm.Nack()
			}
			return ctx.Err()

		case <-timer.C:
			if len(batch) > 0 {
				c.flush(ctx, batch, wmMsgs)
				batch = nil
				wmMsgs = nil
			}

		case wmMsg, ok := <-msgChan:
			if !ok {
				if len(batch) > 0 {
					c.flush(ctx, batch, wmMsgs)
				}
				return nil
			}

			if len(batch) == 0 {
				timer.Reset(c.batchWait)
			}

			var batchFromNats []protocol.Message
			if _, err := protocol.UnmarshalMessageBatch(wmMsg.Payload, &batchFromNats); err != nil {
				log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to unmarshal batch")
				wmMsg.Nack()
				continue
			}

			log.Debug().Str("pipeline_id", c.pipelineID).Any("data", wmMsg).Msg("Received message from NATS")

			// T0-2: Pre-scan batch for schema changes BEFORE appending wmMsg to wmMsgs
			hasSchemaChange := false
			for i := range batchFromNats {
				if batchFromNats[i].Op == protocol.OpSchemaChange {
					hasSchemaChange = true
					break
				}
			}

			// T0-2: If schema change detected and we have pending batch, flush prior batch
			// WITHOUT including the current wmMsg (which wraps the schema change)
			if hasSchemaChange && len(batch) > 0 {
				c.flush(ctx, batch, wmMsgs)
				batch = nil
				wmMsgs = nil
			}

			// Only append wmMsg to wmMsgs if no schema change (schema wmMsgs go to schemaWMMsgs)
			if !hasSchemaChange {
				wmMsgs = append(wmMsgs, wmMsg)
			}

			for i := range batchFromNats {
				m := &batchFromNats[i]
				if m.Op == protocol.OpDrainMarker {
					c.mu.RLock()
					isDraining := c.isDraining
					c.mu.RUnlock()

					if isDraining {
						if len(batch) > 0 {
							c.flush(ctx, batch, wmMsgs)
							batch = nil
							wmMsgs = nil
						}
						wmMsg.Ack()
						log.Info().Str("pipeline_id", c.pipelineID).Msg("Received drain marker, finishing consumer")
						return nil
					} else {
						// Stale drain marker from a previous session, ignore and ack
						wmMsg.Ack()
						log.Info().Str("pipeline_id", c.pipelineID).Msg("Received stale drain marker, ignoring")
						continue
					}
				}

				if m.Op == protocol.OpSchemaChange {
					// T0-2: Carry schema-change wmMsg in separate slice; only ack AFTER ApplySchema succeeds
					schemaWMMsgs = append(schemaWMMsgs, wmMsg)

					if m.Schema == nil && m.Diff != nil {
						log.Info().Str("pipeline_id", c.pipelineID).Str("table", m.Table).Interface("added_cols", m.Diff.Added).Msg("Constructing schema from diff")
						m.Schema = &protocol.SchemaMetadata{
							Table:   m.Table,
							Columns: m.Diff.Added,
						}
					}

					var applyErr error
					if m.Schema != nil {
						transformedMsgs, err := c.processMessages(ctx, []protocol.Message{*m})
						if err != nil {
							log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Error transforming schema change")
							c.updateTableError(m.SourceID, m.Table)
							// T0-2: Nack and continue - do NOT abort consumer loop on DDL failure
							wmMsg.Nack()
							schemaWMMsgs = schemaWMMsgs[:len(schemaWMMsgs)-1] // remove from slice
							continue
						}
						if len(transformedMsgs) > 0 {
							transformed := transformedMsgs[0]
							if err := c.sink.ApplySchema(ctx, transformed); err != nil {
								log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Error applying schema change")
								c.updateTableError(m.SourceID, m.Table)
								// T0-2: Nack and continue - do NOT abort consumer loop on DDL failure
								wmMsg.Nack()
								schemaWMMsgs = schemaWMMsgs[:len(schemaWMMsgs)-1] // remove from slice
								continue
							}
						} else {
							log.Warn().Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Schema change filtered out by transformer")
						}
					}

					// T0-2: Only ack AFTER ApplySchema succeeds (tracked via applyErr)
					applyErr = nil // no error means success
					if applyErr == nil {
						wmMsg.Ack()
						schemaWMMsgs = schemaWMMsgs[:len(schemaWMMsgs)-1] // remove from slice after ack
					}

					// Emit Ack only if CorrelationID is present (indicates proactive evolution)
					if m.CorrelationID != "" {
						ack := protocol.Message{
							Op:            protocol.OpSchemaChangeAck,
							CorrelationID: m.CorrelationID,
							Table:         m.Table,
							SourceID:      m.SourceID,
							SinkID:        c.sinkID,
							Timestamp:     time.Now(),
						}
						ackData, _ := ack.MarshalMsg(nil)
						ackTopic := protocol.AcksTopic(c.pipelineID)
						if err := c.publisher.Publish(ackTopic, message.NewMessage(m.UUID, ackData)); err != nil {
							log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Failed to publish schema change ack")
						}
					}

					continue
				}

				batch = append(batch, *m)
			}

			// If batch is full, flush now
			if len(batch) >= c.batchSize {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				c.flush(ctx, batch, wmMsgs)
				batch = nil
				wmMsgs = nil
			}
		}
	}
}


func (c *Consumer) updateTableError(sourceID, table string) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	key := sourceID + "." + table
	s, ok := c.stats[key]
	if !ok {
		s = &protocol.TableStats{Status: "ERROR"}
		c.stats[key] = s
	}
	s.ErrorCount++
	s.Status = "ERROR"
	s.UpdatedAt = time.Now()

	metrics.SyncErrors.WithLabelValues(c.pipelineID, sourceID, table).Inc()
	statsData, _ := json.Marshal(s)
	statsKey := protocol.TableStatsKey(c.pipelineID, sourceID, c.sinkID, table)
	if _, err := c.kv.Put(statsKey, statsData); err != nil {
		log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", table).Msg("Failed to update table stats")
	}
}

func (c *Consumer) flush(ctx context.Context, batch []protocol.Message, wmMsgs []*message.Message) {
	if len(batch) == 0 {
		return
	}
	toUpload, err := c.processMessages(ctx, batch)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Transformation failed, handling as batch error")
		c.handleSinkError(ctx, batch, wmMsgs, err)
		return
	}
	if len(toUpload) == 0 {
		// Every message in batch was terminally decided by transformer
		// filtering (processMessages can drop rows, e.g. via a transformer's
		// keep=false at :184-186) — nothing will ever be written for those
		// LSNs by anyone. That IS the durability decision, so it must be
		// confirmed the same as a successful sink write, or the source's
		// AckManager waits for a RecordAck that will never come and the
		// watermark — and the replication slot behind it — freezes forever.
		if !c.publishRecordAck(ctx, batch) {
			for _, m := range wmMsgs {
				m.Nack()
			}
			return
		}
		for _, m := range wmMsgs {
			m.Ack()
			c.retryMu.Lock()
			delete(c.retries, m.UUID)
			c.retryMu.Unlock()
		}
		return
	}

	if err := c.sink.BatchUpload(ctx, toUpload); err != nil {
		c.handleSinkError(ctx, batch, wmMsgs, err)
		return
	}
	log.Debug().Int("count", len(toUpload)).Str("pipeline_id", c.pipelineID).Msg("Consumer: Batch upload successful")

	// Ordering (crash-window fix, plan §3.WI-5): publish the RecordAck BEFORE
	// acking the wmMsgs, not after. A crash between BatchUpload and this point
	// has no JetStream redelivery to regenerate the ack once wmMsgs are acked,
	// which would silently stall the watermark. Publishing first means a crash
	// before the wmMsg ack simply causes JetStream to redeliver the batch; the
	// re-emitted RecordAck is harmless because AckManager.Confirm(lsn <=
	// watermark) is a no-op.
	//
	// The LSN set comes from `batch` (the full terminally-handled input), not
	// `toUpload` (the post-transformer-filter subset): any row a transformer
	// dropped from toUpload was still terminally decided — it will never be
	// written — so its LSN must still be confirmed, or that LSN is stuck in
	// the source's AckManager.pending forever even though the wmMsg carrying
	// it gets acked below.
	if !c.publishRecordAck(ctx, batch) {
		// Publish failed after bounded retries: never proceed silently (a lost
		// ack would permanently stall the watermark). Nack so the whole flush
		// re-runs on redelivery.
		for _, m := range wmMsgs {
			m.Nack()
		}
		return
	}

	for _, m := range wmMsgs {
		m.Ack()
		c.retryMu.Lock()
		delete(c.retries, m.UUID)
		c.retryMu.Unlock()
	}

	c.updateStats(toUpload)
	if time.Since(c.lastCleanupTime) > retryCleanupInterval {
		c.cleanupOldRetries()
		c.lastCleanupTime = time.Now()
	}
}

// publishRecordAck publishes a single protocol.RecordAck envelope on
// AcksTopic covering every LSN in msgs whose fate is terminally decided
// (excluding OpSnapshot and LSN-0 messages, whose durability story is
// JetStream + chunk-job state, not the LSN watermark; and OpDrainMarker/
// OpSchemaChangeAck, which never carry a replication LSN). Callers must pass
// the full set of messages whose disposition is now final — durably
// written, deliberately filtered by a transformer, or routed to DLQ/isolated
// as poison are all terminal and must be confirmed, since the source is
// waiting to learn the LSN will never need replaying. If msgs carries no
// LSNs worth acking, this is a no-op success. Retries a bounded number of
// times on publish failure; returns false only once retries are exhausted,
// signalling the caller to Nack rather than silently stalling the watermark.
func (c *Consumer) publishRecordAck(ctx context.Context, msgs []protocol.Message) bool {
	var lsns []uint64
	var sourceID string
	for _, m := range msgs {
		if m.Op == protocol.OpDrainMarker || m.Op == protocol.OpSchemaChangeAck {
			continue
		}
		if sourceID == "" {
			sourceID = m.SourceID
		}
		if m.Op == protocol.OpSnapshot || m.LSN == 0 {
			continue
		}
		lsns = append(lsns, m.LSN)
	}
	if len(lsns) == 0 {
		return true
	}

	recordAck := protocol.RecordAck{
		PipelineID: c.pipelineID,
		SourceID:   sourceID,
		SinkID:     c.sinkID,
		LSNs:       lsns,
		Timestamp:  time.Now(),
	}
	payload, err := recordAck.MarshalMsg(nil)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to marshal RecordAck")
		return false
	}
	envelope := protocol.Message{
		Op:        protocol.OpRecordAck,
		SourceID:  sourceID,
		SinkID:    c.sinkID,
		Payload:   payload,
		Timestamp: time.Now(),
	}
	envData, err := envelope.MarshalMsg(nil)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to marshal RecordAck envelope")
		return false
	}

	ackTopic := protocol.AcksTopic(c.pipelineID)
	var lastErr error
	for attempt := 1; attempt <= recordAckMaxAttempts; attempt++ {
		pubErr := c.publisher.Publish(ackTopic, message.NewMessage(uuid.New().String(), envData))
		if pubErr == nil {
			return true
		}
		lastErr = pubErr

		if attempt == recordAckMaxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			attempt = recordAckMaxAttempts // stop retrying; ctx is dead
		case <-time.After(recordAckRetryBackoff):
		}
	}

	log.Error().Err(lastErr).Str("pipeline_id", c.pipelineID).Msg("RecordAck publish exhausted retries; Nacking batch for JetStream redelivery")
	return false
}

// T0-2: flushWithFilter flushes batch while using acksFilter to decide which wmMsgs to ack.
// This allows the caller to exclude schema-change wrappers from being acked prematurely.
func (c *Consumer) flushWithFilter(ctx context.Context, batch []protocol.Message, wmMsgs []*message.Message, acksFilter func(*message.Message) bool) {
	if len(batch) == 0 {
		return
	}
	toUpload, err := c.processMessages(ctx, batch)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Transformation failed, handling as batch error")
		c.handleSinkError(ctx, batch, wmMsgs, err)
		return
	}
	if len(toUpload) == 0 {
		// Same terminal-decision reasoning as flush's len(toUpload)==0 branch:
		// transformer-filtered rows in batch will never be written by anyone,
		// so their LSNs must still be confirmed before acking.
		if !c.publishRecordAck(ctx, batch) {
			for _, m := range wmMsgs {
				if acksFilter == nil || acksFilter(m) {
					m.Nack()
				}
			}
			return
		}
		for _, m := range wmMsgs {
			if acksFilter == nil || acksFilter(m) {
				m.Ack()
			}
			c.retryMu.Lock()
			delete(c.retries, m.UUID)
			c.retryMu.Unlock()
		}
		return
	}

	if err := c.sink.BatchUpload(ctx, toUpload); err != nil {
		c.handleSinkError(ctx, batch, wmMsgs, err)
		return
	}
	log.Debug().Int("count", len(toUpload)).Str("pipeline_id", c.pipelineID).Msg("Consumer: Batch upload successful")

	// Same publish-before-ack ordering as flush (see its comment), and the
	// same batch-not-toUpload LSN-set reasoning: any row a transformer
	// dropped from toUpload was still terminally decided and must still be
	// confirmed.
	if !c.publishRecordAck(ctx, batch) {
		for _, m := range wmMsgs {
			if acksFilter == nil || acksFilter(m) {
				m.Nack()
			}
		}
		return
	}

	for _, m := range wmMsgs {
		if acksFilter == nil || acksFilter(m) {
			m.Ack()
		}
		c.retryMu.Lock()
		delete(c.retries, m.UUID)
		c.retryMu.Unlock()
	}

	c.updateStats(toUpload)
	if time.Since(c.lastCleanupTime) > retryCleanupInterval {
		c.cleanupOldRetries()
		c.lastCleanupTime = time.Now()
	}
}

func (c *Consumer) handleSinkError(ctx context.Context, batch []protocol.Message, wmMsgs []*message.Message, err error) {
	c.statsMu.Lock()
	for _, m := range batch {
		key := m.SourceID + "." + m.Table
		s, ok := c.stats[key]
		if !ok {
			s = &protocol.TableStats{Status: "ERROR"}
			c.stats[key] = s
		}
		s.ErrorCount++
		s.Status = "ERROR"
		s.UpdatedAt = time.Now()

		metrics.SyncErrors.WithLabelValues(c.pipelineID, m.SourceID, m.Table).Inc()
		statsData, _ := s.MarshalMsg(nil)
		statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, c.sinkID, m.Table)
		if _, err := c.kv.Put(statsKey, statsData); err != nil {
			log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Failed to update table stats")
		} else {
			log.Debug().Str("pipeline_id", c.pipelineID).Str("table", m.Table).Uint64("total_synced", s.TotalSynced).Msg("Successfully updated table stats in KV")
		}
	}
	c.statsMu.Unlock()

	shouldIsolate := false
	c.retryMu.Lock()
	now := time.Now()
	for _, m := range wmMsgs {
		entry := c.retries[m.UUID]
		entry.count++
		entry.lastRetry = now
		c.retries[m.UUID] = entry
		if entry.count > c.retryConfig.MaxRetries {
			shouldIsolate = true
		}
	}
	c.retryMu.Unlock()

	if shouldIsolate {
		log.Warn().Str("pipeline_id", c.pipelineID).Msg("Batch failed repeatedly, switching to Isolation Mode")
		c.isolatePoisonBatch(ctx, wmMsgs)
		return
	}

	log.Error().Err(err).Str("pipeline_id", c.pipelineID).Int("batch_size", len(wmMsgs)).Msg("Sink upload failed, Nacking batch for JetStream redelivery")

	backoff := c.retryConfig.InitialInterval
	maxAttempts := 0
	c.retryMu.Lock()
	for _, m := range wmMsgs {
		if c.retries[m.UUID].count > maxAttempts {
			maxAttempts = c.retries[m.UUID].count
		}
	}
	c.retryMu.Unlock()

	for i := 1; i < maxAttempts; i++ {
		backoff *= 2
		if backoff > c.retryConfig.MaxInterval {
			backoff = c.retryConfig.MaxInterval
			break
		}
	}

	for _, m := range wmMsgs {
		m.Nack()
	}

	if backoff > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
	} else {
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

func (c *Consumer) updateStats(batch []protocol.Message) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	latestByTable := make(map[string]protocol.Message)
	countsByTable := make(map[string]int)
	for _, m := range batch {
		key := m.SourceID + "." + m.Table
		latestByTable[key] = m
		countsByTable[key]++
	}

	now := time.Now()
	for key, m := range latestByTable {
		// Snapshot rows carry no meaningful LSN (Critical 11's second half):
		// writing EgressLSN from them would poison the pipeline's resume floor
		// (pipeline.go min-EgressLSN scan) with a value that has nothing to do
		// with replication progress.
		if m.Op != protocol.OpSnapshot {
			checkpoint := protocol.Checkpoint{
				EgressLSN: m.LSN,
				LastPK:    m.PK,
				Status:    "ACTIVE",
				UpdatedAt: now,
			}
			cpData, err := checkpoint.MarshalMsg(nil)
			if err == nil {
				cpKey := protocol.EgressCheckpointKey(c.pipelineID, m.SourceID, c.sinkID, m.Table)
				if _, err := c.kv.Put(cpKey, cpData); err != nil {
					log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Error updating egress checkpoint")
				}
			}
		}

		s, ok := c.stats[key]
		if !ok {
			s = &protocol.TableStats{Status: "ACTIVE"}
			c.stats[key] = s
		}
		s.Status = "ACTIVE"
		count := uint64(countsByTable[key])
		if count > 0 {
			s.TotalSynced += count
			metrics.RecordsSynced.WithLabelValues(c.pipelineID, m.SourceID, m.Table).Add(float64(count))
		}
		s.LastSourceTS = m.Timestamp
		s.LastProcessedTS = now
		s.LagMS = now.Sub(m.Timestamp).Milliseconds()
		s.UpdatedAt = now

		metrics.PipelineLag.WithLabelValues(c.pipelineID, m.SourceID, m.Table).Set(float64(s.LagMS))

		statsData, err := s.MarshalMsg(nil)
		if err == nil {
			statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, c.sinkID, m.Table)
			if _, err := c.kv.Put(statsKey, statsData); err != nil {
				log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Error updating table stats")
			}
		}
	}
}

func (c *Consumer) Drain(targetLSN uint64) {
	c.mu.Lock()
	c.targetLSN = targetLSN
	c.isDraining = true
	c.mu.Unlock()
}

func (c *Consumer) checkDrained(currentLSN uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.isDraining {
		return false
	}
	if currentLSN >= c.targetLSN && c.targetLSN > 0 {
		return true
	}
	return false
}

func (c *Consumer) isolatePoisonBatch(ctx context.Context, wmMsgs []*message.Message) {
	for _, wmMsg := range wmMsgs {
		var msgs []protocol.Message
		if _, err := protocol.UnmarshalMessageBatch(wmMsg.Payload, &msgs); err != nil {
			// The payload can't be parsed, so the LSNs it carried are
			// unrecoverable here — there is nothing to confirm. This gap
			// predates WI-5 (an unparseable payload was already unrecoverable
			// data) and is out of this WI's scope; routing to DLQ is still the
			// right terminal action for the message itself.
			log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to unmarshal message for isolation, routing to DLQ")
			c.routeToDLQ(wmMsg)
			continue
		}

		toUpload, err := c.processMessages(ctx, msgs)
		if err != nil {
			log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("msg_id", wmMsg.UUID).Msg("Transformation failed in isolation, routing to DLQ")
			c.routeToDLQWithAck(ctx, wmMsg, msgs)
			continue
		}
		if len(toUpload) == 0 {
			// Every message in msgs was filtered out by a transformer — a
			// terminal decision identical to flush's len(toUpload)==0 branch.
			if !c.publishRecordAck(ctx, msgs) {
				wmMsg.Nack()
				continue
			}
			wmMsg.Ack()
			c.retryMu.Lock()
			delete(c.retries, wmMsg.UUID)
			c.retryMu.Unlock()
			continue
		}

		if err := c.sink.BatchUpload(ctx, toUpload); err != nil {
			log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("msg_id", wmMsg.UUID).Msg("Message failed in isolation")

			c.retryMu.Lock()
			entry := c.retries[wmMsg.UUID]
			attempts := entry.count
			c.retryMu.Unlock()

			if attempts >= c.retryConfig.MaxRetries && c.retryConfig.EnableDLQ {
				log.Warn().Str("pipeline_id", c.pipelineID).Str("msg_id", wmMsg.UUID).Msg("Message exceeded MaxRetries, routing to DLQ")
				c.routeToDLQWithAck(ctx, wmMsg, msgs)
			} else {
				wmMsg.Nack()
			}
		} else {
			// Durable write succeeded in isolation mode — the same terminal
			// decision as the main flush path, and it must be confirmed the
			// same way, or a moderately long transient sink outage (enough to
			// exceed MaxRetries, no optional feature required) freezes the
			// slot even though every row landed in the sink.
			if !c.publishRecordAck(ctx, msgs) {
				wmMsg.Nack()
				continue
			}
			wmMsg.Ack()
			c.retryMu.Lock()
			delete(c.retries, wmMsg.UUID)
			c.retryMu.Unlock()
		}
	}
}

// routeToDLQWithAck emits a RecordAck for msgs' LSNs before routing wmMsg to
// the DLQ. Routing to DLQ is itself a terminal durability decision — the row
// will never be written by anyone — so the source must be told the LSN will
// never need replaying, exactly as if it had been durably written.
func (c *Consumer) routeToDLQWithAck(ctx context.Context, wmMsg *message.Message, msgs []protocol.Message) {
	if !c.publishRecordAck(ctx, msgs) {
		wmMsg.Nack()
		return
	}
	c.routeToDLQ(wmMsg)
}

func (c *Consumer) routeToDLQ(msg *message.Message) {
	dlqTopic := protocol.DLQTopic(c.pipelineID)
	if err := c.publisher.Publish(dlqTopic, msg); err != nil {
		log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("CRITICAL - Failed to route message to DLQ")
		msg.Nack()
		return
	}
	msg.Ack()
	c.retryMu.Lock()
	delete(c.retries, msg.UUID)
	c.retryMu.Unlock()
}

func (c *Consumer) cleanupOldRetries() {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	cutoff := time.Now().Add(-1 * time.Hour)
	for uuid, entry := range c.retries {
		if entry.lastRetry.Before(cutoff) {
			delete(c.retries, uuid)
		}
	}
}

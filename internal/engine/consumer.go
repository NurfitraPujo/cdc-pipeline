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
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const retryCleanupInterval = 5 * time.Minute

type Consumer struct {
	pipelineID       string
	sinkID           string
	subscriber       stream.Subscriber
	publisher        stream.Publisher // for DLQ
	sink             sink.Sink
	transformers     []transformer.Transformer
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

func NewConsumer(pipelineID, sinkID string, sub stream.Subscriber, pub stream.Publisher, snk sink.Sink, transformers []transformer.Transformer, kv nats.KeyValue, batchSize int, batchWait time.Duration, retry protocol.RetryConfig, preHook sink.PreTransformHook, postHook sink.PostTransformHook) *Consumer {
	names := make([]string, len(transformers))
	for i, t := range transformers {
		names[i] = t.Name()
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

func (c *Consumer) processMessages(ctx context.Context, msgs []protocol.Message) []protocol.Message {
	var correlationIDs []string
	if c.preTransformHook != nil {
		correlationIDs = c.preTransformHook(ctx, c.pipelineID, c.transformerNames, msgs)
	}

	if len(c.transformers) == 0 {
		if c.postTransformHook != nil {
			c.postTransformHook(ctx, c.pipelineID, correlationIDs, c.transformerNames, msgs, msgs, nil)
		}
		return msgs
	}

	processed := make([]protocol.Message, 0, len(msgs))
	filteredIndices := make([]int, 0)

	for i, m := range msgs {
		current := &m
		keep := true
		var err error

		for _, t := range c.transformers {
			current, keep, err = t.Transform(ctx, current)
			if err != nil {
				log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("transformer", t.Name()).Msg("Transformation error")
			}
			if !keep {
				break
			}
		}

		if !keep || current == nil {
			filteredIndices = append(filteredIndices, i)
		}
		if keep && current != nil {
			processed = append(processed, *current)
		}
	}

	if c.postTransformHook != nil {
		c.postTransformHook(ctx, c.pipelineID, correlationIDs, c.transformerNames, msgs, processed, filteredIndices)
	}

	return processed
}

func (c *Consumer) Run(ctx context.Context, topic string) error {
	msgChan, err := c.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS: %w", err)
	}

	var batch []protocol.Message
	var wmMsgs []*message.Message
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

			wmMsgs = append(wmMsgs, wmMsg)

			for i := range batchFromNats {
				m := &batchFromNats[i]
				if m.Op == "drain_marker" {
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
					if len(batch) > 0 {
						c.flush(ctx, batch, wmMsgs)
						batch = nil
						// wmMsg is still needed for the schema change itself if we want to ack it here
						wmMsgs = []*message.Message{wmMsg}
					}

					if m.Schema == nil && m.Diff != nil {
						log.Info().Str("pipeline_id", c.pipelineID).Str("table", m.Table).Interface("added_cols", m.Diff.Added).Msg("Constructing schema from diff")
						m.Schema = &protocol.SchemaMetadata{
							Table:   m.Table,
							Columns: m.Diff.Added,
						}
					}

					if m.Schema != nil {
						if err := c.sink.ApplySchema(ctx, *m); err != nil {
							log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Error applying schema change")
							c.updateTableError(m.SourceID, m.Table)
							wmMsg.Nack()
							return fmt.Errorf("failed to apply schema change: %w", err)
						}
					}

					// Emit Ack only if CorrelationID is present (indicates proactive evolution)
					if m.CorrelationID != "" {
						ack := protocol.Message{
							Op:            protocol.OpSchemaChangeAck,
							CorrelationID: m.CorrelationID,
							Table:         m.Table,
							SourceID:      m.SourceID,
							Timestamp:     time.Now(),
						}
						ackData, _ := ack.MarshalMsg(nil)
						ackTopic := protocol.AcksTopic(c.pipelineID)
						if err := c.publisher.Publish(ackTopic, message.NewMessage(m.UUID, ackData)); err != nil {
							log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Failed to publish schema change ack")
						}
					}

					if len(batchFromNats) == 1 {
						wmMsg.Ack()
						wmMsgs = nil
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
	toUpload := c.processMessages(ctx, batch)
	if len(toUpload) == 0 {
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

	for _, m := range wmMsgs {
		m.Ack()
		c.retryMu.Lock()
		delete(c.retries, m.UUID)
		c.retryMu.Unlock()
	}

	// NEW: Publish acks back to the producer topic for ALL uploaded messages
	// This ensures the source only advances its LSN when the data is in the sink.
	ackTopic := protocol.AcksTopic(c.pipelineID)
	for _, m := range toUpload {
		if m.Op == "drain_marker" || m.Op == protocol.OpSchemaChangeAck {
			continue
		}
		ack := protocol.Message{
			Op:       "ack",
			SourceID: m.SourceID,
			Table:    m.Table,
			LSN:      m.LSN,
		}
		ackData, _ := ack.MarshalMsg(nil)
		if err := c.publisher.Publish(ackTopic, message.NewMessage(m.UUID, ackData)); err != nil {
			log.Warn().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to publish record ack")
		}
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
			log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to unmarshal message for isolation, routing to DLQ")
			c.routeToDLQ(wmMsg)
			continue
		}

		toUpload := c.processMessages(ctx, msgs)
		if len(toUpload) == 0 {
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
				c.routeToDLQ(wmMsg)
			} else {
				wmMsg.Nack()
			}
		} else {
			wmMsg.Ack()
			c.retryMu.Lock()
			delete(c.retries, wmMsg.UUID)
			c.retryMu.Unlock()
		}
	}
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

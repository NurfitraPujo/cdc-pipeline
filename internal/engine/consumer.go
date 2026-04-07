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

type Consumer struct {
	pipelineID   string
	subscriber   stream.Subscriber
	publisher    stream.Publisher // for DLQ
	sink         sink.Sink
	transformers []transformer.Transformer
	kv           nats.KeyValue
	batchSize    int
	batchWait    time.Duration
	retryConfig  protocol.RetryConfig
	retries      map[string]retryEntry // UUID -> retry info with timestamp
	retryMu      sync.Mutex
	stats        map[string]*protocol.TableStats
	statsMu      sync.Mutex

	// Drain control
	mu         sync.RWMutex
	isDraining bool
	targetLSN  uint64
}

type retryEntry struct {
	count     int
	lastRetry time.Time
}

func NewConsumer(pipelineID string, sub stream.Subscriber, pub stream.Publisher, snk sink.Sink, transformers []transformer.Transformer, kv nats.KeyValue, batchSize int, batchWait time.Duration, retry protocol.RetryConfig) *Consumer {
	return &Consumer{
		pipelineID:   pipelineID,
		subscriber:   sub,
		publisher:    pub,
		sink:         snk,
		transformers: transformers,
		kv:           kv,
		batchSize:    batchSize,
		batchWait:    batchWait,
		retryConfig:  retry,
		retries:      make(map[string]retryEntry),
		stats:        make(map[string]*protocol.TableStats),
	}
}

func (c *Consumer) LoadStats(sourceID string, tables []string) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	for _, table := range tables {
		key := protocol.TableStatsKey(c.pipelineID, sourceID, table)
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
	if len(c.transformers) == 0 {
		return msgs
	}

	processed := make([]protocol.Message, 0, len(msgs))
	for _, m := range msgs {
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

		if keep && current != nil {
			processed = append(processed, *current)
		}
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
	defer timer.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Apply transformations
		toUpload := c.processMessages(ctx, batch)
		if len(toUpload) == 0 {
			// All messages were filtered out
			for _, m := range wmMsgs {
				m.Ack()
				c.retryMu.Lock()
				delete(c.retries, m.UUID)
				c.retryMu.Unlock()
			}
			// Cleanup old retry entries to prevent unbounded growth
			c.cleanupOldRetries()
			batch = nil
			wmMsgs = nil
			return nil
		}

		if err := c.sink.BatchUpload(ctx, toUpload); err != nil {
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
				statsData, err := s.MarshalMsg(nil)
				if err == nil {
					statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, m.Table)
					if _, err := c.kv.Put(statsKey, statsData); err != nil {
						log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Failed to update table stats")
					}
				}
			}
			c.statsMu.Unlock()

			// Check if any message in this batch should be isolated
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
				batch = nil
				wmMsgs = nil
				// Cleanup old retry entries
				c.cleanupOldRetries()
				return nil
			}

			// Nack all messages in the batch to trigger NATS JetStream redelivery.
			log.Error().Err(err).Str("pipeline_id", c.pipelineID).Int("batch_size", len(wmMsgs)).Msg("Sink upload failed, Nacking batch for JetStream redelivery")

			// Exponential backoff sleep
			backoff := c.retryConfig.InitialInterval
			maxAttempts := 0
			c.retryMu.Lock()
			for _, m := range wmMsgs {
				if c.retries[m.UUID].count > maxAttempts {
					maxAttempts = c.retries[m.UUID].count
				}
			}
			c.retryMu.Unlock()

			log.Info().Str("pipeline_id", c.pipelineID).Int("max_attempts", maxAttempts).Msg("Max attempts for batch so far")

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

			batch = nil
			wmMsgs = nil

			if backoff > 0 {
				time.Sleep(backoff)
			} else {
				time.Sleep(5 * time.Second)
			}
			return nil
		}

		for _, m := range wmMsgs {
			m.Ack()
			c.retryMu.Lock()
			delete(c.retries, m.UUID)
			c.retryMu.Unlock()
		}

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
				cpKey := protocol.EgressCheckpointKey(c.pipelineID, m.SourceID, m.Table)
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
			// #nosec G115 -- counts are within uint64 range
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
				statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, m.Table)
				if _, err := c.kv.Put(statsKey, statsData); err != nil {
					log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Error updating table stats")
				}
			}
		}

		batch = nil
		wmMsgs = nil
		// Stop current timer and create new one to avoid race conditions
		timer.Stop()
		timer = time.NewTimer(c.batchWait)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
			if c.checkDrained(0) {
				return nil
			}
		case wmMsg, ok := <-msgChan:
			if !ok {
				return nil
			}

			var batchFromNats []protocol.Message
			if _, err := protocol.UnmarshalMessageBatch(wmMsg.Payload, &batchFromNats); err != nil {
				log.Error().Err(err).Str("pipeline_id", c.pipelineID).Msg("Failed to unmarshal batch")
				wmMsg.Nack()
				continue
			}

			for _, m := range batchFromNats {
				if m.Op == "drain_marker" {
					if err := flush(); err != nil {
						return err
					}
					wmMsg.Ack()
					return nil
				}

				// If it's a schema change or delete, flush current batch FIRST
				if m.Op == "schema_change" || m.Op == "delete" {
					if err := flush(); err != nil {
						wmMsg.Nack()
						return err
					}
				}

				if m.Op == "schema_change" && m.Schema != nil {
					if err := c.sink.ApplySchema(ctx, *m.Schema); err != nil {
						log.Error().Err(err).Str("pipeline_id", c.pipelineID).Str("table", m.Table).Msg("Error applying schema change")
						wmMsg.Nack()
						return fmt.Errorf("failed to apply schema change: %w", err)
					}
					// Flush schema change immediately as its own "batch" if needed,
					// but here we just append and flush below.
				}

				batch = append(batch, m)
				if len(batch) >= c.batchSize || m.Op == "schema_change" || m.Op == "delete" {
					wmMsgs = append(wmMsgs, wmMsg)
					if err := flush(); err != nil {
						return err
					}
					if c.checkDrained(m.LSN) {
						return nil
					}
					wmMsgs = nil
					continue
				}
			}
			if wmMsg != nil {
				wmMsgs = append(wmMsgs, wmMsg)
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

		// Apply transformations
		toUpload := c.processMessages(ctx, msgs)
		if len(toUpload) == 0 {
			wmMsg.Ack()
			c.retryMu.Lock()
			delete(c.retries, wmMsg.UUID)
			c.retryMu.Unlock()
			continue
		}

		// Try uploading this single Watermill message (which might be a sub-batch)
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
				// Still within retry limits or DLQ disabled, Nack to try again later
				wmMsg.Nack()
			}
		} else {
			// Success in isolation!
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
		// If we can't even send to DLQ, we MUST Nack to avoid data loss,
		// even if it causes Head-of-Line blocking.
		msg.Nack()
		return
	}
	// Successfully routed to DLQ, so we can Ack the original
	msg.Ack()
	c.retryMu.Lock()
	delete(c.retries, msg.UUID)
	c.retryMu.Unlock()
}

// cleanupOldRetries removes retry entries older than 1 hour to prevent unbounded growth
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

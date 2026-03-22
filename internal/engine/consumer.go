package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/metrics"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

type Consumer struct {
	subscriber stream.Subscriber
	sink       sink.Sink
	kv         nats.KeyValue
	pipelineID string
	batchSize  int
	batchWait  time.Duration
	targetLSN  uint64
	mu         sync.RWMutex
	isDraining bool

	// Monitoring
	statsMu sync.Mutex
	stats   map[string]*protocol.TableStats
}

func NewConsumer(pipelineID string, sub stream.Subscriber, snk sink.Sink, kv nats.KeyValue, batchSize int, batchWait time.Duration) *Consumer {
	return &Consumer{
		pipelineID: pipelineID,
		subscriber: sub,
		sink:       snk,
		kv:         kv,
		batchSize:  batchSize,
		batchWait:  batchWait,
		stats:      make(map[string]*protocol.TableStats),
	}
}

func (c *Consumer) Run(ctx context.Context, topic string) error {
	msgChan, err := c.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS: %w", err)
	}

	var batch []protocol.Message
	var wmMsgs []*message.Message
	timer := time.NewTimer(c.batchWait)
	startTime := time.Now()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := c.sink.BatchUpload(ctx, batch); err != nil {
			// Update error counts for all tables in the failed batch
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
				
				// Prometheus
				metrics.SyncErrors.WithLabelValues(c.pipelineID, m.SourceID, m.Table).Inc()

				statsData, _ := s.MarshalMsg(nil)
				statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, m.Table)
				// #nosec G104 -- non-critical stats update
				c.kv.Put(statsKey, statsData)
			}
			c.statsMu.Unlock()

			for _, m := range wmMsgs {
				m.Nack()
			}
			return fmt.Errorf("failed to upload batch to sink: %w", err)
		}

		for _, m := range wmMsgs {
			m.Ack()
		}

		// Update checkpoints AND stats for all tables affected in this batch
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
			// 1. Update Checkpoint
			checkpoint := protocol.Checkpoint{
				EgressLSN: m.LSN,
				LastPK:    m.PK,
				Status:    "ACTIVE",
				UpdatedAt: now,
			}
			cpData, _ := checkpoint.MarshalMsg(nil)
			cpKey := protocol.EgressCheckpointKey(c.pipelineID, m.SourceID, m.Table)
			if _, err := c.kv.Put(cpKey, cpData); err != nil {
				return fmt.Errorf("failed to update egress checkpoint: %w", err)
			}

			// 2. Update Stats
			s, ok := c.stats[key]
			if !ok {
				s = &protocol.TableStats{Status: "ACTIVE"}
				c.stats[key] = s
			}
			s.Status = "ACTIVE"
			count := uint64(countsByTable[key])
			if count > 0 {
				s.TotalSynced += count
				// Prometheus
				metrics.RecordsSynced.WithLabelValues(c.pipelineID, m.SourceID, m.Table).Add(float64(count))
			}
			s.LastSourceTS = m.Timestamp
			s.LastProcessedTS = now
			s.LagMS = now.Sub(m.Timestamp).Milliseconds()
			s.UpdatedAt = now
			
			// Prometheus Lag
			metrics.PipelineLag.WithLabelValues(c.pipelineID, m.SourceID, m.Table).Set(float64(s.LagMS))

			// Simple RPS calculation since last start
			elapsed := now.Sub(startTime).Seconds()
			if elapsed > 0 {
				s.RPS = float64(s.TotalSynced) / elapsed
			}

			statsData, _ := s.MarshalMsg(nil)
			statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, m.Table)
			if _, err := c.kv.Put(statsKey, statsData); err != nil {
				return fmt.Errorf("failed to update table stats: %w", err)
			}
		}

		batch = nil
		wmMsgs = nil
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(c.batchWait)
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
				log.Printf("Error: Failed to unmarshal MessagePack batch payload: %v", err)
				wmMsg.Nack()
				continue
			}

			for _, m := range batchFromNats {
				batch = append(batch, m)
				if len(batch) >= c.batchSize {
					if err := flush(); err != nil {
						return err
					}
					if c.checkDrained(m.LSN) {
						wmMsg.Ack()
						return nil
					}
				}
			}
			wmMsgs = append(wmMsgs, wmMsg)
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
	if currentLSN >= c.targetLSN {
		return true
	}
	return false
}

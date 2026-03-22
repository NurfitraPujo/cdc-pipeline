package engine

import (
	"context"
	"encoding/json"
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
	pipelineID string
	subscriber stream.Subscriber
	sink       sink.Sink
	kv         nats.KeyValue
	batchSize  int
	batchWait  time.Duration
	stats      map[string]*protocol.TableStats
	statsMu    sync.Mutex

	// Drain control
	mu         sync.RWMutex
	isDraining bool
	targetLSN  uint64
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

func (c *Consumer) Run(ctx context.Context, topic string) error {
	msgChan, err := c.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS: %w", err)
	}

	var batch []protocol.Message
	var wmMsgs []*message.Message
	timer := time.NewTimer(c.batchWait)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := c.sink.BatchUpload(ctx, batch); err != nil {
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
				statsKey := protocol.TableStatsKey(c.pipelineID, m.SourceID, m.Table)
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
			cpData, _ := checkpoint.MarshalMsg(nil)
			cpKey := protocol.EgressCheckpointKey(c.pipelineID, m.SourceID, m.Table)
			if _, err := c.kv.Put(cpKey, cpData); err != nil {
				return fmt.Errorf("failed to update egress checkpoint: %w", err)
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
				log.Printf("Consumer [%s]: Failed to unmarshal batch: %v", c.pipelineID, err)
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

				if m.Op == "schema_change" && m.Schema != nil {
					if err := c.sink.ApplySchema(ctx, *m.Schema); err != nil {
						log.Printf("Error applying schema change for %s: %v", m.Table, err)
						wmMsg.Nack()
						return fmt.Errorf("failed to apply schema change: %w", err)
					}
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

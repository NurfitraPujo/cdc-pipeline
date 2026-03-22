package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type Pipeline struct {
	id         string
	producer   *Producer
	consumer   *Consumer
	config     protocol.PipelineConfig
	srcConfigs map[string]protocol.SourceConfig
	
	finished   chan struct{}
	drainOnce  sync.Once
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewPipeline(id string, p *Producer, c *Consumer, cfg protocol.PipelineConfig) *Pipeline {
	return &Pipeline{
		id:       id,
		producer: p,
		consumer: c,
		config:   cfg,
		finished: make(chan struct{}),
	}
}

func (p *Pipeline) ID() string {
	return p.id
}

func (p *Pipeline) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)
	
	p.wg.Add(2)
	
	// Start Producer
	go func() {
		defer p.wg.Done()
		
		if len(p.config.Sources) == 0 {
			log.Printf("No sources defined for pipeline %s", p.id)
			return
		}

		sourceID := p.config.Sources[0]
		
		// 1. Get Source Config
		srcKey := protocol.SourceConfigKey(sourceID)
		entry, err := p.producer.kv.Get(srcKey)
		if err != nil {
			log.Printf("Failed to fetch source config %s: %v", sourceID, err)
			return
		}
		var srcCfg protocol.SourceConfig
		if err := json.Unmarshal(entry.Value(), &srcCfg); err != nil {
			log.Printf("Failed to unmarshal source config %s: %v", sourceID, err)
			return
		}

		// 2. Get Checkpoints for all tables
		checkpoints := make(map[string]protocol.Checkpoint)
		for _, table := range p.config.Tables {
			cpKey := protocol.IngressCheckpointKey(p.id, sourceID, table)
			cpEntry, err := p.producer.kv.Get(cpKey)
			if err == nil {
				var cp protocol.Checkpoint
				if err := json.Unmarshal(cpEntry.Value(), &cp); err == nil {
					checkpoints[table] = cp
				}
			}
		}

		// Update Producer to handle multi-table checkpoints if needed
		// For now, we pass the first found LSN or 0 to the source start
		// In a real multi-table CDC, go-pq-cdc uses one slot for all tables,
		// so the lowest LSN among all tables is the safe starting point.
		var minLSN uint64
		var first = true
		for _, cp := range checkpoints {
			if first || cp.IngressLSN < minLSN {
				minLSN = cp.IngressLSN
				first = false
			}
		}
		
		initialCP := protocol.Checkpoint{IngressLSN: minLSN}

		// 3. Load Egress Stats for the consumer
		p.consumer.LoadStats(sourceID, p.config.Tables)

		lsn, err := p.producer.Run(p.ctx, srcCfg, initialCP)
		if err != nil && err != context.Canceled {
			log.Printf("Producer for pipeline %s failed: %v", p.id, err)
		}
		
		// Phase 1 Handover: notify consumer of target LSN
		p.consumer.Drain(lsn)
	}()

	// Start Consumer
	go func() {
		defer p.wg.Done()
		topic := fmt.Sprintf("daya.pipeline.%s.ingest", p.id)
		err := p.consumer.Run(p.ctx, topic)
		if err != nil && err != context.Canceled {
			log.Printf("Consumer for pipeline %s failed: %v", p.id, err)
		}
		
		// If consumer stops, the whole pipeline is considered finished for this config generation
		close(p.finished)
	}()

	return nil
}

func (p *Pipeline) Drain() error {
	p.drainOnce.Do(func() {
		log.Printf("Draining pipeline %s", p.id)
		p.producer.Drain()
	})
	return nil
}

func (p *Pipeline) Finished() <-chan struct{} {
	return p.finished
}

func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.cancel()
	
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

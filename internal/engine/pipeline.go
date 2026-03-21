package engine

import (
	"context"
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
	checkpoints map[string]protocol.Checkpoint
	
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
	
	var lastLSN uint64
	var prodErr error
	
	// Start Producer
	go func() {
		defer p.wg.Done()
		// For simplicity, assuming single source for now as per skeleton
		// In real world, would iterate p.config.Sources
		lsn, err := p.producer.Run(p.ctx, protocol.SourceConfig{}, protocol.Checkpoint{})
		lastLSN = lsn
		prodErr = err
		if err != nil && err != context.Canceled {
			log.Printf("Producer for pipeline %s failed: %v", p.id, err)
		}
		
		// When producer stops (either normally or via Drain), 
		// if we are draining, notify consumer of the target LSN
		p.consumer.Drain(lastLSN)
	}()

	// Start Consumer
	go func() {
		defer p.wg.Done()
		topic := fmt.Sprintf("daya.pipeline.%s.>", p.id) // Listen to all sources/tables for this pipeline
		err := p.consumer.Run(p.ctx, topic)
		if err != nil && err != context.Canceled {
			log.Printf("Consumer for pipeline %s failed: %v", p.id, err)
		}
		
		// If consumer stops, the whole pipeline is considered finished for this config generation
		close(p.finished)
	}()

	_ = prodErr // Use in actual implementation
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

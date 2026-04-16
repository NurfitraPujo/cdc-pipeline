package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/rs/zerolog/log"
)

type Pipeline struct {
	id                string
	producer          *Producer
	consumers         []*Consumer
	config            protocol.PipelineConfig
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	finished          chan struct{}
	dynamicTablesChan chan []string
}

func NewPipeline(id string, prod *Producer, consumers []*Consumer, cfg protocol.PipelineConfig) *Pipeline {
	return &Pipeline{
		id:                id,
		producer:          prod,
		consumers:         consumers,
		config:            cfg,
		finished:          make(chan struct{}),
		dynamicTablesChan: make(chan []string),
	}
}

func (p *Pipeline) ID() string {
	return p.id
}

func (p *Pipeline) Start(ctx context.Context) error {
	// Link the pipeline lifecycle to the provided context
	p.ctx, p.cancel = context.WithCancel(ctx)

	log.Info().Str("pipeline_id", p.id).Int("num_consumers", len(p.consumers)).Msg("Starting pipeline")

	// Start all consumers
	for _, cons := range p.consumers {
		p.wg.Add(1)
		go func(c *Consumer) {
			defer p.wg.Done()
			topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.id)
			if err := c.Run(p.ctx, topic); err != nil && err != context.Canceled {
				log.Error().Err(err).Str("pipeline_id", p.id).Str("sink_id", c.sinkID).Msg("Consumer failed")
			}
		}(cons)
	}

	// Start producer goroutine
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		// 1. Resolve Sources
		if len(p.config.Sources) == 0 {
			log.Warn().Str("pipeline_id", p.id).Msg("No sources defined")
			return
		}
		sourceID := p.config.Sources[0]
		srcKey := protocol.SourceConfigKey(sourceID)
		entry, err := p.producer.kv.Get(srcKey)
		if err != nil {
			log.Error().Err(err).Str("pipeline_id", p.id).Str("source_id", sourceID).Msg("Failed to get source config")
			return
		}

		var srcCfg protocol.SourceConfig
		if err := json.Unmarshal(entry.Value(), &srcCfg); err != nil {
			log.Error().Err(err).Str("pipeline_id", p.id).Str("source_id", sourceID).Msg("Failed to unmarshal source config")
			return
		}

		// Apply pipeline overrides
		if p.config.BatchSize > 0 {
			srcCfg.BatchSize = p.config.BatchSize
		}
		if p.config.BatchWait > 0 {
			srcCfg.BatchWait = p.config.BatchWait
		}
		srcCfg.Tables = p.config.Tables
		// Ensure unique slot for every worker instance to avoid contention on reload
		// Use pipeline ID suffix for stable slot naming across restarts (preserves LSN continuity)
		if srcCfg.Type == "postgres" && srcCfg.SlotName != "" {
			srcCfg.SlotName = fmt.Sprintf("%s_%s", srcCfg.SlotName, p.id)
		}

		// 2. Get Checkpoints for all tables
		minLSN := uint64(0)
		for _, table := range p.config.Tables {
			cpKey := protocol.IngressCheckpointKey(p.id, sourceID, table)
			cpEntry, err := p.producer.kv.Get(cpKey)
			if err == nil {
				var cp protocol.Checkpoint
				if err := json.Unmarshal(cpEntry.Value(), &cp); err == nil {
					if minLSN == 0 || cp.IngressLSN < minLSN {
						minLSN = cp.IngressLSN
					}
				}
			}
		}

		initialCP := protocol.Checkpoint{IngressLSN: minLSN}

		// 3. Load Egress Stats for all consumers
		for _, cons := range p.consumers {
			cons.LoadStats(sourceID, p.config.Tables)
		}

		// 4. Setup dynamic table handling
		p.producer.SetDynamicTablesChan(p.dynamicTablesChan)

		lsn, err := p.producer.Run(p.ctx, srcCfg, initialCP)
		if err != nil && err != context.Canceled {
			log.Error().Err(err).Str("pipeline_id", p.id).Msg("Producer failed. Shutting down pipeline (fail-fast).")
			p.cancel() // Stop all consumers
			return
		}

		// In a drain scenario, the producer finishes normally.
		// We should tell all consumers to drain until this LSN.
		log.Info().Str("pipeline_id", p.id).Uint64("lsn", lsn).Msg("Producer finished. Signaling all consumers to drain.")
		for _, cons := range p.consumers {
			cons.Drain(lsn)
		}
	}()

	// Background waiter to close finished channel
	go func() {
		p.wg.Wait()
		close(p.finished)
	}()

	return nil
}

func (p *Pipeline) Drain() error {
	log.Info().Str("pipeline_id", p.id).Msg("Draining pipeline")
	return p.producer.Drain()
}

func (p *Pipeline) Finished() <-chan struct{} {
	return p.finished
}

func (p *Pipeline) Shutdown(ctx context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.finished:
		return nil
	}
}

func (p *Pipeline) SignalDynamicTables(tables []string) {
	select {
	case p.dynamicTablesChan <- tables:
		log.Info().Str("pipeline_id", p.id).Int("num_tables", len(tables)).Msg("Dynamic tables signal received")
	case <-p.ctx.Done():
		log.Warn().Str("pipeline_id", p.id).Msg("Pipeline context cancelled, cannot signal dynamic tables")
	}
}

func (p *Pipeline) DynamicTablesChan() <-chan []string {
	return p.dynamicTablesChan
}

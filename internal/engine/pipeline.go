package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/rs/zerolog/log"
)

type Pipeline struct {
	id         string
	producer   *Producer
	consumer   *Consumer
	config     protocol.PipelineConfig
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	finished   chan struct{}
}

func NewPipeline(id string, prod *Producer, cons *Consumer, cfg protocol.PipelineConfig) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		id:       id,
		producer: prod,
		consumer: cons,
		config:   cfg,
		ctx:      ctx,
		cancel:   cancel,
		finished: make(chan struct{}),
	}
}

func (p *Pipeline) ID() string {
	return p.id
}

func (p *Pipeline) Start(ctx context.Context) error {
	log.Info().Str("pipeline_id", p.id).Msg("Starting pipeline")

	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		topic := fmt.Sprintf("cdc_pipeline_%s_ingest", p.id)
		if err := p.consumer.Run(p.ctx, topic); err != nil && err != context.Canceled {
			log.Error().Err(err).Str("pipeline_id", p.id).Msg("Consumer failed")
		}
	}()

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
		if srcCfg.Type == "postgres" && srcCfg.SlotName != "" {
			srcCfg.SlotName = fmt.Sprintf("%s_%d", srcCfg.SlotName, time.Now().UnixNano())
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

		// 3. Load Egress Stats for the consumer
		p.consumer.LoadStats(sourceID, p.config.Tables)

		lsn, err := p.producer.Run(p.ctx, srcCfg, initialCP)
		if err != nil && err != context.Canceled {
			log.Error().Err(err).Str("pipeline_id", p.id).Msg("Producer failed")
		}
		
		// In a drain scenario, the producer finishes.
		// We should tell the consumer to drain until this LSN.
		log.Info().Str("pipeline_id", p.id).Uint64("lsn", lsn).Msg("Producer finished. Signaling consumer drain.")
		p.consumer.Drain(lsn)
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
	p.cancel()
	
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.finished:
		return nil
	}
}

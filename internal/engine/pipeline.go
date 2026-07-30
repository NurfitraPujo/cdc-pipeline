package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
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

	// auxWg tracks goroutines that are lifecycle-adjacent but must NOT gate
	// Finished()/p.wg.Wait() — e.g. the dynamic-tables handler goroutine.
	// Finished() has to mean "producer + consumers done" so the graceful
	// Drain() path (which never calls p.cancel()) can complete promptly;
	// auxWg is only waited on in Shutdown, after p.cancel() has fired.
	auxWg sync.WaitGroup
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

		if err := p.runProducer(); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error().Err(err).Str("pipeline_id", p.id).Msg("Producer failed. Cancelling pipeline so consumers exit and the supervisor can restart it.")
			}
			// Any error return (including config-load failures) must cancel the
			// pipeline: otherwise consumers keep running on p.ctx forever, wg.Wait()
			// never returns, finished never closes, and the supervisor heartbeats
			// "Running" for a pipeline that has stopped ingesting (Critical 13).
			p.cancel()
		}
	}()

	// Background waiter to close finished channel
	go func() {
		p.wg.Wait()
		close(p.finished)
	}()

	return nil
}

// runProducer resolves the source config, wires up the producer, and runs it
// to completion. It returns a non-nil error for every failure path — including
// the config-load failures (KV get, unmarshal, decrypt) that used to return
// silently — so the caller (the goroutine started in Start) knows to cancel
// the pipeline. The one path that must NOT trigger a cancel-before-drain is
// the normal completion path below: the producer drained cleanly, and
// consumers must be signalled to drain via cons.Drain(lsn) before anything
// tears down p.ctx.
func (p *Pipeline) runProducer() error {
	// 1. Resolve Sources
	if len(p.config.Sources) == 0 {
		log.Warn().Str("pipeline_id", p.id).Msg("No sources defined")
		return fmt.Errorf("pipeline %s: no sources defined", p.id)
	}
	sourceID := p.config.Sources[0]
	srcKey := protocol.SourceConfigKey(sourceID)
	entry, err := p.producer.kv.Get(srcKey)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", p.id).Str("source_id", sourceID).Msg("Failed to get source config")
		return fmt.Errorf("getting source config for %s: %w", sourceID, err)
	}

	var srcCfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &srcCfg); err != nil {
		log.Error().Err(err).Str("pipeline_id", p.id).Str("source_id", sourceID).Msg("Failed to unmarshal source config")
		return fmt.Errorf("unmarshalling source config for %s: %w", sourceID, err)
	}
	if err := srcCfg.Decrypt(); err != nil {
		log.Error().Err(err).Str("pipeline_id", p.id).Str("source_id", sourceID).Msg("Failed to decrypt source config")
		return fmt.Errorf("decrypting source config for %s: %w", sourceID, err)
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
		srcCfg.SlotName = fmt.Sprintf("%s_%s", srcCfg.SlotName, strings.ReplaceAll(p.id, "-", "_"))
	}

	// 2. Get Checkpoints for all tables (use EgressLSN for resume safety)
	minLSN := uint64(0)
	for _, table := range p.config.Tables {
		// Pull from egress checkpoints for all configured sinks
		for _, sinkID := range p.config.Sinks {
			cpKey := protocol.EgressCheckpointKey(p.id, sourceID, sinkID, table)
			cpEntry, err := p.producer.kv.Get(cpKey)
			if err == nil {
				var cp protocol.Checkpoint
				if _, err := cp.UnmarshalMsg(cpEntry.Value()); err == nil {
					if cp.EgressLSN > 0 && (minLSN == 0 || cp.EgressLSN < minLSN) {
						minLSN = cp.EgressLSN
					}
				}
			}
		}
	}

	initialCP := protocol.Checkpoint{IngressLSN: minLSN}

	// 3. Load Egress Stats for all consumers
	for _, cons := range p.consumers {
		cons.LoadStats(sourceID, p.config.Tables)
	}

	// 4. Setup dynamic table handling. Bound to p.ctx so it exits instead of
	// leaking, but tracked on p.auxWg (NOT p.wg): dynamicTablesChan is never
	// closed, so this goroutine's only exit is ctx cancellation, which does
	// NOT happen on the graceful Drain() path. Putting it on p.wg would make
	// Finished() hang for the full drain timeout on every normal drain/stop.
	// See SetDynamicTablesChan and Pipeline.Shutdown.
	p.producer.SetDynamicTablesChan(p.ctx, &p.auxWg, p.dynamicTablesChan)

	lsn, err := p.producer.Run(p.ctx, srcCfg, initialCP)
	if err != nil && !errors.Is(err, context.Canceled) {
		if errors.Is(err, errPublishRetriesExhausted) && p.ctx.Err() == nil {
			log.Warn().Err(err).Str("pipeline_id", p.id).Msg("Producer exhausted publisher retries; attempting one recovery run")
			lsn, err = p.recoverProducer(srcCfg, initialCP)
		}

		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Str("pipeline_id", p.id).Msg("Producer failed after recovery policy. Shutting down pipeline.")
			return fmt.Errorf("running producer: %w", err)
		}
	}

	// In a drain scenario, the producer finishes normally.
	// We should tell all consumers to drain until this LSN.
	// IMPORTANT: this is the normal/graceful completion path. Consumers must be
	// signalled to drain via cons.Drain(lsn) BEFORE we return (and before any
	// p.cancel() happens in the caller) — draining, not cancellation, is what
	// stops them here. Returning nil below means the caller does NOT cancel.
	log.Info().Str("pipeline_id", p.id).Uint64("lsn", lsn).Msg("Producer finished. Signaling all consumers to drain.")
	for _, cons := range p.consumers {
		cons.Drain(lsn)
	}

	return nil
}

func (p *Pipeline) recoverProducer(srcCfg protocol.SourceConfig, checkpoint protocol.Checkpoint) (uint64, error) {
	lsn, err := p.producer.Run(p.ctx, srcCfg, checkpoint)
	if err != nil {
		return lsn, fmt.Errorf("recovering producer after publisher retry exhaustion: %w", err)
	}
	return lsn, nil
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
		// p.cancel() above (or an earlier producer error) is what unblocks the
		// auxiliary goroutines tracked on p.auxWg (e.g. the dynamic-tables
		// handler) — wait for them here rather than on p.wg, so Finished()
		// itself stays fast on the graceful-drain path.
		auxDone := make(chan struct{})
		go func() {
			p.auxWg.Wait()
			close(auxDone)
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-auxDone:
		}

		// Close all transformers after pipeline goroutines have finished
		p.closeTransformers()
		return nil
	}
}

func (p *Pipeline) closeTransformers() {
	for _, cons := range p.consumers {
		for _, ct := range cons.transformers {
			if closeable, ok := ct.Transformer.(transformer.CloseableTransformer); ok {
				if err := closeable.Close(); err != nil {
					log.Warn().Err(err).Str("pipeline_id", p.id).Str("transformer", ct.Transformer.Name()).Msg("Failed to close transformer")
				}
			}
		}
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

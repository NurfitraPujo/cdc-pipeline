package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	pqcdc "github.com/Trendyol/go-pq-cdc/pq"
	_ "github.com/jackc/pgx/v5/stdlib"
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

	// slotConfirmedFlushLSN resolves the source's replication slot's
	// current confirmed_flush_lsn for the WI-7 §3 operator warning
	// (warnIfSlotAheadOfMinLSN). Defaults to queryPostgresConfirmedFlushLSN
	// (a real, short-lived DB connection); tests override it to avoid a
	// live PostgreSQL dependency. The bool return is false whenever the
	// value could not be determined (any error), in which case the
	// warning check is skipped -- this is pure observability and must
	// never fail pipeline startup.
	slotConfirmedFlushLSN func(srcCfg protocol.SourceConfig) (uint64, bool)
}

func NewPipeline(id string, prod *Producer, consumers []*Consumer, cfg protocol.PipelineConfig) *Pipeline {
	return &Pipeline{
		id:                    id,
		producer:              prod,
		consumers:             consumers,
		config:                cfg,
		finished:              make(chan struct{}),
		dynamicTablesChan:     make(chan []string),
		slotConfirmedFlushLSN: queryPostgresConfirmedFlushLSN,
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
	//
	// minLSN is the MIN over every (table, sink) egress checkpoint. This is
	// deliberately a floor keyed on the LEAST-active table, so a table with
	// low/no recent write traffic (which keeps an old egress checkpoint
	// indefinitely) does not get skipped ahead of by Hydrate. It is only
	// ever used for Hydrate/initialCP -- never compared against the slot
	// position (see sinkFrontierLSN below for that).
	minLSN := uint64(0)
	// sinkFrontier[sinkID] is the MAX EgressLSN across that sink's tables
	// -- i.e. how far that sink has actually gotten, not the pipeline's
	// stalest table. sinkHasCheckpoint tracks whether a sink produced ANY
	// checkpoint at all (a newly-added sink has none), so the frontier
	// comparison below can be skipped rather than silently treating an
	// unknown sink as caught up.
	sinkFrontier := make(map[string]uint64, len(p.config.Sinks))
	sinkHasCheckpoint := make(map[string]bool, len(p.config.Sinks))
	for _, cfgEntry := range p.config.Tables {
		// Normalise the config-shaped entry the same way the hot path's
		// msgTableRef does, so this reads the same checkpoint key the
		// consumer wrote (MULTI_SCHEMA_PLAN.md §3 Stage 1).
		ref := tableRefFromConfigEntry(cfgEntry)
		// Pull from egress checkpoints for all configured sinks
		for _, sinkID := range p.config.Sinks {
			cpKey := protocol.EgressCheckpointKey(p.id, sourceID, sinkID, ref)
			cpEntry, err := p.producer.kv.Get(cpKey)
			if err == nil {
				var cp protocol.Checkpoint
				if _, err := cp.UnmarshalMsg(cpEntry.Value()); err == nil {
					if cp.EgressLSN > 0 && (minLSN == 0 || cp.EgressLSN < minLSN) {
						minLSN = cp.EgressLSN
					}
					sinkHasCheckpoint[sinkID] = true
					if cp.EgressLSN > sinkFrontier[sinkID] {
						sinkFrontier[sinkID] = cp.EgressLSN
					}
				}
			}
		}
	}

	// WI-7: minLSN (above) is observability/Hydrate input ONLY -- it must
	// never feed StartLSN (the replication slot's own confirmed_flush_lsn
	// is now the sole resume authority, see source.go Start).
	//
	// The operator warning below is a SEPARATE, stricter comparison: the
	// slot vs. the durable FRONTIER (min over sinks of that sink's own
	// max EgressLSN), not vs. minLSN. minLSN tracks the least-active
	// table and is routinely far behind the slot on any pipeline with
	// tables at unequal write rates -- comparing the slot against it
	// would fire on essentially every restart and print a false "data
	// loss" message. The frontier is the actual per-sink durable
	// position; under the WI-4/WI-5 invariant the slot must never be
	// ahead of the frontier, so seeing it ahead there genuinely means the
	// invariant was violated upstream (e.g. a pre-upgrade slot that older
	// code over-advanced) and rows between the frontier and the slot
	// position cannot be replayed on a future failover.
	//
	// If any configured sink has no checkpoint at all yet, its frontier
	// is unknown (not zero), so the whole check is skipped rather than
	// judged against an assumed-zero frontier.
	if len(p.config.Sinks) > 0 {
		frontierKnown := true
		frontierLSN := uint64(0)
		first := true
		for _, sinkID := range p.config.Sinks {
			if !sinkHasCheckpoint[sinkID] {
				frontierKnown = false
				break
			}
			f := sinkFrontier[sinkID]
			if first || f < frontierLSN {
				frontierLSN = f
				first = false
			}
		}
		if frontierKnown {
			p.warnIfSlotAheadOfSinkFrontier(srcCfg, frontierLSN)
		} else {
			log.Debug().Str("pipeline_id", p.id).Msg("sink durable frontier unknown (at least one configured sink has no egress checkpoint yet); skipping slot-vs-frontier check")
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

// warnIfSlotAheadOfSinkFrontier is a best-effort, non-fatal check: it
// resolves the replication slot's confirmed_flush_lsn via
// p.slotConfirmedFlushLSN and logs a warning if it is meaningfully ahead
// of frontierLSN (the caller-computed min-over-sinks of each sink's own
// max EgressLSN -- i.e. the durable frontier, NOT the min-over-tables
// minLSN used for Hydrate). Under the WI-4/WI-5 invariant the slot must
// never advance past an LSN a sink has not durably written, so the slot
// outrunning the frontier means that invariant was violated upstream of
// this code (e.g. by a pre-upgrade build that over-advanced the slot) --
// see plan §6's rollback note. Comparing against the frontier (rather
// than minLSN, which tracks the least-active table and lags the slot on
// every pipeline with unevenly-written tables) is what keeps this from
// firing on the normal steady state. This is pure observability: it must
// never block or fail pipeline startup.
func (p *Pipeline) warnIfSlotAheadOfSinkFrontier(srcCfg protocol.SourceConfig, frontierLSN uint64) {
	if srcCfg.Type != "postgres" || srcCfg.SlotName == "" || p.slotConfirmedFlushLSN == nil {
		return
	}

	slotLSN, ok := p.slotConfirmedFlushLSN(srcCfg)
	if !ok {
		return
	}

	if frontierLSN > 0 && slotLSN > frontierLSN {
		log.Warn().
			Str("pipeline_id", p.id).
			Str("slot", srcCfg.SlotName).
			Uint64("slot_confirmed_flush_lsn", slotLSN).
			Uint64("sink_frontier_lsn", frontierLSN).
			Msg("replication slot confirmed_flush_lsn is ahead of the durable sink frontier; the at-least-once invariant was violated upstream and rows between the sink frontier and the slot position cannot be replayed on failover")
	}
}

// queryPostgresConfirmedFlushLSN is the real (non-test) implementation of
// Pipeline.slotConfirmedFlushLSN: it opens a short-lived connection to the
// source database and reads the replication slot's confirmed_flush_lsn.
// Any error (connection, query, parse) reports ok=false; callers treat
// that as "unknown, skip the check" since this is observability-only.
func queryPostgresConfirmedFlushLSN(srcCfg protocol.SourceConfig) (uint64, bool) {
	u := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", srcCfg.Host, srcCfg.Port),
		User:   url.UserPassword(srcCfg.User, srcCfg.PassEncrypted),
		Path:   srcCfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()

	db, err := sql.Open("pgx", u.String())
	if err != nil {
		log.Debug().Err(err).Msg("queryPostgresConfirmedFlushLSN: failed to open source connection")
		return 0, false
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var confirmedFlushLSN string
	row := db.QueryRowContext(ctx, `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1`, srcCfg.SlotName)
	if err := row.Scan(&confirmedFlushLSN); err != nil {
		log.Debug().Err(err).Str("slot", srcCfg.SlotName).Msg("queryPostgresConfirmedFlushLSN: failed to read confirmed_flush_lsn")
		return 0, false
	}

	lsn, err := pqcdc.ParseLSN(confirmedFlushLSN)
	if err != nil {
		log.Debug().Err(err).Str("confirmed_flush_lsn", confirmedFlushLSN).Msg("queryPostgresConfirmedFlushLSN: failed to parse confirmed_flush_lsn")
		return 0, false
	}

	return uint64(lsn), true
}

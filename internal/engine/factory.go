package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/sink"
	_ "github.com/NurfitraPujo/cdc-pipeline/internal/sink/databend"
	_ "github.com/NurfitraPujo/cdc-pipeline/internal/sink/postgresdebug"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source/postgres"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	transformernats "github.com/NurfitraPujo/cdc-pipeline/internal/transformer/nats"
	go_nats "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// defaultRetryConfig is the RetryConfig CreateWorker falls back to when a
// pipeline config carries no explicit Retry block. See the WS-5 comment at
// its call site for why the Duration fields matter.
func defaultRetryConfig() protocol.RetryConfig {
	return protocol.RetryConfig{
		MaxRetries:      3,
		InitialInterval: time.Second,
		MaxInterval:     30 * time.Second,
	}
}

// sinkSubscriberLatencyMargin (WS-5 item 6 / followups.md item 3) is added
// on top of the derived worst-case transform time in deriveAckWait, to
// absorb the sink's own write latency after a transformer (if any) has
// already returned -- deriveAckWait's batchSize*timeout term only bounds
// time spent inside a chunked transformer round trip.
const sinkSubscriberLatencyMargin = 10 * time.Second

// defaultAckWait is both the historical flat AckWait this replaces and the
// floor deriveAckWait never goes below -- a pipeline with no nats/protobuf
// processor (nothing to derive a worst case from) keeps today's behaviour
// exactly.
const defaultAckWait = 30 * time.Second

// defaultMaxAckWait is the default ceiling on deriveAckWait's output when
// PipelineFactory.AckWaitCeiling is unset (see that field's doc comment),
// and the value used throughout this file's comments as "the ceiling"
// unless a specific override is under discussion. Ratified value: 10
// minutes -- see docs/decisions/0022-ackwait-ceiling-ten-minutes.md for the
// full trade-off (why not the original unbounded derivation, why not
// something shorter).
//
// Summary of that ADR's reasoning: duplicate delivery is safe by design in
// this system (0008-at-least-once-with-sink-side-idempotency.md --
// REPLACE INTO ... ON (pk) rewrites the same row on a replayed LSN, and
// AckManager.Confirm at or below the watermark is a no-op), so a long
// AckWait buys no *correctness*, only avoided redundant work -- and that
// is worth trading away past a certain point, because MaxAckPending =
// BatchSize*2 means the entire pipeline queues behind one unacked batch
// for the full AckWait if a consumer is hard-crashed, OOM-killed, or
// network-partitioned (with no circuit breaker yet -- a separate, still-
// open WS-5 gap -- that is the realistic failure this must be sized
// against, not the pathological one-oversized-record-per-chunk case that
// motivated the original unbounded derivation). Going shorter is also
// wrong: during a degradation where the dependency is slow but not down, a
// premature redelivery's own attempt also times out, which is a genuine
// failure that counts toward MaxRetries/DLQ (consumer.go's
// handleSinkError) -- amplifying failure at exactly the moment retries
// should be patient, not aggressive.
const defaultMaxAckWait = 10 * time.Minute

// deriveAckWait computes the per-sink ingest subscriber's NATS AckWait
// (WS-5 item 6, docs/todos/custom_object_cdc_followups.md item 3). Before
// this, every subscriber used a flat 30s AckWait regardless of BatchSize or
// WS-3 chunking: a batch chunked into several serial nats/protobuf requests
// (or one held by a future circuit breaker) could still be "being worked"
// past 30s, and JetStream would redeliver it to another consumer while the
// first was still in flight -- duplicate in-flight work at exactly the
// moment the dependency is already struggling.
//
// The worst case this derives from: WS-3's chunker can, in the pathological
// case of one oversized record per chunk, split a BatchSize batch into up
// to BatchSize serial requests against the nats/protobuf transformer, each
// bounded by that transformer's own per-request timeout (its configured
// timeout_ms, or the WS-5 item 4 default computed by
// transformernats.DefaultTimeoutMs if unset). BatchSize * that timeout
// bounds the whole batch's transform time; sinkSubscriberLatencyMargin
// absorbs the sink's own write after the transformer returns. That product
// is then clamped to [defaultAckWait, ceiling] -- ceiling defaults to
// defaultMaxAckWait but is caller-configurable (PipelineFactory.AckWaitCeiling)
// specifically because the realistic worst case (a handful of chunks, not
// BatchSize of them -- see defaultMaxAckWait's comment) is much smaller
// than the pathological one this derivation is mathematically bounded
// against, and an operator may need to tune the trade-off for their own
// workload without a code change.
//
// A pipeline with no nats/protobuf processor has nothing to derive a
// worst case from and keeps defaultAckWait, matching pre-fix behaviour.
func deriveAckWait(cfg protocol.PipelineConfig, ceiling time.Duration) time.Duration {
	if ceiling <= 0 {
		ceiling = defaultMaxAckWait
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	maxTimeoutMs := 0
	for _, p := range cfg.Processors {
		if p.Type != "nats/protobuf" {
			continue
		}
		timeoutMs := transformernats.DefaultTimeoutMs(batchSize)
		if raw, ok := p.Options["timeout_ms"]; ok {
			if f, ok := raw.(float64); ok && f > 0 {
				timeoutMs = int(f)
			}
		}
		if timeoutMs > maxTimeoutMs {
			maxTimeoutMs = timeoutMs
		}
	}

	if maxTimeoutMs == 0 {
		return defaultAckWait
	}

	worst := time.Duration(batchSize)*time.Duration(maxTimeoutMs)*time.Millisecond + sinkSubscriberLatencyMargin
	if worst < defaultAckWait {
		return defaultAckWait
	}
	if worst > ceiling {
		return ceiling
	}
	return worst
}

// PipelineFactory creates PipelineWorker instances.
type PipelineFactory struct {
	KV          go_nats.KeyValue
	Publisher   stream.Publisher
	NatsURL     string
	WorkerGroup string

	// AckWaitCeiling overrides deriveAckWait's ceiling (defaultMaxAckWait,
	// 10 minutes) for every pipeline this factory creates. Zero means "use
	// the default" -- see docs/decisions/0022-ackwait-ceiling-ten-minutes.md
	// for why 10 minutes and not something else; this field exists so an
	// operator who needs a different trade-off for their own workload can
	// tune it without a code change.
	AckWaitCeiling time.Duration
}

// CreateWorker builds a full Pipeline from configuration.
func (f *PipelineFactory) CreateWorker(workerCtx context.Context, id string, cfg protocol.PipelineConfig) (PipelineWorker, error) {
	log.Info().Str("pipeline_id", id).Int("num_sinks", len(cfg.Sinks)).Msg("Creating worker for pipeline")

	var success bool
	var subscribers []*nats.NatsSubscriber
	var snks []sink.Sink
	var allTransformers []ConfiguredTransformer

	// Cleanup on failure
	defer func() {
		if !success {
			for _, sub := range subscribers {
				sub.Close()
			}
			for _, snk := range snks {
				snk.Stop()
			}
			// Close any transformers already constructed before the failure.
			// NatsProtoTransformer (internal/transformer/nats/protobuf.go) opens a
			// live nats.Connect in its constructor, so with >=2 sinks/processors a
			// failure on a later processor previously leaked every earlier
			// transformer's NATS connection, repeating on each retry cycle.
			for _, ct := range allTransformers {
				if closeable, ok := ct.Transformer.(transformer.CloseableTransformer); ok {
					if err := closeable.Close(); err != nil {
						log.Warn().Err(err).Str("pipeline_id", id).Str("transformer", ct.Transformer.Name()).Msg("Failed to close transformer during factory cleanup")
					}
				}
			}
		}
	}()

	if len(cfg.Sources) == 0 {
		return nil, fmt.Errorf("no sources defined for pipeline %s", id)
	}
	sourceID := cfg.Sources[0]
	sourceKey := protocol.SourceConfigKey(sourceID)
	sourceEntry, err := f.KV.Get(sourceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get source config: %w", err)
	}

	var srcCfg protocol.SourceConfig
	if err := json.Unmarshal(sourceEntry.Value(), &srcCfg); err != nil {
		return nil, err
	}
	if err := srcCfg.Decrypt(); err != nil {
		return nil, fmt.Errorf("failed to decrypt source config: %w", err)
	}

	// Currently only postgres is supported, but could be a registry too
	// WithKV wires the same NATS KV bucket the engine uses for checkpoints
	// (f.KV) and this pipeline's ID: without it, PostgresSource.pipelineID
	// stays "" and every gauge it exports (cdc_source_slot_lag_bytes,
	// cdc_source_pending_lsns, cdc_source_ack_watermark,
	// cdc_source_slot_lag_probe_last_success_timestamp_seconds) carries an
	// empty pipeline label, breaking dashboards/alerts keyed on it, and
	// persistWatermark short-circuits (kv == nil check), so the
	// SourceWatermarkKey twin the WI-7 bake period depends on is never
	// written. See OPS-1.
	src := postgres.NewPostgresSource(sourceID).WithKV(id, f.KV)

	// Create a sink and subscriber for each configured sink
	var activeSinkIDs []string
	var preHooks []sink.PreTransformHook
	var postHooks []sink.PostTransformHook

	for _, sinkID := range cfg.Sinks {
		sinkKey := protocol.SinkConfigKey(sinkID)
		sinkEntry, err := f.KV.Get(sinkKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get sink config for sink %s: %w", sinkID, err)
		}

		var snkCfg protocol.SinkConfig
		if err := json.Unmarshal(sinkEntry.Value(), &snkCfg); err != nil {
			return nil, err
		}
		if err := snkCfg.Decrypt(); err != nil {
			return nil, fmt.Errorf("failed to decrypt sink config: %w", err)
		}

		// Use Sink Registry
		snk, err := sink.New(snkCfg.Type, sinkID, snkCfg.DSN, snkCfg.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to create sink %s: %w", sinkID, err)
		}

		// Startup-time schema validation (MULTI_SCHEMA_PLAN.md §7.4 item 2).
		// Sinks implementing SchemaValidator get every target schema checked --
		// and, where auto-provisioning is enabled, created -- before the worker
		// is reported as started. Without this the check fires lazily on the
		// first ApplySchema, which turns a permanently-unsatisfiable target
		// (e.g. a missing database with auto_create_schema disabled) into an
		// unbounded redelivery loop on the schema path rather than a clean
		// startup failure.
		if validator, ok := snk.(sink.SchemaValidator); ok {
			refs := make([]protocol.TableRef, 0, len(cfg.Tables))
			for _, t := range cfg.Tables {
				ref, err := protocol.ParseTableRef(t)
				if err != nil {
					return nil, fmt.Errorf("pipeline %s: invalid table %q: %w", id, t, err)
				}
				refs = append(refs, ref)
			}
			if err := validator.ValidateSchemas(workerCtx, refs); err != nil {
				return nil, fmt.Errorf("sink %s: %w", sinkID, err)
			}
		}

		// Wire up hooks if it's a debug sink
		var preHook sink.PreTransformHook
		var postHook sink.PostTransformHook
		if sink.IsDebug(snkCfg.Type) {
			if capturer, ok := snk.(sink.DebugCapturer); ok {
				preHook = capturer.CaptureBefore
				postHook = capturer.CaptureAfter
			}
		}

		snks = append(snks, snk)
		activeSinkIDs = append(activeSinkIDs, sinkID)
		preHooks = append(preHooks, preHook)
		postHooks = append(postHooks, postHook)

		// Throughput Tuning
		maxAckPending := snkCfg.MaxAckPending
		if maxAckPending <= 0 {
			maxAckPending = cfg.BatchSize * 2
			if maxAckPending < 1000 {
				maxAckPending = 1000
			}
		}

		// Durable name with group isolation
		durableName := fmt.Sprintf("cdc-worker-%s-sink-%s", id, sinkID)
		if f.WorkerGroup != "" {
			durableName = fmt.Sprintf("%s-%s", f.WorkerGroup, durableName)
		}

		streamName := fmt.Sprintf("cdc_pipeline_%s_ingest", id)
		sub, err := nats.NewNatsSubscriber(f.NatsURL, durableName, streamName, maxAckPending, deriveAckWait(cfg, f.AckWaitCeiling))
		if err != nil {
			return nil, fmt.Errorf("failed to create subscriber for sink %s: %w", sinkID, err)
		}
		subscribers = append(subscribers, sub)
	}

	// WS-5 item 3 / docs/todos/custom_object_cdc_followups.md item 4: the
	// old default RetryConfig{MaxRetries: 3} left InitialInterval and
	// MaxInterval at their zero value, so handleSinkError's doubling loop
	// stayed at backoff=0 and fell through to a flat, un-jittered 5s retry
	// -- a tight loop against a dependency that is, by construction,
	// already degraded. defaultRetryConfig mirrors RetryConfig's own
	// documented example values (config.go's swaggertype example:"1s" /
	// example:"30s" tags). consumer.go's handleSinkError also floors a
	// zero interval independently (defaultRetryInitialInterval /
	// defaultRetryMaxInterval), so a pipeline-config-supplied *RetryConfig
	// that leaves the Durations unset (permitted by RetryConfig.Validate's
	// non-Required Min) is protected too, not just this default path.
	retry := defaultRetryConfig()
	if cfg.Retry != nil {
		retry = *cfg.Retry
	}

	// Create a subscriber for the producer to handle schema evolution acks
	prodDurableName := fmt.Sprintf("cdc-worker-%s-producer-acks", id)
	if f.WorkerGroup != "" {
		prodDurableName = fmt.Sprintf("%s-%s", f.WorkerGroup, prodDurableName)
	}
	prodStreamName := protocol.AcksTopic(id)
	prodSub, err := nats.NewNatsSubscriber(f.NatsURL, prodDurableName, prodStreamName, 100, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber for producer %s: %w", id, err)
	}
	subscribers = append(subscribers, prodSub)

	// Fetch primary source config for the producer
	srcID := cfg.Sources[0]
	srcEntry, err := f.KV.Get(protocol.SourceConfigKey(srcID))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch source config %s for producer: %w", srcID, err)
	}
	var srcConfig protocol.SourceConfig
	if err := json.Unmarshal(srcEntry.Value(), &srcConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal source config %s: %w", srcID, err)
	}
	if err := srcConfig.Decrypt(); err != nil {
		return nil, fmt.Errorf("failed to decrypt source config: %w", err)
	}

	prod := NewProducer(id, f.NatsURL, cfg, src, f.Publisher, prodSub, f.KV, srcConfig)

	var consumers []*Consumer
	for i, snk := range snks {
		sinkID := activeSinkIDs[i]
		sub := subscribers[i]
		preHook := preHooks[i]
		postHook := postHooks[i]

		var consumerTransformers []ConfiguredTransformer
		for _, pCfg := range cfg.Processors {
			// WS-8 item 2: an unregistered processor type, or a factory that
			// errors, used to only log and continue -- the pipeline then ran
			// completely untransformed while still reporting "Running". Make
			// it fatal for the whole pipeline instead: CreateWorker returning
			// an error here means startNewWorker (config/manager.go) never
			// registers a worker, and the supervisor's heartbeat loop stays
			// in "Retrying" rather than ever claiming "Running".
			tf, ok := transformer.GetTransformer(pCfg.Type)
			if !ok {
				return nil, fmt.Errorf("pipeline %s: processor %q references unregistered transformer type %q", id, pCfg.Name, pCfg.Type)
			}

			// Plumb the pipeline ID into the processor's options (without
			// mutating the caller's map) so transformers that report metrics
			// per-pipeline (WS-9) can label them correctly.
			opts := pCfg.Options
			if _, exists := opts["pipeline_id"]; !exists {
				merged := make(map[string]interface{}, len(opts)+1)
				for k, v := range opts {
					merged[k] = v
				}
				merged["pipeline_id"] = id
				opts = merged
			}
			// WS-5 item 4: also plumb the pipeline's effective batch size so
			// a transformer without an explicit timeout_ms (e.g.
			// nats/protobuf) can scale its default request timeout with it
			// (transformernats.DefaultTimeoutMs) instead of using a flat
			// value regardless of how large a batch it might have to
			// process. Options wins if the processor config already sets
			// its own batch_size for some reason.
			if _, exists := opts["batch_size"]; !exists {
				merged := make(map[string]interface{}, len(opts)+1)
				for k, v := range opts {
					merged[k] = v
				}
				merged["batch_size"] = float64(cfg.BatchSize)
				opts = merged
			}

			t, err := tf(opts)
			if err != nil {
				return nil, fmt.Errorf("pipeline %s: failed to create transformer %q for processor %q: %w", id, pCfg.Type, pCfg.Name, err)
			}
			ct := ConfiguredTransformer{
				Transformer:    t,
				OperationTypes: pCfg.OperationTypes,
			}
			consumerTransformers = append(consumerTransformers, ct)
			allTransformers = append(allTransformers, ct)
		}

		cons := NewConsumer(id, sinkID, sub, f.Publisher, snk, consumerTransformers, f.KV, cfg.BatchSize, cfg.BatchWait, retry, preHook, postHook)
		consumers = append(consumers, cons)
	}

	pipe := NewPipeline(id, prod, consumers, cfg)
	pipe.Start(workerCtx)

	success = true
	return pipe, nil
}

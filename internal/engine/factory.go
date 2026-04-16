package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/sink"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source/postgres"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	go_nats "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// PipelineFactory creates PipelineWorker instances.
type PipelineFactory struct {
	KV          go_nats.KeyValue
	Publisher   stream.Publisher
	NatsURL     string
	WorkerGroup string
}

// CreateWorker builds a full Pipeline from configuration.
func (f *PipelineFactory) CreateWorker(workerCtx context.Context, id string, cfg protocol.PipelineConfig) (PipelineWorker, error) {
	log.Info().Str("pipeline_id", id).Int("num_sinks", len(cfg.Sinks)).Msg("Creating worker for pipeline")

	var success bool
	var subscribers []*nats.NatsSubscriber
	var snks []sink.Sink

	// Cleanup on failure
	defer func() {
		if !success {
			for _, sub := range subscribers {
				sub.Close()
			}
			for _, snk := range snks {
				snk.Stop()
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

	// Currently only postgres is supported, but could be a registry too
	src := postgres.NewPostgresSource(sourceID)

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

		// Use Sink Registry
		snk, err := sink.New(snkCfg.Type, sinkID, snkCfg.DSN, snkCfg.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to create sink %s: %w", sinkID, err)
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

		sub, err := nats.NewNatsSubscriber(f.NatsURL, durableName, maxAckPending, 30*time.Second)
		if err != nil {
			return nil, fmt.Errorf("failed to create subscriber for sink %s: %w", sinkID, err)
		}
		subscribers = append(subscribers, sub)
	}

	retry := protocol.RetryConfig{MaxRetries: 3}
	if cfg.Retry != nil {
		retry = *cfg.Retry
	}

	// Create a subscriber for the producer to handle schema evolution acks
	prodDurableName := fmt.Sprintf("cdc-worker-%s-producer-acks", id)
	if f.WorkerGroup != "" {
		prodDurableName = fmt.Sprintf("%s-%s", f.WorkerGroup, prodDurableName)
	}
	prodSub, err := nats.NewNatsSubscriber(f.NatsURL, prodDurableName, 100, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber for producer %s: %w", id, err)
	}
	subscribers = append(subscribers, prodSub)

	prod := NewProducer(id, f.NatsURL, cfg, src, f.Publisher, prodSub, f.KV)

	var consumers []*Consumer
	for i, snk := range snks {
		sinkID := activeSinkIDs[i]
		sub := subscribers[i]
		preHook := preHooks[i]
		postHook := postHooks[i]

		var consumerTransformers []transformer.Transformer
		for _, pCfg := range cfg.Processors {
			tf, ok := transformer.GetTransformer(pCfg.Type)
			if !ok {
				log.Warn().Str("pipeline_id", id).Str("type", pCfg.Type).Msg("Transformer type not registered")
				continue
			}
			t, err := tf(pCfg.Options)
			if err != nil {
				log.Error().Err(err).Str("pipeline_id", id).Str("type", pCfg.Type).Msg("Failed to create transformer")
				continue
			}
			consumerTransformers = append(consumerTransformers, t)
		}

		cons := NewConsumer(id, sinkID, sub, f.Publisher, snk, consumerTransformers, f.KV, cfg.BatchSize, cfg.BatchWait, retry, preHook, postHook)
		consumers = append(consumers, cons)
	}

	pipe := NewPipeline(id, prod, consumers, cfg)
	pipe.Start(workerCtx)

	success = true
	return pipe, nil
}

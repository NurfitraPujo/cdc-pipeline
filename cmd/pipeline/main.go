package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/config"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/logger"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/metrics"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
        "bitbucket.com/daya-engineering/daya-data-pipeline/internal/transformer"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink/databend"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source/postgres"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

//go:embed config.example.yaml
var defaultConfigFile []byte

func main() {
	// 1. Initialize Logger
	logLvl := os.Getenv("LOG_LEVEL")
	if logLvl == "" {
		logLvl = "info"
	}
	isDev := os.Getenv("ENV") != "production"
	logger.Init(logLvl, isDev)

	log.Info().Msg("Daya Data Pipeline starting...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = go_nats.DefaultURL
	}

	nc, err := go_nats.Connect(natsURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to NATS")
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get JetStream context")
	}

	kv, err := js.KeyValue(protocol.KVBucketName)
	if err != nil {
		kv, err = js.CreateKeyValue(&go_nats.KeyValueConfig{
			Bucket: protocol.KVBucketName,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get or create KV bucket")
		}
	}

	if err := bootstrapKV(kv); err != nil {
		log.Warn().Err(err).Msg("Failed to bootstrap KV")
	}

	factory := func(workerCtx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		log.Info().Str("pipeline_id", id).Msg("Factory creating worker for pipeline")

		var success bool
		var pub *nats.NatsPublisher
		var sub *nats.NatsSubscriber
		var snk *databend.DatabendSink

		// Cleanup on failure
		defer func() {
			if !success {
				if pub != nil {
					pub.Close()
				}
				if sub != nil {
					sub.Close()
				}
				if snk != nil {
					snk.Stop()
				}
			}
		}()

		if len(cfg.Sources) == 0 {
			return nil, fmt.Errorf("no sources defined for pipeline %s", id)
		}
		sourceID := cfg.Sources[0]
		sourceKey := protocol.SourceConfigKey(sourceID)
		sourceEntry, err := kv.Get(sourceKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get source config: %w", err)
		}

		var srcCfg protocol.SourceConfig
		if err := json.Unmarshal(sourceEntry.Value(), &srcCfg); err != nil {
			return nil, err
		}

		src := postgres.NewPostgresSource(sourceID)

		sinkID := cfg.Sinks[0]
		sinkKey := protocol.SinkConfigKey(sinkID)
		sinkEntry, err := kv.Get(sinkKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get sink config: %w", err)
		}

		var snkCfg protocol.SinkConfig
		if err := json.Unmarshal(sinkEntry.Value(), &snkCfg); err != nil {
			return nil, err
		}

		snk, err = databend.NewDatabendSink(sinkID, snkCfg.DSN)
		if err != nil {
			return nil, err
		}

		pub, err = nats.NewNatsPublisher(natsURL)
		if err != nil {
			return nil, err
		}

		maxAckPending := cfg.BatchSize * 2
		if maxAckPending < 1000 {
			maxAckPending = 1000
		}

		sub, err = nats.NewNatsSubscriber(natsURL, fmt.Sprintf("daya-worker-%s", id), maxAckPending, 30*time.Second)
		if err != nil {
			return nil, err
		}

		retry := protocol.RetryConfig{MaxRetries: 3} // Default
		if cfg.Retry != nil {
			retry = *cfg.Retry
		}

		prod := engine.NewProducer(id, cfg, src, pub, kv)
		var transformers []transformer.Transformer
		for _, pCfg := range cfg.Processors {
			f, ok := transformer.GetTransformer(pCfg.Type)
			if !ok {
				log.Warn().Str("pipeline_id", id).Str("type", pCfg.Type).Msg("Transformer type not registered")
				continue
			}
			t, err := f(pCfg.Options)
			if err != nil {
				log.Error().Err(err).Str("pipeline_id", id).Str("type", pCfg.Type).Msg("Failed to create transformer")
				continue
			}
			transformers = append(transformers, t)
		}

		cons := engine.NewConsumer(id, sub, pub, snk, transformers, kv, cfg.BatchSize, cfg.BatchWait, retry)

		pipe := engine.NewPipeline(id, prod, cons, cfg)
		pipe.Start(workerCtx)

		success = true
		return pipe, nil
	}

	mgr := config.NewConfigManager(kv, factory)
	if err := mgr.Watch(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to start config watcher")
	}

	workerID, _ := os.Hostname()
	if workerID == "" {
		workerID = "unknown-worker"
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		startTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				hb := protocol.WorkerHeartbeat{
					WorkerID:  workerID,
					Status:    "online",
					UptimeSec: int64(t.Sub(startTime).Seconds()),
					UpdatedAt: t,
				}
				data, _ := json.Marshal(hb)
				if _, err := kv.Put(protocol.WorkerHeartbeatKey(workerID), data); err != nil {
					log.Warn().Err(err).Msg("Failed to update worker heartbeat")
				}
				metrics.WorkerHeartbeat.WithLabelValues(workerID).Set(float64(t.Unix()))
			}
		}
	}()

	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8081"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if nc.Status() == go_nats.CONNECTED {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("READY"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("NATS NOT CONNECTED"))
		}
	})
	mux.Handle("/metrics", promhttp.Handler())

	healthSrv := &http.Server{
		Addr: ":" + healthPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info().Str("port", healthPort).Msg("Health check server started")
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Health check server failed")
		}
	}()

	log.Info().Str("worker_id", workerID).Msg("Daya Data Pipeline Worker started. Waiting for configuration...")
	<-ctx.Done()
	log.Info().Msg("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr.Stop(shutdownCtx)
}

func bootstrapKV(kv go_nats.KeyValue) error {
	keys, err := kv.Keys()
	if err == nil && len(keys) > 0 {
		return nil
	}

	log.Info().Msg("KV bucket empty. Bootstrapping from embedded config.example.yaml...")

	var seed struct {
		Auth      protocol.UserConfig       `yaml:"auth"`
		Global    protocol.GlobalConfig     `yaml:"global"`
		Sources   []protocol.SourceConfig   `yaml:"sources"`
		Sinks     []protocol.SinkConfig     `yaml:"sinks"`
		Pipelines []protocol.PipelineConfig `yaml:"pipelines"`
	}

	if err := yaml.Unmarshal(defaultConfigFile, &seed); err != nil {
		return err
	}

	authData, _ := json.Marshal(seed.Auth)
	if _, err := kv.Put(protocol.KeyAuthConfig, authData); err != nil {
		log.Warn().Err(err).Msg("Failed to bootstrap auth config")
	}

	globalData, _ := json.Marshal(seed.Global)
	if _, err := kv.Put(protocol.KeyGlobalConfig, globalData); err != nil {
		log.Warn().Err(err).Msg("Failed to bootstrap global config")
	}

	for _, sc := range seed.Sources {
		data, _ := json.Marshal(sc)
		if _, err := kv.Put(protocol.SourceConfigKey(sc.ID), data); err != nil {
			log.Warn().Err(err).Str("source_id", sc.ID).Msg("Failed to bootstrap source")
		}
	}
	for _, sc := range seed.Sinks {
		data, _ := json.Marshal(sc)
		if _, err := kv.Put(protocol.SinkConfigKey(sc.ID), data); err != nil {
			log.Warn().Err(err).Str("sink_id", sc.ID).Msg("Failed to bootstrap sink")
		}
	}
	for _, pc := range seed.Pipelines {
		data, _ := json.Marshal(pc)
		if _, err := kv.Put(protocol.PipelineConfigKey(pc.ID), data); err != nil {
			log.Warn().Err(err).Str("pipeline_id", pc.ID).Msg("Failed to bootstrap pipeline")
		}
	}

	log.Info().Msg("Bootstrapping complete.")
	return nil
}

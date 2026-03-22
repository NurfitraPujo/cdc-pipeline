package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/config"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source/postgres"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink/databend"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

//go:embed config.example.yaml
var defaultConfigFile []byte

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = go_nats.DefaultURL
	}

	nc, err := go_nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to get JetStream context: %v", err)
	}

	kv, err := js.KeyValue(protocol.KVBucketName)
	if err != nil {
		// Attempt to create bucket if it doesn't exist
		kv, err = js.CreateKeyValue(&go_nats.KeyValueConfig{
			Bucket: protocol.KVBucketName,
		})
		if err != nil {
			log.Fatalf("Failed to get or create KV bucket: %v", err)
		}
	}

	// Bootstrap KV if empty
	if err := bootstrapKV(kv); err != nil {
		log.Printf("Warning: Failed to bootstrap KV: %v", err)
	}

	// Define Worker Factory
	factory := func(workerCtx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		log.Printf("Factory creating worker for pipeline %s with config: %+v", id, cfg)
		
		// 1. Resolve Sources (For now, we assume the first source in the list)
		if len(cfg.Sources) == 0 {
			return nil, fmt.Errorf("no sources defined for pipeline %s", id)
		}
		
		sourceID := cfg.Sources[0]
		sourceKey := protocol.SourceConfigKey(sourceID)
		sourceEntry, err := kv.Get(sourceKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get source config for %s: %w", sourceID, err)
		}
		
		var srcCfg protocol.SourceConfig
		if err := json.Unmarshal(sourceEntry.Value(), &srcCfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal source config %s: %w", sourceID, err)
		}
		
		// 1. Initialize Source
		src := postgres.NewPostgresSource(sourceID)
		
		// 2. Initialize Publisher/Subscriber
		pub, err := nats.NewNatsPublisher(natsURL)
		if err != nil {
			return nil, err
		}
		sub, err := nats.NewNatsSubscriber(natsURL, fmt.Sprintf("daya-worker-%s", id))
		if err != nil {
			return nil, err
		}
		
		// 3. Initialize Sink (Using default Databend for now, but should also be resolved from KV)
		snk, err := databend.NewDatabendSink("databend-sink", "http://root:@localhost:8000")
		if err != nil {
			return nil, err
		}
		
		// 4. Create Engine components using overrides from merged cfg
		prod := engine.NewProducer(id, src, pub, kv)
		cons := engine.NewConsumer(id, sub, snk, kv, cfg.BatchSize, cfg.BatchWait)
		
		pipe := engine.NewPipeline(id, prod, cons, cfg)
		
		// Start the pipeline in the background
		if err := pipe.Start(workerCtx); err != nil {
			return nil, err
		}
		
		return pipe, nil
	}

	// Initialize ConfigManager
	mgr := config.NewConfigManager(kv, factory)
	
	if err := mgr.Watch(ctx); err != nil {
		log.Fatalf("Failed to start config watcher: %v", err)
	}

	// 5. Worker Heartbeat
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
				kv.Put(protocol.WorkerHeartbeatKey(workerID), data)
			}
		}
	}()

	// 6. Health Check Server (for Kubernetes)
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8081"
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			if nc.Status() == go_nats.CONNECTED {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("READY"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("NATS NOT CONNECTED"))
			}
		})
		log.Printf("Health check server started on :%s", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			log.Printf("Health check server failed: %v", err)
		}
	}()

	log.Printf("Daya Data Pipeline Worker [%s] started. Waiting for configuration...", workerID)
	<-ctx.Done()
	log.Println("Shutting down...")
	
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr.Stop(shutdownCtx)
}

func bootstrapKV(kv go_nats.KeyValue) error {
	keys, err := kv.Keys()
	// If bucket is not empty (err == nil and keys found), skip bootstrapping
	if err == nil && len(keys) > 0 {
		return nil
	}

	log.Println("KV bucket empty. Bootstrapping from embedded config.example.yaml...")

	var seed struct {
		Auth      protocol.UserConfig       `yaml:"auth"`
		Global    protocol.GlobalConfig     `yaml:"global"`
		Sources   []protocol.SourceConfig   `yaml:"sources"`
		Pipelines []protocol.PipelineConfig `yaml:"pipelines"`
	}

	if err := yaml.Unmarshal(defaultConfigFile, &seed); err != nil {
		return fmt.Errorf("failed to unmarshal embedded config: %w", err)
	}

	// 0. Seed Auth
	authData, _ := json.Marshal(seed.Auth)
	if _, err := kv.Put(protocol.KeyAuthConfig, authData); err != nil {
		return fmt.Errorf("failed to seed auth config: %w", err)
	}

	// 1. Seed Global
	globalData, _ := json.Marshal(seed.Global)
	if _, err := kv.Put(protocol.KeyGlobalConfig, globalData); err != nil {
		return fmt.Errorf("failed to seed global config: %w", err)
	}

	// 2. Seed Sources
	for _, sc := range seed.Sources {
		data, _ := json.Marshal(sc)
		if _, err := kv.Put(protocol.SourceConfigKey(sc.ID), data); err != nil {
			return fmt.Errorf("failed to seed source %s: %w", sc.ID, err)
		}
	}

	// 3. Seed Pipelines
	for _, pc := range seed.Pipelines {
		data, _ := json.Marshal(pc)
		if _, err := kv.Put(protocol.PipelineConfigKey(pc.ID), data); err != nil {
			return fmt.Errorf("failed to seed pipeline %s: %w", pc.ID, err)
		}
	}

	log.Println("Bootstrapping complete.")
	return nil
}

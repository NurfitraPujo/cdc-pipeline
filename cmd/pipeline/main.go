package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/config"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source/postgres"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink/databend"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
)

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

	log.Println("Daya Data Pipeline Worker started. Waiting for configuration...")
	<-ctx.Done()
	log.Println("Shutting down...")
}

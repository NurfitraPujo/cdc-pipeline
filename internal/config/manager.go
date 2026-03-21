package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
)

type WorkerFactory func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error)

type ConfigManager struct {
	kv            nats.KeyValue
	factory       WorkerFactory
	workers       map[string]engine.PipelineWorker
	workersMu     sync.RWMutex
	globalConfig  map[string]any
	globalConfigMu sync.RWMutex
}

func NewConfigManager(kv nats.KeyValue, factory WorkerFactory) *ConfigManager {
	return &ConfigManager{
		kv:      kv,
		factory: factory,
		workers: make(map[string]engine.PipelineWorker),
	}
}

func (m *ConfigManager) Watch(ctx context.Context) error {
	// 1. Watch global config
	globalWatcher, err := m.kv.Watch("global.config")
	if err != nil {
		return fmt.Errorf("failed to watch global config: %w", err)
	}

	// 2. Watch pipeline configs
	pipelineWatcher, err := m.kv.Watch("pipelines.*.config")
	if err != nil {
		return fmt.Errorf("failed to watch pipeline configs: %w", err)
	}

	go m.handleGlobalUpdates(ctx, globalWatcher)
	go m.handlePipelineUpdates(ctx, pipelineWatcher)

	return nil
}

func (m *ConfigManager) handleGlobalUpdates(ctx context.Context, watcher nats.KeyWatcher) {
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			log.Printf("Global config updated: %s", entry.Key())
			var cfg map[string]any
			if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
				log.Printf("Error unmarshaling global config: %v", err)
				continue
			}
			m.globalConfigMu.Lock()
			m.globalConfig = cfg
			m.globalConfigMu.Unlock()
			// Potentially trigger reload for all workers if global changes affect them
		}
	}
}

func (m *ConfigManager) handlePipelineUpdates(ctx context.Context, watcher nats.KeyWatcher) {
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			pipelineID := extractPipelineID(entry.Key())
			if pipelineID == "" {
				continue
			}

			if entry.Operation() == nats.KeyValuePurge || entry.Operation() == nats.KeyValueDelete {
				m.stopWorker(ctx, pipelineID)
				continue
			}

			var cfg protocol.PipelineConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
				log.Printf("Error unmarshaling pipeline config %s: %v", pipelineID, err)
				continue
			}

			m.transitionWorker(ctx, pipelineID, cfg)
		}
	}
}

func (m *ConfigManager) transitionWorker(ctx context.Context, id string, cfg protocol.PipelineConfig) {
	m.workersMu.Lock()
	defer m.workersMu.Unlock()

	oldWorker, exists := m.workers[id]
	if exists {
		log.Printf("Initiating two-phase transition for pipeline %s", id)
		go func(w engine.PipelineWorker, newCfg protocol.PipelineConfig) {
			// Phase 1: Drain
			if err := w.Drain(); err != nil {
				log.Printf("Error draining worker %s: %v", id, err)
			}
			// Wait for finished signal
			<-w.Finished()
			// Phase 2: Shutdown
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30) // Use a reasonable timeout
			defer cancel()
			w.Shutdown(shutdownCtx)

			// Start new worker
			m.startNewWorker(ctx, id, newCfg)
		}(oldWorker, cfg)
	} else {
		m.startNewWorker(ctx, id, cfg)
	}
}

func (m *ConfigManager) startNewWorker(ctx context.Context, id string, cfg protocol.PipelineConfig) {
	newWorker, err := m.factory(ctx, id, cfg)
	if err != nil {
		log.Printf("Error creating worker for pipeline %s: %v", id, err)
		return
	}
	m.workersMu.Lock()
	m.workers[id] = newWorker
	m.workersMu.Unlock()
	log.Printf("Started new worker for pipeline %s", id)
}

func (m *ConfigManager) stopWorker(ctx context.Context, id string) {
	m.workersMu.Lock()
	defer m.workersMu.Unlock()
	if w, ok := m.workers[id]; ok {
		log.Printf("Stopping pipeline %s", id)
		w.Drain()
		go func(worker engine.PipelineWorker) {
			<-worker.Finished()
			worker.Shutdown(context.Background())
		}(w)
		delete(m.workers, id)
	}
}

func extractPipelineID(key string) string {
	// key is pipelines.{id}.config
	var id string
	fmt.Sscanf(key, "pipelines.%s.config", &id)
	return id
}

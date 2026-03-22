package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
)

type WorkerFactory func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error)

type ConfigManager struct {
	kv             nats.KeyValue
	factory        WorkerFactory
	workers        map[string]engine.PipelineWorker
	workersMu      sync.RWMutex
	globalConfig   protocol.GlobalConfig
	globalConfigMu sync.RWMutex
}

func NewConfigManager(kv nats.KeyValue, factory WorkerFactory) *ConfigManager {
	return &ConfigManager{
		kv:      kv,
		factory: factory,
		workers: make(map[string]engine.PipelineWorker),
		globalConfig: protocol.GlobalConfig{
			BatchSize: 1000,
			BatchWait: 5 * time.Second,
		},
	}
}

func (m *ConfigManager) Watch(ctx context.Context) error {
	// 1. Prime Global Config (Get latest if it exists)
	if entry, err := m.kv.Get(protocol.KeyGlobalConfig); err == nil {
		var cfg protocol.GlobalConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
			m.globalConfigMu.Lock()
			m.globalConfig = cfg
			m.globalConfigMu.Unlock()
			log.Printf("Global config primed from KV")
		}
	}

	// 2. Start Watchers
	globalWatcher, err := m.kv.Watch(protocol.KeyGlobalConfig)
	if err != nil {
		return fmt.Errorf("failed to watch global config: %w", err)
	}

	pipelineWatcher, err := m.kv.Watch(protocol.PrefixPipelineConfig + "*")
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
			var cfg protocol.GlobalConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
				log.Printf("Error unmarshaling global config: %v", err)
				continue
			}
			m.globalConfigMu.Lock()
			m.globalConfig = cfg
			m.globalConfigMu.Unlock()

			// Trigger reload for all active workers to apply new global defaults
			m.reloadAllWorkers(ctx)
		}
	}
}

func (m *ConfigManager) reloadAllWorkers(ctx context.Context) {
	m.workersMu.RLock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	m.workersMu.RUnlock()

	for _, id := range ids {
		// Re-fetch pipeline config from KV to get latest and apply hierarchy
		key := protocol.PipelineConfigKey(id)
		entry, err := m.kv.Get(key)
		if err != nil {
			log.Printf("Failed to re-fetch config for pipeline %s during global reload: %v", id, err)
			continue
		}

		var cfg protocol.PipelineConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			log.Printf("Error unmarshaling config for pipeline %s during global reload: %v", id, err)
			continue
		}

		m.applyHierarchy(&cfg)
		// No lock held here, transitionWorker will acquire its own lock
		m.transitionWorker(ctx, id, cfg)
	}
}

func (m *ConfigManager) Stop(ctx context.Context) {
	m.workersMu.Lock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	m.workersMu.Unlock()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()
			m.stopWorker(ctx, pid)
		}(id)
	}
	
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		log.Println("Graceful shutdown timed out")
	case <-done:
		log.Println("All pipelines stopped gracefully")
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
			log.Printf("Pipeline update received: %s (Op: %v)", entry.Key(), entry.Operation())
			pipelineID := extractPipelineID(entry.Key())
			if pipelineID == "" {
				log.Printf("Failed to extract pipeline ID from key: %s", entry.Key())
				continue
			}

			if entry.Operation() == nats.KeyValuePurge || entry.Operation() == nats.KeyValueDelete {
				log.Printf("Pipeline %s deleted", pipelineID)
				m.stopWorker(ctx, pipelineID)
				continue
			}

			var cfg protocol.PipelineConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
				log.Printf("Error unmarshaling pipeline config %s: %v", pipelineID, err)
				continue
			}

			// Apply hierarchy: Global Defaults -> Pipeline Overrides
			m.applyHierarchy(&cfg)

			m.transitionWorker(ctx, pipelineID, cfg)
		}
	}
}

func (m *ConfigManager) applyHierarchy(cfg *protocol.PipelineConfig) {
	m.globalConfigMu.RLock()
	defer m.globalConfigMu.RUnlock()

	if cfg.BatchSize == 0 {
		cfg.BatchSize = m.globalConfig.BatchSize
	}
	if cfg.BatchWait == 0 {
		cfg.BatchWait = m.globalConfig.BatchWait
	}
}

func (m *ConfigManager) transitionWorker(ctx context.Context, id string, cfg protocol.PipelineConfig) {
	m.workersMu.Lock()
	oldWorker, exists := m.workers[id]
	m.workersMu.Unlock()

	// 1. Mark as Transitioning in KV
	ts := protocol.PipelineTransitionState{
		ID:        id,
		Status:    "Transitioning",
		StartedAt: time.Now(),
	}
	tsData, _ := json.Marshal(ts)
	m.kv.Put(protocol.TransitionStateKey(id), tsData)

	if exists {
		log.Printf("Initiating two-phase transition for pipeline %s", id)
		go func(w engine.PipelineWorker, newCfg protocol.PipelineConfig) {
			log.Printf("Transition Phase 1: Draining worker %s", id)
			if err := w.Drain(); err != nil {
				log.Printf("Error draining worker %s: %v", id, err)
			}
			<-w.Finished()
			log.Printf("Transition Phase 1 Complete: Worker %s drained", id)

			log.Printf("Transition Phase 2: Shutting down worker %s", id)
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w.Shutdown(shutdownCtx)
			log.Printf("Transition Phase 2 Complete: Worker %s shut down", id)

			m.startNewWorker(ctx, id, newCfg)
			
			// 2. Clear Transitioning state
			m.kv.Delete(protocol.TransitionStateKey(id))
		}(oldWorker, cfg)
	} else {
		log.Printf("Starting initial worker for pipeline %s", id)
		m.startNewWorker(ctx, id, cfg)
		// 2. Clear Transitioning state
		m.kv.Delete(protocol.TransitionStateKey(id))
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
	log.Printf("Successfully started new worker for pipeline %s", id)
}

func (m *ConfigManager) stopWorker(ctx context.Context, id string) {
	m.workersMu.Lock()
	w, ok := m.workers[id]
	if ok {
		delete(m.workers, id)
	}
	m.workersMu.Unlock()

	if ok {
		log.Printf("Stopping pipeline %s", id)
		w.Drain()
		go func(worker engine.PipelineWorker) {
			<-worker.Finished()
			worker.Shutdown(context.Background())
		}(w)
	}
}

func extractPipelineID(key string) string {
	if !strings.HasPrefix(key, protocol.PrefixPipelineConfig) {
		return ""
	}
	return strings.TrimPrefix(key, protocol.PrefixPipelineConfig)
}

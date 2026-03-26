package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
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

func (m *ConfigManager) GetKV() nats.KeyValue {
	return m.kv
}

func (m *ConfigManager) Watch(ctx context.Context) error {
	// 1. Prime Global Config (Get latest if it exists)
	if entry, err := m.kv.Get(protocol.KeyGlobalConfig); err == nil {
		var cfg protocol.GlobalConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
			m.globalConfigMu.Lock()
			m.globalConfig = cfg
			m.globalConfigMu.Unlock()
			log.Info().Msg("Global config primed from KV")
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
			log.Info().Str("key", entry.Key()).Msg("Global config updated")
			var cfg protocol.GlobalConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
				log.Error().Err(err).Msg("Error unmarshaling global config")
				continue
			}
			m.globalConfigMu.Lock()
			m.globalConfig = cfg
			m.globalConfigMu.Unlock()

			// Trigger reload for all active workers to apply new global defaults
			time.Sleep(2 * time.Second)
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
		key := protocol.PipelineConfigKey(id)
		entry, err := m.kv.Get(key)
		if err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Failed to re-fetch config during global reload")
			continue
		}

		var cfg protocol.PipelineConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Error unmarshaling config during global reload")
			continue
		}

		m.applyHierarchy(&cfg)
		m.transitionWorker(ctx, id, cfg)
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
			log.Info().
				Str("key", entry.Key()).
				Str("operation", entry.Operation().String()).
				Msg("Pipeline update received")

			pipelineID := extractPipelineID(entry.Key())
			if pipelineID == "" {
				log.Error().Str("key", entry.Key()).Msg("Failed to extract pipeline ID from key")
				continue
			}

			if entry.Operation() == nats.KeyValuePurge || entry.Operation() == nats.KeyValueDelete {
				log.Info().Str("pipeline_id", pipelineID).Msg("Pipeline deleted")
				m.stopWorker(ctx, pipelineID)
				continue
			}

			var cfg protocol.PipelineConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
				log.Error().
					Err(err).
					Str("pipeline_id", pipelineID).
					Msg("Error unmarshaling pipeline config")
				continue
			}

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
	if cfg.Retry == nil {
		cfg.Retry = &m.globalConfig.Retry
	}
}

func (m *ConfigManager) transitionWorker(ctx context.Context, id string, cfg protocol.PipelineConfig) {
	m.workersMu.Lock()
	oldWorker, exists := m.workers[id]
	m.workersMu.Unlock()

	// 1. Mark as Transitioning in KV to prevent supervisor from restarting it during intentional reload
	ts := protocol.PipelineTransitionState{
		ID:        id,
		Status:    "Transitioning",
		StartedAt: time.Now(),
	}
	tsData, _ := json.Marshal(ts)
	if _, err := m.kv.Put(protocol.TransitionStateKey(id), tsData); err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("Failed to set transition state")
	}

	if exists {
		log.Info().Str("pipeline_id", id).Msg("Initiating two-phase transition")
		// #nosec G118 -- Intentional background transition
		go func(w engine.PipelineWorker, newCfg protocol.PipelineConfig) {
			// Ensure cleanup of transition state if this goroutine exits for any reason
			defer func() {
				if err := m.kv.Delete(protocol.TransitionStateKey(id)); err != nil {
					log.Warn().Err(err).Str("pipeline_id", id).Msg("Failed to clear transition state")
				}
			}()

			log.Info().Str("pipeline_id", id).Msg("Transition Phase 1: Draining worker")
			if err := w.Drain(); err != nil {
				log.Error().Err(err).Str("pipeline_id", id).Msg("Error draining worker")
			}
			select {
			case <-w.Finished():
				log.Info().Str("pipeline_id", id).Msg("Transition Phase 1 Complete: Worker drained")
			case <-time.After(30 * time.Second):
				log.Warn().Str("pipeline_id", id).Msg("Drain timed out, forcing shutdown")
			}

			log.Info().Str("pipeline_id", id).Msg("Transition Phase 2: Shutting down worker")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := w.Shutdown(shutdownCtx); err != nil {
				log.Error().Err(err).Str("pipeline_id", id).Msg("Error shutting down worker")
			}
			log.Info().Str("pipeline_id", id).Msg("Transition Phase 2 Complete: Worker shut down")

			// Small stabilization delay
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}

			m.startNewWorker(ctx, id, newCfg)
		}(oldWorker, cfg)
	} else {
		log.Info().Str("pipeline_id", id).Msg("Starting initial worker")
		m.startNewWorker(ctx, id, cfg)
		// Best effort cleanup for initial start
		_ = m.kv.Delete(protocol.TransitionStateKey(id))
	}
}

func (m *ConfigManager) startNewWorker(ctx context.Context, id string, cfg protocol.PipelineConfig) {
	newWorker, err := m.factory(ctx, id, cfg)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", id).Msg("Error creating worker")
		return
	}
	m.workersMu.Lock()
	m.workers[id] = newWorker
	m.workersMu.Unlock()
	log.Info().Str("pipeline_id", id).Msg("Successfully started new worker")

	// --- SUPERVISOR GOROUTINE ---
	go func(worker engine.PipelineWorker, pid string) {
		<-worker.Finished()
		
		// When worker finishes, check if it was intentional (Transitioning)
		_, err := m.kv.Get(protocol.TransitionStateKey(pid))
		if err == nats.ErrKeyNotFound {
			// CRASH DETECTED: It finished but we aren't transitioning.
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				log.Error().Str("pipeline_id", pid).Msg("SUPERVISOR: Pipeline crashed unexpectedly! Restarting now...")
				// Re-fetch latest config and restart. 
				// Note: startNewWorker will spawn a NEW supervisor for the next instance.
				entry, err := m.kv.Get(protocol.PipelineConfigKey(pid))
				if err != nil {
					log.Error().Err(err).Str("pipeline_id", pid).Msg("SUPERVISOR: Failed to re-fetch config")
					return
				}
				var latestCfg protocol.PipelineConfig
				if err := json.Unmarshal(entry.Value(), &latestCfg); err != nil {
					log.Error().Err(err).Str("pipeline_id", pid).Msg("SUPERVISOR: Failed to unmarshal config")
					return
				}
				m.applyHierarchy(&latestCfg)
				m.startNewWorker(ctx, pid, latestCfg)
			}
		}
	}(newWorker, id)
}

func (m *ConfigManager) stopWorker(ctx context.Context, id string) {
	m.workersMu.Lock()
	w, ok := m.workers[id]
	if ok {
		delete(m.workers, id)
	}
	m.workersMu.Unlock()

	if ok {
		log.Info().Str("pipeline_id", id).Msg("Stopping pipeline")
		// Mark as "Transitioning" so supervisor doesn't restart it
		ts := protocol.PipelineTransitionState{ID: id, Status: "Stopping", StartedAt: time.Now()}
		tsData, _ := json.Marshal(ts)
		if _, err := m.kv.Put(protocol.TransitionStateKey(id), tsData); err != nil {
			log.Warn().Err(err).Str("pipeline_id", id).Msg("Failed to set transition state for stop")
		}

		if err := w.Drain(); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Error draining worker during stop")
		}
		// #nosec G118 -- Intentional background cleanup
		go func(worker engine.PipelineWorker, pid string) {
			<-worker.Finished()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := worker.Shutdown(shutdownCtx); err != nil {
				log.Error().Err(err).Str("pipeline_id", pid).Msg("Error shutting down worker during stop")
			}
			if err := m.kv.Delete(protocol.TransitionStateKey(pid)); err != nil {
				log.Warn().Err(err).Str("pipeline_id", pid).Msg("Failed to clear transition state after stop")
			}
		}(w, id)
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
		log.Warn().Msg("Graceful shutdown timed out")
	case <-done:
		log.Info().Msg("All pipelines stopped gracefully")
	}
}

func extractPipelineID(key string) string {
	if !strings.HasPrefix(key, protocol.PrefixPipelineConfig) {
		return ""
	}
	return strings.TrimPrefix(key, protocol.PrefixPipelineConfig)
}

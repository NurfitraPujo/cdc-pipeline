package config

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type WorkerFactory func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error)

type ConfigManager struct {
	ctx            context.Context
	cancel         context.CancelFunc
	kv             nats.KeyValue
	factory        WorkerFactory
	workers        map[string]engine.PipelineWorker
	configs        map[string]protocol.PipelineConfig // Track current configs for comparison
	revisions      map[string]uint64                  // Track last seen revision
	workersMu      sync.RWMutex
	globalConfig   protocol.GlobalConfig
	globalConfigMu sync.RWMutex
}

func NewConfigManager(kv nats.KeyValue, factory WorkerFactory) *ConfigManager {
	cfg := protocol.GlobalConfig{
		BatchSize:          1000,
		BatchWait:          5 * time.Second,
		DrainTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		StabilizationDelay: 2 * time.Second,
		CrashRecoveryDelay: 5 * time.Second,
		GlobalReloadDelay:  2 * time.Second,
	}
	cfg.SetDefaults()
	return &ConfigManager{
		kv:           kv,
		factory:      factory,
		workers:      make(map[string]engine.PipelineWorker),
		configs:      make(map[string]protocol.PipelineConfig),
		revisions:    make(map[string]uint64),
		globalConfig: cfg,
	}
}

func (m *ConfigManager) GetKV() nats.KeyValue {
	return m.kv
}

func (m *ConfigManager) UpdateSchemaStateCAS(ctx context.Context, pipelineID, table string, state protocol.SchemaEvolutionState, revision uint64) (uint64, error) {
	key := protocol.SchemaEvolutionKey(pipelineID, table)
	val, err := state.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}

	if revision == 0 {
		return m.kv.Create(key, val)
	}
	return m.kv.Update(key, val, revision)
}

func (m *ConfigManager) GetSchemaState(pipelineID, table string) (protocol.SchemaEvolutionState, uint64, error) {
	key := protocol.SchemaEvolutionKey(pipelineID, table)
	entry, err := m.kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return protocol.SchemaEvolutionState{Status: protocol.SchemaStatusStable}, 0, nil
		}
		return protocol.SchemaEvolutionState{}, 0, err
	}

	var state protocol.SchemaEvolutionState
	_, err = state.UnmarshalMsg(entry.Value())
	return state, entry.Revision(), err
}

// getGlobalConfig returns a copy of the current global config with defaults applied
func (m *ConfigManager) getGlobalConfig() protocol.GlobalConfig {
	m.globalConfigMu.RLock()
	defer m.globalConfigMu.RUnlock()
	cfg := m.globalConfig
	cfg.SetDefaults()
	return cfg
}

func (m *ConfigManager) Watch(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)
	
	// 1. Prime Global Config (Get latest if it exists)
	if entry, err := m.kv.Get(protocol.KeyGlobalConfig); err == nil {
		var cfg protocol.GlobalConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
			cfg.SetDefaults()
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

	go m.handleGlobalUpdates(m.ctx, globalWatcher)
	go m.handlePipelineUpdates(m.ctx, pipelineWatcher)

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
			cfg.SetDefaults()

			// Idempotency check: Skip if config is the same
			m.globalConfigMu.RLock()
			isSame := reflect.DeepEqual(m.globalConfig, cfg)
			m.globalConfigMu.RUnlock()
			if isSame {
				log.Debug().Msg("Global config unchanged, skipping reload of all workers")
				continue
			}

			m.globalConfigMu.Lock()
			m.globalConfig = cfg
			m.globalConfigMu.Unlock()

			// Trigger reload for all active workers to apply new global defaults
			time.Sleep(cfg.GlobalReloadDelay)
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

		// Update cache during global reload
		m.workersMu.Lock()
		m.configs[id] = cfg
		m.revisions[id] = entry.Revision()
		m.workersMu.Unlock()

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

			pipelineID := extractPipelineID(entry.Key())
			if pipelineID == "" {
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

			// Idempotency check: Skip if revision is older than what we have
			// or if the configuration is semantically identical.
			m.workersMu.Lock()
			lastRev := m.revisions[pipelineID]
			currentCfg, exists := m.configs[pipelineID]

			if exists && entry.Revision() <= lastRev {
				m.workersMu.Unlock()
				continue
			}

			if exists && reflect.DeepEqual(currentCfg, cfg) {
				m.revisions[pipelineID] = entry.Revision()
				m.workersMu.Unlock()
				log.Debug().Str("pipeline_id", pipelineID).Uint64("rev", entry.Revision()).Msg("Config unchanged, skipping restart")
				continue
			}

			// Smart reload: detect if ONLY Tables changed
			var newTables []string
			if exists && m.onlyTablesChanged(currentCfg, cfg, &newTables) {
				m.workersMu.Unlock() // Unlock early as we are not modifying the worker map
				m.workersMu.RLock()
				worker, workerExists := m.workers[pipelineID]
				m.workersMu.RUnlock()

				if workerExists {
					log.Info().
						Str("pipeline_id", pipelineID).
						Strs("new_tables", newTables).
						Msg("Smart reload: signaling dynamic table addition")

					// Update config in memory for future comparisons
					m.workersMu.Lock()
					m.configs[pipelineID] = cfg
					m.revisions[pipelineID] = entry.Revision()
					m.workersMu.Unlock()
					
					// This is a conceptual method on the Worker interface
					worker.SignalDynamicTables(newTables)
				}
				continue
			}

			// Update cache and proceed with transition
			m.configs[pipelineID] = cfg
			m.revisions[pipelineID] = entry.Revision()
			m.workersMu.Unlock()

			log.Info().
				Str("pipeline_id", pipelineID).
				Uint64("revision", entry.Revision()).
				Msg("Pipeline configuration changed, triggering transition")

			m.transitionWorker(ctx, pipelineID, cfg)
		}
	}
}

func (m *ConfigManager) onlyTablesChanged(oldCfg, newCfg protocol.PipelineConfig, newTables *[]string) bool {
	// Create copies to avoid modifying the originals
	oldCopy := oldCfg
	newCopy := newCfg

	// Temporarily blank out the tables to compare the rest of the struct
	oldCopy.Tables = nil
	newCopy.Tables = nil

	if !reflect.DeepEqual(oldCopy, newCopy) {
		return false
	}

	// If the rest of the struct is identical, check if tables were only added
	if len(newCfg.Tables) > len(oldCfg.Tables) {
		oldTablesSet := make(map[string]bool)
		for _, t := range oldCfg.Tables {
			oldTablesSet[t] = true
		}
		for _, t := range newCfg.Tables {
			if !oldTablesSet[t] {
				*newTables = append(*newTables, t)
			}
		}
		return len(*newTables) > 0 // It's only a table change if new tables were actually found
	}

	return false
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func processorConfigsEqual(a, b []protocol.ProcessorConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || !mapsEqual(a[i].Options, b[i].Options) {
			return false
		}
	}
	return true
}

func mapsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func retryConfigEqual(a, b *protocol.RetryConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.MaxRetries == b.MaxRetries && a.InitialInterval == b.InitialInterval && a.MaxInterval == b.MaxInterval && a.EnableDLQ == b.EnableDLQ
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
	tsData, err := json.Marshal(ts)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", id).Msg("Failed to marshal transition state")
		return
	}
	if _, err := m.kv.Put(protocol.TransitionStateKey(id), tsData); err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("Failed to set transition state")
	}

	if exists {
		log.Info().Str("pipeline_id", id).Msg("Initiating two-phase transition")
		// Capture timeout values at start of transition
		timeouts := m.getGlobalConfig()
		// #nosec G118 -- Intentional background transition
		go func(w engine.PipelineWorker, newCfg protocol.PipelineConfig, drainTimeout, shutdownTimeout, stabilizationDelay time.Duration) {
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
			case <-time.After(drainTimeout):
				log.Warn().Str("pipeline_id", id).Msg("Drain timed out, forcing shutdown")
			}

			log.Info().Str("pipeline_id", id).Msg("Transition Phase 2: Shutting down worker")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			if err := w.Shutdown(shutdownCtx); err != nil {
				log.Error().Err(err).Str("pipeline_id", id).Msg("Error shutting down worker")
			}
			log.Info().Str("pipeline_id", id).Msg("Transition Phase 2 Complete: Worker shut down")

			// Small stabilization delay
			select {
			case <-ctx.Done():
				return
			case <-time.After(stabilizationDelay):
			}

			m.startNewWorker(ctx, id, newCfg)
		}(oldWorker, cfg, timeouts.DrainTimeout, timeouts.ShutdownTimeout, timeouts.StabilizationDelay)
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

	// --- HEARTBEAT & SUPERVISOR GOROUTINE ---
	go m.monitorWorker(ctx, newWorker, id, cfg)
}

func (m *ConfigManager) monitorWorker(ctx context.Context, worker engine.PipelineWorker, pid string, cfg protocol.PipelineConfig) {
	// Heartbeat setup
	ticker := time.NewTicker(2 * time.Second) // More frequent for testing
	defer ticker.Stop()
	startTime := time.Now()
	workerID := fmt.Sprintf("%s-%s", pid, startTime.Format("05.000")) // Unique ID per instance

	// Supervisor setup
	crashRecoveryDelay := m.getGlobalConfig().CrashRecoveryDelay

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done(): // Extra safety: stop if manager itself stops
			return
		case <-worker.Finished():
			// When worker finishes, check if it was intentional (Transitioning)
			_, err := m.kv.Get(protocol.TransitionStateKey(pid))
			if err == nats.ErrKeyNotFound {
				// CRASH DETECTED: It finished but we aren't transitioning.
				select {
				case <-ctx.Done():
					return
				case <-time.After(crashRecoveryDelay):
					log.Error().Str("pipeline_id", pid).Msg("SUPERVISOR: Pipeline crashed unexpectedly! Restarting now...")
					// Re-fetch latest config and restart.
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

					// Synchronize cache so Watch events don't re-trigger
					m.workersMu.Lock()
					m.configs[pid] = latestCfg
					m.revisions[pid] = entry.Revision()
					m.workersMu.Unlock()

					m.startNewWorker(ctx, pid, latestCfg)
				}
			}
			return // End this monitoring goroutine
		case t := <-ticker.C:
			hb := protocol.WorkerHeartbeat{
				WorkerID:  workerID,
				Status:    "Running", // If it's ticking, it's running
				UptimeSec: int64(t.Sub(startTime).Seconds()),
				UpdatedAt: t,
			}
			data, _ := json.Marshal(hb)
			// Use the generic pipeline ID for the key so it's predictable in tests
			if _, err := m.kv.Put(protocol.WorkerHeartbeatKey(pid), data); err != nil {
				log.Warn().Err(err).Str("pipeline_id", pid).Msg("Failed to update worker heartbeat")
			}
			metrics.WorkerHeartbeat.WithLabelValues(workerID).Set(float64(t.Unix()))
		}
	}
}


func (m *ConfigManager) stopWorker(ctx context.Context, id string) {
	m.workersMu.Lock()
	w, ok := m.workers[id]
	if ok {
		delete(m.workers, id)
		delete(m.configs, id)
		delete(m.revisions, id)
	}
	m.workersMu.Unlock()

	if ok {
		log.Info().Str("pipeline_id", id).Msg("Stopping pipeline")
		// Mark as "Transitioning" so supervisor doesn't restart it
		ts := protocol.PipelineTransitionState{ID: id, Status: "Stopping", StartedAt: time.Now()}
		tsData, err := json.Marshal(ts)
		if err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Failed to marshal transition state for stop")
		} else if _, err := m.kv.Put(protocol.TransitionStateKey(id), tsData); err != nil {
			log.Warn().Err(err).Str("pipeline_id", id).Msg("Failed to set transition state for stop")
		}

		if err := w.Drain(); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Error draining worker during stop")
		}

		// Wait for drain to finish
		<-w.Finished()

		// Perform shutdown synchronously
		shutdownTimeout := m.getGlobalConfig().ShutdownTimeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := w.Shutdown(shutdownCtx); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Error shutting down worker during stop")
		}
		if err := m.kv.Delete(protocol.TransitionStateKey(id)); err != nil {
			log.Warn().Err(err).Str("pipeline_id", id).Msg("Failed to clear transition state after stop")
		}
	}
}

func (m *ConfigManager) Stop(ctx context.Context) {
	if m.cancel != nil {
		m.cancel()
	}

	m.workersMu.RLock()

	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	m.workersMu.RUnlock()

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

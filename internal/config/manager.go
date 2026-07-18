package config

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// WorkerFactory constructs a new PipelineWorker for the given pipeline ID and configuration.
// Implementations must return a fully-running worker; they must never block.
type WorkerFactory func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error)

// heartbeatSubjectPrefix is the NATS pub/sub subject prefix used for worker heartbeat
// notifications. The full subject is `heartbeats.worker.<pipeline_id>`. The pub/sub
// channel is cheap (no JetStream persistence); see the T1-31 ticket for the rationale.
const heartbeatSubjectPrefix = "heartbeats.worker."

// supervisorRevisionsKey is the NATS KV bucket key under which the manager persists the
// per-pipeline last-seen revision map on shutdown. See the T1-27 ticket for context.
const supervisorRevisionsKey = "cdc.supervisor.revisions"

// heartbeatKVWritesTotal counts every NATS KV write triggered by the worker supervisor for
// heartbeat purposes. Operators should monitor this counter to confirm the migration
// described in T1-31 (heartbeat off KV onto a pub/sub subject) is reducing write pressure.
var heartbeatKVWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_heartbeat_kv_writes_total",
	Help: "Total number of NATS KV writes triggered for worker heartbeat status updates.",
})

type ConfigManager struct {
	ctx            context.Context
	cancel         context.CancelFunc
	kv             nats.KeyValue
	factory        WorkerFactory
	workers        map[string]engine.PipelineWorker
	configs        map[string]protocol.PipelineConfig // Track current configs for comparison
	revisions      map[string]uint64                  // Track last seen revision per pipeline
	supervisors    map[string]context.CancelFunc      // Track supervisor cancel functions
	workersMu      sync.RWMutex
	globalConfig   protocol.GlobalConfig
	globalConfigMu sync.RWMutex

	// natsConn is an optional reference to a NATS connection that callers may inject
	// via SetNatsConn. When set, worker heartbeats are published via regular NATS
	// pub/sub (cheap, no persistence) at a 2s cadence; a slow KV write at a 15s
	// cadence remains for the API to render status. See T1-31.
	natsConnMu sync.RWMutex
	natsConn   *nats.Conn
}

// NewConfigManager constructs a ConfigManager backed by the supplied NATS KeyValue
// bucket. The manager defaults to KV-only heartbeats until SetNatsConn is called.
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
		supervisors:  make(map[string]context.CancelFunc),
		globalConfig: cfg,
	}
}

// SetNatsConn registers an optional NATS connection used for publishing worker
// heartbeat messages on a regular pub/sub subject. Pass nil to disable pub/sub
// heartbeats and fall back to KV-only status updates.
//
// This method is safe to call at any time after construction; it is optional and
// preserves backward compatibility with callers that do not supply a connection.
func (m *ConfigManager) SetNatsConn(nc *nats.Conn) {
	m.natsConnMu.Lock()
	defer m.natsConnMu.Unlock()
	m.natsConn = nc
}

// getNatsConn returns the currently registered NATS connection or nil when none has
// been set. The returned pointer is a snapshot; callers must not assume it remains
// valid across long-lived operations.
func (m *ConfigManager) getNatsConn() *nats.Conn {
	m.natsConnMu.RLock()
	defer m.natsConnMu.RUnlock()
	return m.natsConn
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

	// 1b. Restore supervisor last-seen revisions so out-of-order watcher events
	// inherited from a previous restart can be detected safely. See T1-27.
	if entry, err := m.kv.Get(supervisorRevisionsKey); err == nil {
		var revs map[string]uint64
		if err := json.Unmarshal(entry.Value(), &revs); err == nil && len(revs) > 0 {
			m.workersMu.Lock()
			for id, rev := range revs {
				if rev > m.revisions[id] {
					m.revisions[id] = rev
				}
			}
			m.workersMu.Unlock()
			log.Info().Int("count", len(revs)).Msg("Restored supervisor revisions from KV")
		} else if err != nil {
			log.Warn().Err(err).Msg("Failed to unmarshal supervisor revisions")
		}
	} else if err != nil && err != nats.ErrKeyNotFound {
		log.Warn().Err(err).Msg("Failed to read supervisor revisions from KV")
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
		case entry, ok := <-watcher.Updates():
			if !ok {
				log.Warn().Msg("watcher closed, exiting global config watcher")
				return
			}
			// NATS KV Watch sends a nil entry as an initial-emit sentinel
			// (e.g. when the watched key does not yet exist). The comma-ok
			// above catches channel close — these are independent cases, so
			// we keep both checks to avoid a CPU spin on close while still
			// tolerating legitimate nil entries.
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
		case entry, ok := <-watcher.Updates():
			if !ok {
				log.Warn().Msg("watcher closed, exiting pipeline config watcher")
				return
			}
			if entry == nil {
				continue
			}

			pipelineID := extractPipelineID(entry.Key())
			if pipelineID == "" {
				continue
			}

			if entry.Operation() == nats.KeyValuePurge || entry.Operation() == nats.KeyValueDelete {
				// T1-27: ignore stale delete markers whose revision does not exceed what
				// we have already observed. Otherwise an out-of-order watcher redelivery
				// could terminate a freshly-started worker that has not yet generated a
				// new revision for this pipeline.
				m.workersMu.Lock()
				lastRev := m.revisions[pipelineID]
				if entry.Revision() <= lastRev {
					m.workersMu.Unlock()
					log.Debug().
						Str("pipeline_id", pipelineID).
						Uint64("event_rev", entry.Revision()).
						Uint64("last_rev", lastRev).
						Msg("Stale pipeline delete event ignored")
					continue
				}
				m.revisions[pipelineID] = entry.Revision()
				m.workersMu.Unlock()

				log.Info().Str("pipeline_id", pipelineID).Uint64("revision", entry.Revision()).Msg("Pipeline deleted")
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

			// Smart reload: detect if ONLY Tables changed. The worker pointer is
			// captured under the WRITE lock so we never observe a torn or stale
			// reference between releasing the write lock and reacquiring read.
			// This addresses T1-28's "concurrently-confused map lookup".
			var newTables []string
			if exists && m.onlyTablesChanged(currentCfg, cfg, &newTables) {
				worker, workerExists := m.workers[pipelineID]
				m.configs[pipelineID] = cfg
				m.revisions[pipelineID] = entry.Revision()
				m.workersMu.Unlock()

				if workerExists {
					log.Info().
						Str("pipeline_id", pipelineID).
						Strs("new_tables", newTables).
						Msg("Smart reload: signaling dynamic table addition")

					// This is a conceptual method on the Worker interface.
					// The pointer was captured atomically with the maps above; it
					// remains valid even though we have released the lock.
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
	parentCtx := m.ctx
	if parentCtx == nil {
		parentCtx = ctx
	}

	m.workersMu.Lock()
	if cancel, exists := m.supervisors[id]; exists {
		cancel()
	}
	supCtx, supCancel := context.WithCancel(parentCtx)
	m.supervisors[id] = supCancel
	m.workersMu.Unlock()

	newWorker, err := m.factory(supCtx, id, cfg)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", id).Msg("Error creating worker")
		// Spawn supervisor with nil worker to trigger start retry loop (attempt = 1)
		go m.monitorWorker(supCtx, nil, id, cfg, 1)
		return
	}

	m.workersMu.Lock()
	m.workers[id] = newWorker
	m.workersMu.Unlock()
	log.Info().Str("pipeline_id", id).Msg("Successfully started new worker")

	// --- HEARTBEAT & SUPERVISOR GOROUTINE ---
	go m.monitorWorker(supCtx, newWorker, id, cfg, 0)
}

const (
	// maxBackoffAttempt caps the attempt counter for exponential backoff to prevent
	// integer overflow when shifting (1 << (attempt-1)). At attempt=63, a signed
	// int shift overflows to a negative number, causing a tight crash loop.
	// Capping at 15 limits max backoff to 2^14 * baseDelay = ~21 hours with default
	// 5s base, well beyond any practical retry scenario.
	maxBackoffAttempt = 15
	// maxBackoff is the absolute ceiling on backoff delay regardless of attempt count.
	maxBackoff = 60 * time.Second
	// fastHeartbeatInterval is the cadence at which the heartbeat goroutine fires
	// the fast (pub/sub) heartbeat. The slow KV write runs at slowHeartbeatInterval.
	fastHeartbeatInterval = 2 * time.Second
	// slowHeartbeatInterval is the cadence at which the heartbeat goroutine fires
	// the slow KV write so that the API can render status. See T1-31.
	slowHeartbeatInterval = 15 * time.Second
)

func (m *ConfigManager) getBackoffDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	// Clamp attempt to prevent shift overflow.
	if attempt > maxBackoffAttempt {
		attempt = maxBackoffAttempt
	}
	baseDelay := m.getGlobalConfig().CrashRecoveryDelay
	if baseDelay <= 0 {
		baseDelay = 5 * time.Second
	}

	// Exponential backoff: baseDelay * 2^(attempt-1)
	multiplier := 1 << (attempt - 1)
	delay := baseDelay * time.Duration(multiplier)

	if delay > maxBackoff {
		delay = maxBackoff
	}

	// Add 10% random jitter
	jitter := time.Duration(float64(delay) * 0.1 * (2*rand.Float64() - 1))
	return delay + jitter
}

// writeHeartbeatKV persists the heartbeat to NATS KV, bumps the heartbeat KV write
// counter, and updates the Prometheus gauge for visibility. It is the single
// chokepoint for KV writes emitted by the supervisor and guarantees that the
// counter increments exactly once per write (success or failure). When the write
// fails, the counter still increments so operators can correlate with NATS errors.
func (m *ConfigManager) writeHeartbeatKV(pid string, hb protocol.WorkerHeartbeat, data []byte) {
	if _, err := m.kv.Put(protocol.WorkerHeartbeatKey(pid), data); err != nil {
		log.Warn().Err(err).Str("pipeline_id", pid).Msg("Failed to update worker heartbeat in KV")
	}
	heartbeatKVWritesTotal.Inc()
	metrics.WorkerHeartbeat.WithLabelValues(hb.WorkerID).Set(float64(hb.UpdatedAt.Unix()))
}

// publishHeartbeatPS publishes the heartbeat message on a NATS pub/sub subject
// (`heartbeats.worker.<pipeline_id>`). The operation is best-effort: when no NATS
// connection has been registered, the call is a no-op so the heartbeat degrades to
// KV-only updates without panicking. See T1-31.
func (m *ConfigManager) publishHeartbeatPS(pid string, hb protocol.WorkerHeartbeat, data []byte) {
	nc := m.getNatsConn()
	if nc == nil {
		return
	}
	subject := heartbeatSubjectPrefix + pid
	if err := nc.Publish(subject, data); err != nil {
		log.Warn().Err(err).Str("subject", subject).Msg("Failed to publish worker heartbeat over pub/sub")
	}
}

func (m *ConfigManager) attemptRestart(ctx context.Context, pid string, cfg protocol.PipelineConfig, attempt int) {
	// Re-fetch latest config and restart.
	var latestCfg protocol.PipelineConfig
	var revision uint64
	entry, err := m.kv.Get(protocol.PipelineConfigKey(pid))
	if err != nil {
		log.Warn().Err(err).Str("pipeline_id", pid).Msg("SUPERVISOR: Failed to re-fetch config from KV, falling back to cached config")
		m.workersMu.RLock()
		cachedCfg, cachedExists := m.configs[pid]
		revision = m.revisions[pid]
		m.workersMu.RUnlock()
		if cachedExists {
			latestCfg = cachedCfg
		} else {
			latestCfg = cfg
		}
	} else {
		if err := json.Unmarshal(entry.Value(), &latestCfg); err != nil {
			log.Error().Err(err).Str("pipeline_id", pid).Msg("SUPERVISOR: Failed to unmarshal config")
			m.workersMu.RLock()
			cachedCfg, cachedExists := m.configs[pid]
			revision = m.revisions[pid]
			m.workersMu.RUnlock()
			if cachedExists {
				latestCfg = cachedCfg
			} else {
				latestCfg = cfg
			}
		} else {
			revision = entry.Revision()
		}
	}
	m.applyHierarchy(&latestCfg)

	// Synchronize cache so Watch events don't re-trigger
	m.workersMu.Lock()
	m.configs[pid] = latestCfg
	m.revisions[pid] = revision
	m.workersMu.Unlock()

	m.startNewWorker(ctx, pid, latestCfg)
}

func (m *ConfigManager) monitorWorker(ctx context.Context, worker engine.PipelineWorker, pid string, cfg protocol.PipelineConfig, attempt int) {
	if worker == nil {
		delay := m.getBackoffDelay(attempt)
		log.Warn().Str("pipeline_id", pid).Int("attempt", attempt).Dur("delay", delay).Msg("SUPERVISOR: Pipeline failed to start initially. Retrying with backoff...")

		// Write retry heartbeat immediately so the API surfaces the "Retrying"
		// status as soon as the supervisor enters the loop.
		now := time.Now()
		hb := protocol.WorkerHeartbeat{
			WorkerID:  fmt.Sprintf("%s-retry", pid),
			Status:    "Retrying",
			UptimeSec: 0,
			UpdatedAt: now,
		}
		data, _ := json.Marshal(hb)
		m.writeHeartbeatKV(pid, hb, data)
		m.publishHeartbeatPS(pid, hb, data)

		delayTimer := time.NewTimer(delay)
		defer delayTimer.Stop()

		// Retrying is operationally significant state; we keep the existing
		// 2s KV writes here so dashboards stay accurate even when pub/sub is
		// down. The fast ticker also fans out via pub/sub when available.
		retryKVTicker := time.NewTicker(fastHeartbeatInterval)
		defer retryKVTicker.Stop()
		retryPSTicker := time.NewTicker(fastHeartbeatInterval)
		defer retryPSTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.ctx.Done():
				return
			case <-delayTimer.C:
				m.workersMu.RLock()
				_, configExists := m.configs[pid]
				m.workersMu.RUnlock()
				if !configExists {
					log.Info().Str("pipeline_id", pid).Msg("SUPERVISOR: Pipeline config no longer exists, stopping retry loop")
					return
				}

				m.attemptRestart(ctx, pid, cfg, attempt)
				return
			case t := <-retryKVTicker.C:
				hb := protocol.WorkerHeartbeat{
					WorkerID:  fmt.Sprintf("%s-retry", pid),
					Status:    "Retrying",
					UptimeSec: 0,
					UpdatedAt: t,
				}
				data, _ := json.Marshal(hb)
				m.writeHeartbeatKV(pid, hb, data)
			case t := <-retryPSTicker.C:
				hb := protocol.WorkerHeartbeat{
					WorkerID:  fmt.Sprintf("%s-retry", pid),
					Status:    "Retrying",
					UptimeSec: 0,
					UpdatedAt: t,
				}
				data, _ := json.Marshal(hb)
				m.publishHeartbeatPS(pid, hb, data)
				metrics.WorkerHeartbeat.WithLabelValues(fmt.Sprintf("%s-retry", pid)).Set(float64(t.Unix()))
			}
		}
	}

	// Heartbeat setup. We run two tickers: a fast (2s) one that fans out via
	// pub/sub and a slow (15s) one that writes to KV so the API can read status
	// out-of-band. See T1-31 for the rationale.
	startTime := time.Now()
	workerID := fmt.Sprintf("%s-%s", pid, startTime.Format("05.000")) // Unique ID per instance

	fastTicker := time.NewTicker(fastHeartbeatInterval)
	defer fastTicker.Stop()
	slowTicker := time.NewTicker(slowHeartbeatInterval)
	defer slowTicker.Stop()

	// Write initial heartbeat immediately so the API renders "Running" right
	// after the worker is registered.
	hb := protocol.WorkerHeartbeat{
		WorkerID:  workerID,
		Status:    "Running",
		UptimeSec: 0,
		UpdatedAt: startTime,
	}
	data, _ := json.Marshal(hb)
	m.writeHeartbeatKV(pid, hb, data)
	m.publishHeartbeatPS(pid, hb, data)

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done(): // Extra safety: stop if manager itself stops
			return
		case <-worker.Finished():
			// When worker finishes, check if it was intentional (Transitioning/Stopping)
			m.workersMu.RLock()
			currentWorker, exists := m.workers[pid]
			m.workersMu.RUnlock()

			if !exists || currentWorker != worker {
				// Stopped or replaced by another worker instance
				return
			}

			_, err := m.kv.Get(protocol.TransitionStateKey(pid))
			// If key not found (no transition active) OR if we can't contact NATS, we treat as crash
			if err == nats.ErrKeyNotFound || err != nil {
				// CRASH DETECTED: It finished but we aren't transitioning.
				// Clear it from the workers map immediately since it's dead
				m.workersMu.Lock()
				if m.workers[pid] == worker {
					delete(m.workers, pid)
				}
				m.workersMu.Unlock()

				uptime := time.Since(startTime)
				var nextAttempt int
				if uptime >= 10*time.Second {
					log.Info().Str("pipeline_id", pid).Msg("SUPERVISOR: Worker crashed after stabilization period. Resetting backoff counter.")
					nextAttempt = 1
				} else {
					nextAttempt = attempt + 1
				}

				delay := m.getBackoffDelay(nextAttempt)
				log.Error().Str("pipeline_id", pid).Int("next_attempt", nextAttempt).Dur("delay", delay).Msg("SUPERVISOR: Pipeline crashed unexpectedly! Restarting with backoff...")

				// Write retry heartbeat immediately
				now := time.Now()
				hb := protocol.WorkerHeartbeat{
					WorkerID:  fmt.Sprintf("%s-retry", pid),
					Status:    "Retrying",
					UptimeSec: 0,
					UpdatedAt: now,
				}
				data, _ := json.Marshal(hb)
				m.writeHeartbeatKV(pid, hb, data)
				m.publishHeartbeatPS(pid, hb, data)

				delayTimer := time.NewTimer(delay)
				defer delayTimer.Stop()

				// Retrying is operationally significant state; we keep the
				// existing 2s KV cadence here so dashboards stay accurate.
				restartKVTicker := time.NewTicker(fastHeartbeatInterval)
				defer restartKVTicker.Stop()
				restartPSTicker := time.NewTicker(fastHeartbeatInterval)
				defer restartPSTicker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-m.ctx.Done():
						return
					case <-delayTimer.C:
						m.workersMu.RLock()
						_, configExists := m.configs[pid]
						m.workersMu.RUnlock()
						if !configExists {
							log.Info().Str("pipeline_id", pid).Msg("SUPERVISOR: Pipeline config no longer exists, stopping retry loop")
							return
						}

						m.attemptRestart(ctx, pid, cfg, nextAttempt)
						return
					case t := <-restartKVTicker.C:
						hb := protocol.WorkerHeartbeat{
							WorkerID:  fmt.Sprintf("%s-retry", pid),
							Status:    "Retrying",
							UptimeSec: 0,
							UpdatedAt: t,
						}
						data, _ := json.Marshal(hb)
						m.writeHeartbeatKV(pid, hb, data)
					case t := <-restartPSTicker.C:
						hb := protocol.WorkerHeartbeat{
							WorkerID:  fmt.Sprintf("%s-retry", pid),
							Status:    "Retrying",
							UptimeSec: 0,
							UpdatedAt: t,
						}
						data, _ := json.Marshal(hb)
						m.publishHeartbeatPS(pid, hb, data)
						metrics.WorkerHeartbeat.WithLabelValues(fmt.Sprintf("%s-retry", pid)).Set(float64(t.Unix()))
					}
				}
			}
			return // End this monitoring goroutine
		case t := <-fastTicker.C:
			hb := protocol.WorkerHeartbeat{
				WorkerID:  workerID,
				Status:    "Running",
				UptimeSec: int64(t.Sub(startTime).Seconds()),
				UpdatedAt: t,
			}
			data, _ := json.Marshal(hb)
			// Fast path: pub/sub only, no KV write.
			m.publishHeartbeatPS(pid, hb, data)
			metrics.WorkerHeartbeat.WithLabelValues(workerID).Set(float64(t.Unix()))
		case t := <-slowTicker.C:
			hb := protocol.WorkerHeartbeat{
				WorkerID:  workerID,
				Status:    "Running",
				UptimeSec: int64(t.Sub(startTime).Seconds()),
				UpdatedAt: t,
			}
			data, _ := json.Marshal(hb)
			// Slow path: KV write for API status rendering.
			m.writeHeartbeatKV(pid, hb, data)
			metrics.WorkerHeartbeat.WithLabelValues(workerID).Set(float64(t.Unix()))
		}
	}
}

func (m *ConfigManager) InternalCrashWorker(ctx context.Context, id string) {
	m.workersMu.RLock()
	w, ok := m.workers[id]
	m.workersMu.RUnlock()

	if ok {
		log.Warn().Str("pipeline_id", id).Msg("CRASHING pipeline (SIMULATED)")
		// We DO NOT set TransitionState, which will trigger the supervisor to restart it.
		// We call Shutdown directly without Drain to simulate a sudden failure.
		// Important: We do NOT delete it from the map here, the supervisor will handle the restart
		// and replace the map entry.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.Shutdown(shutdownCtx); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("Error during simulated crash shutdown")
		}
	}
}

func (m *ConfigManager) stopWorker(ctx context.Context, id string) {
	m.workersMu.Lock()
	w, ok := m.workers[id]
	delete(m.workers, id)
	delete(m.configs, id)
	delete(m.revisions, id)
	if cancel, exists := m.supervisors[id]; exists {
		cancel()
		delete(m.supervisors, id)
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

		// T1-30: bound the drain wait so a hung worker cannot leak the supervisor
		// goroutine forever. The drain timeout mirrors the global config so
		// operators have a single knob; if a worker is intentionally cancelled via
		// the supervisor context we still proceed to shutdown rather than block.
		drainTimeout := m.getGlobalConfig().DrainTimeout
		waitTimer := time.NewTimer(drainTimeout)
		defer waitTimer.Stop()
		select {
		case <-w.Finished():
			log.Info().Str("pipeline_id", id).Msg("Worker drained successfully")
		case <-waitTimer.C:
			log.Warn().Str("pipeline_id", id).Dur("drain_timeout", drainTimeout).Msg("Drain timed out, proceeding to shutdown")
		case <-ctx.Done():
			log.Warn().Str("pipeline_id", id).Msg("Drain interrupted by context cancellation; proceeding to shutdown")
		}

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

	// T1-27: persist the last-seen revisions snapshot so that a subsequent
	// process restart can detect out-of-order watcher events. Best-effort
	// because Stop should never fail-fast on observability writes.
	m.workersMu.RLock()
	revisionsSnapshot := make(map[string]uint64, len(m.revisions))
	for id, rev := range m.revisions {
		revisionsSnapshot[id] = rev
	}
	m.workersMu.RUnlock()
	if len(revisionsSnapshot) > 0 {
		if data, err := json.Marshal(revisionsSnapshot); err == nil {
			if _, err := m.kv.Put(supervisorRevisionsKey, data); err != nil {
				log.Warn().Err(err).Msg("Failed to persist supervisor revisions")
			}
		} else {
			log.Warn().Err(err).Msg("Failed to marshal supervisor revisions")
		}
	}

	m.workersMu.Lock()
	for _, cancel := range m.supervisors {
		cancel()
	}
	m.supervisors = make(map[string]context.CancelFunc)
	m.workersMu.Unlock()

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

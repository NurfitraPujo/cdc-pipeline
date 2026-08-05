package config

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestMain(m *testing.M) {
	logger.Init("debug", true)
	m.Run()
}

type MockWorker struct {
	id        string
	finished  chan struct{}
	closeOnce sync.Once
	drained   bool
	shutdown  bool
	cfg       protocol.PipelineConfig
}

func (m *MockWorker) ID() string { return m.id }
func (m *MockWorker) Drain() error {
	log.Info().Str("pipeline_id", m.id).Msg("MockWorker Drain called")
	m.drained = true
	go func() {
		time.Sleep(50 * time.Millisecond)
		m.closeOnce.Do(func() {
			close(m.finished)
		})
	}()
	return nil
}
func (m *MockWorker) Finished() <-chan struct{} { return m.finished }
func (m *MockWorker) Shutdown(ctx context.Context) error {
	log.Info().Str("pipeline_id", m.id).Msg("MockWorker Shutdown called")
	m.shutdown = true
	return nil
}
func (m *MockWorker) SignalDynamicTables(tables []string) {
	log.Info().Str("pipeline_id", m.id).Int("num_tables", len(tables)).Msg("MockWorker SignalDynamicTables called")
}

func TestConfigManager_Transitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	// 1. Start NATS Container
	natsC, err := tc_nats.Run(ctx,
		"nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-js"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	bucket := protocol.KVBucketName
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	var workerCount int32
	factory := func(ctx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		log.Info().
			Int32("worker_count", atomic.LoadInt32(&workerCount)+1).
			Str("pipeline_id", id).
			Int("batch_size", cfg.BatchSize).
			Msg("Mock Factory creating worker")
		atomic.AddInt32(&workerCount, 1)
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}

	mgr := NewConfigManager(kv, factory)
	mgrCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Watch(mgrCtx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	// 1. Set Global Config
	globalCfg := protocol.GlobalConfig{BatchSize: 500, BatchWait: 2 * time.Second}
	gData, _ := json.Marshal(globalCfg)
	kv.Put(protocol.KeyGlobalConfig, gData)
	time.Sleep(500 * time.Millisecond)

	// 2. Trigger initial start (No overrides)
	log.Info().Msg("Test: Sending initial config (no overrides)")
	cfg := protocol.PipelineConfig{ID: "p1", Name: "Test Pipeline", Sources: []string{"src1"}, Sinks: []string{"sink1"}}
	data, _ := json.Marshal(cfg)
	kv.Put(protocol.PipelineConfigKey("p1"), data)

	// Wait for start
	time.Sleep(1500 * time.Millisecond)
	if atomic.LoadInt32(&workerCount) < 1 {
		t.Fatalf("Expected at least 1 worker to be started, got %d", atomic.LoadInt32(&workerCount))
	}

	// Verify global defaults applied
	mgr.workersMu.RLock()
	w1, ok := mgr.workers["p1"].(*MockWorker)
	if !ok || w1 == nil {
		mgr.workersMu.RUnlock()
		t.Fatalf("Pipeline p1 not found or nil")
	}
	if w1.cfg.BatchSize != 500 {
		t.Errorf("Expected BatchSize 500 from global, got %d", w1.cfg.BatchSize)
	}
	mgr.workersMu.RUnlock()

	// 3. Trigger update (With override)
	// We sleep long enough to avoid race with handleGlobalUpdates's 2s sleep
	log.Info().Msg("Test: Waiting for potential global reload to settle...")
	time.Sleep(3000 * time.Millisecond)

	log.Info().Msg("Test: Sending updated config (with BatchSize override)")
	cfg.BatchSize = 999
	data, _ = json.Marshal(cfg)
	kv.Put(protocol.PipelineConfigKey("p1"), data)

	// Wait for transition
	time.Sleep(4000 * time.Millisecond)

	// Verify override applied
	mgr.workersMu.RLock()
	w2, ok := mgr.workers["p1"].(*MockWorker)
	if !ok || w2 == nil {
		mgr.workersMu.RUnlock()
		t.Fatalf("Pipeline p1 not found or nil after update")
	}
	if w2.cfg.BatchSize != 999 {
		t.Errorf("Expected BatchSize 999 from override, got %d", w2.cfg.BatchSize)
	}
	mgr.workersMu.RUnlock()

	// 4. Test Stop
	log.Info().Msg("Test: Stopping manager")
	mgr.Stop(ctx)

	mgr.workersMu.RLock()
	if len(mgr.workers) != 0 {
		t.Errorf("Expected 0 workers after Stop, got %d", len(mgr.workers))
	}
	mgr.workersMu.RUnlock()
}

func TestConfigManager_RetrySupervisor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	// 1. Start NATS Container
	natsC, err := tc_nats.Run(ctx,
		"nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-js"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	bucket := protocol.KVBucketName
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	// Set Global Config with short recovery delay for fast test
	globalCfg := protocol.GlobalConfig{
		BatchSize:          500,
		BatchWait:          2 * time.Second,
		CrashRecoveryDelay: 100 * time.Millisecond,
	}
	gData, _ := json.Marshal(globalCfg)
	kv.Put(protocol.KeyGlobalConfig, gData)
	time.Sleep(100 * time.Millisecond)

	// We want to simulate factory failures:
	// - First 2 times it returns error.
	// - 3rd time it succeeds.
	var factoryCalls int32
	var createdWorkers int32

	factory := func(ctx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		calls := atomic.AddInt32(&factoryCalls, 1)
		if calls <= 2 {
			return nil, fmt.Errorf("simulated temporary factory error %d", calls)
		}
		atomic.AddInt32(&createdWorkers, 1)
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}

	mgr := NewConfigManager(kv, factory)
	mgrCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Watch(mgrCtx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	cfg := protocol.PipelineConfig{ID: "p-retry", Name: "Test Pipeline Retry", Sources: []string{"src1"}, Sinks: []string{"sink1"}}
	data, _ := json.Marshal(cfg)
	kv.Put(protocol.PipelineConfigKey("p-retry"), data)

	// Wait and check that supervisor retried and eventually succeeded
	time.Sleep(1500 * time.Millisecond)

	calls := atomic.LoadInt32(&factoryCalls)
	workers := atomic.LoadInt32(&createdWorkers)
	if calls < 3 {
		t.Errorf("Expected at least 3 factory calls, got %d", calls)
	}
	if workers != 1 {
		t.Errorf("Expected exactly 1 successfully created worker, got %d", workers)
	}

	// Verify that the running worker is registered
	mgr.workersMu.RLock()
	w, ok := mgr.workers["p-retry"].(*MockWorker)
	mgr.workersMu.RUnlock()
	if !ok || w == nil {
		t.Fatalf("Worker not found or nil")
	}

	// Verify that the heartbeat was updated to "Running" eventually
	entry, err := kv.Get(protocol.WorkerHeartbeatKey("p-retry"))
	if err != nil {
		t.Fatalf("Failed to get heartbeat: %v", err)
	}
	var hb protocol.WorkerHeartbeat
	if err := json.Unmarshal(entry.Value(), &hb); err != nil {
		t.Fatalf("Failed to unmarshal heartbeat: %v", err)
	}
	if hb.Status != "Running" {
		t.Errorf("Expected heartbeat status 'Running', got %q", hb.Status)
	}
}

func TestConfigManager_CrashAndRetryFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	// Start NATS Container
	natsC, err := tc_nats.Run(ctx,
		"nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-js"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	bucket := protocol.KVBucketName
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	globalCfg := protocol.GlobalConfig{
		BatchSize:          500,
		BatchWait:          2 * time.Second,
		CrashRecoveryDelay: 100 * time.Millisecond,
	}
	gData, _ := json.Marshal(globalCfg)
	kv.Put(protocol.KeyGlobalConfig, gData)
	time.Sleep(100 * time.Millisecond)

	var factoryCalls int32
	var workersCreated int32
	var failRestart int32 = 1 // Flag to fail the next restart
	var activeMockWorker *MockWorker

	factory := func(ctx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		calls := atomic.AddInt32(&factoryCalls, 1)
		if calls == 1 {
			// First start succeeds
			activeMockWorker = &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}
			atomic.AddInt32(&workersCreated, 1)
			return activeMockWorker, nil
		}
		// Second start fails
		if atomic.LoadInt32(&failRestart) == 1 {
			return nil, fmt.Errorf("simulated restart failure")
		}
		// Third start succeeds
		activeMockWorker = &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}
		atomic.AddInt32(&workersCreated, 1)
		return activeMockWorker, nil
	}

	mgr := NewConfigManager(kv, factory)
	mgrCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Watch(mgrCtx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	cfg := protocol.PipelineConfig{ID: "p-crash", Name: "Test Pipeline Crash", Sources: []string{"src1"}, Sinks: []string{"sink1"}}
	data, _ := json.Marshal(cfg)
	kv.Put(protocol.PipelineConfigKey("p-crash"), data)

	// Wait for start
	time.Sleep(300 * time.Millisecond)

	// Simulate crash of the first worker
	if activeMockWorker == nil {
		t.Fatalf("activeMockWorker is nil; worker never started (check config validation)")
	}
	activeMockWorker.Drain() // This closes finished channel in mock

	// Wait for retry attempt (which fails)
	time.Sleep(500 * time.Millisecond)

	// Check that we retried but failed to start (so no active worker in mgr.workers)
	mgr.workersMu.RLock()
	_, running := mgr.workers["p-crash"]
	mgr.workersMu.RUnlock()
	if running {
		t.Errorf("Expected worker not to be running after failed restart")
	}

	// Verify KV heartbeat status is "Retrying"
	entry, err := kv.Get(protocol.WorkerHeartbeatKey("p-crash"))
	if err == nil {
		var hb protocol.WorkerHeartbeat
		if err := json.Unmarshal(entry.Value(), &hb); err == nil {
			if hb.Status != "Retrying" {
				t.Errorf("Expected heartbeat status 'Retrying' during retry loop, got %q", hb.Status)
			}
		}
	}

	// Now allow restart to succeed
	atomic.StoreInt32(&failRestart, 0)

	// Wait for next retry attempt to succeed
	time.Sleep(1000 * time.Millisecond)

	mgr.workersMu.RLock()
	_, running = mgr.workers["p-crash"]
	mgr.workersMu.RUnlock()
	if !running {
		t.Errorf("Expected worker to be running after successful restart")
	}
}

func TestGetBackoffDelayOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	bucket := protocol.KVBucketName
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	factory := func(ctx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}
	mgr := NewConfigManager(kv, factory)

	// Verify that extremely large attempt values do not cause overflow.
	for _, attempt := range []int{63, 64, 100, 1000} {
		delay := mgr.getBackoffDelay(attempt)
		if delay < 0 {
			t.Errorf("getBackoffDelay(%d) returned negative duration: %v", attempt, delay)
		}
		if delay > 60*time.Second {
			t.Errorf("getBackoffDelay(%d) exceeded 60s cap: %v", attempt, delay)
		}
	}

	// Sanity-check boundary: attempt=1 should be roughly baseDelay (5s) without capping.
	delay1 := mgr.getBackoffDelay(1)
	if delay1 <= 0 {
		t.Errorf("getBackoffDelay(1) should be positive, got %v", delay1)
	}

	// attempt=15 (maxBackoffAttempt) must not overflow.
	delay15 := mgr.getBackoffDelay(15)
	if delay15 <= 0 {
		t.Errorf("getBackoffDelay(15) should be positive, got %v", delay15)
	}
	if delay15 > 60*time.Second {
		t.Errorf("getBackoffDelay(15) exceeded 60s cap: %v", delay15)
	}
}

// TestConfigManager_HonoursDesiredState is WS-1's core assertion: ConfigManager
// must not assume every configured pipeline should be running. A pipeline
// written with desired_state=paused must never get a worker, and a running
// pipeline whose desired_state flips to stopped must have its worker torn
// down without ConfigManager treating that as a crash needing a restart.
func TestConfigManager_HonoursDesiredState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: protocol.KVBucketName})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	var workerCount int32
	factory := func(_ context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		atomic.AddInt32(&workerCount, 1)
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}

	mgr := NewConfigManager(kv, factory)
	mgrCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Watch(mgrCtx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	// 1. A pipeline configured paused from the start must never get a worker.
	pausedCfg := protocol.PipelineConfig{
		ID: "paused1", Name: "Paused Pipeline",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
		DesiredState: protocol.DesiredStatePaused,
	}
	data, _ := json.Marshal(pausedCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey("paused1"), data); err != nil {
		t.Fatalf("Failed to put paused pipeline config: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists := mgr.workers["paused1"]
	mgr.workersMu.RUnlock()
	if exists {
		t.Errorf("expected no worker for a pipeline configured desired_state=paused")
	}
	if atomic.LoadInt32(&workerCount) != 0 {
		t.Errorf("expected 0 workers started, got %d", atomic.LoadInt32(&workerCount))
	}

	// 2. A running pipeline whose desired_state flips to stopped must have
	// its worker torn down.
	runningCfg := protocol.PipelineConfig{
		ID: "run1", Name: "Running Pipeline",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
	}
	data, _ = json.Marshal(runningCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey("run1"), data); err != nil {
		t.Fatalf("Failed to put running pipeline config: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists = mgr.workers["run1"]
	mgr.workersMu.RUnlock()
	if !exists {
		t.Fatalf("expected a worker for the running pipeline before it is stopped")
	}

	runningCfg.DesiredState = protocol.DesiredStateStopped
	data, _ = json.Marshal(runningCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey("run1"), data); err != nil {
		t.Fatalf("Failed to put desired_state=stopped update: %v", err)
	}
	time.Sleep(2000 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists = mgr.workers["run1"]
	mgr.workersMu.RUnlock()
	if exists {
		t.Errorf("expected worker to be torn down after desired_state changed to stopped")
	}
}

// TestConfigManager_PauseDuringTransition reproduces the WS-1 validator
// finding: pausing a pipeline while transitionWorker's async two-phase
// goroutine is in flight must not let that goroutine's startNewWorker call
// resurrect the paused pipeline. Before the fix, honourDesiredState was only
// consulted at the watcher call sites (handlePipelineUpdates/
// reloadAllWorkers), so a pause landing mid-transition raced a
// pre-pause-config startNewWorker call that was not gated on desired_state
// at all.
func TestConfigManager_PauseDuringTransition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: protocol.KVBucketName})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	factory := func(_ context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}

	mgr := NewConfigManager(kv, factory)
	// Shrink StabilizationDelay so the test doesn't need to wait on the
	// production default, while still leaving a window to race the pause
	// against transitionWorker's goroutine.
	mgr.globalConfig.StabilizationDelay = 800 * time.Millisecond
	mgrCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Watch(mgrCtx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	// 1. Start a running pipeline.
	cfg := protocol.PipelineConfig{
		ID: "race1", Name: "Race Pipeline",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
	}
	data, _ := json.Marshal(cfg)
	if _, err := kv.Put(protocol.PipelineConfigKey("race1"), data); err != nil {
		t.Fatalf("Failed to put initial config: %v", err)
	}
	time.Sleep(1000 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists := mgr.workers["race1"]
	mgr.workersMu.RUnlock()
	if !exists {
		t.Fatalf("expected initial worker for race1 before triggering a transition")
	}

	// 2. Trigger a config-driven transition (existing worker -> two-phase
	// drain/shutdown/stabilize/startNewWorker goroutine).
	cfg.BatchSize = 777
	data, _ = json.Marshal(cfg)
	if _, err := kv.Put(protocol.PipelineConfigKey("race1"), data); err != nil {
		t.Fatalf("Failed to put update triggering transition: %v", err)
	}

	// 3. Immediately pause the pipeline, landing inside the transition
	// goroutine's stabilization window (drain+shutdown settle in well under
	// 800ms; the pause below arrives at t=~200ms).
	time.Sleep(200 * time.Millisecond)
	cfg.DesiredState = protocol.DesiredStatePaused
	data, _ = json.Marshal(cfg)
	if _, err := kv.Put(protocol.PipelineConfigKey("race1"), data); err != nil {
		t.Fatalf("Failed to put pause update: %v", err)
	}

	// 4. Wait past the stabilization delay so the in-flight transition
	// goroutine's startNewWorker call has had a chance to fire, then assert
	// no worker was resurrected for the paused pipeline.
	time.Sleep(2000 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists = mgr.workers["race1"]
	mgr.workersMu.RUnlock()
	if exists {
		t.Errorf("expected no worker for race1 after it was paused mid-transition, but one exists")
	}
}

// TestConfigManager_StartNewWorkerBackstop_RejectsDirectRunning is RM-1's
// manager-layer backstop test (plan section 4.4 invariant 1, "Running is
// never entered from Stopped or NeedsResnapshot without passing through
// Snapshotting"). It writes the lifecycle record (protocol.LifecycleStateKey)
// directly to KV, bypassing the API layer's own RM-1 check entirely, so this
// proves startNewWorker's own defence holds even when nothing upstream of it
// enforces the invariant -- exactly the "even if the API check is bypassed"
// scenario the fix is required to cover.
func TestConfigManager_StartNewWorkerBackstop_RejectsDirectRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: protocol.KVBucketName})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	var workerCount int32
	factory := func(_ context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		atomic.AddInt32(&workerCount, 1)
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}

	for _, tc := range []struct {
		name  string
		state protocol.State
	}{
		{"Stopped", protocol.StateStopped},
		{"NeedsResnapshot", protocol.StateNeedsResnapshot},
		// Paused: the transition table routes Paused->EventStart through
		// Resuming, gated on the SlotAlive guard -- a plain worker start
		// out of Paused would resume from a possibly-dead slot with no
		// guard at all, which is the scenario this backstop must also
		// cover.
		{"Paused", protocol.StatePaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := NewConfigManager(kv, factory)
			mgrCtx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := mgr.Watch(mgrCtx); err != nil {
				t.Fatalf("Failed to start watcher: %v", err)
			}

			id := "backstop-" + tc.name

			// Lifecycle record forbids a direct Running -- written straight
			// to KV, not through protocol.Transition, deliberately: this
			// test is simulating "the API-layer check got bypassed", not
			// exercising a legal lifecycle move.
			rec := protocol.PipelineLifecycleRecord{State: tc.state, UpdatedAt: time.Now()}
			recData, _ := json.Marshal(rec)
			if _, err := kv.Put(protocol.LifecycleStateKey(id), recData); err != nil {
				t.Fatalf("Failed to put lifecycle record: %v", err)
			}

			// desired_state=running (the zero value) reaches startNewWorker
			// via the normal config-watch path -- this is the exact shape
			// invariant 1 says must never produce a plain worker.
			cfg := protocol.PipelineConfig{
				ID: id, Name: "Backstop Pipeline",
				Sources: []string{"src1"}, Sinks: []string{"sink1"},
			}
			data, _ := json.Marshal(cfg)
			if _, err := kv.Put(protocol.PipelineConfigKey(id), data); err != nil {
				t.Fatalf("Failed to put pipeline config: %v", err)
			}

			time.Sleep(1500 * time.Millisecond)

			mgr.workersMu.RLock()
			_, exists := mgr.workers[id]
			mgr.workersMu.RUnlock()
			if exists {
				t.Errorf("expected no worker for a pipeline whose lifecycle record is %s, but one was started", tc.state)
			}

			mgr.Stop(ctx)
		})
	}

	if atomic.LoadInt32(&workerCount) != 0 {
		t.Errorf("expected 0 workers started across Stopped/NeedsResnapshot/Paused cases, got %d", atomic.LoadInt32(&workerCount))
	}
}

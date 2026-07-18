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
	defer natsC.Terminate(ctx)

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
	cfg := protocol.PipelineConfig{ID: "p1", Name: "Test Pipeline"}
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
	defer natsC.Terminate(ctx)

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

	cfg := protocol.PipelineConfig{ID: "p-retry", Name: "Test Pipeline Retry"}
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
	defer natsC.Terminate(ctx)

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

	cfg := protocol.PipelineConfig{ID: "p-crash", Name: "Test Pipeline Crash"}
	data, _ := json.Marshal(cfg)
	kv.Put(protocol.PipelineConfigKey("p-crash"), data)

	// Wait for start
	time.Sleep(300 * time.Millisecond)

	// Simulate crash of the first worker
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
	defer natsC.Terminate(ctx)

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

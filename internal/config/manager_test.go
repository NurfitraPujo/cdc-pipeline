package config

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

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
	log.Printf("MockWorker %s Drain called", m.id)
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
	log.Printf("MockWorker %s Shutdown called", m.id)
	m.shutdown = true
	return nil
}

func TestConfigManager_Transitions(t *testing.T) {
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
		log.Printf("Mock Factory creating worker %d for %s with BatchSize %d", atomic.LoadInt32(&workerCount)+1, id, cfg.BatchSize)
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
	log.Println("Test: Sending initial config (no overrides)")
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
	log.Println("Test: Waiting for potential global reload to settle...")
	time.Sleep(3000 * time.Millisecond)

	log.Println("Test: Sending updated config (with BatchSize override)")
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
	log.Println("Test: Stopping manager")
	mgr.Stop(ctx)
	
	mgr.workersMu.RLock()
	if len(mgr.workers) != 0 {
		t.Errorf("Expected 0 workers after Stop, got %d", len(mgr.workers))
	}
	mgr.workersMu.RUnlock()
}

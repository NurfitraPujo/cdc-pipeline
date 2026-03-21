package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
)

type MockWorker struct {
	id       string
	finished chan struct{}
	drained  bool
	shutdown bool
}

func (m *MockWorker) ID() string { return m.id }
func (m *MockWorker) Drain() error {
	log.Printf("MockWorker %s Drain called", m.id)
	m.drained = true
	// Simulate async finishing
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(m.finished)
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
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Skip("NATS not running, skipping integration test")
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	bucket := fmt.Sprintf("test_config_%d", time.Now().UnixNano())
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}
	defer js.DeleteKeyValue(bucket)

	var workerCount int32
	factory := func(ctx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		log.Printf("Mock Factory creating worker %d for %s", atomic.LoadInt32(&workerCount)+1, id)
		atomic.AddInt32(&workerCount, 1)
		return &MockWorker{id: id, finished: make(chan struct{})}, nil
	}

	mgr := NewConfigManager(kv, factory)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Watch(ctx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	// 1. Trigger initial start
	log.Println("Test: Sending initial config")
	cfg := protocol.PipelineConfig{ID: "p1", Name: "Test Pipeline"}
	data, _ := json.Marshal(cfg)
	if _, err := kv.Put("pipelines.p1.config", data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Wait for start
	time.Sleep(1000 * time.Millisecond)
	if atomic.LoadInt32(&workerCount) != 1 {
		t.Errorf("Expected 1 worker to be started, got %d", atomic.LoadInt32(&workerCount))
	}

	// 2. Trigger update (transition)
	log.Println("Test: Sending updated config")
	cfg.Name = "Updated Pipeline"
	data, _ = json.Marshal(cfg)
	if _, err := kv.Put("pipelines.p1.config", data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Wait for transition (Drain -> Shutdown -> Start New)
	time.Sleep(2000 * time.Millisecond)
	if atomic.LoadInt32(&workerCount) != 2 {
		t.Errorf("Expected 2 total workers (1 initial + 1 after transition), got %d", atomic.LoadInt32(&workerCount))
	}
}

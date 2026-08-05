package config

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

// TestConfigManager_FinalizeStop_DropsSlotAndTransitionsToStopped exercises
// WS-5's end-to-end stop path: a lifecycle record already at Stopping (as
// StopPipeline, internal/api/handler.go, writes before flipping
// desired_state), a config flip to desired_state=stopped, and asserts that
// once the worker has drained, finalizeStop (via honourDesiredState) calls
// the installed SlotDropper and persists Stopping -> Stopped.
func TestConfigManager_FinalizeStop_DropsSlotAndTransitionsToStopped(t *testing.T) {
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

	var dropCalls int32
	mgr.SetSlotDropper(func(_ context.Context, _ string, _ protocol.PipelineConfig) error {
		atomic.AddInt32(&dropCalls, 1)
		return nil
	})

	if err := mgr.Watch(ctx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	pipelineID := "stop1"
	runningCfg := protocol.PipelineConfig{
		ID: pipelineID, Name: "Stop me",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
	}
	data, _ := json.Marshal(runningCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey(pipelineID), data); err != nil {
		t.Fatalf("Failed to put running pipeline config: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists := mgr.workers[pipelineID]
	mgr.workersMu.RUnlock()
	if !exists {
		t.Fatalf("expected a worker for the running pipeline before stopping it")
	}

	// Mirror StopPipeline's write ordering: lifecycle record (Stopping)
	// before desired_state, so finalizeStop's guard finds what it expects.
	if err := mgr.putLifecycleRecord(pipelineID, protocol.PipelineLifecycleRecord{
		State: protocol.StateStopping, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Failed to seed Stopping lifecycle record: %v", err)
	}

	runningCfg.DesiredState = protocol.DesiredStateStopped
	data, _ = json.Marshal(runningCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey(pipelineID), data); err != nil {
		t.Fatalf("Failed to put desired_state=stopped update: %v", err)
	}
	time.Sleep(2000 * time.Millisecond)

	mgr.workersMu.RLock()
	_, exists = mgr.workers[pipelineID]
	mgr.workersMu.RUnlock()
	if exists {
		t.Errorf("expected worker to be torn down after desired_state changed to stopped")
	}

	if got := atomic.LoadInt32(&dropCalls); got != 1 {
		t.Errorf("expected the slot dropper to be called exactly once, got %d", got)
	}

	rec, ok := mgr.getLifecycleRecord(pipelineID)
	if !ok {
		t.Fatalf("expected a persisted lifecycle record after finalizeStop")
	}
	if rec.State != protocol.StateStopped {
		t.Errorf("expected lifecycle state Stopped after finalizeStop, got %s", rec.State)
	}
}

// TestConfigManager_FinalizeStop_SkipsWhenRecordNotStopping asserts
// finalizeStop's no-op guard: a desired_state=stopped config whose
// lifecycle record was never advanced to Stopping (e.g. a config authored
// directly, bypassing StopPipeline) must not have its slot dropped or its
// lifecycle record touched -- only StopPipeline's own Stopping record may
// hand off to finalizeStop.
func TestConfigManager_FinalizeStop_SkipsWhenRecordNotStopping(t *testing.T) {
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

	var dropCalls int32
	mgr.SetSlotDropper(func(_ context.Context, _ string, _ protocol.PipelineConfig) error {
		atomic.AddInt32(&dropCalls, 1)
		return nil
	})

	pipelineID := "stop-direct"
	cfg := protocol.PipelineConfig{
		ID: pipelineID, Name: "Directly authored stopped config",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
		DesiredState: protocol.DesiredStateStopped,
	}
	data, _ := json.Marshal(cfg)
	if _, err := kv.Put(protocol.PipelineConfigKey(pipelineID), data); err != nil {
		t.Fatalf("Failed to put config: %v", err)
	}

	if err := mgr.Watch(ctx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	if got := atomic.LoadInt32(&dropCalls); got != 0 {
		t.Errorf("expected the slot dropper NOT to be called without a prior Stopping record, got %d calls", got)
	}
	if _, ok := mgr.getLifecycleRecord(pipelineID); ok {
		t.Errorf("expected no lifecycle record to be written for a config that never went through StopPipeline")
	}
}

// TestConfigManager_FinalizeStop_DropFailureLandsOnFailed is the RM-2
// regression: a slot-drop failure must not leave the pipeline wedged at
// Stopping forever (there is no "next reconcile" once desired_state is
// already "stopped" -- see finalizeStop's doc comment). It must instead
// land on StateFailed, with the drop error captured in Reason, so that
// POST /start ({Failed, start}, internal/api/handler.go) can recover it.
func TestConfigManager_FinalizeStop_DropFailureLandsOnFailed(t *testing.T) {
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

	var dropCalls int32
	mgr.SetSlotDropper(func(_ context.Context, _ string, _ protocol.PipelineConfig) error {
		atomic.AddInt32(&dropCalls, 1)
		return fmt.Errorf("pg: replication slot is active")
	})

	if err := mgr.Watch(ctx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	pipelineID := "stop-drop-fails"
	runningCfg := protocol.PipelineConfig{
		ID: pipelineID, Name: "Stop me but drop fails",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
	}
	data, _ := json.Marshal(runningCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey(pipelineID), data); err != nil {
		t.Fatalf("Failed to put running pipeline config: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	// Mirror StopPipeline's write ordering: lifecycle record (Stopping)
	// before desired_state, so finalizeStop's guard finds what it expects.
	if err := mgr.putLifecycleRecord(pipelineID, protocol.PipelineLifecycleRecord{
		State: protocol.StateStopping, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Failed to seed Stopping lifecycle record: %v", err)
	}

	runningCfg.DesiredState = protocol.DesiredStateStopped
	data, _ = json.Marshal(runningCfg)
	if _, err := kv.Put(protocol.PipelineConfigKey(pipelineID), data); err != nil {
		t.Fatalf("Failed to put desired_state=stopped update: %v", err)
	}
	time.Sleep(2000 * time.Millisecond)

	if got := atomic.LoadInt32(&dropCalls); got != 1 {
		t.Errorf("expected the slot dropper to be called exactly once, got %d", got)
	}

	rec, ok := mgr.getLifecycleRecord(pipelineID)
	if !ok {
		t.Fatalf("expected a persisted lifecycle record after finalizeStop's failure path")
	}
	if rec.State != protocol.StateFailed {
		t.Errorf("expected lifecycle state Failed after a slot-drop failure, got %s (a permanently wedged Stopping is the RM-2 bug)", rec.State)
	}
	if rec.Reason == "" {
		t.Errorf("expected the drop error to be captured in Reason")
	}
}

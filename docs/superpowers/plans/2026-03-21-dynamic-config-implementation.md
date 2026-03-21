# Dynamic Configuration Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a dynamic configuration system using NATS KV with two-phase graceful transitions (Drain then Shutdown).

**Architecture:** A central `ConfigManager` watches NATS KV and orchestrates `PipelineWorker` instances.

**Tech Stack:** Go 1.26+, NATS KV.

---

### Task 1: Refined Interfaces & Signal Protocol

**Files:**
- Modify: `internal/engine/provider.go` (if exists, or create)
- Create: `internal/engine/worker.go`

- [ ] **Step 1: Define PipelineWorker interface**
```go
package engine

import "context"

type PipelineWorker interface {
    ID() string
    Drain() error
    Finished() <-chan struct{}
    Shutdown(ctx context.Context) error
}
```
- [ ] **Step 2: Commit**
`git add internal/engine && git commit -m "feat: define pipeline worker interface for dynamic config"`

---

### Task 3: ConfigManager with NATS KV Watcher

**Files:**
- Create: `internal/config/manager.go`

- [ ] **Step 1: Implement ConfigManager struct**
- [ ] **Step 2: Implement NATS KV watcher for 'global.config' and 'pipelines.*.config'**
- [ ] **Step 3: Implement hierarchical config merging logic**
- [ ] **Step 4: Implement orchestration logic (Drain -> Wait -> Shutdown -> Start New)**
- [ ] **Step 5: Commit**
`git add internal/config && git commit -m "feat: implement config manager with nats kv watcher"`

---

### Task 4: Update Producer & Consumer for Two-Phase Transition

**Files:**
- Modify: `internal/engine/producer.go`
- Modify: `internal/engine/consumer.go`

- [ ] **Step 1: Update Producer to support Drain signal (stop fetching)**
- [ ] **Step 2: Update Consumer to signal Finished when buffers are empty and checkpoint is committed**
- [ ] **Step 3: Implement PipelineWorker interface in a combined struct**
- [ ] **Step 4: Commit**
`git add internal/engine && git commit -m "feat: support two-phase transition in producer and consumer"`

---

### Task 5: Integration & Bootstrapping

**Files:**
- Modify: `cmd/pipeline/main.go`

- [ ] **Step 1: Initialize ConfigManager at startup**
- [ ] **Step 2: Hand over pipeline lifecycle management to ConfigManager**
- [ ] **Step 3: Commit**
`git add cmd/pipeline && git commit -m "feat: integrate config manager into main entry point"`

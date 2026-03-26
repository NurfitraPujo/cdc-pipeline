# Dynamic Configuration System - Detailed Design Specification

**Status:** Approved (2026-03-21)
**Authors:** Gemini CLI

## Overview
CDC Data Pipeline requires a dynamic configuration system that allows updates to pipeline and global settings without restarting the entire process. This is achieved by using NATS KV as the single source of truth and a centralized `ConfigManager` that orchestrates a two-phase graceful transition for affected worker segments.

## Architecture

### Configuration Storage
- **Source of Truth:** NATS KV bucket (default: `config`).
- **Hierarchy:**
    - `global.config`: Shared settings for all pipelines (e.g., batch size defaults, monitoring intervals).
    - `pipelines.{id}.config`: Pipeline-specific overrides.
- **Format:** JSON-encoded configuration objects.

### ConfigManager (Orchestrator)
- **Watcher:** Uses `kv.Watch()` to receive real-time updates for global and pipeline-specific keys.
- **Registry:** Maintains a map of active `PipelineWorker` instances.
- **Logic:**
    1. Detects a change in configuration.
    2. Merges global defaults with pipeline-specific overrides.
    3. Initiates a **Two-Phase Transition** for affected pipelines.

## Two-Phase Graceful Transition

To ensure zero data loss and consistent state, configuration updates follow these phases:

### Phase 1: Draining
- **Goal:** Stop receiving new data while finishing work-in-progress.
- **Action:** The `ConfigManager` sends a "Drain" signal to the active `PipelineWorker`.
- **Worker Behavior:** 
    - The `Producer` stops fetching new messages from the `Source`.
    - Internal channels and buffers continue to be processed by the `Consumer`.
    - The `Consumer` continues to upload batches to the `Sink`.

### Phase 2: Shutdown & Restart
- **Goal:** Finalize state and swap configurations.
- **Action:** Once the worker confirms all internal buffers are empty and the final `EgressCheckpoint` is committed to NATS KV, it sends a "Finished" signal back to the `ConfigManager`.
- **Manager Behavior:**
    - Terminates the old worker instance.
    - Spawns a new `PipelineWorker` instance with the updated configuration.
    - The new worker resumes from the last committed checkpoint.

## Internal Interfaces

```go
type PipelineWorker interface {
    // Drain signals the producer to stop fetching.
    Drain() error
    // Finished returns a channel that closes when draining is complete.
    Finished() <-chan struct{}
    // Shutdown cleans up remaining resources.
    Shutdown(ctx context.Context) error
}
```

## Benefits
- **Zero Downtime:** The main process remains active while individual pipelines reload.
- **Atomic Handoff:** Guaranteed one-way flow of data with no duplicates or gaps during transitions.
- **Observability:** Transitions can be tracked and displayed on the monitoring dashboard.

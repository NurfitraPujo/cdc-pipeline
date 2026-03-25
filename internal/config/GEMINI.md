# Internal Config: Orchestration Hub

The `internal/config` package is the central nervous system of the Daya Data Pipeline's Data Plane. It is responsible for watching configuration changes and orchestrating the lifecycle of worker goroutines.

## Core Features

- **`ConfigManager`**:
    - Watches **NATS KV** (`daya-dp-config` bucket) for updates to global settings and individual pipeline configurations.
    - Implements **Hierarchical Configuration**: Merges global defaults with pipeline-specific overrides.
- **Two-Phase Transition Protocol**:
    - Handles zero-downtime, LSN-consistent reloads.
    - **Phase 1: Drain**: Signals the Producer to stop fetching and sends a `drain_marker`.
    - **Phase 2: Shutdown**: Waits for the Consumer to finish processing up to the marker, then closes all resources.
- **Autonomous Supervision**:
    - Spawns a supervisor goroutine for every worker instance.
    - Detects unexpected crashes (when a worker finishes without a `Transitioning` flag).
    - Triggers automatic restarts with a 5-second stabilization delay and exponential backoff.

## Key Files

- **`manager.go`**: Core implementation of `ConfigManager`, the reload protocol, and the supervisor.
- **`manager_test.go`**: Integration tests using **Testcontainers (NATS)** to verify complex transition and crash recovery logic.

## Conventions

- **Concurrency Safety**: Uses `sync.RWMutex` to protect access to active worker maps and global configurations.
- **Context Management**: Respects standard Go `context.Context` for graceful shutdown of watchers and supervisors.
- **Transition State**: Persists a `PipelineTransitionState` in NATS KV during reloads to inform the Control Plane and prevent supervisor race conditions.

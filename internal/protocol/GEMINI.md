# Internal Protocol: Shared Schema & Serialization

The `internal/protocol` package defines the shared data structures used across the API and the Pipeline worker. It also handles high-performance serialization using MessagePack.

## Core Features

- **Data Models**:
    - **`Message`**: The fundamental unit of CDC data, containing table info, operation type (insert/update/delete/snapshot), LSN, and payload. Now includes cryptographic **Correlation IDs** (SHA256-based) for secured acknowledgments and transformation lineage auditing.
    - **`PipelineConfig`**, **`SourceConfig`**, **`SinkConfig`**: Life-cycle and connectivity definitions.
    - **`Checkpoint`**: Persisted state tracking ingress/egress progress.
    - **`TableStats`**: Real-time metrics for each synced table.
    - **Evolution State**: Defines the distributed state machine lifecycle: `Initial`, `Snapshotting` (active paginated fetch), `Draining` (replaying JetStream buffer), `ApplyingSchema`, `Verifying`, `SteadyState`. Fully JSON-serialized with explicit fencing tokens.
- **MessagePack (`msgp`)**:
    - Uses code generation to provide zero-allocation, high-speed serialization.
    - Significantly more efficient than JSON for high-throughput CDC data.
- **Validation**:
    - Uses **`ozzo-validation`** for declarative schema validation of all configuration types.

## Key Files

- **`config.go`**, **`message.go`**, **`state.go`**: Hand-written struct definitions and validation logic.
- **`*_gen.go`**: Machine-generated MessagePack implementation. **Do not edit manually.**
- **`config_test.go`**: Validation rule verification.

## Conventions

- **Generating Code**: Run `go generate ./internal/protocol/...` after modifying structs to update the MessagePack logic.
- **NATS Key Construction**: Centralizes key path generation (e.g., `PipelineConfigKey`, `TableStatsKey`) to ensure consistency between Control Plane and Data Plane.
- **Zero Values**: Explicitly handle zero values in configuration overrides (e.g., `BatchSize == 0` triggers falling back to global config).

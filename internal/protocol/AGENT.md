# Internal Protocol: Shared Schema & Serialization

The `internal/protocol` package defines the shared data structures used across the API and the Pipeline worker. It also handles high-performance serialization using MessagePack.

## Core Features

- **Data Models**:
    - **`Message`**: The fundamental unit of CDC data. **`Message.Table` is always BARE** ("orders"); the schema travels in the sibling field **`Message.TableSchema`** (empty normalises to `"public"`). Never encode a schema into `Table` -- every bare-name comparison in the codebase (the `cdc_snapshot_` guards, debug-sink filters, transformer allowlists) depends on it staying bare. Contains table info, operation type (insert/update/delete/snapshot), LSN, and payload. Now includes **Correlation IDs** for secured acknowledgments and transformation lineage.
    - **`PipelineConfig`**, **`SourceConfig`**, **`SinkConfig`**: Life-cycle and connectivity definitions.
    - **`Checkpoint`**: Persisted state tracking ingress/egress progress.
    - **`TableStats`**: Real-time metrics for each synced table.
    - **Evolution State**: New states for the distributed state machine: `Initial`, `Snapshotting`, `Draining`, `ApplyingSchema`, `Verifying`, `SteadyState`. Fully JSON-serialized for reliable persistence.
- **MessagePack (`msgp`)**:
    - Uses code generation to provide zero-allocation, high-speed serialization.
    - Significantly more efficient than JSON for high-throughput CDC data.
- **Validation**:
    - Uses **`ozzo-validation`** for declarative schema validation of all configuration types.

## Key Files

- **`table_ref.go`**: `TableRef`, the canonical table identity. `String()` = qualified `schema.table` (display, logs, sink targets, metric labels); `KeyToken()` = NATS/JetStream-safe form (bare for `public`, `schema=table` otherwise); `ParseTableRef` / `NormalizeSchema` / `TableRefFromKeyToken` for the boundaries. Derive a `TableRef` ONCE at each boundary and thread it -- never re-derive from a raw string mid-function.
- **`config.go`**, **`message.go`**, **`state.go`**: Hand-written struct definitions and validation logic.
- **`*_gen.go`**: Machine-generated MessagePack implementation. **Do not edit manually.**
- **`config_test.go`**: Validation rule verification.

## Conventions

- **Generating Code**: Run `go generate ./internal/protocol/...` after modifying structs to update the MessagePack logic.
- **NATS Key Construction**: Centralizes key path generation (e.g., `PipelineConfigKey`, `TableStatsKey`). **Every table-bearing key builder takes a `TableRef`, not a string**, so a raw config value cannot construct a key; the table segment is always `KeyToken()` to ensure consistency between Control Plane and Data Plane.
- **Zero Values**: Explicitly handle zero values in configuration overrides (e.g., `BatchSize == 0` triggers falling back to global config).

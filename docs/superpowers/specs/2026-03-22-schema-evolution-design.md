# Schema Evolution & Dynamic Table Discovery - Design Specification

**Status:** Approved (2026-03-22)
**Authors:** Gemini CLI

## Overview
Daya Data Pipeline must support dynamic custom tables built by platform users and automatic schema evolution (DDL propagation). This is achieved by utilizing the PostgreSQL logical replication stream's `Relation` messages as the primary trigger for discovery and structural updates, ensuring high availability and zero-ops management.

## Architecture

### 1. Active Discovery (Producer)
- **Publication Scope:** The PostgreSQL publication must be created with `FOR ALL TABLES` to ensure the CDC stream includes events for newly created tables.
- **Trigger:** The `Producer` intercepts every `Relation` message in the logical replication stream.
- **Filtering:** 
    - The `SourceConfig` is expanded to include a `Schemas` list (e.g., `["public", "tenant_custom"]`).
    - The `Producer` checks if the `schema` in the `Relation` message matches the allowed schemas AND if the table matches the `PipelineConfig.Tables` wildcard pattern (e.g., `*`).
- **Dynamic Onboarding:** If a new valid table is detected:
    1. The `Producer` updates the `TableMetadata` in NATS KV.
    2. The `Producer` publishes a `SCHEMA_EVOLUTION` event to the NATS `ingest` topic.
    3. The `Producer` initiates a `TableSnapshotter` goroutine for the new table.
    4. Real-time CDC events for the new table are buffered or discarded until the snapshot is complete (using LSN cut-over).

### 2. Schema Evolution (DDL Propagation)
- **Detection:** When a `Relation` message indicates a change in columns (name, type, or count) compared to the stored `TableMetadata`, a structural change is detected.
- **Propagation:** The `Producer` emits an `OP_SCHEMA_CHANGE` message to NATS containing the new metadata and the target table.
- **Barrier Pattern:** To ensure data integrity, the `Producer` MUST publish the schema change event *before* any data events related to the new structure.

### 3. Ordered Execution (Consumer)
- **Constraint:** The `Consumer` must not process data events for a table until its schema is synchronized in the Sink (Databend).
- **Reaction:**
    1. Upon receiving `OP_SCHEMA_CHANGE`, the `Consumer` pauses processing for that specific table.
    2. The `Consumer` generates and executes the corresponding `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE` command in Databend.
    3. The `Consumer` uses a robust type-mapper to convert PostgreSQL types to Databend types.
    4. Once the DDL is successful, the `Consumer` resumes processing data events for the table.

## Internal Interface Changes

```go
type SourceConfig struct {
    // ...
    Schemas []string `msg:"schemas" yaml:"schemas" json:"schemas"`
}

type Message struct {
    // ...
    Op string `msg:"op"` // "insert", "update", "delete", "snapshot", "schema_change"
}
```

## Benefits
- **Zero-Restart Discovery:** New tables are synced without interrupting existing data flows.
- **Isolated Evolution:** Schema changes in one table do not block others.
- **Data Integrity:** The "Schema Barrier" ensures that the Sink always has the correct structure before data arrives.

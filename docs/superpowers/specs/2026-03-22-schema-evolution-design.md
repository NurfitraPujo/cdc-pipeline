# Schema Evolution & Dynamic Table Discovery - Design Specification

**Status:** Approved (2026-03-22)
**Authors:** Gemini CLI

## Overview
CDC Data Pipeline must support dynamic custom tables built by platform users and automatic schema evolution (DDL propagation). This is achieved by utilizing the PostgreSQL logical replication stream's `Relation` messages as the primary trigger for discovery and structural updates, ensuring high availability and zero-ops management.

## Architecture

### 1. Active Discovery (Producer)
- **Publication Scope:** The PostgreSQL publication must be created with `FOR ALL TABLES` to ensure the CDC stream includes events for newly created tables.
- **Trigger 1 (Event-Driven):** The `Producer` intercepts every `Relation` message in the logical replication stream.
- **Trigger 2 (Background Polling):** A background goroutine periodically queries `information_schema.tables` to detect new tables that haven't had activity yet (and thus haven't sent a `Relation` message).
- **Filtering:** 
    - The `SourceConfig` is expanded to include a `Schemas` list (e.g., `["public", "tenant_custom"]`).
    - The `Producer` checks if the `schema` in the `Relation` message matches the allowed schemas AND if the table matches the `PipelineConfig.Tables` wildcard pattern (e.g., `*`).
- **Dynamic Onboarding:** If a new valid table is detected:
    1. The `Producer` updates the `TableMetadata` in NATS KV.
    2. The `Producer` publishes a `schema_change` event to the NATS `ingest` topic.
    3. The `Producer` captures the **Current LSN** as the `SnapshotLSN`.
    4. The `Producer` initiates a `TableSnapshotter` goroutine for the new table.
    5. **LSN Cut-over:** CDC data for this table with `LSN < SnapshotLSN` is discarded; data with `LSN >= SnapshotLSN` is buffered until the snapshot completes.

### 2. Schema Evolution (DDL Propagation)
- **Detection:** When a `Relation` message indicates a change in columns (name, type, or count) compared to the stored `TableMetadata`, a structural change is detected.
- **Propagation:** The `Producer` emits an `OP_SCHEMA_CHANGE` message to NATS containing the new `SchemaMetadata`.
- **Barrier Pattern:** To ensure data integrity, the `Producer` MUST publish the schema change event *before* any data events related to the new structure. NATS JetStream ordering guarantees this sequence.

### 3. Ordered Execution (Consumer)
- **Constraint:** The `Consumer` must not process data events for a table until its schema is synchronized in the Sink (Databend).
- **Reaction:**
    1. Upon receiving `OP_SCHEMA_CHANGE`, the `Consumer` pauses processing for that specific table (using internal locking or partitioning).
    2. The `Consumer` generates and executes the corresponding `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE` command in Databend.
    3. Once the DDL is successful, the `Consumer` resumes processing data events for the table.

## Internal Interface Changes

```go
type SourceConfig struct {
    // ...
    Schemas []string `msg:"schemas" yaml:"schemas" json:"schemas"`
}

type SchemaMetadata struct {
    Table     string            `msg:"tbl" json:"table"`
    Schema    string            `msg:"sch" json:"schema"`
    Columns   map[string]string `msg:"cols" json:"columns"` // Name -> Type
    PKColumns []string          `msg:"pks" json:"pk_columns"`
}

type Message struct {
    // ...
    Op     string          `msg:"op"` // "insert", "update", "delete", "snapshot", "schema_change"
    Schema *SchemaMetadata `msg:"meta,omitempty" json:"schema,omitempty"`
}
```

## Benefits
- **Zero-Restart Discovery:** New tables are synced without interrupting existing data flows.
- **Isolated Evolution:** Schema changes in one table do not block others.
- **Data Integrity:** The "Schema Barrier" ensures that the Sink always has the correct structure before data arrives.

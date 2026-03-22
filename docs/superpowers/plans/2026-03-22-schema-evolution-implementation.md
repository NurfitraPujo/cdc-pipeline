# Schema Evolution & Dynamic Discovery Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automate table discovery and schema evolution (DDL) propagation from PostgreSQL to Databend.

**Architecture:** Producer-driven discovery via `Relation` messages and background polling, with ordered DDL execution in the Consumer using a "Schema Barrier."

**Tech Stack:** Go 1.26+, NATS JetStream, go-pq-cdc, Databend.

---

### Task 1: Protocol & Schema Metadata

**Files:**
- Modify: `internal/protocol/config.go`
- Modify: `internal/protocol/message.go`
- Modify: `internal/protocol/state.go`

- [ ] **Step 1: Add Schemas to SourceConfig**
- [ ] **Step 2: Add SchemaMetadata and OpSchemaChange**
- [ ] **Step 3: Run `go generate ./internal/protocol/...`**
- [ ] **Step 4: Commit**
`git add internal/protocol && git commit -m "chore: add schema evolution structs to protocol"`

---

### Task 2: Producer Active Discovery (Part 1 - Events & Metadata)

**Files:**
- Modify: `internal/source/postgres/source.go`

- [ ] **Step 1: Intercept Relation messages in handler**
- [ ] **Step 2: Implement Dynamic Metadata Capture from format.Relation**
- [ ] **Step 3: Emit `schema_change` event to NATS**
- [ ] **Step 4: Implement Table Discovery Trigger**
When a new table is found via `Relation`, capture `SnapshotLSN` and signal the Orchestrator.
- [ ] **Step 5: Commit**
`git add internal/source/postgres && git commit -m "feat: implement relation-based discovery and metadata capture"`

---

### Task 3: Producer Active Discovery (Part 2 - Poller & Orchestration)

**Files:**
- Modify: `internal/source/postgres/source.go`
- Modify: `internal/engine/producer.go`

- [ ] **Step 1: Implement background Discovery Poller**
Query `information_schema.tables` periodically.
- [ ] **Step 2: Implement Dynamic Table Handshake**
Signal the orchestrator when the poller finds a new table.
- [ ] **Step 3: Implement Auto-Config Update in Producer Orchestrator**
Update `PipelineConfig` in NATS KV when new tables are found to preserve state across restarts.
- [ ] **Step 4: Commit**
`git add internal/source/postgres internal/engine/producer.go && git commit -m "feat: implement background table discovery poller and orchestrator handshake"`

---

### Task 4: Consumer Schema Barrier & DDL Execution

**Files:**
- Modify: `internal/engine/consumer.go`
- Modify: `internal/sink/databend/sink.go`

- [ ] **Step 1: Implement DDL Generation in Databend Sink**
- [ ] **Step 2: Implement Barrier logic in Consumer**
Block data for table X until its specific `schema_change` is applied.
- [ ] **Step 3: Commit**
`git add internal/engine/consumer.go internal/sink/databend && git commit -m "feat: implement consumer-side ddl execution and barrier"`

---

### Task 5: Snapshot Cut-over & Verification

**Files:**
- Modify: `internal/engine/producer.go`
- Create: `internal/engine/discovery_test.go`

- [ ] **Step 1: Implement LSN Cut-over logic**
Ensure the `Producer` discards CDC events with `LSN < SnapshotLSN` and buffers events with `LSN >= SnapshotLSN` for the new table.
- [ ] **Step 2: Implement Snapshot Initiation**
Spawn `TableSnapshotter` goroutines dynamically for new tables.
- [ ] **Step 3: Add integration tests for Dynamic Discovery**
Verify: Create table -> Auto-detect -> Snapshot -> CDC transition.
- [ ] **Step 4: Commit**
`git add internal/engine && git commit -m "feat: implement snapshot cut-over and verify discovery flow"`

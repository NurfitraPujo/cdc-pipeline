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
```go
type SourceConfig struct {
    // ...
    Schemas []string `msg:"schemas" yaml:"schemas" json:"schemas"`
}
```
- [ ] **Step 2: Add SchemaMetadata and OpSchemaChange**
```go
type SchemaMetadata struct {
    Table     string            `msg:"tbl" json:"table"`
    Schema    string            `msg:"sch" json:"schema"`
    Columns   map[string]string `msg:"cols" json:"columns"`
    PKColumns []string          `msg:"pks" json:"pk_columns"`
}

type Message struct {
    // ...
    Op     string          `msg:"op"` // Add "schema_change"
    Schema *SchemaMetadata `msg:"meta,omitempty" json:"schema,omitempty"`
}
```
- [ ] **Step 3: Run `go generate ./internal/protocol/...`**
- [ ] **Step 4: Commit**
`git add internal/protocol && git commit -m "chore: add schema evolution structs to protocol"`

---

### Task 2: Producer Active Discovery (Part 1 - Events)

**Files:**
- Modify: `internal/source/postgres/source.go`

- [ ] **Step 1: Intercept Relation messages in handler**
Compare incoming `Relation` against NATS KV `TableMetadata`.
- [ ] **Step 2: Implement Dynamic Metadata Capture**
Extract column names and types from `format.Relation`.
- [ ] **Step 3: Emit `schema_change` event to NATS**
- [ ] **Step 4: Commit**
`git add internal/source/postgres && git commit -m "feat: implement relation-based discovery in producer"`

---

### Task 3: Producer Active Discovery (Part 2 - Poller)

**Files:**
- Modify: `internal/source/postgres/source.go`
- Modify: `internal/engine/producer.go`

- [ ] **Step 1: Implement background Discovery Poller**
Query `information_schema.tables` periodically.
- [ ] **Step 2: Implement Auto-Config Update**
Update `PipelineConfig` in NATS KV when a new table matches the wildcard.
- [ ] **Step 3: Commit**
`git add internal/source/postgres internal/engine/producer.go && git commit -m "feat: implement background table discovery poller"`

---

### Task 4: Consumer Schema Barrier & DDL Execution

**Files:**
- Modify: `internal/engine/consumer.go`
- Modify: `internal/sink/databend/sink.go`

- [ ] **Step 1: Implement DDL Generation in Databend Sink**
Type mapper (Postgres -> Databend) and `CREATE/ALTER TABLE` SQL generator.
- [ ] **Step 2: Implement Barrier logic in Consumer**
Block data for table X until DDL is applied.
- [ ] **Step 3: Commit**
`git add internal/engine/consumer.go internal/sink/databend && git commit -m "feat: implement consumer-side ddl execution and barrier"`

---

### Task 5: Snapshot Cut-over & Verification

**Files:**
- Modify: `internal/engine/producer.go`
- Create: `internal/engine/discovery_test.go`

- [ ] **Step 1: Implement LSN Cut-over logic**
Ensure snapshot handover for a single table is atomic.
- [ ] **Step 2: Add integration tests for Dynamic Discovery**
- [ ] **Step 3: Commit**
`git add internal/engine && git commit -m "feat: implement snapshot cut-over and verify discovery"`

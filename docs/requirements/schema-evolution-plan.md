# Schema Evolution Handling Plan

## Status
Approved

## Date
2026-04-16

## Problem Statement

PostgreSQL's CDC does not publish DDL/schema changes - it only publishes data changes (Insert/Update/Delete). The Relation message contains current schema, but Postgres does NOT send a special "schema changed" signal. This means:

1. **Discovery interval (30s)** only detects NEW tables via `information_schema.tables`, not schema changes to existing tables
2. When a column is ADDED to an existing table, CDC events will suddenly include the new column in the Relation message
3. The current implementation sends `schema_change` on EVERY Relation message (even if schema unchanged)
4. But if no data changes occur after `ALTER TABLE ADD COLUMN`, the schema change is never detected
5. If data changes DO occur before discovery catches up, the consumer receives data with columns the sink doesn't have

### Critical Issue
CDC data arrives asynchronously from discovery. The CDC event contains a new column (`phone`), but the consumer/sink doesn't know about it yet. The sink tries to INSERT with a column the table doesn't have, causing failures or data loss.

Furthermore, in a **distributed environment**, handling this pause requires robust synchronization to prevent data loss or split-brain (multiple workers interleaving operations), and robust security to prevent Denial of Service (DoS) via schema-bombing or SQL injection via malicious column names.

---

## Current Architecture (FLAWED)

```text
Time -->

T=0s:   Postgres:  ALTER TABLE users ADD COLUMN phone
        (No CDC message for DDL!)

T=0-30s: Discovery interval hasn't run yet

T=31s:  CDC:       INSERT (id, name, age, phone) 
        (Relation message now includes "phone" column)
                    |
                    v
        Producer.Run() --> sends to NATS (includes schema from Relation)
                    |
                    v
        Consumer.Run() --> sink.ApplySchema() adds column
                    |     BUT: batch upload happens IMMEDIATELY after
                    v     schema change - no wait for commit!
        DatabendSink.uploadTableBatch() 
        --> REPLACE INTO users (id, name, age, phone) 
        --> Databend column "phone" doesn't exist yet! 
        --> FAILS or DATA LOSS
```

**The bug:** Consumer flushes batch before Databend has committed the schema change. The sink's `ApplySchema()` runs but the subsequent `BatchUpload()` runs immediately after, before the ALTER TABLE has committed in Databend.

---

## Proposed Design (Secure & Distributed)

### 1. Proactive Schema Discovery (Enhancement to existing discovery)

The `discoverTables()` should be expanded to detect schema changes on **existing** tables:

```go
func (s *PostgresSource) discoverTables(ctx, db, srcConfig, ...) {
    // Existing: detect NEW tables
    // ...
    
    // NEW: detect SCHEMA CHANGES on existing tables
    for tableName := range knownTables {
        cachedSchema := getCachedSchema(tableName)  // from KV
        currentSchema := s.getTableMetadata(ctx, db, "public", tableName)
        
        // Explicitly check for column additions, type changes, or drops
        if schemaChanged(cachedSchema, currentSchema) {
            emitSchemaChange(tableName, currentSchema)
            updateCachedSchema(tableName, currentSchema)
        }
    }
}
```

### 2. Inline Schema Verification & Security Sanitization

When a CDC event arrives with column data that doesn't match our cached schema, we must validate, sanitize, and freeze:

1. **Detect discrepancy** - Data has columns not in our cache, or types have changed.
2. **Sanitize & Validate** - Ensure new column names match `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL injection.
3. **Signal schema change proactively** - BEFORE sending to consumer, emit event with a cryptographically secure `CorrelationID`.
4. **Buffer/Halt CDC events** - Route messages to a distributed NATS JetStream buffer until the sink commits the change.

### 3. Per-Table Distributed Buffering (NATS JetStream)

**CRITICAL FIX:** Using a local file WAL causes data loss if a Kubernetes pod crashes while buffering. Buffering MUST be distributed.

1. **Freeze table X** - Route all incoming CDC events for table X to a temporary NATS JetStream topic: `cdc.pipeline.{id}.buffer.{table}`.
2. **Proactively fetch full schema** - `getTableMetadata()` for table X.
3. **Emit schema_change message** - to NATS (with `CorrelationID`).
4. **Wait for egress commit acknowledgment** - Sink confirms schema change via `schema_ack` topic containing the same `CorrelationID`.
5. **Unfreeze table X** - Drain the JetStream buffer to the main stream, delete the buffer, and resume normal flow.

### 4. Concurrency & Split-Brain (CAS Fencing Tokens)

To prevent two workers from concurrently managing schema evolution for the same table:
1. Distributed locks in NATS KV must use **Compare-And-Swap (CAS)** with **Fencing Tokens (KV Revisions)**.
2. Any update to the `SchemaEvolutionState` or flush operation must verify the KV revision hasn't changed. If it has, the worker knows it lost the lock (e.g., due to a long GC pause) and must safely abort.

### 5. Security & Resilience (DoS Protection)

1. **Schema Circuit Breaker:** An attacker could rapidly execute `ALTER TABLE` to freeze the pipeline (DoS). Implement a rate limit (e.g., max 5 schema changes per minute per table). Exceeding this marks the table as `Suspended` for manual review.
2. **Strict Payload Size Limits:** Widening types (`VARCHAR` -> `TEXT`) can be exploited by inserting massive payloads to OOM the pipeline. Enforce absolute byte-size limits per CDC message.
3. **Acknowledgment Timeout:** If the consumer dies before sending `schema_ack`, the producer must timeout (e.g., 60s). Upon timeout, the producer intentionally crashes (`Cancel->Sleep->Close`) to trigger supervisor recovery.

---

## Key Data Structures

```go
// State persisted in NATS KV (with Revision tracking for CAS)
type SchemaEvolutionState struct {
    Status         string            `msg:"status" json:"status"`
    FrozenAt       time.Time         `msg:"f_at" json:"frozen_at"`
    Columns        map[string]string `msg:"cols" json:"columns"`
    BufferedCount  int               `msg:"b_cnt" json:"buffered_count"`
    CorrelationID  string            `msg:"c_id" json:"correlation_id"`
    ChangesThisMin int               `msg:"c_min" json:"changes_this_min"`
    LastChangeAt   time.Time         `msg:"l_at" json:"last_change_at"`
}

// Schema diff for type change detection
type SchemaDiff struct {
    Table        string               `msg:"tbl" json:"table"`
    Timestamp    time.Time            `msg:"ts" json:"timestamp"`
    Source       string               `msg:"src" json:"source"`
    Added        map[string]string    `msg:"add" json:"added"`
    Removed      []string             `msg:"rem" json:"removed"`
    TypeChanges  map[string]TypeChange `msg:"type" json:"type_changes"`
    CorrelationID string              `msg:"c_id" json:"correlation_id"`
}

type TypeChange struct {
    OldType    string `msg:"old" json:"old_type"`
    NewType    string `msg:"new" json:"new_type"`
    ChangeType string `msg:"type" json:"change_type"` // "Widening", "Narrowing", "FamilyChange"
}

const (
    OpSchemaChangeAck = "schema_change_ack"
)
```

---

## Message Flow for Schema Evolution

```text
Producer.Run() Main Loop:
├── Receive CDC batch from source
├── For each message:
│   ├── Extract columns and types
│   ├── Compare with cached schema
│   ├── If discrepancy:
│   │   ├── Check Circuit Breaker (max 5/min). If tripped -> Suspend Table.
│   │   ├── Sanitize column names (Regex). If invalid -> DLQ/Alert.
│   │   ├── Attempt CAS Lock in NATS KV (acquire Fencing Token)
│   │   ├── If lock acquired:
│   │   │   ├── Set state = Frozen, generate CorrelationID
│   │   │   ├── Create JetStream buffer topic for table
│   │   │   ├── Route message to JetStream buffer
│   │   │   └── Emit schema_change(CorrelationID)
│   │   └── If lock NOT acquired (already frozen):
│   │       └── Route message to JetStream buffer
│   └── If no discrepancy:
│       └── Forward message normally
├── After processing batch:
│   ├── Listen for schema_ack with matching CorrelationID
│   ├── If Ack received AND KV Revision matches (CAS):
│   │   ├── Drain JetStream buffer to main stream
│   │   ├── Delete JetStream buffer
│   │   └── Set state = Stable
│   └── If Timeout:
│       └── Crash Producer (Cancel->Sleep->Close) for supervisor restart
└── Continue
```

---

## Detailed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement secure, distributed schema evolution handling using NATS JetStream buffering, CAS fencing tokens, and schema circuit breakers.

**Architecture:** Distributed state machine via NATS KV with CAS revisions. Buffering delegated to JetStream streams (`cdc.pipeline.{id}.buffer.{table}`). Cryptographic correlation IDs prevent ack spoofing.

**Tech Stack:** Go, NATS JetStream, NATS KV, Watermill

---

### Task 1: Update Protocol Definitions

**Files:**
- Modify: `internal/protocol/state.go`
- Modify: `internal/protocol/message.go`

- [ ] **Step 1: Add SchemaEvolutionState to `internal/protocol/state.go`**

```go
type SchemaEvolutionState struct {
	Status         string            `msg:"status" json:"status"`
	FrozenAt       time.Time         `msg:"f_at" json:"frozen_at"`
	Columns        map[string]string `msg:"cols" json:"columns"`
	BufferedCount  int               `msg:"b_cnt" json:"buffered_count"`
	CorrelationID  string            `msg:"c_id" json:"correlation_id"`
	ChangesThisMin int               `msg:"c_min" json:"changes_this_min"`
	LastChangeAt   time.Time         `msg:"l_at" json:"last_change_at"`
}

const (
	SchemaStatusStable       = "stable"
	SchemaStatusFrozen       = "frozen"
	SchemaStatusTypeConflict = "type_conflict"
	SchemaStatusSuspended    = "suspended"
)

func SchemaEvolutionKey(pipelineID, table string) string {
	return fmt.Sprintf("cdc.pipeline.%s.schema_evolution.%s", pipelineID, table)
}
```

- [ ] **Step 2: Add SchemaDiff and CorrelationID to `internal/protocol/message.go`**

```go
type TypeChange struct {
	OldType    string `msg:"old" json:"old_type"`
	NewType    string `msg:"new" json:"new_type"`
	ChangeType string `msg:"type" json:"change_type"`
}

type SchemaDiff struct {
	Table         string               `msg:"tbl" json:"table"`
	Timestamp     time.Time            `msg:"ts" json:"timestamp"`
	Source        string               `msg:"src" json:"source"`
	Added         map[string]string    `msg:"add" json:"added"`
	Removed       []string             `msg:"rem" json:"removed"`
	TypeChanges   map[string]TypeChange `msg:"type" json:"type_changes"`
	CorrelationID string               `msg:"c_id" json:"correlation_id"`
}

const (
	OpSchemaChange    = "schema_change"
	OpSchemaChangeAck = "schema_change_ack"
)

// Update Message struct
type Message struct {
    // ... existing fields ...
    CorrelationID string      `msg:"c_id,omitempty" json:"correlation_id,omitempty"`
    Diff          *SchemaDiff `msg:"diff,omitempty" json:"diff,omitempty"`
}
```

- [ ] **Step 3: Regenerate MessagePack code**

Run: `go generate ./internal/protocol/...`

- [ ] **Step 4: Commit**

```bash
git add internal/protocol/state.go internal/protocol/message.go internal/protocol/*_gen.go
git commit -m "protocol: add schema evolution types and constants"
```

---

### Task 2: Implement CAS State Management in ConfigManager

**Files:**
- Modify: `internal/config/manager.go`

- [ ] **Step 1: Implement `UpdateSchemaStateCAS`**

```go
func (m *ConfigManager) UpdateSchemaStateCAS(ctx context.Context, pipelineID, table string, state protocol.SchemaEvolutionState, revision uint64) (uint64, error) {
	key := protocol.SchemaEvolutionKey(pipelineID, table)
	val, err := state.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}

	if revision == 0 {
		return m.kv.Create(key, val)
	}
	return m.kv.Update(key, val, revision)
}
```

- [ ] **Step 2: Implement `GetSchemaState`**

```go
func (m *ConfigManager) GetSchemaState(pipelineID, table string) (protocol.SchemaEvolutionState, uint64, error) {
	key := protocol.SchemaEvolutionKey(pipelineID, table)
	entry, err := m.kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return protocol.SchemaEvolutionState{Status: protocol.SchemaStatusStable}, 0, nil
		}
		return protocol.SchemaEvolutionState{}, 0, err
	}

	var state protocol.SchemaEvolutionState
	_, err = state.UnmarshalMsg(entry.Value())
	return state, entry.Revision(), err
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/config/manager.go
git commit -m "config: implement CAS-based schema state management"
```

---

### Task 4: Enhance Producer with Distributed Buffering

**Files:**
- Modify: `internal/engine/producer.go`

- [ ] **Step 1: Add `schemaEvolution` helper struct to Producer**

```go
type tableEvolution struct {
    status         string
    revision       uint64
    correlationID  string
    cachedSchema   map[string]string
    lastCheckAt    time.Time
}

// In Producer struct
muEvo sync.RWMutex
evoStates map[string]*tableEvolution // table name -> evolution state
```

- [ ] **Step 2: Implement `detectSchemaChange`**

```go
func (p *Producer) detectSchemaChange(msg protocol.Message) (*protocol.SchemaDiff, bool) {
    // Compare msg.Data columns with p.evoStates[msg.Table].cachedSchema
    // Return SchemaDiff if different
}
```

- [ ] **Step 3: Implement JetStream Buffering Logic**

```go
func (p *Producer) bufferToJetStream(table string, msg protocol.Message) error {
    topic := fmt.Sprintf("cdc_pipeline_%s_buffer_%s", p.pipelineID, table)
    // Publish to specific buffer topic
}
```

- [ ] **Step 4: Update `Run` loop to handle discrepancies**

```go
// Inside Loop:
if diff, changed := p.detectSchemaChange(m); changed {
    // 1. Acquire Lock/CAS
    // 2. Set State = Frozen
    // 3. Generate CorrelationID
    // 4. Emit OpSchemaChange
    // 5. Buffer Message
}
```

- [ ] **Step 5: Commit**

```bash
git add internal/engine/producer.go
git commit -m "engine/producer: implement inline schema detection and buffering"
```

---

### Task 5: Enhance Consumer with Acknowledgment

**Files:**
- Modify: `internal/engine/consumer.go`

- [ ] **Step 1: Update `ApplySchema` handling in `Run` loop**

```go
if m.Op == protocol.OpSchemaChange {
    if err := c.sink.ApplySchema(ctx, m.Schema); err != nil {
        return err
    }
    // Emit Ack
    ack := protocol.Message{
        Op: protocol.OpSchemaChangeAck,
        CorrelationID: m.CorrelationID,
        Table: m.Table,
        Timestamp: time.Now(),
    }
    // Publish to cdc_pipeline_{id}_acks topic
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/engine/consumer.go
git commit -m "engine/consumer: implement schema change acknowledgment"
```

---

### Task 6: Finalize Recovery & Resilience

**Files:**
- Modify: `internal/engine/pipeline.go`
- Modify: `internal/engine/producer.go`

- [ ] **Step 1: Implement WAL/JetStream Buffer Recovery on Producer Startup**

- [ ] **Step 2: Implement Ack Timeout Handling in Producer**

- [ ] **Step 3: Implement Schema Circuit Breaker in `detectSchemaChange`**

- [ ] **Step 4: Commit**

```bash
git commit -m "engine: finalize recovery and resilience for schema evolution"
```

# Schema Evolution Handling Plan

## Status
Draft

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

---

## Current Architecture (FLAWED)

```
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

### Why Relation Message Doesn't Help

PostgreSQL's CDC sends a `Relation` message that contains the current schema whenever a table's structure is first referenced or changes. However:
1. Postgres sends this as part of the data stream, not as a separate "schema change" event
2. The Relation message is sent WITH the data, not BEFORE it
3. There's no transactional boundary between Relation and Insert/Update/Delete

---

## Proposed Design

### 1. Proactive Schema Discovery (Enhancement to existing discovery)

The `discoverTables()` should be expanded to detect schema changes on **existing** tables:

```go
func (s *PostgresSource) discoverTables(ctx, db, srcConfig, ...) {
    // Existing: detect NEW tables
    for rows.Next() {
        if !knownTables["public."+tableName] {
            // ... handle new table
        }
    }
    
    // NEW: detect SCHEMA CHANGES on existing tables
    for tableName := range knownTables {
        cachedSchema := getCachedSchema(tableName)  // from KV
        currentSchema := s.getTableMetadata(ctx, db, "public", tableName)
        
        if schemaChanged(cachedSchema, currentSchema) {
            // Emit schema_change message PROACTIVELY
            emitSchemaChange(tableName, currentSchema)
            updateCachedSchema(tableName, currentSchema)
        }
    }
}
```

### 2. Inline Schema Verification in CDC Handler

When a CDC event arrives with column data that doesn't match our cached schema, we must:

1. **Detect the discrepancy** - data has columns not in our cache
2. **Signal schema change proactively** - BEFORE sending to consumer
3. **Buffer/Halt CDC events for that table** - until schema change commits at sink

```go
// In source.go CDC handler
case *format.Insert:
    // Get cached schema
    cachedCols := getCachedColumns(msg.TableName)
    
    // Check for new columns in incoming data
    incomingCols := extractColumns(msg.Decoded)
    newCols := findDiscrepancy(incomingCols, cachedCols)
    
    if len(newCols) > 0 {
        // Schema change detected! 
        // 1. Halt this batch from flowing
        // 2. Signal schema change upstream
        // 3. Buffer this message
    }
```

### 3. Per-Table CDC Event Buffering

When a schema discrepancy is detected for table X:

1. **Freeze table X** - Buffer all incoming CDC events for table X
2. **Proactively fetch full schema** - `getTableMetadata()` for table X
3. **Emit schema_change message** - to NATS (consumer/sink applies it)
4. **Wait for egress commit acknowledgment** - sink confirms schema change
5. **Unfreeze table X** - Process buffered messages + resume normal flow

```
┌─────────────────────────────────────────────────────────────┐
│ CDC Event arrives for table "users" with new column "phone" │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ Check cached schema vs incoming data                       │
│ Schema discrepancy FOUND: "phone" not in cache            │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────┐
│ Buffer "users" events   │     │ Emit schema_change for     │
│ (don't send to NATS)    │     │ "users" with full schema  │
└─────────────────────────┘     └─────────────────────────────┘
                                          │
                                          ▼
                            ┌─────────────────────────────┐
                            │ Consumer receives, calls    │
                            │ sink.ApplySchema()          │
                            │ sink.AddColumn("phone")     │
                            └─────────────────────────────┘
                                          │
                                          ▼
                            ┌─────────────────────────────┐
                            │ Schema change COMMITTED     │
                            │ (column exists in Databend)│
                            └─────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────┐
│ Unfreeze "users": flush buffered events + resume CDC      │
└─────────────────────────────────────────────────────────────┘
```

### 4. Egress Commit Acknowledgment for Schema Changes

The consumer needs a way to signal back that schema change was committed. Options:

1. **Dedicated NATS reply topic** - Consumer publishes to `cdc.pipeline.{id}.schema_ack` after successful `ApplySchema`
2. **Synchronous commit mode** - Producer waits for ack before resuming (complex, slows pipeline)
3. **KV-based phase flag** - Set `tableX.schema_status = "committed"` in KV, producer polls

**Recommended: Option 1** - dedicated acknowledgment topic is clean and decoupled.

### 5. Key Data Structures

```go
// In producer.go - new struct
type schemaEvolution struct {
    mu            sync.Mutex
    frozenTables  map[string]chan struct{}  // table -> unfreeze signal
    buffers       map[string][]Message      // table -> buffered CDC events
    pendingSchema map[string]bool           // table -> awaiting commit
    
    // WAL for persistent buffering
    walDir        string                   // e.g., "schema_evo/{pipelineID}/{table}/"
    
    // Distributed lock
    kv            nats.KeyValue
    lockTTL       time.Duration
}

// Schema evolution state persisted in KV
type SchemaEvolutionState struct {
    Status       string            // "Stable", "Frozen", "Updating", "TypeConflict", "Queued"
    FrozenAt     time.Time
    Columns      map[string]string // current known schema (column -> type)
    BufferedLSN  uint64            // highest LSN in buffer
    LockTimestamp time.Time        // when lock was acquired
    LockSource   string             // "discovery" or "cdc"  
    QueuePosition int              // if queued, position in queue
    PendingDiff  *SchemaDiff       // if queued, the pending change
}

// Schema diff for type change detection
type SchemaDiff struct {
    Table        string
    Timestamp    time.Time
    Source       string             // "discovery" or "cdc"
    Added        map[string]string  // columns added (name -> type)
    Removed      []string           // columns removed
    TypeChanges  map[string]TypeChange // columns with type changes
}

type TypeChange struct {
    OldType string
    NewType string
    ChangeType string // "Widening", "Narrowing", "FamilyChange"
}

// State values for schema evolution
const (
    SchemaStatusStable      = "stable"       // Normal operation
    SchemaStatusFrozen      = "frozen"       // CDC events being buffered
    SchemaStatusUpdating    = "updating"     // Schema change in progress
    SchemaStatusTypeConflict = "type_conflict" // Type change requires manual intervention
    SchemaStatusQueued       = "queued"       // Queued for sequential processing
)

// Lock key pattern
const SchemaLockKeyPattern = "cdc.pipeline.%s.schema_lock.%s" // pipelineID, table

### 6. Message Flow for Schema Evolution

```
Producer.Run() Main Loop:
├── Receive CDC batch from source
├── For each message:
│   ├── Extract columns from message data
│   ├── Compare with cached schema for that table
│   ├── If discrepancy:
│   │   ├── Attempt to acquire distributed lock
│   │   ├── If lock acquired:
│   │   │   ├── Set table state = Frozen
│   │   │   ├── Buffer message to WAL (persistent)
│   │   │   ├── Mark table as pending schema change
│   │   │   └── Trigger proactive schema fetch
│   │   ├── If lock NOT acquired but timestamps differ:
│   │   │   ├── If new change is different from locked change: queue it
│   │   │   └── If same or earlier: discard incoming
│   │   └── If lock NOT acquired and our timestamp is earlier: wait
│   └── If no discrepancy:
│       └── Forward message normally
├── After processing batch:
│   ├── If any tables marked pending:
│   │   ├── Emit schema_change messages
│   │   └── Wait for acknowledgment on schema_ack topic
│   └── If any tables now unfrozen:
│       ├── Flush buffered messages from WAL
│       ├── Clean up WAL files
│       └── Resume normal CDC for table
└── Continue
```

### 7. WAL Structure for Persistent Buffering

```
schema_evo/
└── {pipelineID}/
    └── {sourceID}/
        └── {table}/
            ├── state.json          # SchemaEvolutionState
            ├── lock.json           # Lock metadata
            └── buffer/
                ├── 0000000000000000_0000000000000ABC.msgpack  # LSN range
                └── 0000000000000ABC_0000000000000DEF.msgpack
```

- `state.json`: Current SchemaEvolutionState
- `lock.json`: Lock holder info (timestamp, source)
- `buffer/*.msgpack`: Message batches buffered at specific LSN ranges
- On flush: delete buffer files, update state.json to Stable

---

## Files to Modify

| File | Changes |
|------|---------|
| `internal/source/postgres/source.go` | Add schema caching, discrepancy detection in CDC handler, proactive schema fetch, smart diff with type change detection |
| `internal/engine/producer.go` | Add `schemaEvolution` struct, WAL-based buffering, distributed lock acquisition, buffering logic, unfreeze mechanism |
| `internal/protocol/state.go` | Add schema evolution state constants and key builder functions |
| `internal/protocol/message.go` | Add `Op: "schema_change_proactive"` and `SchemaDiff` type for proactive schema changes |
| `internal/engine/consumer.go` | Add schema commit acknowledgment publishing to `schema_ack` topic |
| `internal/engine/pipeline.go` | Wire up schema acknowledgment channel, WAL recovery on startup |
| `internal/config/manager.go` | Store/retrieve schema metadata and schema evolution state in KV |

---

## Test Scenario: `TestE2E_SchemaEvolution`

The test does:
1. Start pipeline with `users_evo` table (initial sync)
2. `ALTER TABLE ADD COLUMN phone TEXT`
3. Insert row with new column (`INSERT INTO users_evo (name, age, phone) VALUES (...)`)
4. Assert data synced with new column (query Databend, check `phone` column has value)

**Current failure point:** 
- Step 2: `ALTER TABLE` generates NO CDC message
- Step 3: CDC event arrives at T~31s with `phone` column in Relation message
- Discovery won't run until T=30s (if started at T=0)
- But even if discovery runs, the CDC data arrives at consumer before discovery schema change is processed
- Consumer calls `sink.ApplySchema()` then IMMEDIATELY calls `BatchUpload()` - no commit wait
- Databend rejects INSERT because `phone` column doesn't exist yet

**Success criteria after fix:**
1. `ALTER TABLE ADD COLUMN` does NOT generate CDC → rely on inline detection
2. CDC event at T=31s contains `phone` column → producer detects discrepancy with cache
3. Producer ACQUIRES distributed lock, sets state=Frozen, buffers event to WAL
4. Producer EMITS proactive schema_change with full schema (including `phone`)
5. Consumer receives, calls `sink.ApplySchema()` which executes ALTER TABLE in Databend
6. Consumer PUBLISHES to `schema_ack` topic after ALTER TABLE commits
7. Producer RECEIVES ack, releases lock, state=Stable, FLUSHES buffered events
8. Buffered INSERT with `phone` is now processed successfully

---

## Implementation Phases

### Phase 1: Core Infrastructure (Foundation)
**Goal:** Establish schema caching, distributed lock mechanism, and basic buffering

| Task | Description | Files |
|------|-------------|-------|
| 1.1 | Add schema cache to Producer (`cachedSchemas map[string]map[string]bool`) | `producer.go` |
| 1.2 | Add `SchemaEvolutionState` struct and KV persistence | `state.go`, `producer.go` |
| 1.3 | Implement distributed lock with NATS KV (`schema_lock.{table}`) | `producer.go` |
| 1.4 | Add timestamp-based conflict resolution logic | `producer.go` |
| 1.5 | Add `schemaEvolution` struct with mutex and frozen map | `producer.go` |

### Phase 2: Persistent Buffering with WAL
**Goal:** Ensure no data loss if pipeline crashes mid-evolution

| Task | Description | Files |
|------|-------------|-------|
| 2.1 | Design and implement WAL directory structure | `producer.go` |
| 2.2 | Add `WriteBuffer()` to persist messages to WAL | `producer.go` |
| 2.3 | Add `ReadBuffer()` and `FlushBuffer()` to recover and drain | `producer.go` |
| 2.4 | Add WAL cleanup on successful flush | `producer.go` |
| 2.5 | Add WAL recovery on startup (check for frozen tables in KV) | `pipeline.go` |

### Phase 3: Inline Detection & Smart Diff
**Goal:** Detect schema changes from CDC data, compute diffs with type change classification

| Task | Description | Files |
|------|-------------|-------|
| 3.1 | Add `SchemaDiff` struct with Added/Removed/TypeChanges | `message.go` |
| 3.2 | Add `CompareSchema()` function (discrepancy detection) | `source.go` |
| 3.3 | Add `ClassifyTypeChange()` - Widening vs Narrowing vs FamilyChange | `source.go` |
| 3.4 | Add auto-apply for Widening changes, alert for others | `producer.go` |
| 3.5 | Add column removal detection (ignore, log warning) | `source.go` |

### Phase 4: Egress Acknowledgment & Sequential Queue
**Goal:** Wait for Databend to commit schema change before flushing buffer

| Task | Description | Files |
|------|-------------|-------|
| 4.1 | Consumer: Add `schema_ack` topic publishing after `ApplySchema` succeeds | `consumer.go` |
| 4.2 | Producer: Subscribe to `schema_ack` topic | `pipeline.go`, `consumer.go` |
| 4.3 | Producer: Implement `unfreeze()` on ack receipt | `producer.go` |
| 4.4 | Producer: Implement sequential queue for same-table changes | `producer.go` |
| 4.5 | Producer: Implement queue position tracking and ordering | `producer.go` |

### Phase 5: Testing & Edge Cases
**Goal:** Verify all scenarios pass, handle edge cases

| Task | Description |
|------|-------------|
| 5.1 | Run `TestE2E_SchemaEvolution` - basic ADD COLUMN |
| 5.2 | Test type widening (INTEGER → BIGINT) - should auto-apply |
| 5.3 | Test type narrowing (BIGINT → INTEGER) - should alert, not freeze |
| 5.4 | Test column DROP - should ignore, log warning |
| 5.5 | Test concurrent changes on same table - should queue sequentially |
| 5.6 | Test concurrent changes on different tables - should process in parallel |
| 5.7 | Test lock contention (discovery vs CDC at same time) |
| 5.8 | Test restart recovery (crash mid-evolution, restart, resume) |

---

## Test Coverage Requirements

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `TestE2E_SchemaEvolution` | ADD COLUMN | Freeze → schema_change → ack → flush → success |
| `TestSchema_TypeWidening` | INTEGER → BIGINT | Auto-apply, no freeze needed |
| `TestSchema_TypeNarrowing` | BIGINT → INTEGER | Alert, state=TypeConflict, no data loss |
| `TestSchema_ColumnDrop` | DROP COLUMN | Ignore, log warning, continue processing |
| `TestSchema_ConcurrentSameTable` | Two ADD COLUMN rapid | Sequential queue, process in order |
| `TestSchema_ConcurrentDiffTable` | ADD col on two tables | Parallel processing, separate locks |
| `TestSchema_LockContention` | Discovery + CDC same time | Earlier timestamp wins, later queued/discarded |
| `TestSchema_RestartRecovery` | Crash during freeze | WAL recovery, re-acquire lock, resume |

---

## Key Design Decisions Summary

| Decision | Choice |
|----------|--------|
| Column DROP | Ignore, let data engineer handle manually |
| Type Widening | Auto-apply, no freeze needed |
| Type Narrowing | Alert, set state=TypeConflict, await manual |
| Type Family Change | Alert, set state=TypeConflict, await manual |
| Concurrent changes | Sequential queue per table, by timestamp |
| Lock conflict | Earlier timestamp wins; if same, discovery wins |
| Buffer persistence | WAL file per pipeline/table with LSN ranges |
| Restart recovery | Check KV for frozen tables, reconstruct from WAL |
| Ack mechanism | NATS topic `cdc.pipeline.{id}.schema_ack` |

---

## Open Questions - RESOLVED

### 1. Column DROP handling
**Decision: Ignore.** Do NOT drop column in sink. If a column is dropped in Postgres, we leave it in Databend. Data engineer handles cleanup manually. Only handle ADD COLUMN.

### 2. Type change handling
**Recommendation:**
- **Widening changes** (safe, auto-apply):
  - `VARCHAR(n)` → `VARCHAR(m)` where `m > n`
  - `INTEGER` → `BIGINT` / `SMALLINT` → `INTEGER`
  - `NUMERIC(p,s)` → `NUMERIC(p',s)` where `p' > p`
- **Narrowing changes** (unsafe, requires manual intervention):
  - `VARCHAR(100)` → `VARCHAR(50)` - data loss possible
  - `TEXT` → `VARCHAR(50)` - data loss possible
  - `INTEGER` → `SMALLINT` - overflow possible
- **Type family changes** (requires manual intervention):
  - `INTEGER` → `TEXT` / `VARCHAR` - semantic change
  - `TIMESTAMP` → `TEXT` - lose time operations
  - `BOOLEAN` → `INTEGER` -完全不同语义

**Implementation:** When detecting type change, compute `schemaDiff` with `ChangeType` field. If type is narrowing or family change, set table state to `TypeConflict` instead of auto-frozen. Log alert and await manual resolution.

### 3. Multiple concurrent schema changes
**Decision: Sequential queue by discovery timestamp.**
- Each schema change is assigned a discovery timestamp (when detected)
- Changes for the same table are queued in order of timestamp
- Process one schema change at a time per table (freeze → apply → unfreeze → next)
- Different tables can have their schema changes processed in parallel

### 4. Distributed Lock for Race Conditions
**Decision: Distributed lock using NATS KV with timestamp-based conflict resolution.**

**Race scenarios:**
1. Discovery interval detects `ALTER TABLE ADD COLUMN phone` at T=30s
2. CDC inline detection sees `phone` in INSERT data at T=31s (before discovery runs again)

**Lock mechanism:**
```
Key: cdc.pipeline.{id}.schema_lock.{table}
Value: {timestamp, source: "discovery"|"cdc", columns: {...}}

Lock Duration: 30 seconds (configurable)
```

**Conflict resolution:**
- If lock exists and `incoming_timestamp > lock_timestamp`: discard incoming, already processing
- If lock exists and `incoming_timestamp <= lock_timestamp`: wait for lock release
- If lock exists but `schema_diff` is DIFFERENT from locked schema:
  - Queue the new change (it arrived later, so it goes after current processing)
  - Mark as `queued` with position in queue
- If lock doesn't exist: acquire lock with timestamp

**Priority:** Discovery source wins if timestamps equal (scheduled probe is authoritative).

### 5. Restart Behavior - Persistent Frozen State
**Decision: Persist to NATS KV and local WAL.**

**Schema evolution state stored in KV:**
```
Key: cdc.pipeline.{id}.sources.{sid}.schema_evolution.{table}
Value: {
  Status: "Frozen" | "Updating" | "Stable" | "TypeConflict",
  FrozenAt: timestamp,
  Columns: map[string]string,  // current known schema
  BufferedLSN: uint64,         // highest LSN in buffer
  LockTimestamp: timestamp,
  LockSource: "discovery" | "cdc",
  QueuePosition: int,         // if queued
}
```

**Buffered CDC messages:**
- Stored in local persistent WAL (not in-memory) since crash during freeze could lose data
- Use a file-based WAL: `schema_evo/{pipelineID}/{table}/buffer_{lsn_start}_{lsn_end}.msgpack`
- On restart: read WAL, reconstruct buffer, re-acquire lock, resume

**Restart recovery flow:**
1. On startup, check KV for `Status = "Frozen"` tables
2. For each frozen table:
   - Re-acquire distributed lock (may need to wait if another instance has it)
   - Reconstruct buffer from WAL
   - Resume from where left off (send schema_change, wait for ack, flush buffer)

---

## Revised Implementation Plan

### Phase 1: Schema Caching & Distributed Lock (Week 1)
1. Add schema cache to Producer (`cachedSchemas map[string]map[string]bool`)
2. Add NATS KV-based distributed lock for schema changes
3. Lock key: `cdc.pipeline.{id}.schema_lock.{table}`
4. Implement timestamp-based conflict resolution
5. Unit tests for lock contention

### Phase 2: Per-Table Buffering with Persistent WAL (Week 1-2)
1. Add `schemaEvolution` struct with mutex, frozen map
2. Implement file-based WAL for buffered messages
3. Modify Producer.Run() to buffer messages for frozen tables
4. Add WAL recovery on restart
5. Add unfreeze mechanism with WAL cleanup

### Phase 3: Proactive Schema Fetching & Type Change Detection (Week 2)
1. Add method to fetch full schema when discrepancy found
2. Implement smart diff with `ChangeType` enum (Added, Removed, Widened, Narrowed, TypeFamilyChange)
3. Handle widening changes automatically
4. Mark narrowing/family changes as `TypeConflict`, alert, await manual
5. Emit `schema_change` message with schema diff info

### Phase 4: Egress Acknowledgment & Sequential Queue (Week 2-3)
1. Consumer publishes to schema_ack topic after successful ApplySchema
2. Producer listens on schema_ack, unfreezes table on receipt
3. Implement sequential queue for multiple changes on same table
4. Queue ordering by discovery timestamp

### Phase 5: Testing & Edge Cases (Week 3)
1. Run `TestE2E_SchemaEvolution` to verify basic flow
2. Add test for type change scenarios
3. Add test for concurrent schema changes on same/different tables
4. Add test for restart recovery mid-evolution
5. Add test for lock contention between discovery and CDC
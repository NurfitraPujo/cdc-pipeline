# Remediation Proposal: Dynamic Table Discovery (v2)

## Status
Proposed (Remediation)

## Date
2026-04-15

## Author
CDC Pipeline Team

---

## Summary
This proposal remediates the critical architectural, synchronization, and state management flaws found in the original "Dynamic Table Discovery Without Pipeline Restart" design. Instead of bypassing the central `PipelineConfig`, this design leverages a "Smart Reload" mechanism in the `ConfigManager`. It also introduces transaction-safe snapshotting to prevent data corruption and per-table state tracking to avoid redundant backfills on restart.

---

## Addressed Flaws from Original Design
1. **Split-Brain Configuration:** The original design bypassed NATS KV `PipelineConfig` updates to dodge a pipeline restart.
2. **Race Conditions / Data Corruption:** `ALTER PUBLICATION` and `pgx COPY` were not synchronized, risking old snapshot data overwriting newer CDC updates.
3. **Massive Re-backfills:** Pipeline restarts would blindly re-trigger snapshots for all dynamic tables due to a lack of per-table state tracking.
4. **Concurrency Panics:** Modifying a slice pointer (`msgs *[]Message`) across goroutines in `SnapshotTable` risked memory corruption.
5. **go-pq-cdc Startup Conflicts:** Native library resets at startup would drop dynamically added tables.

---

## Remediated Architecture & Design

### 1. Smart Configuration Reloads (No Split-Brain)
We must maintain `PipelineConfig` as the absolute source of truth. When a new table is discovered, the pipeline **must** update the `PipelineConfig` in NATS KV. 

**Modification in `ConfigManager` (`internal/config/manager.go`):**
Instead of unconditionally triggering `transitionWorker` (Drain → Shutdown → Restart) on every config change, `ConfigManager` will compute a "diff" between the old and new `PipelineConfig`.
- **If the ONLY change** is appended tables to the `Tables` array:
  - Do **not** restart the worker.
  - Send a dynamic control signal (e.g., via a Go channel to the running `Pipeline` orchestrator, or a specific NATS control topic) containing the new tables.
- **If any other fields change** (e.g., `BatchSize`, `SlotName`):
  - Trigger the standard `transitionWorker` cycle.

### 2. Per-Table State Tracking (Preventing Re-backfills)
To prevent the pipeline from re-snapshotting dynamic tables on every restart, we need granular state tracking with strict transition rules.

**New NATS KV Key:** `cdc.pipeline.{id}.sources.{sid}.tables.{table}.state`

**Explicit State Behaviors & Invariants:**

| State Value | Meaning | Expected Behavior & Actions | When is it set? |
|-------------|---------|-----------------------------|-----------------|
| *(Key Missing)* | Table newly discovered, never synced. | 1. Execute `ALTER PUBLICATION ADD TABLE`. <br>2. Set state to `Snapshotting`. <br>3. Begin snapshot process. | N/A (Initial condition) |
| `Snapshotting` | Worker crashed or stopped mid-snapshot. | 1. Execute `ALTER PUBLICATION ADD TABLE`. <br>2. Re-start snapshot process (since snapshots are idempotent based on the new Snapshot LSN). | Set immediately *before* calling `pgx COPY`. |
| `CDC` | Snapshot finished; normal CDC is flowing. | 1. If restarting, execute `ALTER PUBLICATION ADD TABLE` to ensure replication is active. <br>2. **Skip** `pgx COPY`. Proceed normally. | Set immediately *after* `pgx COPY` finishes and messages are flushed to NATS. |
| `Failed` | Snapshot encountered unrecoverable error. | 1. Log error, alert observability layer. <br>2. Halt processing for this specific table (do not resume snapshot or emit CDC). <br>3. Await manual intervention or automated retry loop. | Set if `pgx COPY` or KV operations fail repeatedly. |

### 3. Transaction-Safe Snapshot Synchronization (Watermarking)
To prevent snapshot rows from overwriting newer CDC updates (or vice versa), the `Producer` must synchronize the stream using strict invariants.

**The Workflow & Invariants:**
1. **Add to Publication:** Execute `ALTER PUBLICATION <name> ADD TABLE <tableName>`. From this moment, `go-pq-cdc` will start receiving CDC events for the new table.
2. **Pause & Queue CDC Emission:** The `Producer` intercepts any incoming CDC events for the *new table*. 
   - **Invariant:** No CDC event for a table in `Snapshotting` state can be flushed to the sink. They must be queued in memory (or dropped, if relying on the replication slot to hold them, but queueing is faster).
3. **Snapshot with Watermark:** 
   - Begin a transaction: `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ`.
   - Query the current WAL LSN: `SELECT pg_current_wal_lsn()`. This becomes the **Snapshot LSN**.
   - Execute `pgx COPY` to stream the snapshot.
   - Emit snapshot messages. **Critically**, tag these messages with `LSN = Snapshot LSN`.
4. **Transition State & Flush:**
   - Write `CDC` to the per-table KV state key.
   - **Resume CDC Emission:** The `Producer` stops queueing for this table. 
   - **Invariant:** The `Producer` iterates over the queued CDC events, immediately discarding any event where `LSN <= Snapshot LSN`. It resumes processing for events where `LSN > Snapshot LSN`.
   - Emit a `schema_change` message to downstream consumers.

*Note on Failure Invariant:* If the snapshot process fails (transitions to `Failed`), all queued CDC events for the table MUST be discarded to prevent out-of-order partial data if the process restarts later.

### 4. Concurrency-Safe Snapshotting
The `SnapshotTable` method must not mutate the main fetch loop's message slice pointer.

**Modified Signature:**
```go
func (s *PostgresSource) SnapshotTable(ctx context.Context, db *sql.DB, tableName string, msgChan chan<- protocol.Message) error
```
- The snapshot routine runs in its own goroutine and pushes `snapshot` messages to a dedicated `msgChan`. 
- The main `Producer` `select` loop reads from this channel and merges it into the batch buffer safely, respecting the `BatchSize` and `triggerFlush` logic.

### 5. Fixing Startup Conflicts with `go-pq-cdc`
Since `go-pq-cdc` initializes the publication on startup using the static `Tables` array, we must ensure it doesn't accidentally drop dynamic tables.
Because we fixed the Split-Brain issue (Step 1), `PipelineConfig.Tables` will *always* contain both static and dynamically discovered tables. 
When the pipeline restarts, `factory.CreateWorker()` passes the fully updated `PipelineConfig` to `go-pq-cdc`, meaning the library will natively include all tables during its startup checks. No custom startup `ALTER PUBLICATION` logic is required.

---

## Data Flow Diagram (Remediated)

```text
Discovery Interval (2s)
        │
        ▼
┌───────────────────────────────────────┐
│ discoverTables()                      │
│ 1. Query information_schema           │
│ 2. Find table NOT in config.Tables    │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│ Pipeline Config Update                │
│ 1. Append table to p.config.Tables    │
│ 2. kv.Put(PipelineConfigKey)          │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│ ConfigManager (Smart Reload)          │
│ 1. Detects ONLY Tables changed        │
│ 2. Sends signal to running Worker     │
│ 3. NO RESTART TRIGGERED               │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│ Producer (Worker)                     │
│ 1. ALTER PUBLICATION ADD TABLE        │
│ 2. Check table state in KV            │
│ 3. If state == Missing | Snapshotting │
│    - Set state = Snapshotting         │
│    - Start Snapshot Routine           │
│    - Queue new table's CDC events     │
│ 4. If state == CDC, proceed normally  │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│ Snapshot Routine                      │
│ 1. BEGIN REPEATABLE READ              │
│ 2. Get Snapshot LSN                   │
│ 3. pgx COPY -> msgChan (thread-safe)  │
│ 4. Update KV state to 'CDC'           │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│ Producer (Worker)                     │
│ 1. Resume new table CDC emission      │
│ 2. Drop queued CDC <= Snapshot LSN    │
│ 3. Emit schema_change                 │
└───────────────────────────────────────┘
```

---

## Action Plan & Files Affected

| File | Proposed Change |
|------|-----------------|
| `internal/config/manager.go` | Implement "Smart Reload" in `handlePipelineUpdates` to diff configs and signal workers instead of restarting them. |
| `internal/engine/producer.go` | Add logic to handle dynamic table signals from `ConfigManager`. Add CDC event queueing/filtering based on Snapshot LSN. Add KV state check and transitions. |
| `internal/source/postgres/source.go` | Add `SnapshotTable` with thread-safe channel design and Repeatable Read isolation. |
| `internal/protocol/state.go` | Add `TableState` enum constants (`Snapshotting`, `CDC`, `Failed`) and key builder function `TableStateKey`. |

---

## Conclusion
This remediated design fulfills the business requirement (zero downtime / no pipeline restart for new tables) while maintaining absolute architectural integrity. By keeping `PipelineConfig` as the single source of truth, synchronizing snapshots with LSN watermarks, and defining strict invariant-driven per-table state transitions, we eliminate the risks of data corruption, concurrency panics, and split-brain configurations.
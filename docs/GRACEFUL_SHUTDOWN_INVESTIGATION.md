# E2E Graceful Shutdown Testing - Investigation Summary

**Date:** 2026-04-17  
**Status:** Investigation Complete - Bug Detected

---

## Task Overview

Create e2e tests for **Priority 1: Graceful Shutdown Under Load** to validate:
- Zero message loss during shutdown
- Exactly-once delivery semantics on restart
- Drain phase completion before shutdown

---

## Tests Created

File: `internal/test/e2e/graceful_shutdown_test.go`

### Passing Tests

| Test | Description | Result |
|------|-------------|--------|
| `TestE2E_GracefulShutdown_ZeroMessageLoss` | Verifies all synced data preserved after clean shutdown | PASS |
| `TestE2E_GracefulShutdown_ExactlyOnceOnRestart` | Verifies no duplicates after restart with pre-synced data | PASS |
| `TestE2E_GracefulShutdown_PreSyncedDataPreserved` | Verifies data integrity after shutdown | PASS |

### Tests That Revealed Issues

| Test | Description | Result |
|------|-------------|--------|
| `TestE2E_CDCCrashRecovery_ResumeFromCheckpoint` | **FAILS** - Detects rows inserted during pipeline stop are NOT recovered | BUG DETECTED |
| `TestE2E_CDCCrashRecovery_ExactlyOnceGuarantee` | Passes when restart is immediate | Limited validation |

---

## Root Cause Analysis

### Issue: CDC Messages Lost After Pipeline Restart

**Symptom:**
- Phase1 (50 rows): Inserted and synced successfully
- Phase2 (30 rows): Inserted **while pipeline was stopped**
- After restart: Only 50 rows recovered, **30 rows LOST**

**Test Output:**
```
Phase1: 50 rows, Phase2 inserted while stopped: 30 rows, Phase2 recovered: 0 rows
Lost 30 rows during restart - replication slot may not be resuming from correct LSN
```

### Investigation Findings

#### 1. Checkpoint Mechanism

The system uses a two-level checkpoint system:

- **IngressLSN** (producer.go:265-277): Saved after messages are published to NATS
  ```go
  cp := protocol.Checkpoint{IngressLSN: m.LSN}
  key := protocol.IngressCheckpointKey(p.pipelineID, m.SourceID, m.Table)
  p.kv.Put(key, cpData)
  ```

- **EgressLSN** (consumer.go:425-436): Saved after messages are acked by NATS consumer

#### 2. Restart Flow (pipeline.go:96-111)

```go
// Get Checkpoints for all tables
minLSN := uint64(0)
for _, table := range p.config.Tables {
    cpKey := protocol.IngressCheckpointKey(p.id, sourceID, table)
    cpEntry, err := p.producer.kv.Get(cpKey)
    if err == nil {
        var cp protocol.Checkpoint
        if err := json.Unmarshal(cpEntry.Value(), &cp); err == nil {
            if minLSN == 0 || cp.IngressLSN < minLSN {
                minLSN = cp.IngressLSN
            }
        }
    }
}
initialCP := protocol.Checkpoint{IngressLSN: minLSN}
```

#### 3. Stream Initialization (go-pq-cdc/connector.go)

When connector starts CDC:

```go
stream := replication.NewStream(cfg.ReplicationDSN(), cfg, m, listenerFunc)
// ...
c.stream.Open(ctx)
```

The `stream.lastXLogPos` is **always initialized to 0** (stream.go:82):

```go
lastXLogPos: 0,  // 0 means start from confirmed_flush_lsn
```

**Critical Issue:** When `Snapshot.Enabled = false` and NOT opening from snapshot LSN:
- Stream starts from slot's `confirmed_flush_lsn` (PostgreSQL manages this)
- The checkpoint's `IngressLSN` is **never used** to set `stream.lastXLogPos`

#### 4. ACK Mechanism (source.go:312-316)

```go
select {
case <-s.ackChan:
    lc.Ack()  // Tell PostgreSQL to update confirmed_flush_lsn
case <-sourceCtx.Done():
}
```

But producer uses **non-blocking send** (producer.go:291-296):
```go
select {
case ackChan <- struct{}{}:
case <-ctx.Done():
    return lastLSN, nil
default:
    // No one waiting for ack, that's fine  <-- SILENT FAILURE!
}
```

**Race Condition:** If `ackChan` is full, the ACK to PostgreSQL is never sent, but the code continues.

#### 5. PostgreSQL Slot Behavior

On restart, PostgreSQL uses `confirmed_flush_lsn` to determine where to resume:

- If ACK was received: `confirmed_flush_lsn` points to last processed message
- If ACK was lost: `confirmed_flush_lsn` points to earlier position
- `restart_lsn` is updated by PostgreSQL based on `confirmed_flush_lsn`

From slot info during restart:
```
restartLSN: 26656456
confirmed_flush_lsn: 26656512
```

#### 6. Potential Root Cause

**Theory 1: ACK Race Condition**
- Producer publishes to NATS
- Producer saves IngressLSN checkpoint
- Producer tries non-blocking send to `ackChan` - may FAIL silently
- Source never ACKs to PostgreSQL
- `confirmed_flush_lsn` doesn't advance in PostgreSQL
- On restart, replication starts from old `confirmed_flush_lsn`
- Messages between old and new position are **skipped/lost**

**Theory 2: Snapshot Resume Logic**
```go
// connector.go
if c.cfg.Snapshot.Enabled && c.shouldTakeSnapshot(ctx) {
    // Snapshot path - creates slot first to preserve WAL
} else {
    // CDC resume path - uses existing slot's confirmed_flush_lsn
}
```

If `shouldTakeSnapshot` returns `false` incorrectly (snapshot appears complete but isn't), the system enters CDC resume with stale slot state.

---

## Key Code Locations

| File | Line(s) | Issue |
|------|---------|-------|
| `internal/engine/producer.go` | 291-296 | Non-blocking ACK send - can silently fail |
| `internal/source/postgres/source.go` | 312-316 | Blocking receive on ackChan - waits forever |
| `vendor/go-pq-cdc/pq/replication/stream.go` | 82 | `lastXLogPos: 0` - ignores checkpoint LSN |
| `vendor/go-pq-cdc/connector.go` | 119 | Stream created without checkpoint context |

---

## Recommendations

1. **Fix ACK Race Condition**
   - Make ACK send blocking OR
   - Implement retry mechanism for failed ACK sends OR
   - Use a separate goroutine to handle ACKs asynchronously

2. **Propagate Checkpoint LSN to Stream**
   - Modify `NewConnector` or `startConnector` to accept checkpoint LSN
   - Set `stream.lastXLogPos = checkpoint.IngressLSN` before `stream.Open()`
   - This ensures CDC resumes from exact checkpoint, not from PostgreSQL's `confirmed_flush_lsn`

3. **Add WAL Retention Verification**
   - Before restart, verify slot's `restart_lsn` matches IngressLSN checkpoint
   - If mismatch, log warning or prevent restart until consistent

4. **Additional Testing**
   - Test with slow NATS consumer to trigger ACK race condition
   - Test with high-volume inserts during pipeline stop
   - Test with simulated network partition between producer and source

---

## Files Modified

- `internal/test/e2e/graceful_shutdown_test.go` - New test file with 5 tests

---

## Test Execution Results

```bash
# Run graceful shutdown tests (pass)
go test -v -run "TestE2E_GracefulShutdown" ./internal/test/e2e/ -timeout 180s
# Result: 3 PASS

# Run crash recovery tests (reveals bug)
go test -v -run "TestE2E_CDCCrashRecovery" ./internal/test/e2e/ -timeout 180s
# Result: 1 PASS (limited), 1 FAIL (detects data loss)
```

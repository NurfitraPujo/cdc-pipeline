# Holistic Code Review Remediation Plan

Source: `summaries/architecture_review/holistic_code_review.md` and the 8 supporting subagent reports.

This plan consolidates every finding from the review into prioritized, individually actionable tickets. Each item is scoped small enough to ship behind its own PR. Items are grouped by execution tier; within a tier, order matters where stated.

Sequencing rationale (TL;DR): Tier 0 must land before any production restart because every Critical path can cause silent data loss, lockouts, or unbounded CPU. Tier 1 stabilizes the runtime so Tier 2 hardening does not race against crashes. Tier 2 hardens the public surface. Tier 3 is polish.

---

## Priority Matrix

| Tier | Severity | Count | Goal |
|------|----------|-------|------|
| 0    | Critical |   7   | Stop data loss / DoS / lockout |
| 1    | High     |  16   | Stabilize runtime, plug leaks, fix concurrency |
| 2    | Medium   |  11   | Harden security and operational surface |
| 3    | Low      |   4   | Polish / DX |

---

## Tier 0 - Critical (block any prod restart until merged)

### T0-1. Fake Ack Loop Bypasses Flow Control (Data Loss)
- **Source**: `summaries/architecture_review/subagent_postgres_source_findings.md` finding #1; `subagent_broker_stream_findings.md` #2; `holistic_code_review.md` critical #1.
- **Files**: `internal/source/postgres/source.go` lines 140-180, 320-360.
- **Problem (verified)**: `select { case <-s.ackChan: lc.Ack(); default: lc.Ack() }` advances the PostgreSQL slot LSN before the message is published to NATS or written to the sink. A crash here is permanent data loss.
- **Fix plan**:
  1. Remove the `default:` arm entirely. Do not call `lc.Ack()` inside the replication callback.
  2. Add a thread-safe `AckManager` on `PostgresSource` that tracks pending LSNs using a map-based sliding window:
     ```go
     type AckManager struct {
         mu        sync.Mutex
         pending   map[uint64]bool // LSN -> Acknowledged status
         watermark uint64          // Highest contiguous acknowledged LSN
     }
     func (a *AckManager) Observe(lsn uint64) {
         a.mu.Lock()
         defer a.mu.Unlock()
         a.pending[lsn] = false
     }
     func (a *AckManager) Confirm(lsn uint64) uint64 {
         a.mu.Lock()
         defer a.mu.Unlock()
         a.pending[lsn] = true
         for {
             next := a.watermark + 1
             if acked, ok := a.pending[next]; ok && acked {
                 a.watermark = next
                 delete(a.pending, next)
             } else {
                 break
             }
         }
         return a.watermark
     }
     ```
  3. Spawn a coordinator goroutine that:
     - Receives from `s.ackChan` (a `chan uint64` carrying the acknowledged LSN).
     - Calls `AckManager.Confirm` to calculate the new confirmed watermark.
     - Periodically (via a `500ms` ticker) calls PostgreSQL's `SendStandbyStatusUpdate` carrying the *current, confirmed* watermark LSN (keepalive only; do NOT auto-advance the watermark LSN based on a timer to prevent silent data loss).
  4. Replace `s.ackChan chan struct{}` with `s.ackChan chan uint64` and update producer/consumer to push the LSN corresponding to durable NATS publish / successful sink write.
  5. Document the at-least-once contract on `PostgresSource` and update T1-2 (checkpoint update) at the same time.
- **Acceptance**:
  - With NATS stopped, the source buffers messages and Postgres WAL grows but the slot LSN does not advance.
  - With the sink failing, the source blocks cleanly on `s.ackChan` until configured drain.
  - A simulated crash mid-batch replays the batch exactly once on restart.
- **Verification**:
  - Unit test: mock `lc.Ack()` and assert it is called only after `s.ackChan` receives confirmations.
  - Integration test: assert watermark behavior with a fake NATS that delays publish.
  - Run `go vet ./...` and the existing `source_test.go`.

### T0-2. Premature Acknowledgment of Schema-Alter Messages (Data Loss)
- **Source**: `subagent_engine_schema_findings.md` finding #2; `subagent_broker_stream_findings.md` #1; `holistic_code_review.md` critical #4.
- **Files**: `internal/engine/consumer.go` lines 230-310.
- **Problem (verified)**: The new `wmMsg` is appended to `wmMsgs` before the previous batch is flushed. `c.flush()` acks everything in `wmMsgs`, including the schema-change message, before `ApplySchema` runs.
- **Fix plan**:
  1. Decouple schema-change ack from data-change ack.
  2. Pre-scan the batch for `OpSchemaChange`; if found, flush the prior `batch`/`wmMsgs` first using only their messages.
  3. Carry the schema-change `wmMsg` in a separate `schemaAck []*message.Message` slice, and only `Ack()` those after `c.sink.ApplySchema(ctx, transformed)` returns nil.
  4. If `ApplySchema` errors, `Nack()` the schema message and return; do not abort the consumer loop on a single DDL failure.
  5. Extend `flush()` signature with `acksFilter func(*message.Message) bool` so the calling site decides what to ack.
- **Acceptance**:
  - Replaying a schema-change message after a crash reproduces the DDL exactly once on restart.
  - Failing `ApplySchema` keeps the schema-change message un-acked (NATS redelivers).
- **Verification**: New unit test in `internal/engine` driving a stub `Sink` whose `ApplySchema` errors and asserts `wmMsg.Nack()` and no Ack call.

### T0-3. CDC Event Ordering Scrambling in Transformer (Data Corruption)
- **Source**: `subagent_engine_schema_findings.md` finding #1; `holistic_code_review.md` critical #2.
- **Files**: `internal/transformer/nats/protobuf.go` lines 85-150.
- **Problem (verified)**: `TransformBatch` builds `result = append(passthrough..., transformed...)`, losing original sequence.
- **Fix plan**:
  1. Replace the two-list append with an ordered reassembly:
     ```go
     type slot struct{ idx int; msg protocol.Message; dropped bool }
     results := make([]slot, len(msgs))
     matchIdx := 0
     for i, m := range msgs {
         if t.matchesFilter(m) { matchIdx++ } else { results[i] = slot{idx: i, msg: m} }
     }
     for _, tr := range transformed { /* fill results at original index */ }
     ```
  2. If a matched message is dropped by the transformer, emit a DLQ event rather than silently removing it.
  3. Add a property-test helper that round-trips a randomized batch and asserts `for i, m := range in { if m.Op preserved then result[i].Op == m.Op ordering }`.
- **Acceptance**:
  - Sequence is preserved with mixed matching/passthrough.
  - DLQ receives messages the transformer filters out.
- **Verification**: New unit test `TestTransformBatchPreservesOrder` with adversarial input.

### T0-4. Closed Channel Infinite CPU Spinning (DoS)
- **Source**: `subagent_api_sse_findings.md` #1; `subagent_supervisor_config_findings.md` #1; `holistic_code_review.md` critical #3.
- **Files**: `internal/api/handler.go` line ~1293 (`StreamMetrics`); `internal/config/manager.go` lines 130-170 (`handleGlobalUpdates`) and lines 200-290 (`handlePipelineUpdates`).
- **Problem (verified)**: Both read `case entry := <-watcher.Updates()` without the comma-ok check. When the watcher closes, the loop spins on a zero-value receive.
- **Fix plan**:
  1. Apply a single mechanical change at all three sites:
     ```go
     case entry, ok := <-watcher.Updates():
         if !ok {
             log.Warn().Msg("watcher channel closed; exiting handler")
             return
         }
         if entry == nil { continue }
     ```
  2. Add a `defer` that logs the watcher stop reason.
  3. Optional: factor into a small helper `watchKeyValueUpdates(ctx, watcher, onUpdate)` to prevent regression.
- **Acceptance**: Trigger NATS reconnect while a metrics SSE is open. CPU stays at baseline; goroutine count does not grow.
- **Verification**: Add a unit test that closes the watcher channel and asserts the loop returns within 1 iteration.

### T0-5. Broken Schema Evolution for Qualified Tables (Ingestion Blocker)
- **Source**: `subagent_databend_sink_findings.md` #1; `holistic_code_review.md` critical #5.
- **Files**: `internal/sink/databend/sink.go` lines 175-200 (`getCurrentColumns`), plus the related identifier quoting at lines 90-96.
- **Problem (verified)**: `WHERE table_name = 'mydb.mytable'` returns empty; new columns are silently dropped.
- **Fix plan**:
  1. Split the qualified name on `.`:
     ```go
     schema, table := splitQualified(tableName)
     // query: WHERE table_schema = ? AND table_name = ?
     ```
  2. Tokenize `quoteIdentifier` per part, joining with `.`. Pair this with T1-13 which has the same root cause for the DDL path.
  3. After `getCurrentColumns` returns its result, log the count of columns found for observability.
- **Acceptance**: Add a new column to a Postgres table that has a `db.schema.table` qualifier; it appears in Databend within one CDC cycle.
- **Verification**: Integration test against a real or testcontainer Postgres + Databend.

### T0-6. Missing NATS Subscriber Reconnect (Service Outage)
- **Source**: `subagent_broker_stream_findings.md` #3; `holistic_code_review.md` critical #6.
- **Files**: `internal/stream/nats/subscriber.go` lines 30-60.
- **Fix plan**:
  1. Add `go_nats.MaxReconnects(-1), go_nats.ReconnectWait(2 * time.Second), go_nats.Timeout(5 * time.Second), go_nats.PingInterval(20*time.Second), go_nats.MaxPingsOutstanding(2)` to the `NatsOptions`.
  2. Mirror the publisher's reconnected/disconnected/closed callbacks for observability.
  3. Emit a Prometheus counter on disconnect/reconnect.
- **Acceptance**: Sub-NATS network partition longer than 2 minutes auto-recovers with no manual restart.
- **Verification**: Add a chaos test that drops the NATS socket for 3 minutes and asserts the subscriber resumes.

### T0-7. Frontend 401 Infinite Redirection Loop (Auth Lockout)
- **Source**: `subagent_frontend_findings.md` #1; `subagent_auth_security_findings.md` #1; `holistic_code_review.md` critical #7.
- **Files**: `web/src/api/schema-client.ts` lines 23-50; `web/src/stores/authStore.ts` lines 17-60; `web/src/routes/__root.tsx` line 62.
- **Fix plan**:
  1. Consolidate auth state into the Zustand store as the single source of truth:
     ```ts
     useAuthStore.getState().logout(); // clears token, isAuthenticated, localStorage cdc-auth-storage
     ```
  2. Update the openapi-fetch interceptor to read/write the token via `useAuthStore.getState().token` instead of a separate `cdc_token` localStorage key.
  3. On 401, call `useAuthStore.getState().logout()` and then `window.location.assign("/login")`.
  4. Remove the now-orphaned `cdc_token` cleanup code and any direct `localStorage.setItem("cdc_token", ...)` writes elsewhere.
  5. Add a `__root.tsx` `beforeLoad` guard that explicitly checks both `isAuthenticated` and a token presence to prevent the guard from being misled.
- **Acceptance**: Force-expire a token; the user lands on `/login`, stays there, and re-authenticates without bouncing.
- **Verification**: Cypress/Playwright test that mocks `/api/*` returning 401 and asserts URL stability.

---

## Tier 1 - High Severity (stabilize runtime)

### T1-1. Silent Data Loss on Deserialization Failure in Databend Sink
- **Source**: `subagent_databend_sink_findings.md` #4.
- **Files**: `internal/sink/databend/sink.go` lines 250-260 and 365-380.
- **Fix**: Replace silent `continue` with a `log.Error().Err(err)` plus a structured `protocol.SinkDeadLetterEvent`. Publish to a configurable DLQ NATS subject. Add a circuit-breaker metric `cdc_sink_dlq_total`.
- **Acceptance**: Every dropped row is observable in NATS DLQ + logs.

### T1-2. Checkpoint State `lastCheckpoint` Never Updated
- **Source**: `subagent_postgres_source_findings.md` #3; `holistic_code_review.md` high #15.
- **Files**: `internal/source/postgres/source.go` lines 351-360.
- **Fix**: In `UpdateXLogPos`, assign `s.lastCheckpoint.IngressLSN = lsn` before forwarding to the connector. Add invariant test that the in-memory checkpoint grows monotonically.

### T1-3. Channel Double-Close & Panic on Stream Restart
- **Source**: `subagent_postgres_source_findings.md` #4; `holistic_code_review.md` high #16.
- **Files**: `internal/source/postgres/source.go` lines 322-348 and 380-460.
- **Fix**: In `RestartWithNewTables`, cancel the old context (`s.cancel()`) and explicitly wait for running goroutines using `s.runWg.Wait()` *before* reallocating `s.msgChan` and starting the new connector under `s.mu` (write lock). This ensures that the old session's background cleanup handler completes before the new channel session begins, preventing double-close panics.

### T1-4. Eager Connection Allocation in `Snapshotter`
- **Source**: `subagent_postgres_source_findings.md` #5.
- **Files**: `internal/vendor/go-pq-cdc/pq/snapshot/snapshot.go` lines 50-90.
- **Fix**: Move connection establishment from `Snapshotter.New` into a new `Connect(ctx)` method. Call `Connect` only when `prepareSnapshot` actually runs (gated by `shouldTakeSnapshot`).
- **Note**: This is a vendored patch. Track upstream PR; carry the patch with a `//go-pq-cdc patch:` comment.

### T1-5. Destructive `isClosed` Helper
- **Source**: `subagent_postgres_source_findings.md` #6.
- **Files**: `internal/vendor/go-pq-cdc/pq/replication/stream.go` lines 575-595.
- **Fix**: Replace `isClosed(ch)` with an `atomic.Bool` set under `sync.Once.Do(closeChan)` by the producer goroutine. Replace all call sites with `closed.Load()`.

### T1-6. Unbounded Prometheus Cardinality in Heartbeats
- **Source**: `subagent_supervisor_config_findings.md` #2; `holistic_code_review.md` high #11.
- **Files**: `internal/config/manager.go` lines 620-740.
- **Fix**: Use a stable `pid` as the Prometheus label; put the timestamp in the NATS KV payload only. Add a unit test asserting that repeated restarts do not grow label cardinality beyond `len(workers)`.

### T1-7. Unthrottled Goroutines on `ListPipelines`
- **Source**: `subagent_api_sse_findings.md` #2; `holistic_code_review.md` high #12.
- **Files**: `internal/api/handler.go` lines 275-285.
- **Fix**: Replace the inline `go h.cleanupStaleHeartbeats()` with either `singleflight.Group.Do("cleanup", ...)` or a startup-time background ticker (`time.NewTicker(60*time.Second)` in `cmd/api/main.go`). Add a metric `cdc_api_cleanup_runs_total`.

### T1-8. Default Credentials Seeded on Env Misconfiguration
- **Source**: `subagent_auth_security_findings.md` #2; `holistic_code_review.md` high #13.
- **Files**: `internal/api/auth.go` lines 120-140.
- **Fix**: Replace the denylist with an allowlist: only seed if `ENV == "development"` or `ENV == "dev"`. Allow override of username/password via `DEV_ADMIN_USERNAME` / `DEV_ADMIN_PASSWORD`. Log a loud `WARN` line when seeding occurs.

### T1-9. No Rate Limit on `/login`
- **Source**: `subagent_auth_security_findings.md` #3; `holistic_code_review.md` high #14.
- **Files**: `internal/api/auth.go`, `cmd/api/main.go`.
- **Fix**: Add an IP-based `golang.org/x/time/rate` token bucket (e.g. 1 rps burst 5) mounted via `r.Use(RateLimitMiddleware(...))` for `/api/v1/login`. Expose remaining quota in `X-RateLimit-Remaining`. Add unit test.

### T1-10. Transformer NATS Connection Leak on Worker Reload
- **Source**: `subagent_engine_schema_findings.md` #4.
- **Files**: `internal/transformer/nats/protobuf.go` line 62; `internal/engine/pipeline.go` shutdown path.
- **Fix**: Add a `CloseableTransformer` interface and invoke `Close()` in the worker's teardown. Verify with `lsof`-equivalent metric on goroutine count diff.

### T1-11. CAS Revision Write Failures Silently Swallowed
- **Source**: `subagent_engine_schema_findings.md` #5.
- **Files**: `internal/engine/producer.go` lines 595-620.
- **Fix**: Wrap `persistEvoState` in a retry loop (max 5, 50ms backoff). Escalate to a circuit-breaker that pauses CDC ingestion of that table when retries are exhausted.

### T1-12. Snapshot Pagination Shift on Live Tables
- **Source**: `subagent_engine_schema_findings.md` #6; `holistic_code_review.md` high #20.
- **Files**: `internal/engine/producer.go` lines 835-855.
- **Fix**: Replace `LIMIT/OFFSET` with keyset pagination: `WHERE pk > $last ORDER BY pk LIMIT N`. Persist `$last` per table in NATS KV so resumes work mid-snapshot.

### T1-13. Dotted Namespace Identifier Quoting
- **Source**: `subagent_databend_sink_findings.md` #6; `holistic_code_review.md` high #22.
- **Files**: `internal/sink/databend/sink.go` lines 90-100.
- **Fix**: Refactor `quoteIdentifier` to split on `.` and quote each component. Pair with T0-5.

### T1-14. Parameter / Placeholder Limit Overflow in Bulk Inserts
- **Source**: `subagent_databend_sink_findings.md` #5; `holistic_code_review.md` high #23.
- **Files**: `internal/sink/databend/sink.go` lines 305-345.
- **Fix**: Wrap batch construction in a chunker that respects a configurable `maxPlaceholders` (default 10000). Emit `cdc_sink_chunks_total` metric.

### T1-15. Integer Overflow in Exponential Backoff
- **Source**: `subagent_supervisor_config_findings.md` #4; `holistic_code_review.md` high #21.
- **Files**: `internal/config/manager.go` lines 490-510.
- **Fix**: Cap `attempt` at 15 before shifting. Cap the resulting `time.Duration` at `60 * time.Second`. Add a unit test that simulates `attempt = 1000`.

### T1-16. Browser EventSource Connection Leak
- **Source**: `subagent_frontend_findings.md` #2; `holistic_code_review.md` high #18.
- **Files**: `web/src/hooks/useSSE.ts`; `web/src/routes/pipelines/$id/index.tsx` line 80.
- **Fix**: Replace dependency-array `options` with `optionsRef = useRef(options)` + a `useEffect` to update it. Strip `options` from `useCallback` deps.

### T1-17. Transient Primary Key Cache on Sink Restart
- **Source**: `subagent_databend_sink_findings.md` #2; `holistic_code_review.md` high #19.
- **Files**: `internal/sink/databend/sink.go` lines 20-30, 270-280.
- **Fix**: On sink startup, attempt to load PK information from the Databend `SHOW CREATE TABLE` output, falling back to the cache only when empty. Surface PK resolution as a metric.

### T1-18. Drain -> CDC Race Producing Lost Messages
- **Source**: `subagent_engine_schema_findings.md` #3.
- **Files**: `internal/engine/producer.go` lines 440-495.
- **Fix**: Hold `p.muTableStates.Lock()` atomically across both the empty-queue verification and the local state transition:
  ```go
  p.muTableStates.Lock()
  if p.buffer.Length() == 0 {
      p.tableStates[table] = StateCDC
      p.muTableStates.Unlock()
      return nil
  }
  p.muTableStates.Unlock()
  ```
  This ensures that new incoming CDC events cannot be placed in the buffer queue while the flush routine is exiting.

### T1-19. Sequential Block Publishing in Watermill Publisher
- **Source**: `subagent_broker_stream_findings.md` #7.
- **Files**: `internal/stream/nats/publisher.go` lines 35-55.
- **Fix**: Replace with custom publisher that uses `js.PublishAsync` and awaits pub-confirms at batch boundaries. Defer until T1-20 ack-loop removal is decided.

### T1-20. Per-Record Ack Loop Overhead
- **Source**: `subagent_broker_stream_findings.md` #8.
- **Files**: `internal/engine/consumer.go` lines 415-445; `internal/engine/producer.go` lines 120-165.
- **Fix**: Once T0-1 is in place, eliminate the per-record `AcksTopic`. Acknowledge Postgres WAL directly when the producer receives the NATS JetStream publish-confirm.

### T1-21. In-Memory Retry Tracking Memory Leak
- **Source**: `subagent_broker_stream_findings.md` #4.
- **Files**: `internal/engine/consumer.go` lines 460-485 and 655-675.
- **Fix**: Move delivery-count tracking to NATS JetStream `MaxDeliver`. Configure `DeadLetter` to a NATS stream. Remove the in-memory `c.retries` map.

### T1-22. Fail-Fast Pipeline Collapse on Transient Network Blips
- **Source**: `subagent_broker_stream_findings.md` #5.
- **Files**: `internal/engine/producer.go` lines 300-330; `internal/engine/pipeline.go` lines 120-135.
- **Fix**: Wrap circuit breaker in a backoff-aware `publishWithRetry` that pauses for the cool-down interval. Add a `recover()` step in `pipeline.go` that retries the producer once before shutting down the consumer.

### T1-23. Reflection-Based Serialization Overhead
- **Source**: `subagent_broker_stream_findings.md` #6.
- **Files**: `internal/protocol/message.go` line 49.
- **Fix**: Use `sync.Pool` for buffer reuse and prefer raw `Payload` ([]byte) for transport. Keep `Data` populated only for transformer side-effects.

### T1-24. Detached Tickers Cause Races on Shutdown
- **Source**: `subagent_postgres_source_findings.md` #7.
- **Files**: `internal/source/postgres/source.go` lines 305-335.
- **Fix**: Wrap each ticker in `s.runWg.Add(1)/Done()` and ensure `Stop()` waits for them before closing the DB.

### T1-25. Dynamic Metric Port Increments
- **Source**: `subagent_postgres_source_findings.md` #8.
- **Files**: `internal/source/postgres/source.go` lines 245-260, 405-415.
- **Fix**: Read the port from configuration; share it across hot-restarts. Add a metric `cdc_source_restart_total` so operators can correlate state.

### T1-26. Concurrency Race in `startNewWorker` Deletion Path
- **Source**: `subagent_supervisor_config_findings.md` #3.
- **Files**: `internal/config/manager.go` lines 465-485 and 755-805.
- **Fix**: After `m.factory(...)` returns, check `supCtx.Err()`. If cancelled, immediately `Shutdown()` the worker and skip inserting into `m.workers`.

### T1-27. Out-of-Order KV Delete Events Can Terminate New Workers
- **Source**: `subagent_supervisor_config_findings.md` #7.
- **Files**: `internal/config/manager.go` lines 215-225.
- **Fix**: Compare `entry.Revision()` against `m.revisions[id]`; only act on higher-revision events. Persist last-seen revision to KV on shutdown.

### T1-28. Concurrently-Confused Map Lookup in `handlePipelineUpdates`
- **Source**: `subagent_supervisor_config_findings.md` #6.
- **Files**: `internal/config/manager.go` lines 255-265.
- **Fix**: Read worker pointer under write lock; only upgrade to read when safe.

### T1-29. Supervisor Loop Context Cascade
- **Source**: `subagent_supervisor_config_findings.md` #8.
- **Files**: `internal/config/manager.go` lines 450-465.
- **Fix**: When `m.ctx` is nil, fall back to `context.Background()`, never the parameter `ctx` of the previous supervisor.

### T1-30. Graceful Shutdown Leaks on Drain Timeout
- **Source**: `subagent_supervisor_config_findings.md` #5.
- **Files**: `internal/config/manager.go` lines 780-805.
- **Fix**: Wrap `<-w.Finished()` in `select { case <-w.Finished(): case <-ctx.Done(): }`.

### T1-31. Excessive NATS KV Writes for Heartbeats
- **Source**: `subagent_supervisor_config_findings.md` #9.
- **Files**: `internal/config/manager.go` lines 605-740.
- **Fix**: Lower heartbeat frequency to 10s. Move from KV to a regular NATS pub/sub subject. Keep a slow-firing KV write for the API to render status.

### T1-32. Goroutine Leak in Dynamic Table Snapshotting
- **Source**: `subagent_engine_schema_findings.md` #7.
- **Files**: `internal/engine/producer.go` lines 745-785.
- **Fix**: Pass `p.ctx` into `performChunkedSnapshot` and use `db.QueryContext`. Add a `defer cancel()` pattern in the spawned goroutine.

### T1-33. Lock Contention from Network Inside Lock Scope
- **Source**: `subagent_engine_schema_findings.md` #8.
- **Files**: `internal/engine/producer.go` lines 495-580.
- **Fix**: Clone state under read lock, perform NATS call without the lock, commit revision under write lock.

### T1-34. MessagePack GC Pressure
- **Source**: `subagent_engine_schema_findings.md` #9.
- **Files**: `internal/engine/producer.go` line 258; `internal/engine/consumer.go` line 249.
- **Fix**: Introduce a `sync.Pool` of byte buffers; benchmark before/after.

---

## Tier 2 - Medium (harden security & ops)

| ID  | Finding | File(s) | Fix Summary |
|-----|---------|---------|-------------|
| T2-1 | SSRF in TestSource/Connection | `internal/api/handler.go` lines 1410-1490 | Resolve host and implement a custom `net.Dialer` with a `Control` callback that rejects loopback, link-local, and private RFC 1918 ranges, preventing DNS rebinding attacks. |
| T2-2 | Missing HTTP timeouts | `cmd/api/main.go` lines 150-160 | Set ReadTimeout/WriteTimeout/IdleTimeout on `http.Server`. |
| T2-3 | Silent decryption fallbacks | `internal/protocol/config.go` lines 270-300 | Return errors instead of falling through; fail-fast in bootstrap. |
| T2-4 | Lossy type mapping (decimal/array) | `internal/sink/databend/sink.go` lines 200-235, 320-325; `docs/todos/lossy_type_mappings.md` | Map decimals to DECIMAL(38,9), arrays to VARIANT/ARRAY; expand Go primitive switch. |
| T2-5 | Patched upstream dep risk | `internal/vendor/go-pq-cdc/**` | Open upstream PRs; track patches in `vendor/PATCHES.md`; evaluate pglogrepl. |
| T2-6 | Retry filter ignored | `internal/vendor/go-pq-cdc/internal/retry/retry.go` | Register `RetryIf(rc.If)` in options array inside `retry.go`'s `Do` method, and correct connection.go's check filter callback from `err == nil` to `err != nil`. |
| T2-7 | Slice index overflow in pagination | `internal/api/handler.go` lines 285-345 | Cap `limit <= 100`; verify `start >= 0 && start <= total`. |
| T2-8 | Frontend JSON `//` comments | `web/src/lib/jsonToUpdateRequest.ts` | Strip both `#` and `//` lines before `JSON.parse`. |
| T2-9 | Frontend stale config in editor | `web/src/components/ConfigEditor.tsx` | Add `useEffect(() => setValue(initialValue), [initialValue])`. |
| T2-10 | Frontend Monaco typing reset | `web/src/components/pipelines/ProcessorEditor.tsx` | Maintain a `lastSentOptionsRef` containing the last successfully parsed stringified options; skip updating the editor text if the incoming prop change matches the local ref (indicating it originated from active typing). |
| T2-11 | Insecure CORS for HTTP prod | `internal/api/cors.go` | Force `https://` for prod origins. |

---

## Tier 3 - Low (polish)

| ID | Finding | Fix Summary |
|----|---------|-------------|
| T3-1 | Invalid HTTP headers before SSE watcher | Move `c.Writer.Header().Set()` after watcher init. |
| T3-2 | Missing JWT secret validation | `log.Fatal` on missing `JWT_SECRET`. |
| T3-3 | Missing KDF on encryption key | Require base64-encoded `ENCRYPTION_KEY`; or PBKDF2. |
| T3-4 | Frontend array-index key anti-pattern | Use stable UUID per processor row. |

---

## Cross-Cutting Recommendations

1. **Stopgap for Tier 0**: Until T0-1 ships, lower `wal_keep_size` on monitored Postgres databases and snapshot every restart. The current behavior is unsafe for any environment.
2. **Concurrency helper**: Factor the `case entry, ok := <-...` pattern into `internal/stream/nats/kvwatch.go` so it cannot regress.
3. **Schema evolution observability**: Add metrics `cdc_sink_get_columns_empty_total`, `cdc_engine_schema_alter_ack_total`, `cdc_engine_schema_alter_nack_total`.
4. **Frontend testing gap**: Add Playwright test coverage for `/login`, `/dashboard`, and `/pipelines/$id` flows. The auth-loop bug evaded manual testing.
5. **Vendor patch tracking**: Maintain `internal/vendor/go-pq-cdc/PATCHES.md` listing each local divergence and the upstream issue/PR.
6. **Rate limit policy**: Document the API rate limit defaults in `docs/` so operators know what to tune.
7. **Atomic-level tests**: Add `-race` to CI; the loop and channel bugs are caught reliably by the race detector.
8. **Failure-mode runbook**: Produce `docs/runbooks/incident_recovery.md` covering restart, WAL replay, schema re-apply.

---

## Sequencing & PR Strategy

1. **PR 1 - Tier 0 quick wins (mechanical)**: T0-4 and T0-7. Both are surface-level edits, low risk, ship fast.
2. **PR 2 - Data-Loss and Checkpoint Remediation**: T0-1, T0-2, T0-3, T0-5, T1-2, T1-13. (Bundle behind a feature flag `CDC_AT_LEAST_ONCE` to stage rollout). This avoids checkpoint and LSN git merge conflicts.
3. **PR 3 - NATS & worker stability**: T0-6, T1-3, T1-18, T1-22, T1-26, T1-29.
4. **PR 4 - Resources & observability**: T1-6, T1-7, T1-11, T1-24, T1-25.
5. **PR 5 - Sink efficiency**: T1-1, T1-12, T1-14, T1-17, T1-19, T1-20, T1-21, T1-23.
6. **PR 6 - Configuration correctness**: T1-15, T1-27, T1-28, T1-30, T1-31.
7. **PR 7 - Vendored patches**: T1-4, T1-5, T1-10, T1-32, T1-33, T1-34.
8. **PR 8 - Frontend fixes**: T1-16, T2-8, T2-9, T2-10, T3-4.
9. **PR 9 - Security hardening**: T1-8, T1-9, T2-1, T2-2, T2-3, T2-11, T3-1, T3-2, T3-3.

Each PR must include:
- Tests for the failure mode described in the ticket.
- A benchmark or flame-graph if performance is in scope.
- A runbook or docs note if operator behavior changes.
- `go vet ./...`, `go test -short ./...`, and `-race` runs locally.

---

## Verification & Rollout

- `go vet ./...`
- `go test -short -race ./...`
- Container integration suite `go test -tags=integration ./...` (Postgres + Databend testcontainers).
- Load smoke: 1k msg/s for 10 minutes; assert no goroutine growth beyond a 5% envelope.
- Production rollout via shadow traffic: 24h comparison of NATS JetStream commit counts before/after T0-1.

# Implementation Review — `fix/plan-01a-delivery-source-ack`

**Date:** 2026-07-31
**Scope:** 17 commits ahead of `main`; ~6,400 lines of production + test change across
`internal/source`, `internal/engine`, `internal/stream`, `internal/protocol`, and the vendored
`go-pq-cdc`.
**Method:** five parallel adversarial reviewers, each on a distinct axis (cross-cutting
correctness, concurrency/lifecycle, vendored patches, test integrity, operational readiness).
Every work item had already been individually validated during implementation — this pass
hunted for what survives *between* the pieces.

**Headline:** the core invariant holds, but **two HIGH defects were found, both created by the
composition of separately-correct changes**, and both are on paths that per-item review never
exercised together. Neither is caught by the current test suite.

---

## HIGH-1 — Two concurrent standby writers under `strict_ack=false` (production default)

`internal/vendor/go-pq-cdc/pq/replication/stream.go:523`, `internal/source/postgres/source.go:921-982`

`resolveStrictAck()` returns **OFF when `ENV=production`**, so this is the shipped production path.

- Under OFF, `ManualCommit=false`, so the vendored `lc.Ack` closure is live. It performs *two*
  sends: `UpdateXLogPos` (semaphore-protected) and then a **direct `SendStandbyStatusUpdate`
  with no `standbySem`** (`stream.go:523`).
- WI-7 made the ack coordinator **always-on in both modes** (deliberately, so bake-period
  metrics populate before the flag flip). Under OFF, `Observe` still runs, consumers still
  publish RecordAcks, `Confirm` still advances the watermark — so the coordinator genuinely
  **writes to the wire** every 500ms tick, inside a `standbySem`-held goroutine.

`standbySem` (T0-2) exists precisely to guarantee at most one standby write in flight, because
two concurrent `SendUnbufferedEncodedCopyData` calls interleave protocol frames. The legacy
direct send bypasses it. **Result: truncated/interleaved CopyData frames → corrupted
replication stream.**

*Why per-item review missed it:* T0-2 was reviewed as making `UpdateXLogPos` safe; the OFF-path
restoration was reviewed as faithfully mirroring pre-WI-4 behaviour. Neither considered that
WI-7's always-on coordinator adds a permanent **third** writer alongside the one un-semaphored
send site. The orchestrator's own justification — "the coordinator's write under OFF is
redundant but harmless" — is wrong: harmless to the *position* (the monotonic clamp holds), not
to the *wire*.

**Fix:** suppress the coordinator's on-wire `UpdateXLogPos` when `ManualCommit == false`, keeping
the coordinator running so the watermark gauge still populates. This removes exactly what this
branch added rather than touching pre-existing upstream behaviour.

---

## HIGH-2 — Recover path calls `source.Start()` twice with no `Stop()`; engine never calls `Stop()` at all

`internal/engine/pipeline.go:250-261`, `internal/engine/producer.go:151-177`,
`internal/source/postgres/source.go:635,638,748`

On `errPublishRetriesExhausted` (a NATS outage), `Producer.Run` returns, its `defer cancel()`
*signals* the source goroutines to wind down — but **nothing awaits them**. `recoverProducer`
then immediately calls `Run` again → `source.Start()` again, while the previous session's
coordinator (up to 5s in `UpdateXLogPos`), slot-lag probe (up to 5s in a query), and cleanup
goroutine (100ms sleep) are still live.

During that overlap:
- **Data race on `s.db`/`s.dsn`** — the second `Start` writes them *without* `s.mu`
  (`source.go:635,638`) while the previous session's probe reads `s.db` under `RLock` and other
  goroutines read it lock-free. The prior "safe by happens-before" argument holds only within a
  single Start→Stop lifetime; the recover path breaks that premise.
- **`*sql.DB` pool leak per recover** — the old handle is overwritten and never closed. Same for
  `s.connector`.
- **Cross-session channel writes** — the old session's `triggerFlush` closes over the *field*
  `s.msgChan`, which the second `Start` reassigned.

**Root enabler:** `source.Stop()` is **never called anywhere in `internal/engine`** (verified by
grep). Teardown relies entirely on ctx cancellation, so `connector.Close()`, `db.Close()`, and
the WI-5a Prometheus gauge cleanup **never run in production**. That last point means the
`DeletePartialMatch` cleanup added specifically to prevent latched alerts is inert.

**Fix:** `Producer.Run` must call `p.source.Stop()` (or await `runWg`) before returning on any
path that may be followed by another `Run`; `Start` should reject or serialise re-entry; and the
`s.db`/`s.dsn` writes should move under `s.mu`.

---

## Operational blind spots

**OPS-1 — `pipeline` label is always empty on all four gauges.** `pipelineID` is set only inside
`WithKV`, which has **zero production callers**. So every series exports `pipeline=""`, page
annotations read `"... for /source-name"`, and `persistWatermark` short-circuits — the KV
watermark twin the bake period is meant to lean on is never written. (Previously tracked as
"persistWatermark is dead"; the label impact is broader than that framing suggested.)

**OPS-2 — A T0-3 recurrence would be invisible again.** The `highestSeen`/`idleTrusted` canary is
**log-only and latching**: it logs one Error, sets `idleTrusted = true`, and stops guarding. If
the vendored `buf.flush()` ordering ever regresses, the shape is once more `pending_lsns ≈ 0`,
watermark moving "healthily", one Error line buried in the stream — the exact T0-3 signature this
plan exists to make visible. Needs a counter (`cdc_source_idle_advance_refused_total`) and an
alert on `increase() > 0`.

**OPS-3 — No connector-liveness signal.** A connector that parks after a `wal_sender_timeout`
kill, on a source whose other tables are quiet, yields `pending_lsns == 0`, flat lag, flat
watermark — **no alert at all**. Ingestion has stopped and nothing pages.

---

## Medium / Low

- **M-CORR-1** — `advanceLocked` fires eagerly on `ObserveConfirmed`, so a filtered observation
  at an LSN can advance the watermark before that LSN's *data* observation arrives; the data
  observation is then dropped below-watermark. Rated Medium because the shared-LSN precondition
  could not be constructed from the vendored emission model (the look-ahead rewrites only a
  transaction's *last* message to the commit LSN). **But the `selfAcked` counter exists because we
  believed collisions are real.** Either prove the precondition impossible — in which case that
  machinery is dead weight — or defer the advance. Resolve definitively.
- **L-CORR-2** — JetStream redelivery can double-count `confirms[sink]` when `observed > 1`,
  advancing the watermark one observation early. Same precondition as above.
- **L-CORR-3** — Drain-marker wrappers are appended to `wmMsgs` *and* acked directly (double-ack);
  in the stale branch the marker stays in `wmMsgs`, so a later sink error can Nack an
  already-acked message.
- **L-CONC-1** — Stale `RestartWithNewTables` rationale in ~6 load-bearing comments for a function
  that no longer exists. The R9 cleanup goroutine's "capture the channel because Restart may
  reallocate" design is justified by a dead caller; the only remaining reallocator is a second
  `Start` (HIGH-2).
- **V-1** — T0-1's "byte-for-byte upstream when flag off" claim is stale: T0-2 rewrote
  `UpdateXLogPos` into the goroutine+semaphore form that all three legacy call sites traverse.
  T0-2's own entry documents this correctly; VENDOR.md's T0-1 paragraph does not cross-reference it.
- **V-2** — Highest *silent* re-sync risk is T0-3's `buf.flush()`-before-marker-enqueue ordering.
  Drop or reorder that one line and everything still compiles, flag-off tests still pass, and the
  confirmed-then-never-observed loss class silently returns.

---

## Test integrity

Three tests give more confidence than they earn:

| Test | Verdict | Problem |
|---|---|---|
| `TestDrainBufferedUntilIdle_DoesNotCompleteWhileAckPendingNonZero` | **WEAK/VACUOUS** | Comment claims to guard the `NumPending + NumAckPending` fix, but `fakePendingCounter` returns a single scalar and cannot model the two counts separately — passes identically against the broken implementation. |
| `TestWI5aGauges_ShareIdenticalLabelSet` | **WEAK** | `GetMetricWithLabelValues` validates **arity, not label names**, so a same-arity rename (`{pipeline,source,table}`) would sail through — the exact class that broke the alert join. Its `Eventually` is also vacuous, since the call *creates* the series it checks. |
| `TestStop_WaitsForBackgroundGoroutines` | **WEAK** | `assert.Greater(stopElapsed, time.Microsecond)` is effectively never false. |

**Structural risk:** the e2e guards are solid (`TestPendingCount_CountsDeliveredButUnacked` genuinely
asserts `NumPending==0 / NumAckPending==5 → PendingCount==5`; the strict-ack trio would fail against
the per-event-ack and keepalive bugs). But short mode skips them all — **if someone trusts the unit
layer alone, the drain-sum and label-set regressions ship green.**

Confirmed *fixed*: the previously-flagged B3 vacuity now pins ordering (`firstUpdate < firstStart`),
which a periodic coordinator flush cannot fake.

---

## Verified sound (recorded so it is not re-litigated)

- Publish-before-ack ordering on **all four** terminal RecordAck paths (empty batch, partial drop,
  isolation, DLQ); the `batch`-not-`toUpload` LSN set is correct.
- Ghost Confirms cannot advance the watermark (gated on `observed > 0`); they only pin it.
- B3 fresh-slot seed always uses `confirmed_flush_lsn`, never `pg_current_wal_lsn`, so it cannot
  over-advance; the monotonic clamp makes it a genuine no-op.
- `runWg` accounting is balanced across all seven spawned goroutines; `Stop()` cannot hang.
- T0-3's ordering guarantee is real: `sink` is the sole writer to `messageCH`, `process` the sole
  reader, and `buf.flush()` precedes the marker enqueue.
- `standbySem` is leak-free on every path including panic and caller abandonment.
- `muTableStates` discipline in `transitionTableToCDC` is deadlock-free; no lock is held across an
  unbounded network call.
- No new instance of the R9 close-while-sending pattern (`msgChan` remains the only one).
- `go test -race` clean across source and engine.

---

## Correction to a reviewer finding

One reviewer flagged `internal/vendor/go-pq-cdc/go.sum` as a stray `go mod tidy` artifact. The
*fact* is right (it is newly tracked on this branch) but the *cause* is not: it existed on disk at
`main` and was **untracked** because of the `.gitignore` bug fixed in the same commit — `vendor/`
excluded the directory, and git cannot re-include files under an excluded parent. Tracking it was
deliberate, on the same reasoning as PATCHES.md, and its content is byte-identical to what was
already on disk. Not a defect.

---

## Deploy posture

**With `strict_ack` OFF (the production default): NOT SAFE as-is** — HIGH-1 is specifically the
OFF path, and HIGH-2's recover-path races are mode-independent. Both must be fixed first. (The
operational reviewer rated OFF a GO on observability grounds before HIGH-1 and HIGH-2 were known.)

**Before flipping `strict_ack` ON for a real pipeline:**
1. Fix HIGH-1 and HIGH-2.
2. CI green on the e2e invariants — the plan's own hard prerequisite; nothing runs them today.
3. `worker.alerts.enabled: true` in production, landing *before* the flag.
4. Wire `pipelineID` (OPS-1); add the T0-3 canary counter (OPS-2) and a connector-liveness
   signal (OPS-3).
5. `promtool test rules` in CI with an auto-rendered fixture.
6. Bake period: watermark tracking `confirmed_flush_lsn` stably under production load.

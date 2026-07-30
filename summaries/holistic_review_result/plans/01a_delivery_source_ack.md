# Plan 01a — Delivery Guarantee, Source/Ack Half: End-to-End Ack/Checkpoint Contract

**Scope:** the source side of Fix Sequence 1 — replication-slot advancement, the ack path from
sink back to source, snapshot resume, and the source/engine lifecycle bugs that break the
at-least-once contract. Covers verified findings: **Critical 1, 2, 11, 13**; **High**
RestartWithNewTables, handler-panic deadlock, 1s idle-drain window, ack-before-durable-write
(+ dead `flushWithFilter`/`checkDrained`); **Medium** lsnChan drop, non-monotonic
`UpdateXLogPos`, ackChan blocking, `lastCheckpoint` race.
Sink-internal correctness (Criticals 3–10) is the companion plan 01b and is out of scope here.

All line numbers verified against the working tree at commit `189b7a7`. Every load-bearing
code claim in this plan was independently re-verified against the source (vendored slot-advance
sites, the closed-loop watermark, the dead helpers, the zombie path, the restart plumbing, and
crucially the vendored snapshot-resume machinery) — all confirmed, no refutations. Revision 2
folds in that verification: see the **Verified-fact addenda** call-outs inline and the expanded
WI-5a (WAL-retention gate), WI-6 (restart rebind protocol), and §7 Q3 (replay state machine).

> **Hard prerequisite:** the `strict_ack` flag may not be flipped on in production until Fix
> Sequence 3 (CI) is landed and green — the headline invariant is proven only by e2e tests
> 20–27, which nothing runs today (Critical 20). Manual verification is not an acceptable gate
> for a change that can retain unbounded WAL on the source primary. See §6 and §7.

---

## 1. Objective & current-state contract

### Objective

Establish and enforce one invariant, end to end:

> **The PostgreSQL replication slot's confirmed position never advances past an LSN whose
> event has not been durably written by every configured sink.**

Equivalently: `confirmed_flush_lsn <= min over sinks of (highest contiguous durably-written LSN)`.
A crash at any point replays the unconfirmed suffix (at-least-once; duplicates allowed,
loss never).

### What actually happens today (the broken contract, with evidence)

The intended flow is documented in the `PostgresSource` doc comment
(`internal/source/postgres/source.go:60-70`) and in `AckManager`'s doc
(`internal/source/postgres/ack.go:8-21`). None of it is load-bearing. There are **three
independent mechanisms that advance the slot before any downstream write**, and the machinery
that was supposed to gate advancement is a closed loop that never sees a downstream ack:

1. **Per-event `lc.Ack()` (Critical 1).** The replication handler calls `lc.Ack()` for every
   event: data events at `source.go:313`, filtered/unmatched events at `source.go:202, 216,
   233, 242, 249, 266, 320`. In the vendored library, `Ack` is
   `internal/vendor/go-pq-cdc/pq/replication/stream.go:397-402`: it calls
   `s.UpdateXLogPos(walStart)` which (stream.go:448-458) both stores `lastXLogPos` **and
   immediately issues `SendStandbyStatusUpdate`**. The slot advances the instant the event is
   appended to an in-memory batch slice — before the batch is flushed to `msgChan`, before
   NATS publish, before any sink write. Crash anywhere in that window = permanent loss.

2. **Keepalive fast-forward (Critical 2).** `stream.go:299-302` (`handleKeepalive`): every
   server keepalive with `ServerWALEnd > 0` calls `s.UpdateXLogPos(pkm.ServerWALEnd)` —
   confirming WAL the process has not even decoded yet, let alone delivered. Even if per-event
   ack were fixed, any keepalive during an in-flight batch silently confirms past it.
   A third eager-advance exists at `stream.go:334`: undecodable WAL messages call
   `UpdateXLogPos(xld.WALStart)` directly.

3. **Non-monotonic position (Medium).** `stream.go:448-451` sets `lastXLogPos = lsn`
   unconditionally. Interleaving of per-message `walStart` acks and keepalive `ServerWALEnd`
   can move the reported flush position *backwards*.

4. **The AckManager watermark is a closed loop (Critical 1 corollary).** The handler
   `Observe(lsn)`s the LSN (`source.go:285`) and then immediately hands the *same* LSN to the
   coordinator over `lsnChan` (`source.go:292-295`), and `runAckCoordinator` blindly
   `Confirm`s it (`source.go:505-509`). Observe-then-confirm both happen at *produce* time.
   The watermark therefore tracks "produced", not "durably sunk", and the coordinator's
   `conn.UpdateXLogPos(wm)` at `source.go:521` is redundant with the per-event ack. The
   engine's real ack signal — `ackChan` — is drained and **discarded** by the handler
   (`source.go:309-312, 316-319`).

5. **Acks lose their LSN in the engine.** The consumer *does* publish LSN-carrying acks after
   a successful `BatchUpload` (`internal/engine/consumer.go:448-465`: `protocol.Message{Op:
   "ack", LSN: m.LSN, ...}` per uploaded message, onto `protocol.AcksTopic`). The producer
   receives them (`internal/engine/producer.go:182-191`) and throws the LSN away: it forwards
   an anonymous `struct{}{}` on `ackChan` with a `default:` branch that silently drops the
   signal when the buffer is momentarily full. `Source.Start`'s ack channel is typed
   `chan<- struct{}` (`internal/source/provider.go:11`) so the LSN *cannot* survive the hop.

6. **Snapshot resume is structurally broken (Critical 11).** `source.go:412` sets
   `Snapshot.Enabled: checkpoint.IngressLSN == 0`. But snapshot rows carry an LSN
   (`source.go:253`, `format.Snapshot.LSN`) and the producer checkpoints them to KV as
   `IngressLSN` (`producer.go:310-324`); the consumer also writes them as `EgressLSN`
   (`consumer.go:633-645`), which feeds the pipeline's resume LSN (`pipeline.go:104-121`).
   So a crash after the *first* snapshot chunk is published yields `IngressLSN > 0` on
   restart → snapshot disabled → the vendored resumable chunk state
   (`internal/vendor/go-pq-cdc/pq/snapshot/job.go` — `cdc_snapshot_job` /
   `cdc_snapshot_chunks` tables, `job.Completed`, per-chunk claims in `worker.go:151-170`)
   is never consulted → the remaining rows are never emitted. The library already knows how
   to resume; our config gate prevents it from ever trying.

7. **Zombie pipeline (Critical 13).** `internal/engine/pipeline.go:66-87`: the producer
   goroutine early-returns on any config-load error (`kv.Get` at :73-77, unmarshal :79-83,
   decrypt :84-87) **without calling `p.cancel()`**. Consumers keep running forever,
   `p.wg.Wait()` never returns, `finished` (:154-157) never closes, and the supervisor
   heartbeats "Running" for a pipeline that ingests nothing. A transient NATS KV blip at
   startup permanently wedges the pipeline.

8. **RestartWithNewTables breaks the session plumbing (High).** `source.go:675-811`:
   - The ack coordinator and the ack/lsn channels are only created in `Start`
     (`source.go:331-333, 465-469`); a restart never respawns the coordinator nor reallocates
     `ackChan`/`lsnChan` — if the old context died, watermark flushing is dead.
   - `s.msgChan` is reallocated at `source.go:743`, but the engine still owns the *old*
     channel returned by `Start`; the old session's cleanup goroutine closes it
     (`source.go:621-630`), the producer's `Run` loop sees `!ok` (`producer.go:197-213`),
     publishes a **drain marker and returns** — the pipeline drains itself. Meanwhile the new
     session's `triggerFlush` (`source.go:754-769`) sends to the orphaned new channel with a
     `default:` on a cap-1 channel — post-restart batches are silently dropped.
   - `source.go:715` appends `newTables` with no dedup — duplicate publication entries grow
     without bound.
   - The spawned goroutine reads `s.lastCheckpoint` at `source.go:807` after `s.mu` was
     released at :801, racing `UpdateXLogPos`'s write under lock at :641-645 (Medium data
     race).

9. **Handler panic deadlocks the source (High).** `source.go:185-191`: the deferred `recover`
   does not unlock; `mu.Lock()` at :191 is released manually on each branch. A panic in
   `sanitizePayload`, the OID cache, or message construction between Lock and Unlock leaves
   `mu` held forever — the batch-wait ticker's `triggerFlush` (`source.go:381-397`) blocks
   forever, the source stops producing, nothing crashes, nothing restarts.

10. **1s idle window as buffer-empty proof (High).** `producer.go:571-593`
    (`drainBufferedUntilIdle` with `bufferDrainIdleTimeout = time.Second`, :43): a 1-second
    quiet period on the drainer's channel is taken as "buffer empty". JetStream redelivery
    lag > 1s (fully realistic after a NATS restart or under load) strands buffered rows on
    the buffer stream when the table flips to CDC.

11. **Ack-before-durable-write for mixed schema+data batches; the fix is dead code (High).**
    `consumer.go:262-281`: a wmMsg whose payload contains an `OpSchemaChange` is *not*
    appended to `wmMsgs`, yet any *data* messages in the same payload are appended to `batch`
    (:373). The schema path then acks the wrapper at :349 right after `ApplySchema` — before
    those data rows are flushed to the sink. If the process crashes before the next flush the
    rows are acked-and-lost. Latent today only because the producer emits schema changes as
    single-message batches (`producer.go:263`), but nothing enforces that.
    `flushWithFilter` (`consumer.go:476-536`) was written exactly for this and is never
    called; `checkDrained` (`consumer.go:682-692`) was written for LSN-bounded draining and
    is never called — `Consumer.Drain(targetLSN)` (:675-680) stores a `targetLSN` nothing
    reads, so drain depends entirely on a single `drain_marker` message arriving.

12. **ackChan can block the engine (Low/Medium).** `source.go:332` (cap 1000) is drained only
    opportunistically, one non-blocking recv per data event (:309-312); filtered-event
    branches skip the drain. If acks outnumber events (per-message acks vs. batched events)
    or arrive while the stream is idle, the producer's send at `producer.go:184-190` hits the
    `default:` and the ack is lost (today: harmless, because acks are decorative; tomorrow:
    fatal, because they gate the slot).

### Summary picture (today)

```
PG WAL ──> vendored stream ──> handler ── lc.Ack() ───────────────► SLOT ADVANCES (per event)
                 │                │                                       ▲
                 │  keepalive ────┼── UpdateXLogPos(ServerWALEnd) ───────┘  (fast-forward)
                 │                │
                 │                ├─ Observe(lsn) ─┐
                 │                └─ lsnChan ──────┴─► coordinator Confirm(lsn)  } closed loop,
                 │                                     watermark = "produced"    } decorative
                 ▼
              msgChan ─► Producer ─► NATS ingest ─► Consumer ─► Sink.BatchUpload
                                                        │
                              AcksTopic ◄── ack{LSN} ───┘ (LSN present!)
                                 │
                          Producer ack loop ─► ackChan struct{}{} (LSN dropped, default: drop)
                                 │
                          handler drains & discards
```

---

## 2. Target design

### The new contract

```
PG WAL ─► vendored stream ─► handler
             │                  │  data event:    Observe(lsn)            [no lc.Ack]
             │                  │  filtered event: ObserveConfirmed(lsn)  [self-ack via AckManager]
             │  keepalive ──────┼─► KeepaliveFunc(serverWALEnd) ─► AckMgr.IdleAdvance(serverWALEnd)
             ▼                  ▼                                   (only when nothing pending)
          msgChan ─► Producer ─► NATS ingest ─► Consumer ─► Sink.BatchUpload (durable)
                                                    │
                        AcksTopic ◄── RecordAck{SinkID, LSNs[]} ── (one per successful flush)
                            │
                     Producer ack loop ─► ackChan (typed, blocking send w/ ctx)
                            │
                     Source ack coordinator ─► AckMgr.Confirm(lsn, sinkID)
                            │                     watermark = highest contiguous LSN
                            │                     confirmed by ALL sinks
                            └─ ticker 500ms ─► connector.UpdateXLogPos(watermark)
                                                  └► monotonic store + SendStandbyStatusUpdate
                                                       └► SLOT ADVANCES (only here)
```

Resume: **the replication slot itself is the authoritative ingress checkpoint.** Because the
slot now only advances after all-sink durable writes, restarting from
`confirmed_flush_lsn` (the vendored default when `lastXLogPos == 0`, `stream.go:82-85`) is
exactly correct. The KV `Checkpoint` records remain for observability, egress stats, and
dynamic-table snapshot resume — they stop being the replication resume authority
(`cfg.StartLSN` override removed; see WI-7).

Snapshot resume: **the vendored chunk job state is the authoritative snapshot checkpoint.**
`Snapshot.Enabled` becomes unconditional for postgres sources in initial mode; the library's
`shouldTakeSnapshot` → `LoadJob` (`connector.go:339-367`) skips a completed job and resumes
an incomplete one via `cdc_snapshot_chunks` claims. Snapshot rows are excluded from the
LSN/watermark machinery entirely (they are checkpointed by chunk, not by LSN).

### New/changed types and signatures

**`internal/protocol/message.go`** (msgp regeneration required — `go generate ./internal/protocol`):

```go
const (
    // existing ops...
    OpRecordAck OperationType = "record_ack" // replaces the bare "ack" string literal
)

// RecordAck is published by a consumer on AcksTopic after a durable sink write.
// One message per successful flush; LSNs lists every LSN in the flushed batch.
type RecordAck struct {
    PipelineID string   `msg:"pid"`
    SourceID   string   `msg:"sid"`
    SinkID     string   `msg:"snk"`
    LSNs       []uint64 `msg:"lsns"`
    Timestamp  time.Time `msg:"ts"`
}
```

(Wire-compat: `RecordAck` is a new msgp type carried as the payload of a
`protocol.Message{Op: OpRecordAck, Payload: <msgp bytes>}` envelope so the existing
AcksTopic subscriber loop, which unmarshals `protocol.Message`, keeps working; the old
per-message `Op:"ack"` shape is still accepted during rollout — see §6.)

**`internal/source/provider.go`** — the interface change that carries the LSN end to end:

```go
// SourceAck tells the source that one sink has durably written a set of LSNs.
type SourceAck struct {
    SinkID string
    LSNs   []uint64
}

type Source interface {
    Name() string
    // ackers: the sink IDs whose confirmation is required before the slot may
    // advance past an LSN. len(ackers) == required confirms per LSN.
    Start(ctx context.Context, config protocol.SourceConfig,
          checkpoint protocol.Checkpoint, ackers []string,
    ) (msgChan <-chan []protocol.Message, ackChan chan<- SourceAck, err error)
    Stop() error
    AlterPublication(ctx context.Context, tableName string) error
    // Restart tears down the current replication session and starts a new one
    // with the merged table set, returning fresh channels. Replaces the broken
    // in-place RestartWithNewTables.
    Restart(ctx context.Context, newTables []string,
    ) (<-chan []protocol.Message, chan<- SourceAck, error)
    UpdateXLogPos(ctx context.Context, lsn uint64) error
}
```

**`internal/source/postgres/ack.go`** — multi-sink, multiplicity-aware AckManager:

```go
type ackEntry struct {
    observed  int                  // how many events carry this LSN (txn-end rewrite can dedupe)
    confirms  map[string]int      // sinkID -> confirmed count
    selfAcked bool                // filtered events: confirmed at observe time
}

type AckManager struct {
    mu        sync.Mutex
    required  []string            // sink IDs whose confirm is required (from Start ackers)
    pending   map[uint64]*ackEntry
    lsns      []uint64            // sorted observed-unconfirmed LSNs (existing structure)
    watermark uint64
}

func NewAckManager(requiredSinks []string) *AckManager
func (a *AckManager) Observe(lsn uint64)                    // data event produced
func (a *AckManager) ObserveConfirmed(lsn uint64)           // filtered event; no downstream trip
func (a *AckManager) Confirm(lsn uint64, sinkID string) uint64 // one sink durably wrote lsn
func (a *AckManager) Watermark() uint64
func (a *AckManager) PendingCount() int
// IdleAdvance fast-forwards the watermark to serverWALEnd IFF nothing is pending.
// This is the ONLY sanctioned fast-forward; it reinstates keepalive-driven slot
// advancement for idle streams (WAL-bloat protection) without the Critical-2 bug.
func (a *AckManager) IdleAdvance(serverWALEnd uint64) (advanced bool)
func (a *AckManager) Hydrate(watermark uint64)              // unchanged semantics
```

Watermark rule: an LSN is *fully confirmed* when every sinkID in `required` has confirmed it
at least `observed` times, or it is `selfAcked`. `watermark` = highest LSN such that every
observed LSN ≤ it is fully confirmed (existing contiguous-run scan at `ack.go:88-98`,
extended for the entry struct). `Confirm` for `lsn <= watermark` is a no-op (idempotent
against AcksTopic redelivery after restart).

**Vendored `config.Config`** (`internal/vendor/go-pq-cdc/config`):

```go
type Config struct {
    // ...existing...
    // ManualCommit: when true, ListenerContext.Ack() and keepalives do NOT
    // advance the WAL position; the embedding application owns advancement
    // exclusively via Connector.UpdateXLogPos. Default false = upstream behavior.
    ManualCommit bool
    // KeepaliveFunc, when non-nil and ManualCommit is set, receives ServerWALEnd
    // from primary keepalive messages instead of the internal fast-forward.
    KeepaliveFunc func(serverWALEnd pq.LSN)
}
```

**Vendored `stream.UpdateXLogPos`** becomes monotonic (guarded `lsn <= s.lastXLogPos → return`),
detailed in §4.

### Semantics decisions worth stating explicitly

- **Multi-sink gating lives in the source's AckManager**, fed sink identity through the ack
  channel. The producer stays a dumb forwarder; the source is the single authority for "may
  the slot advance", matching its doc comment.
- **Per-LSN confirms, not high-water prefix confirms.** A sink acking batch-max LSN X does
  not prove it wrote all lower LSNs (JetStream redelivery can reorder batches), so
  `RecordAck.LSNs` enumerates the batch's LSNs and the AckManager confirms each. The
  contiguous-run scan converts that into a safe prefix.
- **Filtered events self-ack through the AckManager, never through the slot.** Relation
  messages, unknown-table events, non-data snapshot events (`source.go:202, 216, 233, 242,
  249, 266`) call `ObserveConfirmed(uint64(lc.LSN))` so they cannot stall the watermark, and
  their `lc.Ack()` calls are deleted along with the data-path one.
- **Snapshot rows (`format.Snapshot`, `OpSnapshot`) bypass the watermark entirely**: not
  Observed, LSN field zeroed on the emitted `protocol.Message`, excluded from
  ingress/egress LSN checkpoints. Their durability story is JetStream + chunk-job state.
- **Keepalive-driven advancement survives, gated on emptiness**: `IdleAdvance` keeps
  `confirmed_flush_lsn` tracking `ServerWALEnd` on idle streams so low-traffic databases
  don't accumulate WAL — the legitimate purpose the Critical-2 code was serving.

---

## 3. Ordered work items

Dependencies are noted per item; the order below is a valid topological sort and each item
should land as its own reviewable commit with its tests.

### WI-1 — Vendored library: manual-commit mode, keepalive callback, monotonic position
**Files:** `internal/vendor/go-pq-cdc/config/config.go` (add `ManualCommit`,
`KeepaliveFunc`), `internal/vendor/go-pq-cdc/pq/replication/stream.go`,
`internal/vendor/go-pq-cdc/PATCHES.md` (new entry "T0-1"), `VENDOR.md` (note the patch).
**Change:**
- `stream.go:397-402` (`process`, `lCtx.Ack`): when `s.config.ManualCommit`, `Ack` becomes
  `func() error { return nil }` (position ownership moves entirely to `UpdateXLogPos`).
- `stream.go:292-313` (`handleKeepalive`): when `ManualCommit`, replace the
  `s.UpdateXLogPos(pkm.ServerWALEnd)` at :299-302 with
  `if s.config.KeepaliveFunc != nil { s.config.KeepaliveFunc(pkm.ServerWALEnd) }`.
  The `ReplyRequested` branch (:304-310) keeps sending `LoadXLogPos()` — now guaranteed to
  be the confirmed watermark — but gains a `> 0` guard mirroring `stream.go:241`.
- `stream.go:334` (undecodable-message advance): delete under `ManualCommit` (unobserved
  LSNs cannot stall the AckManager; the next confirmed event advances past them).
- `stream.go:448-459` (`UpdateXLogPos`): add monotonic guard under `s.mu`:
  `if lsn <= s.lastXLogPos { unlock; return }`. Applies in both modes (fixes the Medium
  finding upstream-compatibly; regression risk is nil because a backwards standby update is
  never meaningful).
- `connector.go:291-293` (`StartLSN` seed) needs no change — with `lastXLogPos == 0` the
  monotonic guard admits the seed.
- **Verified-fact addendum:** the `stream.go:397-402` Ack closure sends
  `SendStandbyStatusUpdate` *twice* per ack (once inside `UpdateXLogPos`, once directly at
  :401). Under `ManualCommit` the whole closure no-ops, so this is moot — but note it when
  re-applying the patch. Also, the `slot.NewSlot(..., stream.(slot.XLogUpdater))` audit item
  from §4 is **already closed**: `NewSlot` (connector.go:124) discards the `updater` argument
  (never stored in the `Slot` struct, slot.go:47-56), so no slot code path can advance the
  position. No guard needed there.
**Why:** neutralizes Criticals 1's slot-advance mechanism and Critical 2 at their root,
behind a flag so the vendored lib remains upstream-shaped (see §4 for strategy).
**Deps:** none. Everything else builds on this.

### WI-2 — Protocol: `RecordAck` type, `OpRecordAck`, msgp regeneration
**Files:** `internal/protocol/message.go`, regenerated `message_gen.go`/`message_gen_test.go`.
**Change:** as specified in §2. Also add
`const OpDrainMarker OperationType = "drain_marker"` and replace the string literals.
**Verified-fact addendum:** there are **five** relevant literals in the engine, not three —
`producer.go:199`, `consumer.go:286`, `consumer.go:452`, plus `consumer.go:516` and
`consumer.go:520` inside `flushWithFilter`. The last two are dormant today but go live in
WI-9, so convert all five in this WI to avoid a latent drift when the helper is wired.
**Why:** the ack must carry LSN + sink identity in a first-class, versionable shape.
**Deps:** none (parallel with WI-1).

### WI-3 — Source interface change + AckManager rewrite
**Files:** `internal/source/provider.go`, `internal/source/postgres/ack.go`,
`internal/source/postgres/ack_test.go`, `internal/source/mocks/*`.
**Change:** interface and `AckManager` exactly per §2. Delete the dead duplicate-check
return path noted in the matrix (`ack.go:65-67`). Keep the sorted-slice structure
(`ack.go:60-71`) but store `*ackEntry` values. `Confirm(lsn <= watermark)` is a no-op.
**Why:** foundation for LSN-carrying acks and multi-sink gating.
**Deps:** WI-2 (uses `SourceAck`; conceptually protocol-adjacent).

### WI-4 — PostgresSource: handler, coordinator, channel plumbing
**Files:** `internal/source/postgres/source.go`, `source_remediation_test.go`.
**Change (the core of this plan):**
1. `Start` (`source.go:325-472`):
   - Signature gains `ackers []string`; `s.ackMgr = NewAckManager(ackers)`.
   - `s.ackChan = make(chan source.SourceAck, 1024)`; **delete `s.lsnChan` entirely**
     (:111, :333, and its use at :292-295, :505-508) — the coordinator's input is now the
     engine ack channel, closing the Medium lsnChan-drop finding by construction.
   - Hydration (:340-345): keep, but hydrate from `max(checkpoint.IngressLSN, 0)` purely as
     the watermark floor; do **not** set `cfg.StartLSN` any more (see WI-7).
   - `cfg.ManualCommit = true`; `cfg.KeepaliveFunc = func(lsn pq.LSN) { if s.ackMgr.IdleAdvance(uint64(lsn)) { /* coordinator ticker will flush */ } }`.
   - `Snapshot.Enabled` (:412): `true` whenever the source is configured for initial
     snapshot — no longer keyed on `checkpoint.IngressLSN` (Critical 11; the vendored
     `LoadJob` decides skip/resume/fresh).
2. Handler (`createHandler`, `source.go:183-323`):
   - Restructure for panic safety (High): the entire message-construction critical section
     moves into a helper with `mu.Lock(); defer mu.Unlock()`; the outer closure keeps the
     `recover()` (now guaranteed not to strand the lock) and performs `triggerFlush` /
     AckManager calls after the helper returns. No code path may hold `mu` across a
     blocking operation (`triggerFlush` already runs unlocked, keep it that way).
   - Data events: `s.ackMgr.Observe(uint64(lc.LSN))`; **no `lc.Ack()`** (harmless no-op
     under ManualCommit but delete anyway for clarity).
   - Filtered events (all the early-return branches): `s.ackMgr.ObserveConfirmed(uint64(lc.LSN))`
     when `lc.LSN > 0`; delete their `lc.Ack()` calls.
   - Snapshot data events (`source.go:239-253`): build the message with `LSN: 0`
     (drop `uint64(msg.LSN)`); no Observe. Add `Snapshot: true`-style signaling via the
     existing `Op == OpSnapshot` (sufficient).
   - Delete the opportunistic `ackChan` drain (:309-312, :316-319) — the coordinator now
     owns the channel (fixes N12).
3. `runAckCoordinator` (`source.go:489-525`) becomes:
   ```go
   case ack, ok := <-s.ackChan:
       if !ok { return }
       for _, lsn := range ack.LSNs { s.ackMgr.Confirm(lsn, ack.SinkID) }
   case <-ticker.C:               // 500ms, unchanged cadence
       wm := s.ackMgr.Watermark()
       if wm == 0 || wm == lastFlushedWatermark { continue }
       conn := ...                // snapshot the conn pointer under RLock, then RELEASE
                                  // the lock before the network call (do NOT hold RLock
                                  // across UpdateXLogPos — see below)
       cctx, cancel := context.WithTimeout(ctx, updateXLogPosTimeout /* e.g. 5s */)
       err := conn.UpdateXLogPos(cctx, pq.LSN(wm))   // the ONLY slot-advance call site
       cancel()
       if err != nil { log.Warn(...); metric slot_advance_errors++; continue } // retry next tick
       s.persistWatermark(wm)     // KV observability write, see WI-7
       lastFlushedWatermark = wm
   ```
   **Bound the network call (review finding 4).** `UpdateXLogPos` issues a standby status
   update over the wire. It must run under a `context.WithTimeout`, and the coordinator must
   **not** hold `s.mu`/RLock across it — snapshot the connector pointer under the lock, release,
   then call. Otherwise a hung standby update stalls the coordinator, which (because WI-5 makes
   the producer's ackChan send blocking) applies backpressure through AcksTopic back into the
   consumer and wedges the whole pipeline. A failed/timed-out update is retried on the next
   tick; the watermark is unchanged so nothing is lost.
   The doc comments at :457-469 and :474-488 finally become true.
4. `UpdateXLogPos` (`source.go:633-650`): retire from the `Source` interface's hot path;
   keep as a low-level escape hatch but route its checkpoint write through the same
   mu-guarded copy used by Restart (fixes the `lastCheckpoint` race together with WI-6).
**Why:** installs the authoritative watermark → slot pipeline; fixes Criticals 1/2/11 (source
half), High panic-deadlock, Medium lsnChan/ackChan findings.
**Deps:** WI-1 (ManualCommit/KeepaliveFunc), WI-3 (types).

> ### ⚠ BLOCKERS discovered during WI-1 implementation — resolve before coding WI-4
>
> These were found by validating the shipped WI-1 patch against the real vendored surface.
> Two of them mean the pseudocode above does not compile or does not work as specified.
>
> **B1. `Connector.UpdateXLogPos` takes no context and returns no error.** The real signature is
> `UpdateXLogPos(lsn pq.LSN)` (`vendor/.../connector.go:38,220`). The step-3 pseudocode
> (`err := conn.UpdateXLogPos(cctx, pq.LSN(wm))`) will not build, and §4's patch table never
> included a signature change. Decide explicitly:
> (a) add a **T0-2** vendored patch widening it to `UpdateXLogPos(ctx context.Context, lsn pq.LSN) error`, or
> (b) accept fire-and-forget — which **removes** the plan's "failed update is retried on the next
> tick" behavior and the `slot_advance_errors` metric, and leaves us unable to detect a slot that
> silently stopped advancing.
> Recommendation: (a). Under the new contract the slot write is the single most
> safety-critical call in the system; shipping it without an error signal is not acceptable, and
> WI-5a's alerting is much weaker without it.
>
> **B2. The `context.WithTimeout` bound is ineffective as designed.**
> `SendStandbyStatusUpdate` is literally `func SendStandbyStatusUpdate(_ context.Context, ...)`
> (`vendor/.../stream.go:581`) — it discards the context and ends in
> `conn.Frontend().SendUnbufferedEncodedCopyData(buf)`, a socket write that can block on a full
> TCP send buffer. So review finding 4's timeout does nothing even if B1(a) lands. To actually
> bound it, either patch `SendStandbyStatusUpdate` to honor the ctx (fold into T0-2) or wrap the
> call in a goroutine + `select` at the coordinator. **The "do not hold `s.mu`/RLock across the
> call" half of that guidance remains valid and necessary regardless.**
>
> **B3. New silent LSN-0 window at session start.** Both keepalive reply paths are now
> `LoadXLogPos() > 0`-guarded (`stream.go:241` pre-existing, `:314` added by T0-1). Under
> ManualCommit plus WI-7 (which deletes the `StartLSN` seed), `lastXLogPos` stays 0 until the
> coordinator's first flush, so the process sends **no standby status update at all** during that
> window — where pre-patch, `ReplyRequested` would at least have sent a frame reporting 0.
> `IdleAdvance` normally closes this immediately, but if events are pending from the very first
> keepalive **and** a sink is down, `IdleAdvance` correctly refuses and the window is unbounded →
> PostgreSQL's `wal_sender_timeout` can terminate the walsender. Mitigation: seed `lastXLogPos`
> from `Hydrate(IngressLSN)` with one explicit `UpdateXLogPos` at session start, and/or alert on
> it in WI-5a. This is plan-mandated behavior, not a WI-1 defect, but it is a real operational edge.
>
> **B4. `config.Config` is no longer comparable** (it now holds a `func` field), so `==` on it
> will not compile and `reflect.DeepEqual`/`assert.Equal` on two `Config` values report unequal
> whenever `KeepaliveFunc` is non-nil on one side. **Test 10 must assert individual captured
> fields, not whole-struct equality.**
>
> Bonus: `Config.Print()` (`config/config.go:246-251`) now logs `"manualCommit":true` on every
> connector construction — a free signal that the flag actually took effect. Note it in the
> WI-5a runbook.

### WI-5 — Engine: producer forwards typed acks; consumer emits `RecordAck`; snapshot LSN hygiene
**Files:** `internal/engine/producer.go`, `internal/engine/consumer.go`,
`internal/engine/engine_test.go`.
**Change:**
1. Consumer `flush` (`consumer.go:448-465`) and `flushWithFilter` (:513-529): replace the
   per-message `Op:"ack"` publish with a single
   `protocol.Message{Op: OpRecordAck, SinkID: c.sinkID, SourceID: ..., Payload: msgpEncode(RecordAck{LSNs: batchLSNs})}`
   per successful `BatchUpload`, where `batchLSNs` collects `m.LSN` for every uploaded
   message with `m.LSN > 0` and `m.Op != OpSnapshot`.
   - **Ordering (fixes a crash window — review finding A).** Publish the `RecordAck`
     **before** JetStream-acking the wmMsgs, not after. Today `flush` acks wmMsgs first and
     publishes the ack second (`consumer.go` acks at the top of the block, ack-publish at
     :448-465); under the new contract a crash *between* those two steps loses the ack with
     no redelivery to regenerate it → the watermark stalls silently until new traffic (and WAL
     accumulates meanwhile). Reordering to publish-then-ack means a crash before the wmMsg ack
     simply redelivers the batch; the re-emitted `RecordAck` is idempotent because
     `Confirm(lsn <= watermark)` is a no-op (WI-3). Net order per flush:
     `BatchUpload → publish RecordAck (bounded retry) → ack wmMsgs`.
   - **Publish failure must not proceed silently.** Today's `log.Warn` at :463 means a lost
     ack permanently stalls the watermark. Use a small bounded retry; if it exhausts, Nack the
     batch's wmMsgs so JetStream redelivers and the whole flush re-runs.
   - **Cross-plan invariant (with 01b).** `RecordAck.LSNs` enumerates a whole batch's LSNs, so
     it is only truthful if `BatchUpload` success means **every** LSN in the batch is durably
     written — all-or-nothing. Plan 01b's sink work MUST guarantee this (no partial-table
     commit that returns success). If 01b instead adopts partial-batch semantics, this WI must
     switch to emitting `RecordAck` only for the durably-written subset. Stated as a shared
     invariant I0 in both plans; neither may land its ack/durability change without the other
     agreeing on it.
2. Producer ack loop (`producer.go:167-192`): parse `OpRecordAck`, decode the payload, and
   forward `source.SourceAck{SinkID, LSNs}` on `ackChan` with a **blocking send guarded by
   `ctx`** — delete the `default:` at :188-190. Ack the NATS message only after the send
   succeeds. Keep accepting legacy `Op == "ack"` (single LSN) during rollout (§6).
3. Producer `Run` (`producer.go:133-147`): pass `p.config.Sinks` as the `ackers` argument to
   `p.source.Start`.
4. Producer checkpointing (`producer.go:300-324`): skip `IngressLSN` KV writes for messages
   with `m.Op == OpSnapshot` or `m.LSN == 0`; write `Status: "Snapshotting"` checkpoints
   only from the dynamic-snapshot path (`producer.go:1140-1153`, unchanged — that path is
   PK-based and correct). Consumer `updateStats` (`consumer.go:631-645`): don't write
   `EgressLSN` from `OpSnapshot` messages (prevents snapshot LSNs from poisoning the
   pipeline resume floor at `pipeline.go:104-121`).
**Why:** carries the LSN across the engine (Critical 1's second half) and stops snapshot rows
from masquerading as replication progress (Critical 11's second half).
**Deps:** WI-2, WI-3, WI-4.

### WI-5a — WAL-retention safety: slot-lag observability + the flag-flip gate (review concern 1)
**Files:** `internal/source/postgres/source.go` (a lightweight slot-lag poller),
`internal/metrics/*`, `deploy/helm-chart` (alert rule + doc), release notes / runbook.
**Change:** this is promoted from a §7 risk to a first-class, **gating** work item because it
converts the plan's failure mode from silent data loss into a *source-primary disk-pressure
outage* — a strictly scarier operational surface that must be instrumented before strict-ack
is trusted.
- Add a periodic (~15s) slot-lag probe on the source connection:
  `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots
  WHERE slot_name = $1`, exported as gauge `cdc_source_slot_lag_bytes{pipeline,source,slot}`.
  Also export `cdc_source_pending_lsns` and `cdc_source_ack_watermark` (used by the bake in §6).
- Ship an alert rule (warn + page thresholds) and a runbook entry: a frozen/growing
  `slot_lag_bytes` means a sink is down and WAL is accumulating on the source; the operator
  response is fix-the-sink, and the documented disaster floor is `max_slot_wal_keep_size` on
  the source DB (accepts slot invalidation → forced re-snapshot rather than filling the
  primary's disk).
- **Gate:** `strict_ack` MUST NOT be flipped on for a pipeline until this metric + alert are
  live for its source. Encode this as a checklist item in §6's deploy order, not just prose.
**Why:** the correct at-least-once trade (freeze the slot rather than lose data) is only safe
to operate if the resulting WAL growth is visible and alertable before it endangers the source.
**Deps:** WI-4 (watermark exists), WI-7 (slot is authoritative). Independent of WI-6/WI-9.

### WI-6 — PostgresSource: replace `RestartWithNewTables` with a clean session restart
**Files:** `internal/source/postgres/source.go` (`RestartWithNewTables` → `Restart`),
`internal/source/provider.go`, `internal/engine/producer.go` (dynamic-tables path),
`source_remediation_test.go`.
**Change:**
- `Restart(ctx, newTables)`:
  1. Under `s.mu`: dedup-merge `newTables` into `s.config.Tables` (fixes N14, :715), copy
     `s.lastCheckpoint` **while still holding the lock** (fixes the :801/:807 race).
  2. Tear down exactly as today (:707-713: cancel, connector.Close, `runWg.Wait()`).
  3. Delegate to the same code path as `Start` (extract a shared
     `startSession(ctx, cfg, checkpoint, ackers) (msgChan, ackChan, error)` used by both),
     which allocates fresh `msgChan`+`ackChan`, spawns `startConnector` **and
     `runAckCoordinator`**, and returns the new channels. This deletes the duplicated
     `triggerFlush` with its cap-1 `default:` drop (:754-769) and the never-respawned
     coordinator (:465-469 only-in-Start) in one move.
  4. `Snapshot.Enabled` in the restart config (:729-731) stays `false` — dynamic tables are
     snapshotted by the producer's chunked snapshotter, not the vendored one.
- **Producer-side rebind protocol (expanded — review finding B + concern 2; this is the
  riskiest concurrency in the plan and must be specified before coding).** The failure today
  is that a restart reallocates channels the producer can't see, and the producer's
  `!ok`-on-closed-channel path (`producer.go:197-213`) can't tell "session rotated" from
  "source stopped". Design:
  1. **Single-threaded rebinding: the restart runs *inside* `Producer.Run`'s select, never
     concurrently with it.** Add a request channel `restartReq chan restartRequest` (where
     `restartRequest{ tables []string; done chan sessionChans }`). `handleDynamicTables`/
     `SignalDynamicTables` no longer call `source.Restart` directly — they send a
     `restartRequest` and block on `done`. `Run`'s select handles it inline: calls
     `source.Restart(ctx, req.tables)`, receives the fresh `(msgChan, ackChan)`, rebinds its
     own loop variables, and replies on `done`. Because this happens in the same goroutine
     that reads the channels, there is no rebind race and no window where a producer reads a
     stale channel.
  2. **Both channels rebind together.** `Restart` returns `sessionChans{ msgChan, ackChan }`;
     `Run` rebinds *both* loop variables. The old typed `ackChan` (never reallocated today,
     confirmed) rides the same rotation — this is the gap the first draft left implicit.
  3. **Distinguish rotate from stop by construction, not by inspecting the closed channel.**
     Because the rotation is driven by an explicit `restartReq` the producer itself is
     servicing, the `msgChan` close that the *old* session's cleanup goroutine performs is
     expected and consumed by the rebind step — the producer swaps to the new channel before
     ever selecting on the closed old one. The drain-marker/`!ok` branch at
     `producer.go:197-213` is then reached **only** on genuine source stop (context cancel /
     `Stop()`), which is exactly when a drain is correct. Add an explicit `stopping bool`
     guard so a spurious close can never be misread as stop.
  4. **Ack-coordinator continuity.** `startSession` (shared with `Start`) spawns
     `runAckCoordinator` for the new session; the old coordinator exits on the old session
     ctx cancel. Because the AckManager is *not* torn down across restart (it holds the
     watermark and `required` sinks), in-flight confirms for the pre-rotation LSNs still land.
     Verify the watermark floor survives the rotation (hydrate the new coordinator from the
     live AckManager, don't reconstruct it).
  5. **Failure atomicity.** If `source.Restart` errors, `Run` must reply on `done` with the
     error and keep the *old* channels bound (do not half-rotate). The caller
     (`handleDynamicTables`) surfaces the error; the pipeline stays on the working session.
**Why:** fixes the High restart findings (dead coordinator, orphaned msgChan, self-inflicted
drain, dropped batches), the Medium `lastCheckpoint` race, and closes the ackChan-rebind gap.
**Deps:** WI-4 (shares `startSession`). **Design-gate:** land a small prototype / design note
for this rebind loop and get it reviewed before implementing — it is the one item in this plan
whose concurrency is easy to get subtly wrong.

### WI-7 — Resume authority: slot-first, KV as floor + observability
**Files:** `internal/source/postgres/source.go`, `internal/engine/pipeline.go`.
**Change:**
- Delete the `cfg.StartLSN = pq.LSN(checkpoint.IngressLSN)` override (`source.go:421-423`)
  and the redundant `conn.UpdateXLogPos(checkpoint.IngressLSN)` at `source.go:606-608`.
  Replication resumes from the slot's `confirmed_flush_lsn`, which under the new contract is
  ≤ every sink's durable position — replay-safe by construction. Keep `Hydrate(IngressLSN)`
  as the watermark floor so the first standby update never regresses below KV knowledge
  (harmless with the monotonic guard, but explicit).
- `pipeline.go:103-121` (min-EgressLSN scan): keep computing `minLSN` but use it only for
  `Hydrate`/observability, not for `StartLSN`. Add a warning log if
  `slot confirmed_flush_lsn > minLSN + threshold` (indicates the invariant was violated —
  e.g. by a pre-upgrade slot; see §6 rollback note).
- Add `persistWatermark(wm)` in the coordinator (WI-4 step 3): best-effort KV write of
  `Checkpoint{IngressLSN: wm, Status: "ACTIVE"}` under a new per-source key
  (`protocol.SourceWatermarkKey(pipelineID, sourceID)` in `internal/protocol/config.go`)
  at most once per second. Purely observability/dashboard; failures log and continue.
**Why:** makes the watermark authoritative end to end and removes the second path by which a
stale/poisoned KV LSN (e.g. from snapshot rows, Critical 11) could truncate replay.
**Deps:** WI-4, WI-5.

### WI-8 — Pipeline: kill the zombie (Critical 13)
**Files:** `internal/engine/pipeline.go`.
**Change:** wrap the producer goroutine body (`pipeline.go:63-151`) as
`err := p.runProducer(...)`; on any error return — including the config-load early returns
at :68-87 — call `p.cancel()` before `wg.Done`, so consumers exit, `finished` closes, and
the supervisor's restart logic (manager.go) takes over. The *normal* completion path
(producer drained, :145-150) must NOT cancel before `cons.Drain(lsn)` is signaled — keep the
current ordering there, then let consumers exit via drain (WI-9 makes that robust).
Additionally: `SetDynamicTablesChan`'s goroutine (`producer.go:893-902`) becomes
ctx-aware/`wg`-tracked (fixes N6 in passing since we're editing the lifecycle).
**Why:** a transient KV error must produce a restartable crash, not a silent forever-idle
pipeline heartbeating "Running".
**Deps:** none (can land any time; listed here because its test rides the engine harness).

### WI-9 — Consumer: wire the two dead helpers; deterministic drain
**Files:** `internal/engine/consumer.go`, `internal/engine/producer.go`,
`internal/engine/engine_test.go`.
**Change:**
1. **Make `flushWithFilter` live (mixed schema+data wmMsg).** In `Run`:
   - Delete the `if !hasSchemaChange` gate at `consumer.go:280-281`; always append `wmMsg`
     to `wmMsgs` when the payload contains at least one non-schema message, and *also* track
     it in a `pendingSchema map[*message.Message]int` (count of unapplied schema changes in
     that wrapper) when it contains schema changes.
   - Replace every `c.flush(ctx, batch, wmMsgs)` call site (:226, :237, :244, :274, :293,
     :384) with `c.flushWithFilter(ctx, batch, wmMsgs, func(m) bool { return pendingSchema[m] == 0 })`;
     delete the now-redundant plain `flush` (or make it a thin wrapper calling
     `flushWithFilter(..., nil)`).
   - In the schema branch, on `ApplySchema` success decrement `pendingSchema[wmMsg]`; ack
     the wrapper **only when** its count is zero AND its data rows have been flushed
     (i.e. the wrapper is no longer referenced by the live `wmMsgs` slice). The
     `schemaWMMsgs` bookkeeping at :310, :328, :338, :350 collapses into `pendingSchema`.
   - Net invariant: a wmMsg is acked exactly once, and only after every message in its
     payload is durable (schema applied + data uploaded).
2. **Make `checkDrained` live (LSN-bounded drain).** In `Run`, after each successful
   flush compute `maxFlushedLSN`; `if c.checkDrained(maxFlushedLSN) { flush remainder; return nil }`.
   Also add a periodic backstop while `isDraining`: on the batch timer tick with an empty
   batch, query the subscriber's JetStream consumer for `NumPending == 0` (expose
   `PendingCount(ctx) (uint64, error)` on `stream.Subscriber` / `NatsSubscriber`); if zero,
   return. `drain_marker` (`consumer.go:286-305`) remains the fast path; the drain no longer
   depends on a single unlosable message (fixes N3).
3. **Deterministic buffer drain (1s-idle High).** `producer.go:571-593`
   (`drainBufferedUntilIdle`): replace the `idleTimeout` return condition with the same
   server-side truth: drain until the buffer consumer reports `NumPending == 0` (using the
   `PendingCount` API from step 2) **while `muTableStates` write-lock is held for the final
   verification** (the locking protocol at `producer.go:596-614` + read-lock-through-publish
   at :408-409 already guarantees no concurrent buffer publish during the final check — keep
   it, swap only the emptiness predicate). While here, give the drainer a *stable* durable
   name (`fmt.Sprintf("drainer-%s-%s", p.pipelineID, table)` instead of
   `"drainer-"+uuid.New()` at `producer.go:519`) so redelivery resumes instead of replaying,
   and delete the JetStream consumer after a completed drain. (Full drainer/DeliverPolicy
   rework is Critical 8 in plan 01b; this is the minimal correctness slice needed here.)
**Why:** removes the last ack-before-durable-write path on the consumer, converts both
"time-based proof" hacks into state-based proofs, and finally ships the two helpers written
for exactly these bugs.
**Deps:** WI-2 (OpDrainMarker constant), independent of WI-4/5 otherwise.

### WI-10 — Tests (detailed in §5)
**Deps:** each WI lands with its unit tests; the e2e invariants land after WI-5/WI-7.

---

## 4. Vendored-library strategy

**Decision: patch the vendored copy behind an opt-in config flag (`ManualCommit`), not a
wrapper and not a hard fork of behavior.** Rationale against the alternatives:

- **Wrapper (intercept `Connector`/`Streamer` from outside):** impossible for the two
  critical call sites — `lc.Ack`'s closure (`stream.go:397-402`) and the keepalive handler
  (`stream.go:299-302`) are constructed inside `stream` and never cross a seam we own. We
  would have to wrap `pq.Connection` to swallow `SendStandbyStatusUpdate` frames, which is
  protocol-level surgery far more fragile than a source patch.
- **Config-only upstream feature:** upstream has no manual-commit mode today; we can't wait.
- **Unconditional patch (no flag):** works, but makes the vendored tree diverge in behavior
  for any other consumer and makes the VENDOR.md re-sync workflow (diff → rsync → re-apply,
  `VENDOR.md:14-43`) riskier. A flag keeps default behavior byte-for-byte upstream, so the
  re-apply patch is small and mechanically mergeable.

Concrete patch set (all under `// vendored-patch: T0-1` markers, catalogued in
`internal/vendor/go-pq-cdc/PATCHES.md` following the existing T1-4/T1-5/T2-6 format):

| Site | Change | Guarded by |
|---|---|---|
| `config/config.go` | add `ManualCommit bool`, `KeepaliveFunc func(pq.LSN)` (json/yaml-tagged `manualCommit`; func field excluded from serialization) | — |
| `stream.go:397-402` | `Ack` returns nil without `UpdateXLogPos`/`SendStandbyStatusUpdate` | `ManualCommit` |
| `stream.go:299-302` | keepalive `ServerWALEnd` routed to `KeepaliveFunc` instead of `UpdateXLogPos` | `ManualCommit` |
| `stream.go:334` | undecodable-message `UpdateXLogPos(WALStart)` removed | `ManualCommit` |
| `stream.go:305` | `ReplyRequested` reply gains `LoadXLogPos() > 0` guard | always |
| `stream.go:448-451` | monotonic guard `lsn <= lastXLogPos → return` | always |

Interaction audit (why nothing else advances the slot):
- `sinkLoop` receive-timeout branch (`stream.go:240-249`) sends `LoadXLogPos()` — after the
  patch, `lastXLogPos` only changes via `Connector.UpdateXLogPos` (our coordinator) and the
  `StartLSN` seed (`connector.go:291-293`), so this branch degrades into a correct
  keepalive of the confirmed watermark. Same for the `ReplyRequested` branch.
- `slot.NewSlot(..., stream.(slot.XLogUpdater))` (`connector.go:124`,
  `slot/slot.go:22-23,47`): grep confirms the slot package holds the updater for metrics
  plumbing; verify during implementation that no slot code path invokes `UpdateXLogPos`
  spontaneously (if it does, gate it identically).
- Snapshot events reach the listener via `snapshotHandler` (`connector.go:585-593`) with a
  no-op Ack and no `LSN` in the ListenerContext — consistent with WI-4's "snapshot rows
  bypass the watermark".

Follow VENDOR.md's own recommendation as the medium-term exit: fork
`Trendyol/go-pq-cdc`, commit T0-1 (+existing T1-4/T1-5/T2-6) as branch commits, switch the
`go.mod` replace to the fork, and offer `ManualCommit` upstream — it is a generally useful
feature (it is exactly Kafka's `enable.auto.commit=false`). Track as a separate chore.

---

## 5. Test plan

### Unit — `internal/source/postgres` (extend `ack_test.go`, `source_remediation_test.go`)

The package already has the right scaffolding: `SetConnectorFactory`
(`source.go:146-154`) lets tests inject a fake `cdc.Connector`. Add a
`recordingConnector` fake that records every `UpdateXLogPos` call and exposes a way to feed
`ListenerContext`s into the captured handler.

1. **AckManager multi-sink:** `NewAckManager([]string{"a","b"})`; Observe 100,200,300;
   Confirm(100,"a") → watermark 0; Confirm(100,"b") → 100; Confirm(300,"a")+("b") →
   still 100 (gap at 200); Confirm(200,"a")+("b") → 300.
2. **AckManager multiplicity:** Observe(100) twice (two events sharing a txn LSN);
   Confirm(100,"a") once + Confirm(100,"b") once → watermark 0; second confirm from each →
   advances. (Guards the shared-LSN edge from §7 Q2.)
3. **ObserveConfirmed:** interleave filtered LSNs among data LSNs; watermark passes filtered
   ones without any Confirm.
4. **IdleAdvance:** pending non-empty → no advance, returns false; pending empty → advances
   to serverWALEnd; never regresses.
5. **Confirm idempotency below watermark:** Hydrate(500); Confirm(400,"a") is a no-op
   (models AcksTopic redelivery after restart).
6. **Handler never acks / never advances:** feed Insert/Update/Delete/Relation/unknown-table
   events through the captured handler with a counting `lc.Ack`; assert zero Ack calls and
   zero `UpdateXLogPos` calls until the coordinator is fed a matching `SourceAck` for all
   ackers, then exactly one `UpdateXLogPos(watermark)`.
7. **Handler panic safety:** a message that forces a panic inside construction (e.g. poisoned
   `driver.Valuer` in `sanitizePayload`); assert the source still flushes subsequent events
   (mu not stranded).
8. **Coordinator blocking-ack ingestion:** fill `ackChan` faster than the ticker; assert no
   ack loss and eventual watermark == max fully-confirmed LSN.
9. **Restart:** using the stub factory, call `Restart` with overlapping table names; assert
   (a) tables deduped, (b) new msgChan/ackChan returned and live, (c) a `SourceAck` sent
   post-restart still advances the slot (coordinator respawned — the exact regression from
   `source.go:465-469`), (d) no send on the old channels, (e) no race under `-race` on
   `lastCheckpoint`.
10. **Snapshot config gate:** `Start` with `checkpoint.IngressLSN > 0` still yields
    `cfg.Snapshot.Enabled == true` and `cfg.StartLSN == 0` (assert via the factory's captured
    `config.Config`).

### Unit — vendored (`internal/vendor/go-pq-cdc/pq/replication`)

> **STATUS: tests 11-13 are NOT IMPLEMENTABLE today — deferred, verified twice.** Confirmed
> during WI-1: (a) the vendored directory is a **separate Go module** that cannot build
> standalone — its `go.mod` omits `require` entries for `avast/retry-go/v4`,
> `go-playground/errors`, `lib/pq`, and `gopkg.in/yaml.v2` despite importing them, so it compiles
> only via the root `replace` directive; (b) it contains **zero** `*_test.go` files; (c) the
> behavior is not observable from the root module either — `Streamer` exports `UpdateXLogPos` but
> **not** `LoadXLogPos` (a method on the unexported `stream`), and tests 11/12 additionally need
> `process`/`handleKeepalive` plus an injectable `pq.Connection` set inside `Connect`, all
> unexported. Widening the vendored API purely for testing was rejected.
>
> **Interim coverage:** WI-10 e2e invariants own these behaviors — test 23
> (`TestKeepaliveDoesNotConfirmInflight`) covers test 12's behavior end-to-end against real
> PostgreSQL, which is stronger than a faked-connection unit test.
>
> **Unblocking:** once the module is made independently buildable (tracked separately), tests
> 11-13 become ordinary internal `package replication` tests requiring **zero** API widening.
> Record them against that task so the coverage is not orphaned.

11. **ManualCommit Ack no-op:** with the flag set, `lCtx.Ack()` leaves `lastXLogPos`
    untouched and sends nothing (fake `pq.Connection` counting frontend sends).
12. **Keepalive routing:** ManualCommit + KeepaliveFunc → callback invoked with
    ServerWALEnd, `lastXLogPos` unchanged; flag off → legacy behavior (protects the
    re-sync workflow).
13. **Monotonic UpdateXLogPos:** 100 then 50 → position stays 100.

### Unit — engine (`internal/engine/engine_test.go` + new files)

14. **Producer forwards RecordAck losslessly:** publish N RecordAcks on a fake AcksTopic
    subscriber channel; assert N `SourceAck`s received on ackChan in order, and the NATS msg
    is acked only after the channel send (no `default:` drop path exists any more —
    assert by filling ackChan and checking the producer blocks rather than drops).
15. **Producer skips checkpoint for snapshot/zero-LSN messages:** run a batch containing
    OpSnapshot + OpInsert; assert KV `IngressCheckpointKey` written only for the insert.
16. **Consumer emits one RecordAck per flush with the exact LSN set**, excluding
    OpSnapshot/LSN-0; ack-publish failure → wmMsgs Nacked, nothing acked.
16b. **Ack published before wmMsg ack (review finding A):** assert the publish-then-ack
    ordering — inject a fault that fails after `BatchUpload` succeeds but before wmMsg ack, and
    assert the RecordAck was already published (so redelivery regenerates nothing lost) OR the
    wmMsgs were Nacked; assert there is no interleaving in which wmMsgs are acked while the
    RecordAck publish is still pending.
17. **Mixed schema+data wrapper (the flushWithFilter test):** craft one wmMsg whose payload
    is `[OpSchemaChange, OpInsert, OpInsert]`; assert the wrapper is acked exactly once, and
    only after both `ApplySchema` succeeded **and** `BatchUpload` of the two inserts
    succeeded; crash simulation (fail `BatchUpload`) → wrapper Nacked, not acked. This is
    the direct regression test for consumer.go:280/349/373; the existing
    `TestConsumer_SchemaChange_AckOnlyAfterApplySchema` (`engine_test.go:561`) covers only
    the schema-only wrapper.
18. **checkDrained live:** `Drain(targetLSN)`; feed batches whose max LSN crosses target;
    consumer returns without ever seeing a drain_marker.
19. **Zombie fix:** KV `Get` fails in the producer goroutine → `Finished()` closes within
    the test timeout and consumers exit (regression for pipeline.go:73-87).

### Integration/e2e — `internal/test/e2e` (testcontainers harness: `env.go`,
`containers.go`; slot introspection precedent at `pressure_test.go:99`)

20. **`TestSlotNeverAdvancesBeforeSinkAck` (the headline invariant):** pipeline with a
    gate-able sink (add a `blockableSink` toggle to the debug sink or a test sink). Insert
    rows; while the sink is blocked, poll
    `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=$1` and assert it
    stays < the rows' commit LSN (obtain via `pg_current_wal_lsn()` after the inserts) for
    ≥ several coordinator ticks; unblock; assert it advances ≥ that LSN within a deadline.
21. **`TestCrashBetweenPublishAndSinkReplays`:** kill the worker (cancel worker ctx /
    recreate the engine) after NATS publish but before sink write (block the sink, wait for
    ingest-stream count > 0, tear down); restart against the same slot; assert every row
    lands in the sink (duplicates permitted, loss not) — this is the exact Critical-1 crash
    window.
22. **`TestCrashBeforePublishReplays`:** block the *publisher* (circuit-open or stop NATS),
    let events be handed to msgChan, kill, restart; assert replay from the slot (this used
    to lose data because `lc.Ack` had already advanced it).
23. **`TestKeepaliveDoesNotConfirmInflight`:** block the sink, generate WAL activity on an
    unrelated table (forcing keepalives with high ServerWALEnd), assert
    `confirmed_flush_lsn` frozen; then with an idle, fully-acked pipeline, assert
    `confirmed_flush_lsn` *does* follow `ServerWALEnd` (IdleAdvance working — WAL-bloat
    guard).
24. **`TestSnapshotCrashResume` (Critical 11):** table with enough rows for many chunks
    (`ChunkSize: 8000` at source.go:414 — use a small override); kill the worker after ≥1
    chunk is published but before `cdc_snapshot_job.completed`; restart; assert
    (a) `Snapshot.Enabled` path ran (job resumed: `completed_chunks` grew, not reset),
    (b) every row reaches the sink, (c) rows inserted *after* the snapshot LSN arrive via
    CDC exactly once per the at-least-once contract. Extend `snapshot_test.go` /
    `recovery_test.go`.
25. **`TestRestartWithNewTablesKeepsDelivery`:** dynamic table add mid-stream; assert
    (a) pre-existing tables' events continue without a spurious pipeline drain (the old
    drain-marker-on-restart bug), (b) post-restart acks still advance the slot (coordinator
    respawned), (c) no batch loss across the rotation.
26. **`TestDrainMarkerLost`:** intercept/drop the drain marker in a test publisher; assert a
    draining consumer still terminates via checkDrained/NumPending backstop.
27. **`TestBufferDrainUnderRedeliveryLag`:** freeze a table (schema evolution), buffer rows,
    introduce >1s redelivery lag (short NATS outage), unfreeze; assert zero stranded
    messages on the buffer stream after transition to CDC (regression for the 1s idle
    window).

CI note: none of this runs today (Critical 20, separate fix sequence); tag the new e2e tests
into the existing suite so they're picked up the moment CI lands.

---

## 6. Rollout / migration

### Feature flag

Single flag, source-scoped: `strict_ack` (env `CDC_STRICT_ACK`, default **on** in dev/test,
off→on for prod after bake). It gates in one place — `PostgresSource.Start` — the setting of
`cfg.ManualCommit = true` plus the handler's no-ack behavior. With the flag off, the source
runs the legacy path (per-event ack) while all the *plumbing* (typed ackChan, RecordAck,
AckManager) still runs and can be observed via metrics before it is given authority.
Expose `cdc_source_ack_watermark`, `cdc_source_pending_lsns`, and
`cdc_source_slot_lag_bytes` gauges so the bake period can compare
`watermark` vs `confirmed_flush_lsn` — under legacy mode, `confirmed_flush_lsn > watermark`
is expected; the flag flip is safe when the watermark tracks closely under production load.

### Wire/KV compatibility

- **AcksTopic:** producer accepts both legacy `Op:"ack"` (single LSN, one per message) and
  `OpRecordAck` (batched). Consumers emit `OpRecordAck` once deployed. Because producer and
  consumers deploy together per worker (same binary, `factory.go`), cross-version traffic
  only occurs during a rolling deploy against the shared durable
  (`cdc-worker-<id>-producer-acks`, factory.go:141-150); dual-read covers it. Remove legacy
  parsing one release later.
- **KV `Checkpoint` (`internal/protocol/state.go:10-16`):** no field changes — semantics
  only. `IngressLSN` written by the producer remains "published-to-NATS position"; the new
  `SourceWatermarkKey` entry is additive. Old entries need no migration.
- **Snapshot checkpoints:** pre-upgrade deployments may hold `IngressLSN` values that came
  from snapshot rows. Since WI-7 stops using KV LSNs for `StartLSN`, these stale values
  become inert automatically. The `cdc_snapshot_job` / `cdc_snapshot_chunks` tables in the
  *source* database are already maintained by the vendored library; a mid-snapshot-crashed
  pre-upgrade deployment will, on upgrade, resume its incomplete job — which is the fix
  working as intended.
- **The slot itself:** on first post-upgrade start, `confirmed_flush_lsn` may already be
  *ahead* of the sinks' durable position (the old code over-advanced it). That data is
  already unrecoverable via replication; log the WI-7 warning and, where the gap matters,
  operators re-snapshot (`Resnapshot` config, vendored `connector.go:349-355`). Document
  this in the release notes — the upgrade stops the bleeding; it cannot resurrect
  already-confirmed WAL.

### Deploy order

**Preconditions before any prod flag flip (all mandatory):**
- [ ] Fix Sequence 3 (CI) landed and green, running e2e invariant tests 20–27. The headline
      invariant (`TestSlotNeverAdvancesBeforeSinkAck`) is not something we verify by hand for a
      change that can retain unbounded WAL on the source primary.
- [ ] WI-5a slot-lag metric + alert live for the target pipeline's source.
- [ ] Bake period completed: `cdc_source_ack_watermark` tracks `confirmed_flush_lsn` closely
      under production load (with the flag off, `confirmed_flush_lsn > watermark` is expected;
      the gap should be small and stable, not growing).

1. Release N: all WIs, `strict_ack` off in prod, metrics observed (watermark plumbing live).
2. Release N (config change): once the preconditions above are met, flip `strict_ack` on,
   **pipeline by pipeline**; watch `cdc_source_slot_lag_bytes` and sink lag after each flip.
   Stop and investigate any pipeline whose slot lag grows without a corresponding sink outage.
3. Release N+1: remove legacy ack parsing; make `strict_ack` on-by-default; flag removal in
   N+2.

Rollback at any step = flip the flag off (legacy per-event ack returns; no data-format
change to unwind). Note rollback re-opens the loss window — it is an availability escape
hatch, not a correctness one.

---

## 7. Risks, open questions, sequencing

### Risks

1. **WAL retention when a sink is down (intended, but now visible).** Under strict ack, a
   dead sink freezes `confirmed_flush_lsn` and PostgreSQL retains WAL without bound. This is
   the correct trade (the alternative is silent loss), but it converts a data-loss failure
   into a disk-pressure failure. Mitigation: the `cdc_source_slot_lag_bytes` gauge + alert
   runbook; consider `max_slot_wal_keep_size` on the source DB as the operator-chosen upper
   bound (accepting slot invalidation → forced re-snapshot as the disaster floor).
2. **Ack-channel backpressure loops back into the consumer.** Producer's blocking ackChan
   send (WI-5) means a wedged source coordinator eventually stalls AcksTopic consumption
   (NATS buffers absorb a lot first). Acceptable: the coordinator does O(1) work per ack;
   if it's wedged the pipeline is already broken and backpressure is the honest signal.
   Watch in test 8/14.
3. **Throughput of per-LSN confirms.** `Confirm` is a map op + amortized O(1) front-scan;
   RecordAck batching keeps NATS message counts at one per flush. The pressure test
   (`pressure_test.go`) should be extended with an assertion on sustained watermark
   progression to catch regressions.
4. **Vendored re-sync burden.** T0-1 touches `stream.go`, which upstream changes often.
   Mitigate via the flag-guarded minimal diff and the fork recommendation (§4).
5. **`transitionTableToCDC` deadlock surface.** WI-9's NumPending check runs while holding
   `muTableStates` — it performs a network call under a lock. This mirrors the current
   design (verifyEmpty already does I/O under the lock, `producer.go:596-614`); bound it
   with a context timeout to avoid converting a NATS outage into a stuck producer.

### Open questions

1. **`ackers` for a pipeline whose sink set changes at runtime.** Sinks are fixed at
   worker construction (`factory.go:76-133`), and a config change goes through a full
   worker restart (manager reload), so `Start(..., ackers)` is stable per session. Confirm
   there is no hot-add-sink path; if one appears later, `AckManager` needs a
   `SetRequired` with careful semantics for in-flight LSNs.
2. **Shared-LSN multiplicity.** The vendored `messageBuffer` rewrites the *last* message of
   a transaction to the commit LSN (`stream.go:160-202`); other messages carry distinct
   `walStart`s. We have not proven two messages can never share an LSN, so the AckManager
   tracks per-LSN observed multiplicity (WI-3, test 2) defensively. Verify against a
   multi-statement single-txn e2e case and simplify if provably impossible.
3. **Ack replay after producer restart — promoted to a spec, not a footnote (review concern
   5).** The producer-acks durable (factory.go:141-150) redelivers unacked RecordAcks after a
   crash, so the AckManager will receive `Confirm(lsn, sink)` for LSNs the *new* session has
   not yet `Observe`d (they arrive again only when replication replays from the slot). This is
   a correctness hazard for the multiplicity rule (`confirmed >= observed`): if a confirm
   lands before the observe, and `observed` is later set to N by replay, the pre-counted
   confirms must still be counted — but they must **not** prematurely satisfy the rule before
   `observed` is known. Required state-machine semantics for `ackEntry`:
   - `Confirm(lsn, sink)` for an unknown `lsn` creates an entry with `observed = 0` and records
     `confirms[sink]++`. The entry is **not** eligible for watermark inclusion while
     `observed == 0` (an entry with zero observations is "confirmed by ghosts" and inert).
   - `Observe(lsn)` increments `observed`. Only now can the contiguous-run scan consider it,
     and only when every required sink's `confirms[sink] >= observed`.
   - `Confirm(lsn <= watermark)` remains a no-op (idempotent redelivery).
   - Invariant to test (add to test 2/5): a `Confirm`-before-`Observe` sequence followed by
     the matching `Observe`s yields the same watermark as `Observe`-then-`Confirm`, and an
     entry that is confirmed-but-never-observed never advances the watermark. This closes the
     "confirmed-unobserved entry prematurely confirms" gap.
4. **`SnapshotConfig` for hot-restarted sessions** (WI-6 keeps `Enabled: false` on restart).
   If a worker crashes *between* AlterPublication and the producer's chunked snapshot
   completing for a dynamic table, resume is via the producer's PK checkpoint
   (`producer.go:1028-1053`) — unchanged by this plan, but worth an e2e case in 01b or a
   follow-up.
5. **Who calls `Source.UpdateXLogPos` after this plan?** After WI-4/WI-7 the coordinator is
   the only slot writer. Grep for remaining callers (engine `mocks`, manager) and either
   delete the interface method or document it as test-only.

### Sequencing within this plan

```
WI-1 (vendored)  ──┐
WI-2 (protocol)  ──┼─► WI-3 (AckManager/interface) ─► WI-4 (source core) ─► WI-5 (engine acks)
                   │                                        │                    │
WI-8 (zombie) ─────┤  (independent, land early)             ├─► WI-6 (restart)   ├─► WI-7 (resume authority)
WI-9 (consumer) ───┘  (independent after WI-2)              │                    └─► WI-5a (WAL-lag gate)
                                                            └────────────► e2e invariants (WI-10, tests 20-27)
```

Recommended landing order: **WI-1, WI-2, WI-8, WI-3, WI-4, WI-5, WI-9, WI-6, WI-7, WI-5a,
WI-10** — each commit keeps the tree green; the strict-ack flag stays off until WI-7 **and**
WI-5a land, at which point (with CI running, per the hard prerequisite at the top of this doc)
tests 20-24 plus a live slot-lag alert are the merge/flip gate.

WI-6's rebind protocol carries a **design-gate**: prototype and review the single-threaded
`Run`-loop rebind before implementing it (see WI-6). It is the one piece of this plan whose
concurrency is easy to get subtly wrong, and it is not on the critical path to the flag flip
(dynamic-table restart is orthogonal to the core slot/ack contract), so it can land after
WI-7/WI-5a without blocking the delivery-guarantee win.

# WI-6 design note — restart session rebind protocol

**Status:** design gate. Nothing here is implemented. Plan reference: `01a_delivery_source_ack.md`
§3 WI-6 ("land a small prototype / design note for this rebind loop and get it reviewed before
implementing it").

All line numbers are against the working tree of `fix/plan-01a-delivery-source-ack` at the time
of writing.

---

## 1. Problem statement

`PostgresSource.Restart` (`internal/source/postgres/source.go:1322-1468`) is a renamed copy of the
old `RestartWithNewTables`. Its own doc comment (`:1309-1321`) states it does **not** implement the
rebind protocol. Concretely, today:

| # | Defect | Evidence |
|---|--------|----------|
| R1 | **Ack coordinator is never respawned.** `Start` spawns `runAckCoordinator` at `:791-795`; `Restart` spawns only `startConnector` (`:1461-1465`). After one restart nothing reads `s.ackChan`, and since the WI-5 producer forward is a *blocking* send (`producer.go:217-222`), the producer wedges on the first `RecordAck` after the 1024-slot buffer fills. The slot then never advances again. | `source.go:791`, `:1461`, `producer.go:217` |
| R2 | **Slot-lag probe is never respawned.** Spawned only in `Start` (`:802-806`). After a restart all four WI-5a gauges (`cdc_source_slot_lag_bytes`, `cdc_source_ack_watermark`, `cdc_source_pending_lsns` — the latter via the coordinator ticker — and `slot_lag_probe_last_success`) freeze at their last value, so the "frozen slot" alert silently stops being able to fire at exactly the moment R1 freezes the slot. | `source.go:802`, `:823` |
| R3 | **`ackChan` is not rotated but is returned as if it were.** `Restart` returns `s.ackChan` (`:1467`), the same channel the dead coordinator owned. The caller cannot tell it received a corpse. | `source.go:1467` |
| R4 | **Orphaned `msgChan`.** `Restart` allocates a fresh `s.msgChan` (`:1400`) and returns it (`:1467`), but no caller in production ever calls `Restart` (verified: the only call sites are `source_remediation_test.go` and the gomock stub; `handleDynamicTables` has the call commented out at `producer.go:558` / `:1031`). If it *were* wired as-is, `Producer.Run` would still be selecting on the channel captured at `producer.go:174`, i.e. the old one, which the old session's cleanup goroutine closes. |
| R5 | **Self-inflicted drain.** The producer's `!ok` branch (`producer.go:259-282`) publishes an `OpDrainMarker` and returns from `Run`. It cannot distinguish "session rotated" from "source stopped", so the close performed at `source.go:1243` during a rotation reads as a graceful end-of-stream and tears the pipeline down. |
| R6 | **Dropped batches.** `Restart`'s duplicated `triggerFlush` uses `select { case s.msgChan <- mCopy: default: }` (`:1416-1426`) on a cap-1 channel — batches are silently discarded under any contention. `Start`'s version correctly blocks with a ctx guard (`:675-681`). |
| R7 | **Unlocked field reads.** `Restart`'s `triggerFlush` closes over the *field* `s.msgChan` (`:1423`) and is invoked from handler goroutines with no lock; the return at `:1467` also reads both fields after `s.mu.Unlock()` at `:1458`. `runAckCoordinator` likewise reads the field `s.ackChan` (`:924`) rather than a per-session local. Any real rotation is a data race by construction. |
| R8 | **`lastCheckpoint` race.** `startConnector` is passed `s.lastCheckpoint` read *after* the unlock (`:1464`), racing `UpdateXLogPos` (`:1255-1259`). |
| R9 | **Pre-existing: `close(msgChan)` is ordered by a `time.Sleep`.** `source.go:1236-1244`: the cleanup goroutine does `triggerFlush(); time.Sleep(100 * time.Millisecond); close(msgChan)`. Nothing prevents the batch-wait ticker (`:1066-1075`) or the replication handler from calling `triggerFlush` after that sleep expires → **send on closed channel panic**. My prototype reproduced exactly this shape under `-race` (see §7). This is not caused by WI-6 but WI-6 makes rotation frequent, so it must be fixed as part of it. |

R1–R3 are the "four bugs" class the gate exists for: every one is an ownership mistake, not a
logic error.

---

## 2. The protocol

### 2.1 Shapes

Source side (`internal/source/postgres/source.go`):

```go
// sessionChans is the atomic unit of rotation. Both fields are the
// freshly-allocated locals from startSession -- never re-reads of s.msgChan/
// s.ackChan (fixes R7).
type sessionChans struct {
    Msg  <-chan []protocol.Message
    Acks chan<- source.SourceAck
}

// startSession is the ONE code path used by both Start and Restart.
//   - caller MUST hold s.mu on entry and MUST NOT hold it on the paths that
//     spawn goroutines; startSession does the unlock itself at the documented
//     point and returns with the lock released.
//   - ackers is nil for a restart: the required-sink set is a property of the
//     source's lifetime, not of a session (see §2.4).
func (s *PostgresSource) startSession(
    ctx context.Context,
    cfg protocol.SourceConfig,
    checkpoint protocol.Checkpoint,
    ackers []string, // non-nil only from Start
) (sessionChans, error)
```

`source.Source.Restart` keeps its current signature; internally it returns
`sessionChans` fields. (Optionally widen the interface to return `sessionChans` — cosmetic, and it
churns `engine/mocks`. Recommend keeping the 3-value signature.)

Engine side (`internal/engine/producer.go`):

```go
type restartRequest struct {
    tables []string
    done   chan restartResult // cap 1, allocated by the requester
}

type restartResult struct {
    msg  <-chan []protocol.Message
    acks chan<- source.SourceAck
    err  error
}

// Producer gains: restartReq chan restartRequest  (unbuffered, allocated in NewProducer)
```

`handleDynamicTables` no longer calls `source.Restart`. It sends a `restartRequest` and blocks on
`done`, both guarded by `ctx.Done()`.

### 2.2 Ordering

The whole rotation runs **on `Producer.Run`'s goroutine**, inside the `case req := <-p.restartReq:`
arm. That is the entire concurrency argument: the only reader of `msgChan` is the goroutine
performing the swap, so there is no window in which a stale channel is read.

```
T0  Run selects  case req := <-p.restartReq
T1  Run calls source.Restart(ctx, req.tables)
      S1  s.mu.Lock()
      S2  dedup-merge req.tables into s.config.Tables      (fixes N14)
      S3  cp := s.lastCheckpoint                            (copy UNDER the lock; fixes R8)
      S4  oldCancel := s.cancel; oldConn := s.connector
      S5  s.stopping stays false  (this is a rotation, not a Stop)
      S6  s.mu.Unlock()
      -- teardown --
      S7  oldCancel()            MUST precede S9: parked triggerFlush sends
                                 select on sourceCtx.Done() and only unpark here.
      S8  oldConn.Close()
      S9  s.runWg.Wait()         old startConnector, batch ticker, coordinator,
                                 lag probe, cleanup goroutine all exit.
                                 On return: old msgChan is CLOSED and no
                                 further send to it is possible.
      -- allocate + respawn --
      S10 s.mu.Lock()
      S11 sc, err := s.startSession(ctx, s.config, cp, nil)
            a11  msg := make(chan []protocol.Message, 1); s.msgChan = msg
            a12  ackCh := s.ackChan          // NOT reallocated -- see §2.3
            a13  strictAck := resolveStrictAck()   // re-read: atomic flip point
            a14  build cfg (Snapshot.Enabled=false on restart), conn, err
            a15  on err: s.mu.Unlock(); return err   // §4
            a16  s.connector = conn; s.cancel/s.ctx = new session ctx
            a17  capture batchWait/discoveryInterval/srcConfigCopy under lock
            a18  s.mu.Unlock()
            a19  runWg.Add(1) x3 -> startConnector, runAckCoordinator(ackCh),
                                    runSlotLagProbe
            a20  return sessionChans{Msg: msg, Acks: ackCh}, nil   // locals only
T2  Run drains the OLD msgChan to exhaustion:
        for b := range oldMsg { <normal batch processing> }
    Guaranteed to terminate: S9 returned, so the channel is closed and no
    writer remains.  This is what makes "no batch dropped across rotation"
    true rather than aspirational.  NOTE: this loop must run the same body
    as the steady-state msgChan arm -- factor that body into
    p.handleBatch(ctx, msgs, &lastLSN) first.
T3  Run rebinds BOTH loop variables: msgChan, ackChan = sc.Msg, sc.Acks
T4  Run replies: req.done <- restartResult{sc.Msg, sc.Acks, nil}
T5  handleDynamicTables unblocks and proceeds to snapshot/drain the new table.
```

Sequence diagram:

```
 dynTables g.        Producer.Run            PostgresSource            old session g's
      |                   |                         |                        |
      |--restartReq------>|                         |                        |
      |   (blocks on done)|--Restart(tables)------->|                        |
      |                   |                         |--cancel()------------->| (unparks sends)
      |                   |                         |--connector.Close()---->|
      |                   |                         |--runWg.Wait()--------->| all exit;
      |                   |                         |<---------------------- | oldMsg CLOSED
      |                   |                         |--startSession----+     |
      |                   |                         |   new msgChan    |     |
      |                   |                         |   same ackChan   |     |
      |                   |                         |   same ackMgr    |     |
      |                   |                         |   spawn x3 ------+---->| (new session)
      |                   |<--sessionChans----------|                        |
      |                   |--drain oldMsg to close--|                        |
      |                   |--rebind msg+ack---------|                        |
      |<--restartResult---|                         |                        |
```

### 2.3 `ackChan` is NOT reallocated — and why that does not violate the plan

Plan WI-6 point 2 says "both channels rebind together". `Run` does rebind both loop variables from
`sessionChans` — the API shape the plan asks for is preserved — but on the source side
`startSession` returns the **same** `ackChan` on a restart. Reallocating it would throw away up to
1024 buffered `SourceAck`s that the engine has already durably earned. Those confirms are not
recoverable: the consumer already `Ack()`ed the NATS message (`producer.go:223`) before the source
lost them. Losing them does not lose data, but it permanently pins the watermark below a
confirmed LSN, i.e. a frozen slot with unbounded WAL retention — the precise operational failure
WI-5a exists to alert on.

`ackChan` is therefore **source-lifetime-owned**: allocated in `Start`, never closed (see §3),
handed unchanged to each session's coordinator *as a parameter* (fixing R7's field read at
`:924`). The rotation is still atomic from `Run`'s point of view because `Run` assigns both
variables from one `sessionChans` value.

### 2.4 AckManager continuity — the un-gating hazard

`Start` does `s.ackMgr = NewAckManager(ackers)` (`:624`) and then `Hydrate` (`:630`). **Neither may
happen on a restart.** Two independent reasons:

1. `NewAckManager` discards `pending`/`lsns`, so every in-flight, observed-but-unconfirmed LSN
   vanishes. `advanceLocked` would then see nothing pending and let the watermark run to the
   next confirm — the slot advances past data no sink wrote. Same failure class as T0-3 (`f192fe3`).
2. `Hydrate` is worse than a no-op here: read `ack.go:377-386` — it **clears `a.pending` and
   `a.lsns`** unconditionally when it advances. Calling `Hydrate` on a *live* manager is exactly
   the un-gating bug wearing a "resume" costume.

The rule, stated for review:

> **`s.ackMgr` is allocated exactly once per `Start` and is never replaced, re-`Hydrate`d, or
> mutated by `Restart`. "Hydrating the new coordinator from the live AckManager" means the new
> coordinator goroutine reads `s.ackMgr` — the same pointer — and nothing else.**

Enforcement: `startSession` takes `ackers []string`; it constructs the AckManager **only when
`ackers != nil`**, and `Restart` always passes `nil`. Add a `//nolint`-proof guard:
`if ackers != nil && s.ackMgr != nil && s.ackMgr.PendingCount() > 0 { return error }` — an
assertion that a re-`Start` over a live session is a programming error rather than a silent
un-gate.

Corollary: the required-sink set cannot change across a rotation. A sink added or removed is a
pipeline-level config change and must go through `Stop`/`Start`, not `Restart`. Making the ackers
set mutable mid-life would require an `AckManager.SetRequired` that re-evaluates every pending
entry; out of scope, called out in §7.

**Constraint 4 (`minLSN` floor unsound for a newly-added sink)** falls out of this: `minLSN`
(`pipeline.go:158-191`) skips sinks with no egress checkpoint, so its value can exceed data a new
sink never wrote. Because `Restart` never hydrates, the rotation cannot import that unsound floor.
The unsoundness remains confined to cold `Start`, where WI-7's `sinkHasCheckpoint` guard
(`pipeline.go:167-172`, `:210-213`) already exists for the frontier check. Recommended hardening,
noted here because WI-6 is where it was found: make the `Hydrate` input suffer the same guard —
if any configured sink has **no** egress checkpoint at all, hydrate from `0`, not `minLSN`, and log
it. Slot-first resume (WI-7) makes that safe: `confirmed_flush_lsn` still provides the real floor.

### 2.5 `strict_ack` and T0-3 ordering

- `strictAck := resolveStrictAck()` stays inside `startSession` (a13), so it is resolved once per
  session and a mid-life flip takes effect atomically at the rotation boundary. Both `ManualCommit`
  and the `createHandler` branch derive from that single local — the coherent pairing validated in
  WI-1 is preserved verbatim. A session never straddles the two handler behaviours.
- T0-3 delivers keepalives in-band via `messageCH`, so keepalive-vs-event ordering is a property of
  a single stream. The rotation destroys one stream and builds another; the old stream's ordering
  is fully consumed by the T2 drain before any message from the new stream is read. There is no
  interleaving point, hence no ordering gap. The one thing that would reintroduce one is reading
  the new `msgChan` before the old is drained — hence T2 strictly precedes T3, and `Run` must not
  `select` over both.
- B3's "silent LSN-0 window" does not recur: the new session inherits a non-zero
  `s.ackMgr.Watermark()`, so `startConnector`'s resume-path seed (`:1126`) fires rather than the
  fresh-slot path.

### 2.6 Distinguishing rotate from stop

Two mechanisms, belt and braces:

1. **By construction.** The `!ok` close of the old `msgChan` is consumed by the T2 drain loop, which
   `break`s out of `for range` rather than entering the drain-marker branch. `Run` never `select`s
   on a channel it has already rotated away from.
2. **`stopping` guard.** `PostgresSource` gains `stopping bool` set under `s.mu` by `Stop()` before
   it cancels, and `Producer` gains its own `stopping bool` set when it observes ctx cancellation.
   The drain-marker branch asserts: if `!ok` arrives on the *currently bound* `msgChan` and the
   producer is not `stopping` and no restart is in flight, that is a bug — log loudly, publish the
   drain marker anyway (fail safe: better a drain than a hang), and increment a
   `cdc_producer_unexpected_msgchan_close_total` counter. This is the tripwire that would have
   caught R5.

---

## 3. Ownership table

| Thing | Allocated by | Closed by | Across a rotation |
|---|---|---|---|
| `msgChan` | `startSession` (per session), stored on `s.msgChan` **and** returned as a local | the session's own cleanup goroutine, which captured it by value (`source.go:1235-1244`), **after** an inner `senderWg.Wait()` replaces the `time.Sleep(100ms)` (fixes R9) | **Rotated.** Old closed before `Restart` returns (guaranteed by `runWg.Wait()`); producer drains it to closure at T2, then rebinds. |
| `ackChan` | `Start` only; source-lifetime | **Nobody.** Never closed. Producer exit is signalled by ctx, not by closing the channel — closing it would race the engine's blocking send at `producer.go:217`. GC reclaims it with the source. | **Not rotated** (§2.3). Buffered acks survive; the new coordinator picks up exactly where the old stopped. Passed to the coordinator as a parameter, not read from the field. |
| `ackMgr` | `Start` only (`NewAckManager(ackers)`), hydrated once from the checkpoint | never | **Survives untouched.** Watermark, pending set and required-sink set all carry over. `Restart` must never call `NewAckManager` or `Hydrate` (§2.4). |
| `connector` | `startSession` via `s.connectorFactory` | `Restart`/`Stop` (`Close()`), before `runWg.Wait()` | **Rotated.** New publication table set; `Snapshot.Enabled:false` on restart (dynamic tables are snapshotted by the producer's chunked snapshotter). Old `Close()` must precede `runWg.Wait()` so `conn.Start`'s loop returns. |
| `runAckCoordinator` | `startSession` (`runWg.Add(1)`) | exits on session-ctx `Done` | **Torn down and respawned.** Same `ackMgr`, same `ackChan`, fresh `connector`. Its `lastFlushedWatermark` local resets to 0 — harmless: the first tick re-sends the current watermark, and the vendored store is monotonic. |
| `runSlotLagProbe` | `startSession` (`runWg.Add(1)`) | exits on session-ctx `Done` | **Torn down and respawned** (fixes R2). Label set (`pipeline`, `source`, `slot`) is unchanged across rotation, so the WI-5a alert's `and` join keeps matching and the gauges resume rather than restarting a new series. Do **not** `DeletePartialMatch` on rotation — that is `Stop`'s job only (`:1300-1305`). |
| `dynamicTablesChan` | `Pipeline` (`pipeline.go:57`) | never closed; consumer goroutine exits on ctx (`producer.go:1102-1108`) | **Untouched.** It is pipeline-scoped, tracked on `p.auxWg`, deliberately not on `p.wg` (`pipeline.go:243-248`) so `Finished()` keeps meaning producer+consumers. The rotation adds `restartReq`, which is *also* not on `p.wg`; the requester goroutine's `done` wait is ctx-guarded so it can never outlive the pipeline. |

`runWg` accounting rule, stated so it can be reviewed at the diff level:

> Every goroutine registered on `s.runWg` must (a) be spawned from `startSession` or from
> `startConnector` (itself spawned by `startSession`), (b) select on the **session** ctx it was
> given, never `s.ctx` re-read from the field, and (c) capture every channel it touches as a
> parameter or closure local at spawn time. `runWg.Wait()` in `Restart`/`Stop` is then a complete
> barrier for the session, and `runWg` may be reused for the next session because it is empty at
> the moment `Wait` returns.

---

## 4. Failure atomicity

`Restart` has exactly two failure points, and it has already torn the old session down before both
of them. So "keep the old session" is impossible literally — the connector is closed and the
goroutines are gone. What must be preserved is *usability*, which is weaker and achievable:

**Rule: `Restart` returns either a live session or an error; it never returns a half-built one, and
on error the source is left in a state where the producer can keep running on the channels it
already holds.**

Implementation:

| Failure | When | Behaviour |
|---|---|---|
| Connector build fails (`connectorFactory` error at a14) | after teardown | **Roll back the table merge** (`s.config.Tables = savedTables`, saved at S2), then call `startSession` again with the *original* table set. If that succeeds, return `(sessionChans_of_recovered_session, err)` — a non-nil error **and** usable channels. `Run` binds the returned channels (they are live) and forwards the error to `done`; `handleDynamicTables` marks the table `TableStateFailed` and the pipeline keeps ingesting the pre-existing tables. If the recovery `startSession` *also* fails, return `(zero, err)`; `Run` then treats it as fatal: publish the drain marker and return the error, so `Pipeline` cancels (WI-8) and the supervisor restarts the pipeline. That is the correct escalation — a source that cannot rebuild any session is dead. |
| `ctx` cancelled mid-rotation | any point | The rotation ctx is the pipeline ctx. Every step is either non-blocking or already ctx-guarded; `runWg.Wait()` terminates because cancellation is what the goroutines are waiting for. `startSession` returns `ctx.Err()`; `Restart` does not attempt recovery (pointless — the pipeline is going away). `Run` sets `stopping = true`, replies on `done` with the error, drains the old channel, publishes the drain marker, returns. |
| `req.done` send blocks | requester died | `done` is cap-1 and the requester always reads it, but `Run` still uses `select { case req.done <- res: case <-ctx.Done(): }` so a dead requester cannot wedge the producer. |
| Requester waiting on `done` while `Run` exits | ctx cancel racing a restart | requester's wait is `select { case res := <-req.done: case <-ctx.Done(): return }`. |

Explicitly **not** attempted: rolling back to the *same* connector object. `cdc.Connector` has no
reopen; re-running `startSession` is the only rebuild path, and it is the same code the success
path uses, which is the point.

The `sourceRestartTotal` counter (`:1327`) stays incremented up front. Add
`sourceRestartFailedTotal` and `sourceRestartRecoveredTotal` so the two error tiers are
distinguishable on a dashboard.

---

## 5. Invariants

Numbered so tests can cite them.

- **I1 — No lost pending LSNs.** For every LSN observed before the rotation and not yet confirmed
  by all required sinks, `s.ackMgr` still holds an entry after the rotation.
  *Test:* `ackMgr.PendingCount()` and the exact pending set are equal immediately before and after
  `Restart`; `ackMgr` pointer identity is unchanged.
- **I2 — Watermark never regresses.** `ackMgr.Watermark()` after the rotation ≥ before. Guaranteed
  by I1 plus never calling `NewAckManager`/`Hydrate`; `Hydrate` is monotonic anyway
  (`ack.go:381-382`), but the pending-clear makes "monotonic" insufficient, hence I1.
- **I3 — No batch dropped across rotation.** Every batch written to the old `msgChan` before it
  closed is processed by `Run`. Guaranteed by: `Start`-style blocking `triggerFlush` (kills R6),
  `runWg.Wait()` before returning (no writer survives), and the T2 drain loop.
  *Test:* deterministic sequence numbers, assert no gap.
- **I4 — No spurious drain.** No `OpDrainMarker` is published during a successful rotation, and
  `Run` does not return.
- **I5 — A close of the bound `msgChan` outside a rotation still means stop.** The `!ok` branch is
  reachable only when `stopping` or on genuine source exit; the tripwire counter is 0 in all
  passing tests.
- **I6 — `Finished()` still means producer + consumers done.** Neither `restartReq`'s requester nor
  the rebind is registered on `p.wg`. *Test:* run a rotation, then `Drain()`, assert `Finished()`
  closes within the normal timeout.
- **I7 — Coordinator liveness.** After N rotations there is exactly one live `runAckCoordinator`
  and one live `runSlotLagProbe`, and an ack sent after the Nth rotation advances the watermark.
- **I8 — No send on a closed channel, no data race.** Whole suite green under `-race`; R9's
  `senderWg` ordering replaces the `time.Sleep`.
- **I9 — `strict_ack` never straddles.** Within one session, `ManualCommit` and the handler branch
  derive from one `resolveStrictAck()` call. *Test:* flip the env var mid-life, restart, assert the
  captured `config.Config.ManualCommit` matches the new value and the handler behaviour matches it
  too. (Per WI-1 B4, assert individual fields — `config.Config` is not comparable.)

---

## 6. Test plan

### Source-level unit tests (`internal/source/postgres/source_remediation_test.go`)

Using the existing `SetConnectorFactory` stub.

1. `TestRestartPreservesAckManagerIdentityAndPending` — observe 3 LSNs, confirm 1, `Restart`,
   assert pointer identity, `PendingCount`, and watermark unchanged (I1, I2).
2. `TestRestartRespawnsAckCoordinator` — after `Restart`, send a `SourceAck` on the returned
   ackChan for a pending LSN, assert the watermark advances within the tick interval and
   `conn.UpdateXLogPos` is called on the **new** connector (I7, R1).
3. `TestRestartRespawnsSlotLagProbe` — shrink `slotLagProbeInterval`, assert
   `slot_lag_probe_last_success` advances after a restart (R2).
4. `TestRestartReturnsSameAckChanWithBufferedAcksIntact` — fill the ackChan with acks while no
   coordinator can drain it, `Restart`, assert every buffered ack is eventually confirmed (§2.3).
5. `TestRestartConnectorFailureKeepsSourceUsable` — factory returns an error on the 2nd call,
   succeeds on the 3rd (recovery); assert non-nil error, live channels, table set rolled back (§4).
6. `TestRestartConnectorFailureTwiceIsFatal` — factory always errors; assert `(zero, err)`.
7. `TestRestartUnderRace` — 50 rotations with the handler concurrently flushing, under `-race`
   (I8). This is the test that would have caught R7 and R9.
8. `TestRestartDedupsTables` / `TestRestartCopiesCheckpointUnderLock` (N14, R8).
9. `TestStartOverLiveSessionIsRejected` — the §2.4 assertion.

### Producer-level unit tests (`internal/engine/engine_test.go`, `MockSource`)

10. `TestProducerRebindsBothChannels` — `MockSource.Restart` returns new channels; drive a
    `restartRequest`; assert the producer reads from the new msgChan afterwards and forwards acks
    on the returned ackChan (plan point 2 at the API level).
11. `TestProducerDrainsOldChanBeforeRebind` — pre-load the old msgChan with a batch, close it inside
    the mock `Restart`, assert the batch is published downstream (I3).
12. `TestProducerNoDrainMarkerOnRotation` — assert the test publisher sees zero `OpDrainMarker`
    and `Run` is still running (I4).
13. `TestProducerDrainMarkerStillOnStop` — cancel ctx, assert exactly one drain marker (I5).
14. `TestProducerRestartErrorSurfacesAndKeepsOldSession` — mock returns `(live, err)`; assert
    `handleDynamicTables` marks the table failed and `Run` continues (§4).
15. `TestFinishedAfterRotation` (I6).

### e2e (owned by the concurrent agent — spec only, do not implement here)

Test 25, `TestRestartWithNewTablesKeepsDelivery`, currently skipped because the rebind does not
exist. Once WI-6 lands, un-skip and assert:
(a) rows written to a *pre-existing* table during and after the rotation all reach every sink,
with no pipeline drain (I3, I4);
(b) an ack for a post-rotation LSN advances `confirmed_flush_lsn` (I7 — this is the assertion that
fails today with R1);
(c) sequence-numbered rows across the rotation boundary have no gap (I3);
(d) `cdc_source_slot_lag_bytes` keeps updating after the rotation (R2);
(e) the new table's rows arrive exactly per the at-least-once contract after its chunked snapshot
and buffer drain complete.

Prerequisite: `handleDynamicTables` must actually be wired to the rebind (the calls at
`producer.go:558` and `:1031` are commented out today), otherwise test 25 passes vacuously.

---

## 7. Prototype

Throwaway, `/tmp` only, deleted; no repo files touched. It modelled: a cap-1 msgChan with a
blocking ctx-guarded sender, a cleanup goroutine that closes the captured channel, a `runWg`
barrier, a lifetime-owned ackChan, a `restartRequest`/`done` handshake serviced inside the
consumer's `select`, and a forced mid-rotation failure.

What it proved:

- **The `close`-ordering hazard R9 is real.** The first version copied the production shape
  (`<-ctx.Done(); sleep; close(msg)`) and `-race` immediately reported
  `closechan` vs `chansend` on the same channel. Replacing the sleep with an inner
  `senderWg.Wait()` made it clean. This is a live bug in `source.go:1236-1244` today, independent
  of WI-6.
- **`cancel()` must precede `runWg.Wait()`.** Trivially true in the prototype, but it is the reason
  the whole thing does not deadlock: `Run` is the only reader of `msgChan` and is *inside* the
  restart branch, so a sender parked on `msg <- batch` can only be released by ctx cancellation.
  Get this order wrong and `Restart` hangs forever with no error — the single most likely
  implementation mistake.
- **The T2 drain terminates and loses nothing.** `for b := range oldMsg` after `runWg.Wait()`
  returned drained the tail of every session; batch counts were contiguous across all three
  rotations.
- **No spurious close.** With rotation driven by the request channel, the `!ok` arm was never
  reached (guard asserted with a `panic`); it fired only at genuine stop.
- **Failure atomicity works.** The forced-failure rotation returned a non-nil error *and* a live
  session; the consumer kept receiving. Session counter went to 4 for 3 requested rotations,
  which is the recovery rebuild — visible and expected.

What it did **not** prove: anything about the AckManager, the vendored connector, `runWg` reuse
under a real `startConnector` (which spawns nested goroutines), or the KV/NATS paths. It is a
concurrency-shape check, nothing more.

---

## 8. Risks and open questions

1. **`handleDynamicTables` is not wired to `Restart` at all today** (`producer.go:558`, `:1031`
   are commented out). WI-6 as scoped assumes it is. Wiring it back on is a behaviour change beyond
   the rebind itself: every dynamic table add will now tear down and rebuild the replication
   stream. Is that acceptable operationally, or should the publication `ALTER` alone
   (`AlterPublication`, `source.go:1470`) suffice for tables that need no new decoding state? **I do
   not know whether the restart is actually necessary** — if `ALTER PUBLICATION ... ADD TABLE` is
   picked up by the running walsender for the existing slot, the entire rebind may be unnecessary
   for the common case and should be reserved for config changes that genuinely need a new
   connector. This deserves an answer before implementation; it could shrink WI-6 dramatically.
2. **Plan point 2 ("both channels rebind together") is wrong as literally written** if it means
   reallocating `ackChan`. §2.3 explains; I have implemented the *interface* the plan asks for and
   deliberately not the reallocation. This is the decision most in need of review.
3. **Plan point 5 ("keep the old channels bound, do not half-rotate") is not literally achievable**
   — the old session is destroyed before either failure point. §4 substitutes "rebuild the previous
   table set", which is a weaker but implementable guarantee. Also in need of review.
4. **Plan point 4's "hydrate the new coordinator from the live AckManager"** reads as an action;
   in fact the correct implementation is the *absence* of an action, and taking the phrase at face
   value (calling `Hydrate`) would clear `pending` and cause exactly the un-gating the plan is
   trying to prevent. The phrasing is a trap; §2.4 restates it as a prohibition.
5. **Rotation while a chunked snapshot is in flight.** `performChunkedSnapshot` runs on its own
   goroutine against `s.db`, outside `runWg`. A second dynamic-table add during the first one's
   snapshot triggers a second rotation while the first snapshot publishes. I believe this is safe
   (the snapshot publishes to NATS directly, not through `msgChan`) but I have not verified the
   `tableStates`/buffer interaction across a rotation. Needs a targeted test.
6. **Serialised restarts.** `restartReq` is unbuffered and serviced one at a time, so N
   simultaneous table adds cost N full stream rebuilds. Batching (coalesce all pending requests
   into one rotation) is an obvious optimisation and an obvious source of subtle bugs. Recommend
   shipping the naive version first.
7. **`runWg` reuse.** Reusing one `sync.WaitGroup` across sessions is legal only because `Wait`
   returns with the counter at exactly 0 and all `Add`s for the next session happen after. If any
   goroutine ever does `runWg.Add` from *inside* another `runWg` goroutine after cancellation, this
   breaks with `WaitGroup misuse`. `startConnector` does exactly this pattern today
   (`:1066`, `:1084`, `:1189`, `:1236`) — the adds happen before `conn.Start` returns, which is
   before its own `Done`, so it is currently fine, but it is fragile. Consider a fresh
   `*sync.WaitGroup` per session held in a field, swapped under `s.mu`. I lean toward the per-session
   WaitGroup but did not make it the primary design because it touches more code.
8. **The R9 fix needs an owner.** It is a pre-existing panic risk, not created by WI-6, but WI-6
   makes rotation routine. If WI-6 slips, R9 should be split out and landed on its own.
9. **`minLSN` hardening (§2.4) is a `Start`-path change**, i.e. arguably WI-7's territory, not
   WI-6's. Flagged here because the rebind analysis is where it surfaced; needs an explicit
   assignment.

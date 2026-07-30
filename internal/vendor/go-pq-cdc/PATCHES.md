# Vendored Patches for go-pq-cdc

This document tracks local divergences from the upstream `go-pq-cdc` library.

---

## T1-4: Eager Connection Allocation in `Snapshotter`

**Upstream Issue**: TBD

**Files Modified**:
- `pq/snapshot/snapshot.go`
- `pq/replication/connector.go`

**Problem**: `Snapshotter.New()` opened 7 database connections (metadata, healthcheck, 5 connection pool) even when snapshot is skipped, wasting resources.

**Fix**:
- Moved connection establishment from `New()` into a new `Connect(ctx) error` method.
- `connector.go` now calls `Connect()` only when `shouldTakeSnapshot` returns true before `prepareSnapshot`.

**Backward Compatibility**: Public `New()` signature changed to not take `context.Context`, but `Connect()` is a new method. Callers updated to invoke `Connect()` lazily.

---

## T1-5: Destructive `isClosed` Helper Drains Active Channel Signals

**Upstream Issue**: TBD

**Files Modified**:
- `pq/replication/stream.go`
- `pq/replication/connector.go`

**Problem**: The `isClosed(ch)` helper used `select { case <-ch: return true; default: return false }` which drains buffered channel values when called, causing signal loss.

**Fix**:
- Deleted the `isClosed` helper function.
- Added `sync.Once` fields (`closeSinkEndOnce`, `closeCancelChOnce`, `closeReadyChOnce`) to ensure channels are closed exactly once.
- Replaced all `isClosed()` call sites with `sync.Once.Do()` for safe channel closure.

**Backward Compatibility**: `sync.Once` is internal implementation detail; public API unchanged.

---

## T2-6: Retry Filter Ignored in `retry.Config.Do`

**Upstream Issue**: TBD

**Files Modified**:
- `internal/retry/retry.go`
- `pq/connection.go`

**Problem**: 
1. `retry.Config.Do()` never passed the `If` callback to `retry-go`, so custom retry filters were ignored.
2. `pq/connection.go` had inverted callback logic (`err == nil` instead of `err != nil`).

**Fix**:
1. In `retry.go`: Modified `Do()` to append `retry.RetryIf(rc.If)` to the options.
2. In `connection.go`: Fixed callback to return `err != nil` (retry on error).

**Backward Compatibility**: `Config.Do()` behavior now correctly respects the `If` filter. `OnErrorConfig` signature unchanged.

---

## T0-1: Manual-Commit Mode, Keepalive Callback, and Monotonic Position

**Upstream Issue**: N/A (new opt-in feature, tracked internally as plan `01a_delivery_source_ack.md`)

**Files Modified**:
- `config/config.go`
- `pq/replication/stream.go`

**Problem**: The library always advances (and reports) the replication slot's confirmed
position the instant an event is handed to the application (`ListenerContext.Ack()`), and
independently fast-forwards it on every keepalive (`ServerWALEnd`) and on undecodable WAL
messages — all before the embedding application has any chance to gate advancement on a
downstream durability guarantee (e.g. an at-least-once sink ack). Additionally,
`UpdateXLogPos` stored `lastXLogPos` unconditionally, so interleaving of per-message and
keepalive updates could move the *reported* flush position backwards.

**Fix** (all sites guarded by a new `Config.ManualCommit` flag unless noted "always-on"):
- `config/config.go`: added `ManualCommit bool` (`json:"manualCommit" yaml:"manualCommit"`)
  and `KeepaliveFunc func(pq.LSN)` (`json:"-" yaml:"-"`, excluded from serialization since
  func values aren't marshalable). Default `false` preserves upstream behavior byte-for-byte.
- `stream.go` `process()` — the `lCtx.Ack` closure: under `ManualCommit`, returns `nil`
  immediately without calling `UpdateXLogPos` or sending any standby status update. Position
  ownership moves entirely to the embedder calling `Connector.UpdateXLogPos` directly.
  *(flag-guarded)*
- `stream.go` `handleKeepalive`: under `ManualCommit`, routes `pkm.ServerWALEnd` to
  `Config.KeepaliveFunc` (if non-nil) instead of calling `UpdateXLogPos`. Flag off preserves
  the original fast-forward. *(flag-guarded)*
- `stream.go` `handleXLogData`, undecodable-message path: under `ManualCommit`, the
  `UpdateXLogPos(xld.WALStart)` advance is skipped — an unobserved LSN cannot stall the
  embedder's own ack-tracking; the next confirmed event advances past it. *(flag-guarded)*
- `stream.go` `handleKeepalive`, `ReplyRequested` branch: gained a `s.LoadXLogPos() > 0`
  guard (mirrors the existing guard on the receive-timeout branch) so the reply never reports
  LSN 0 to the primary. *(always-on, both modes)*
- `stream.go` `UpdateXLogPos`: added a monotonic guard. The *stored/reported* position never
  regresses (`lsn > lastXLogPos` before storing), but the standby status update is still sent
  on every call — including when `lsn <= lastXLogPos` — using the clamped stored value, not
  the raw incoming `lsn`. This is deliberately **not** an early-return-and-skip-send: clamping
  what we report is a strictly smaller behavioral delta than removing a send that upstream
  performs, which keeps the flag-off path closer to byte-for-byte upstream and avoids depending
  on the `sinkLoop` receive-timeout heartbeat (`stream.go:239-247`, which already sends a
  standby update roughly every 300ms while `LoadXLogPos() > 0`) surviving a future re-sync.
  *(always-on, both modes)*

**Backward Compatibility**: `ManualCommit` defaults to `false`; with it unset, behavior is
byte-for-byte upstream except for the two always-on guards above (the `ReplyRequested` zero-LSN
guard and the monotonic-report guard), both of which are pure bug fixes with no legacy-mode
behavior change in any reachable, correctly-functioning scenario.

---

## T0-2: Context and Error on UpdateXLogPos; Bounded Standby Write via Goroutine + Semaphore

> ⚠ **This patch is API-BREAKING**, unlike T0-1 (which was purely additive and flag-guarded).
> It changes exported interface signatures in three packages. A future upstream re-sync must
> re-apply it across **every** interface definition listed below, not just the implementations —
> a partial re-apply will fail to compile, which is the desired outcome (loud, not silent).

**Upstream Issue**: N/A (internal requirement, tracked as plan `01a_delivery_source_ack.md`,
blockers B1 and B2)

**Files Modified**:
- `connector.go` (`Connector` interface + `*connector` impl + `StartLSN` seed call site)
- `pq/replication/stream.go` (`Streamer` interface; `*stream.UpdateXLogPos` impl; new
  `standbySem` field on `stream` + its init in `NewStream`; new sentinels `ErrStreamClosed` and
  `ErrStandbyWriteInFlight`; three internal `UpdateXLogPos` call sites. `SendStandbyStatusUpdate`
  itself is **functionally unchanged** — it gains only an explanatory NOTE comment.)
- `pq/slot/slot.go` (`XLogUpdater` interface)

**Problem**: Two separate defects that together made the slot write unboundable and unverifiable.

1. `UpdateXLogPos(lsn pq.LSN)` took no context and returned no error. Under `ManualCommit`
   (T0-1) this is the **only** path that advances the PostgreSQL replication slot, and the
   embedding application's correctness depends on advancing it only after every sink has
   durably written. With no error return, a slot that silently stopped advancing was
   undetectable — the failure mode is unbounded WAL retention on the source primary, i.e. a
   disk-pressure outage, with no signal.
2. Nothing bounded the standby status write. `SendStandbyStatusUpdate` ends in
   `conn.Frontend().SendUnbufferedEncodedCopyData(buf)` — a socket write that can block on a
   full TCP send buffer — and it discards its context, so a caller's `context.WithTimeout`
   around the slot write had no effect whatsoever; the timeout was decorative. A blocked write
   stalls the ack coordinator, which under the new contract stalls slot advancement entirely.

**Fix**:
- `UpdateXLogPos(ctx context.Context, lsn pq.LSN) error` on `Connector`, `Streamer`, and
  `XLogUpdater`, plumbed through `*connector` → `*stream`. The T0-1 monotonic clamp is
  preserved unchanged.
- Two new sentinels, both meaning **"not sent"**, never "not stored" — the T0-1 monotonic store
  happens *before* either can be returned, so the in-memory position did advance:
  - `replication.ErrStreamClosed` — no usable connection (no stream yet, or conn nil/closed).
  - `replication.ErrStandbyWriteInFlight` — a previous standby write is still blocked.

  Callers must skip both with `errors.Is` rather than treating them as failed advances, and
  must not count them in a slot-advance error metric. This matters at the two **seed** call
  sites (`connector.go` `StartLSN`, and the source's checkpoint seed): both deliberately run
  *before* the stream connects, so they return `ErrStreamClosed` on every single startup. Only
  the pointless pre-connect network send was skipped; logging it as a failure would be a lie
  emitted on every process start.
- `SendStandbyStatusUpdate` is left **unchanged from upstream** (context still ignored).
  Bounding happens one level up, in `stream.UpdateXLogPos`, which runs the write on its own
  goroutine and selects on the caller's context. The caller therefore gets a bounded *wait*;
  the write itself is not cancelled and completes or fails whenever the socket drains. A new
  capacity-1 semaphore (`stream.standbySem`) serialises **`UpdateXLogPos`-issued** writes, so a
  slow write plus a caller retrying on its next tick cannot interleave protocol frames with
  itself. When the semaphore is already held, `UpdateXLogPos` returns `ErrStandbyWriteInFlight`
  and starts no second write.

  ⚠ The semaphore does **not** make frame interleaving structurally impossible, and must not be
  read that way. Three sites call `SendStandbyStatusUpdate` **directly**, outside it:
  `stream.go` `sinkLoop` receive-timeout, `handleKeepalive`'s `ReplyRequested` branch, and the
  `process` Ack closure. Under `ManualCommit` the `sinkLoop` site fires roughly every 300ms once
  `LoadXLogPos() > 0`, so it *will* overlap with a blocked `UpdateXLogPos` goroutine. That is
  **pre-existing upstream concurrency on the frontend, not a regression introduced here** — but
  do not delete this guard on the belief that overlap is already impossible.

  **A socket write deadline was tried first and rejected as unsafe** — do not reintroduce it.
  `pq.Connection` does not expose the socket, but the concrete type embeds `*pgconn.PgConn`
  which promotes `Conn() net.Conn`, so it is reachable. It nevertheless does not work here:
  1. `pgconn` installs a `DeadlineContextWatcherHandler` by default, and `sinkLoop` calls
     `ReceiveMessage` with a 300ms deadline every iteration forever, *expecting* it to expire.
     Each expiry runs `SetDeadline(now)` then `SetDeadline(zero)`, clearing a write deadline
     set from another goroutine — defeating the bound in precisely the blocked-write case it
     was meant to cover.
  2. Symmetrically, clearing our deadline afterwards stomps a deadline pgx set for its own
     in-flight cancellation (`Exec`, `Close`, `asyncClose`).
  3. Worst, a write deadline firing mid-frame inside `SendUnbufferedEncodedCopyData` leaves a
     **truncated CopyData frame** on the wire. That path bypasses `PgConn`'s locking and status
     machinery, so nothing marks the connection broken and the next update writes a fresh frame
     onto a corrupted stream — a corruption mode upstream did not have.
- Internal call sites log rather than propagate, deliberately, because none of them should tear
  down replication on a transient write error: the keepalive advance (legacy path) retries on
  the next keepalive; the undecodable-message advance (legacy path) is already non-fatal and
  its enclosing `handleXLogData` returns nothing; the legacy `Ack` closure keeps its upstream
  contract of returning the explicit send's result. All three skip logging `ErrStreamClosed`.
  The `StartLSN` seed logs and continues, since replication correctly falls back to the slot's
  `confirmed_flush_lsn`.
- `handleXLogData` has no context in scope and its call site runs **only** with `ManualCommit`
  off, so it passes `context.Background()`. This is deliberate: threading a bounded context
  there would introduce a bounded wait — i.e. the caller abandoning the write and reporting an
  error — where upstream always blocked to completion.

**Backward Compatibility**: source-incompatible for any external caller of these three
interfaces (this repo is the only consumer).

**Runtime** behavior with `ManualCommit == false` is *not* byte-for-byte upstream — unlike T0-1,
which was. The legacy-mode deltas, all of which flow from bounding the write:

1. **Every `UpdateXLogPos` now spawns a goroutine per write.** All three legacy call sites
   (`handleKeepalive`'s fast-forward, `handleXLogData`'s undecodable path, and the `process` Ack
   closure) go through it, where upstream wrote inline on the calling goroutine.
2. **A legacy caller can now receive `ctx.Err()`** where upstream blocked to completion. In
   practice this surfaces during shutdown as a `keepalive xlog position update failed` or
   `ack xlog position update failed` warning, because the call sites skip only `ErrStreamClosed`,
   not `context.Canceled`. Non-blocking noise; adding `context.Canceled` to those filters is a
   reasonable follow-up.
3. **`ErrStandbyWriteInFlight` can be returned in legacy mode**, skipping a keepalive-driven
   send that upstream would have performed. Harmless — the next keepalive retries — but it is a
   behavioral difference.
4. Errors are now logged where they were previously discarded entirely.

The redundant double-send in the legacy `Ack` closure (`UpdateXLogPos` sends, then `Ack` sends
again) is pre-existing upstream behavior and is retained; that branch is unreachable under
`ManualCommit`.

**Test coverage**: the semaphore, the abandonment path, and the `ErrStandbyWriteInFlight` branch
are **not unit-tested** — they are reachable only under a blocked socket, and the vendored module
has no test harness (tracked separately). They were verified by inspection only. Simulating a
wedged socket belongs with the e2e invariant suite. Operationally, repeated
`ErrStandbyWriteInFlight` is the signal that a standby write is wedged — worth an alert.

---

## Applying Patches

When merging upstream changes, search for `// vendored-patch:` markers to identify patched locations.

Example:
```bash
grep -r "vendored-patch:" internal/vendor/go-pq-cdc/
```

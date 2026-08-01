# Vendored Patches for go-pq-cdc

This document tracks local divergences from the upstream `go-pq-cdc` library.

---

## T1-4: Eager Connection Allocation in `Snapshotter`

**Upstream Issue**: TBD

**Files Modified**:
- `pq/snapshot/snapshot.go`
- `connector.go`

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
- `connector.go`

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

## MS-1: `search_path` Pinning on the Regular (Non-Replication) Connection

**Upstream Issue**: TBD (MULTI_SCHEMA_PLAN.md §3 Stage 2, §8 resolved decision 10)

**Files Modified**:
- `config/config.go`

**Problem**: The embedding application (`internal/source/postgres`) fully qualifies every
identifier it emits (see MULTI_SCHEMA_PLAN.md §2), but this vendored library itself issues several
unqualified queries against `cfg.DSN()` connections (most notably `pq/snapshot/coordinator.go`'s
`cdc_snapshot_job`/`cdc_snapshot_chunks` bookkeeping tables), which resolve against whatever
`search_path` the connection happens to have -- always `public` by default, regardless of which
schema(s) the embedder configured.

**Fix**: Added `Config.SearchPath string`. When non-empty, `DSN()` and `DSNWithoutSSL()` append a
`options=-c search_path=<value>` libpq startup parameter, pinning the connection's `search_path`
for its entire lifetime -- including across reconnects, since every reconnect re-derives the DSN
string from the same `Config.SearchPath` value rather than issuing a one-time `SET` command.
`ReplicationDSN()` is deliberately left unpinned: that connection opens with
`replication=database` and only ever speaks the logical replication protocol (never arbitrary
SQL), so `search_path` has no unqualified identifier to resolve there.

**Known consequence, FIXED by MS-2**: once `SearchPath` names a non-public schema first, the
coordinator's formerly-hardcoded `'public'` existence checks became actively wrong --
`CREATE TABLE cdc_snapshot_job` (unqualified) landed in the pinned schema while the checks still
looked in `public`, so `initTables` never found the table it had just created and re-ran
`CREATE TABLE` (erroring) on every restart. **MS-2 resolves this**; see that section. This
paragraph is retained because MS-1 alone reintroduces the bug if MS-2 is not replayed with it.

**Backward Compatibility**: `SearchPath` defaults to `""`, in which case `DSN()`/`DSNWithoutSSL()`
are byte-identical to before this patch and `ReplicationDSN()` is never touched.

---

## MS-2: Schema-Aware Snapshot Bookkeeping (`cdc_snapshot_*`)

**Upstream Issue**: TBD (MULTI_SCHEMA_PLAN.md §3 Stage 4, §11.2 requirement 7)

**MUST be replayed together with MS-1.** MS-1 pins `search_path`; without MS-2 that pinning
actively breaks snapshotting (see MS-1's "Known consequence").

**Files Modified**:
- `pq/snapshot/snapshot.go`
- `pq/snapshot/coordinator.go`
- `connector.go`

**Problem**: the snapshot coordinator created its `cdc_snapshot_job` / `cdc_snapshot_chunks`
bookkeeping tables with *unqualified* DDL -- so they landed in whatever schema `search_path`
resolved to -- while `tableExists`/`indexExists` hardcoded `table_schema = 'public'` /
`schemaname = 'public'`. Create and check therefore consulted different schemas as soon as MS-1
pinned a non-public `search_path`, and `initTables` re-ran `CREATE TABLE` on every restart. Both
helpers also interpolated the table name directly into the SQL string.

**Fix**: `Snapshotter` gained a `metadataSchema` field, resolved once in `New()` by
`resolveMetadataSchema()` (the first comma-separated entry of `SearchPath`, mirroring how Postgres
itself resolves an unqualified `CREATE`). `initTables` builds `qualifiedJobTable`/
`qualifiedChunksTable` from that single field and passes the same field to `tableExists`/
`indexExists`, which now take an explicit `schema` parameter. Create and check cannot drift,
because there is no second source of the schema name. Identifiers are quoted via
`libpq.QuoteLiteral` as defence in depth. `connector.go` threads `cfg.SearchPath` into `New()`.

**Known residual (benign, documented deliberately)**: DML in `worker.go`, `job.go` and
`coordinator.go` (and `migrateSchema`'s `ALTER TABLE`) remains unqualified and resolves via the
pinned `search_path`. Because create and check agree on `metadataSchema`, this is consistent. It
would only matter against a database that already holds `cdc_snapshot_*` tables in a *later*
`search_path` entry.

**Backward Compatibility**: with `SearchPath == ""`, `resolveMetadataSchema()` yields `public` and
every query is equivalent to the pre-patch hardcoded form.

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

> ⚠ **This claim is superseded for the flag-off path by T0-2's "Runtime" section below.** T0-2
> rewrote `UpdateXLogPos` into a goroutine+semaphore form that **all three legacy
> (`ManualCommit == false`) call sites** traverse -- the keepalive fast-forward, the undecodable-
> message path, and the `Ack` closure -- so "byte-for-byte upstream except the two guards above"
> was only ever true as of T0-1 landing, in isolation. Read T0-2's "Runtime" section (its four
> numbered legacy-mode deltas) before concluding the flag-off path is inert; do not skip
> re-testing it after a re-sync on the strength of this paragraph alone.

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
   practice this could surface during shutdown as a `keepalive xlog position update failed`,
   `xlog position update failed for undecodable message`, or `ack xlog position update failed`
   warning. **Fixed**: all three internal `UpdateXLogPos` call sites in `stream.go` (the
   keepalive fast-forward, the undecodable-message path, and the `process` Ack closure) now also
   skip `context.Canceled` via `goerrors.Is(err, context.Canceled)`, marked
   `// vendored-patch: T0-2`, alongside the pre-existing `ErrStreamClosed` skip. This was
   previously tracked here as an open follow-up; it is no longer open.
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

## T0-3: Deliver Keepalive-Driven WAL-End In Band, Not Inline On the Sink Goroutine

**Upstream Issue**: N/A (internal requirement, tracked internally as plan `01a_delivery_source_ack.md`,
follow-up to T0-1/T0-2 -- a shipping-blocker data-loss bug found in the T0-1 `ManualCommit` design)

**Files Modified**:
- `pq/replication/stream.go` (`Message`/new `keepaliveMarker` type, `handleKeepalive` signature
  and body, `sinkLoop`'s call site, `process()`)

**Problem**: Under `ManualCommit` (T0-1), `handleKeepalive` called `Config.KeepaliveFunc(pkm.ServerWALEnd)`
directly, inline, on the `sink` goroutine. But `sink` is also the sole producer onto `messageCH`
(the queue `process` drains to invoke `listenerFunc`, which is what actually calls the embedder's
`Observe`), and `messageBuffer` additionally holds back the last DML message of each transaction
in a one-message look-ahead (`buf.pending`) until the transaction's commit LSN is known. Calling
`KeepaliveFunc` inline therefore raced ahead of both: a decoded message could be sitting in
`messageCH`, or worse still unflushed in `buf.pending`, at the exact moment a keepalive fired, and
the embedder's ack-tracking (which only learns about an LSN once it reaches `Observe`, itself only
reachable via `listenerFunc`) had no way to know. On a fresh start where WAL already contains a
replay backlog between the slot's start LSN and the primary's current WAL end, the *first*
keepalive typically carries a `ServerWALEnd` already past every buffered commit -- so the
embedder's "nothing pending, safe to fast-forward" watermark logic (`AckManager.IdleAdvance`)
fast-forwarded past the **entire backlog** before a single row of it had been `Observe()`'d. Every
subsequent `Observe` call for that backlog then landed below the now-advanced watermark and was
silently dropped -- a replication-slot-confirmed, application-never-saw-it data loss with no
error, no failed test, and a healthy-looking `PendingCount() == 0` throughout. See
`internal/test/e2e/strict_ack_test.go`'s `TestKeepaliveDoesNotConfirmInflight` for the repro this
patch fixes, and `internal/source/postgres/ack.go`'s `AckManager.IdleAdvance` for the
defence-in-depth added on the application side (`highestSeen`/`idleTrusted`) in case this ordering
guarantee ever regresses again.

**Fix** (guarded by `Config.ManualCommit`; flag off is untouched):
- Added an unexported `keepaliveMarker struct{}` payload type. A `*Message{message: &keepaliveMarker{},
  walStart: int64(pkm.ServerWALEnd)}` is enqueued onto `s.messageCH` in place of calling
  `KeepaliveFunc` directly.
- `handleKeepalive` gained a `buf *messageBuffer` parameter (threaded from `sinkLoop`, which
  already owns `buf`). Before enqueuing the marker it calls `buf.flush()` -- the same method used
  at `STREAM STOP` boundaries -- so any DML message being held in the one-message look-ahead is
  pushed onto `messageCH` *ahead of* the marker, not left behind it. `sink` and `handleKeepalive`
  both run exclusively on the sink goroutine, and `messageCH` is a single-consumer FIFO read only
  by `process`, so everything decoded before this keepalive was received is guaranteed to already
  be enqueued ahead of the marker once this call returns.
- `process()` type-switches on `msg.message`: a `*keepaliveMarker` calls `Config.KeepaliveFunc`
  with the marker's `walStart` and `continue`s immediately, **without** building a
  `ListenerContext` or invoking `listenerFunc` -- a marker must never reach the application
  handler.
- Full-channel handling: `messageCH` (capacity 1000) is written to with a non-blocking `select`
  (`case s.messageCH <- marker: / default:`). If full, the marker is **dropped**, not blocked on.
  This is a deliberate asymmetric choice: `sink` is also the only reader of the replication
  socket, so blocking it risks a `wal_receiver_timeout` disconnect -- strictly worse than skipping
  one idle-advance opportunity. A dropped marker only delays that particular `IdleAdvance` call;
  the next keepalive (they arrive on a steady timer) retries once the backlog has drained under
  `process`'s own pace. Silently *losing* an idle-advance is always safe -- the failure mode this
  patch exists to close is the opposite (advancing too eagerly) -- so erring toward "advance
  later" here is intentional, not an oversight. Logged at `Warn`.

  Note also that `buf.flush()` itself is not guaranteed non-blocking: if `messageCH` is full,
  `buf.flush()`'s own send onto it can block the sink goroutine. The "never block the sink"
  property this patch relies on for the marker (the drop-on-full `select`/`default` above) is
  therefore not absolute -- it holds for the marker itself, not for the flush that precedes it.

**Backward Compatibility**: `ManualCommit` defaults to `false`; with it unset, `handleKeepalive`'s
legacy branch (`UpdateXLogPos`, unchanged) is untouched, `keepaliveMarker` values are never
constructed, and `process()`'s new type-switch branch is simply never taken. This patch is
**behaviour-changing under `ManualCommit == true` only**: a keepalive-driven WAL-end update no
longer reaches `KeepaliveFunc` synchronously with the keepalive's arrival -- it is now delayed
until every previously decoded message has drained through `process`/`listenerFunc`. For an
embedder using `KeepaliveFunc` to drive idle-advance logic (as this repo's `AckManager.IdleAdvance`
does), that delay is the entire point: it is what makes "nothing pending" a sound statement about
stream order.

---

## Re-sync Risk Callouts

Two changes across T0-1..T0-3 are silent-break risks: a future re-sync can drop or reorder
either one, the vendored module will still compile, the flag-off (`ManualCommit == false`) tests
will still pass, and nothing will fail loudly. General "re-apply the patches" advice is not
enough for these two; verify the exact expressions survive, not just that the surrounding
function still exists.

1. **T0-3's `buf.flush()`-before-marker-enqueue ordering**, in `handleKeepalive`. This single
   line-ordering fact is the entire correctness argument for T0-3: it is what guarantees every
   message decoded before a given keepalive is already enqueued ahead of that keepalive's
   `keepaliveMarker` on `messageCH`. Drop the `buf.flush()` call, or reorder it to *after* the
   marker is enqueued, and the confirmed-then-never-`Observe`d data-loss class T0-3 exists to
   close (see `AckManager.IdleAdvance` fast-forwarding past an undelivered replay backlog)
   silently returns -- with `pending_lsns` reading a healthy 0 the entire time. This is exactly
   the failure mode `cdc_source_idle_advance_refused_total` (OPS-2, see
   `internal/source/postgres/ack.go`) exists to catch if it ever does regress, but the ordering
   itself must still be verified on every re-sync, not left to the canary.
2. **The always-on `&& s.LoadXLogPos() > 0` guard** on `handleKeepalive`'s `ReplyRequested`
   branch (T0-1). It is easy to mistake for flag-guarded (it lives inside the same function as
   several `ManualCommit`-gated branches) and drop or move it during a re-sync merge. It is not
   gated: losing it reintroduces replying to the primary with LSN 0 in both modes.

---

## Applying Patches

When merging upstream changes, search for `// vendored-patch:` markers to identify patched locations.

Example:
```bash
grep -r "vendored-patch:" internal/vendor/go-pq-cdc/
```

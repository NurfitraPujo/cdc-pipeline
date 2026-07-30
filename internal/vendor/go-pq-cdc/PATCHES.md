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

## Applying Patches

When merging upstream changes, search for `// vendored-patch:` markers to identify patched locations.

Example:
```bash
grep -r "vendored-patch:" internal/vendor/go-pq-cdc/
```

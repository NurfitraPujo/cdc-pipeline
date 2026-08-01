---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/vendor/go-pq-cdc/PATCHES.md T0-1/T0-2/T0-3
---

# The embedder owns replication-slot advancement (`ManualCommit`)

## Context and Problem Statement

`go-pq-cdc` advanced the replication slot **the instant an event was handed to the application**,
and independently fast-forwarded it on every keepalive and on undecodable WAL messages — all before
the embedder could gate on downstream durability (`PATCHES.md:144-149`).

That is at-most-once: the slot is confirmed, the sink has not written, and a crash loses the row
with no error.

## Decision Drivers

* [0008](0008-at-least-once-with-sink-side-idempotency.md) requires the slot never to pass an LSN
  no sink has durably written.
* [0009](0009-replication-slot-is-the-resume-authority.md) makes the slot the resume authority, so
  the invariant must hold or resume is wrong.
* A slot that silently stops advancing causes unbounded WAL retention — a disk-pressure outage on
  the source primary.

## Decision Outcome

Chosen: patch the vendored library to transfer position ownership to the embedder.

* **T0-1** adds `ManualCommit`. `lc.Ack` returns `nil` without touching the slot; ownership moves
  to the embedder calling `UpdateXLogPos` directly. Two always-on fixes ship with it: never report
  LSN 0 to the primary, and clamp the reported position monotonically.
* **T0-2** widens `UpdateXLogPos` to `(ctx, lsn) error` — **API-breaking across three exported
  interfaces, deliberately**, so a partial re-apply fails to compile ("loud, not silent"). Without
  an error return, "a slot that silently stopped advancing was undetectable"
  (`PATCHES.md:214-219`); without context propagation "the timeout was decorative"
  (`PATCHES.md:220-224`).
* **T0-3** enqueues keepalives **in band** on `messageCH`, flushing held-back DML first.

`runAckCoordinator` is the only call site permitted to advance the slot
(`internal/source/postgres/source.go:1050-1062`), and its ticker "is a KEEPALIVE ONLY: it must
never auto-advance the watermark on its own".

### Consequences

* Good: the slot cannot outrun sink durability, so at-least-once holds end to end.
* Bad: **a dead or slow sink freezes `confirmed_flush_lsn` and WAL accumulates on the primary.**
  This is the correct trade — loss becomes visible backpressure — but
  `internal/source/postgres/source.go:68-73` is explicit that it "is only safe to operate with an
  alert on this metric". Treat `cdc_source_slot_lag` alerting as a hard operational prerequisite,
  not a nice-to-have.
* Bad: eight patches of divergence, with T0-2 API-breaking. See [0007](0007-go-pq-cdc-stays-an-in-tree-fork.md).
* Bad: T0-2's semaphore does not make frame interleaving structurally impossible — three call sites
  send standby status outside it. This is why the coordinator must stand down entirely when
  `CDC_STRICT_ACK` is off (`source.go:1069-1078`).

## More Information

**Why T0-3 exists, and why it is the subtlest of the three.** `handleKeepalive` ran inline on the
`sink` goroutine, which is also the only producer onto `messageCH`, and the message buffer holds
back the last DML of each transaction. So a keepalive could overtake buffered rows. On a fresh
start with a replay backlog, the first keepalive's `ServerWALEnd` was already past every buffered
commit, so `IdleAdvance` fast-forwarded **past the entire backlog** before a single row was
observed — "a replication-slot-confirmed, application-never-saw-it data loss with no error, no
failed test, and a healthy-looking `PendingCount() == 0` throughout" (`PATCHES.md:337-346`).

The correctness argument rests on one line ordering: `buf.flush()` **before** enqueuing the marker.
`TestKeepaliveDoesNotConfirmInflight` is its regression guard.

**Rejected, with a do-not-reintroduce marker:** a socket write deadline on the standby update.
A deadline firing mid-frame leaves a truncated CopyData frame on the wire, nothing marks the
connection broken, and the next update writes onto a corrupted stream — "a corruption mode upstream
did not have" (`PATCHES.md:258-271`).

**`CDC_STRICT_ACK`** is an env var, not a `SourceConfig` field, deliberately: config is msgp-persisted
in KV, and "turning a temporary, release-scoped rollout switch into persisted deploy state would
survive past the flag's own removal" (`source.go:124-134`). OFF is the legacy path and "re-opens the
data-loss window — an availability escape hatch, not a correctness one".

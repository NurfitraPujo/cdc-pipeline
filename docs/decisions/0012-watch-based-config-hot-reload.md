---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/config/manager.go:162-566, rfc/RFC-001-Architecture-and-Design.md §3.2
---

# Config changes propagate by KV watch, applied via a two-phase drain-then-shutdown

## Context and Problem Statement

Changing a pipeline's config while it is replicating must not lose or duplicate rows, and must not
require the API to hold connections to running workers.

## Decision Drivers

* The API is stateless ([0011](0011-nats-as-the-only-control-plane-datastore.md)); it cannot call a
  worker directly.
* Restarting a worker bounces its replication slot and JetStream consumers, which is disruptive to
  tables that were not reconfigured.
* A reload must be distinguishable from a crash, or the supervisor will fight it.

## Decision Outcome

**KV watch is the control channel.** `ConfigManager.Watch` primes global config, then watches
`cdc.config.global` and `cdc.config.pipelines.*` (`manager.go:162-212`). No polling, no RPC.

**Reload is two-phase**, in `transitionWorker` (`manager.go:499-566`):

1. Write `PipelineTransitionState{Status:"Transitioning"}` — the interlock that stops the
   supervisor restarting an intentionally stopped worker (`manager.go:504`). A `defer` clears it on
   every exit path.
2. **Drain** — a *data-integrity* boundary. Stop accepting new work, flush in flight, emit a
   `drain_marker` so consumers know where the stream ends. Bounded by `DrainTimeout`.
3. **Shutdown** — a *resource* boundary. Release the replication slot and consumer bindings before
   the replacement tries to claim them. Bounded by `ShutdownTimeout`.
4. `StabilizationDelay`, then start the new worker.

Timeouts are snapshotted at transition start so a concurrent global-config change cannot move the
goalposts mid-transition (`manager.go:521-523`).

**Additive table changes skip the restart entirely.** `onlyTablesChanged` (`manager.go:404-435`)
blanks `Tables` on both configs and compares the remainder; if everything else is equal and the new
list is strictly longer, it signals the running worker instead of transitioning it. Adding a table
must not interrupt replication of tables already streaming.

### Consequences

* Good: config propagates to every worker with no API→worker coupling.
* Good: a slow sink cannot wedge a reload forever — each phase is independently bounded.
* Bad: **drain timeout expiry proceeds to shutdown anyway** — availability chosen over completeness.
  This is safe rather than lossy: undrained messages stay in JetStream under a durable name that
  survives restart, so the next worker resumes them.
* Bad: `onlyTablesChanged` is **additive-only**. Removing a table falls through to a full
  drain/shutdown/restart. Intentional, but easy to misread as a general fast path.
* Bad: watcher semantics leak into the manager — a nil entry is an initial-emit sentinel, not a
  close, and both cases must be handled to avoid a CPU spin (`manager.go:225-230`).

## More Information

The supervisor treats "worker finished with no transition marker" as a crash, and treats an
unreachable NATS as a crash too (`manager.go:831-834`) — a fail-safe bias toward restarting.
Backoff is `CrashRecoveryDelay * 2^(attempt-1)`, capped at 60s, jittered, then **re-clamped**
because "capping before jitter alone let a capped delay return up to 66s" (`manager.go:637-639`).
The attempt counter is clamped at 15 because "at attempt=63, a signed int shift overflows to a
negative number, causing a tight crash loop" (`manager.go:600-604`). A worker surviving 10s resets
the counter, distinguishing a one-off crash from a crash loop.

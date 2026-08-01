---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: docs/requirements/postgres-debug-sink.md:36-61, internal/engine/factory.go:85-159
---

# Each sink gets its own consumer, durable subscription, checkpoint and DLQ

## Context and Problem Statement

A pipeline may write to several sinks (e.g. Databend for analytics and a Postgres debug sink for
lineage). Either one consumer reads the stream and writes to all sinks, or each sink gets its own
consumer on the same stream.

## Decision Outcome

Chosen: **per-sink consumers** — one shared ingest stream, N distinct JetStream durables, so every
sink sees every message and holds its own cursor (`internal/engine/factory.go:152-159`). Fan-out,
not competing consumers.

The tradeoff table is recorded verbatim in `docs/requirements/postgres-debug-sink.md:36-42`:

| Aspect | Single consumer (fan-out in process) | Per-sink consumers |
|---|---|---|
| Failure isolation | One sink failure blocks all | Each sink isolated |
| Retry handling | Shared retry state | Own retry per sink |
| Checkpoints | Shared egress LSN | Per-sink egress LSN |
| Backpressure | Slow sink blocks all | Slow sink affects only itself |
| NATS ack | Single ack point | Independent ack per consumer |

### Consequences

* Good: a stalled sink accumulates its own backlog up to its own `MaxAckPending`, and cannot nack
  on behalf of a healthy sink, roll back a shared checkpoint, or consume another sink's retry budget.
* Good: the debug sink gets natural before/after capture points without hooking a shared consumer.
* Bad: **the ack set becomes multi-party, and the sink ID is load-bearing.** `c.sinkID` must match
  `p.config.Sinks` string-for-string — "a mismatch here can never be satisfied and permanently
  freezes the replication slot" (`internal/engine/producer.go:200-208`).
* Bad: resume must distinguish min-over-tables from min-over-sinks
  ([0009](0009-replication-slot-is-the-resume-authority.md)), and a newly added sink has an
  *unknown*, not zero, frontier — so the invariant check is skipped rather than judged against an
  assumed zero (`pipeline.go:213-216`).
* Bad: N sinks means N copies of every message in flight.

## More Information

**Poison-pill handling is per sink.** After `MaxRetries` on a batch, the consumer enters *isolation
mode*: it abandons batching and replays each JetStream message individually until the failing one
is identified (`consumer.go:903-967`).

Routing to the DLQ **must** still emit a `RecordAck`, because it is a terminal durability decision —
"the row will never be written by anyone — so the source must be told the LSN will never need
replaying, exactly as if it had been durably written" (`consumer.go:969-972`). Without that, a
moderately long transient outage would freeze the slot even though every row eventually landed.

**Fail-fast on producer death.** The producer cancels the shared pipeline context on any error, so
consumers exit and the supervisor restarts. Otherwise "consumers keep running on p.ctx forever,
wg.Wait() never returns, finished never closes, and the supervisor heartbeats 'Running' for a
pipeline that has stopped ingesting" (`pipeline.go:85-98`) — the zombie state. The *graceful* path
must not cancel: draining, not cancellation, is what stops consumers there (`pipeline.go:267-276`).

---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/source/postgres/source.go:906-914 (WI-7), internal/engine/pipeline.go:156-237
---

# The replication slot's `confirmed_flush_lsn` is the resume authority, not the KV checkpoint

## Context and Problem Statement

On restart the source must choose where to resume. There are two candidate positions: the
PostgreSQL replication slot's own `confirmed_flush_lsn`, or the LSN checkpoints the pipeline
persists in NATS KV.

Trusting KV looks natural — the pipeline wrote it and it is right there. It is also wrong, because
the two can disagree and only one of them is backed by an invariant.

## Decision Drivers

* The slot only advances after every configured sink has durably written the LSN (see
  [0010](0010-embedder-owns-slot-advancement.md)), so by construction it is at or behind every
  sink's durable position.
* A KV write is best-effort and can lag, fail, or be stale relative to the slot.
* Resuming *ahead* of a sink's durable position silently loses rows.

## Considered Options

1. **Slot is the authority; KV is a floor plus observability.**
2. **KV checkpoint is the authority**, seeding `StartLSN` on every start.
3. **Take the max of both.**

## Decision Outcome

Chosen: **option 1**. `cfg.StartLSN` is deliberately left at zero, which the vendored stream reads
as "start from `confirmed_flush_lsn`" (`internal/source/postgres/source.go:906-914`).

KV is still used, in two strictly bounded ways:

* `Hydrate(checkpoint.IngressLSN)` applies the KV watermark as a **floor**, so the first
  `UpdateXLogPos` can never regress below what KV already knows.
* `SourceWatermarkKey` is observability only — "not consulted on resume … exists purely so
  dashboards/operators can see current watermark progress" (`internal/protocol/config.go:117-122`).

`internal/engine/pipeline.go:197` states the rule for the other direction: "minLSN … is
observability/Hydrate input ONLY — it must never feed StartLSN".

### Consequences

* Good: the resume position is backed by an invariant rather than by a best-effort write.
* Good: `persistWatermark` can fail without affecting correctness.
* Bad: correctness now depends on the slot invariant actually holding. A slot over-advanced by
  older code cannot be detected from KV, so `warnIfSlotAheadOfSinkFrontier`
  (`internal/engine/pipeline.go:353-385`) exists to surface it.
* Bad: two different aggregations over egress checkpoints are needed and must not be confused —
  `minLSN` (MIN over all table/sink pairs, a deliberately lagging floor keyed on the least-active
  table) versus the **egress frontier** (MIN over sinks of each sink's MAX). Comparing the slot
  against `minLSN` "would fire on essentially every restart and print a false 'data loss' message"
  (`pipeline.go:200-212`).

## More Information

Two checkpoints exist because they mark different durability boundaries: **ingress** = published to
JetStream (written by the Producer, `internal/engine/producer.go:454-461`); **egress** = durably
written to a sink (written by the Consumer after `BatchUpload` succeeds,
`internal/engine/consumer.go:838-850`). Only egress feeds resume. Snapshot rows are excluded from
egress checkpoints — writing them "would poison the pipeline's resume floor … with a value that has
nothing to do with replication progress" (`consumer.go:834-837`).

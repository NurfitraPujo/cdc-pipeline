---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/source/postgres/source.go:182, internal/sink/provider.go:10, rfc/RFC-001-Architecture-and-Design.md
---

# At-least-once delivery, with idempotency delegated to the sink

## Context and Problem Statement

A CDC pipeline must choose what it guarantees when a process dies mid-batch: that no row is lost
(at-least-once, possible duplicates), or that every row lands exactly once (requiring distributed
coordination between the WAL reader and every sink).

## Decision Drivers

* Losing a row is unrecoverable without a re-snapshot; delivering one twice is recoverable if the
  write is idempotent.
* The primary sink (Databend) supports PK-keyed upsert natively.
* Exactly-once across N heterogeneous sinks needs a transactional outbox or a dedup ledger, and a
  durable store to hold it.

## Considered Options

1. **At-least-once, sinks must be idempotent.**
2. **Exactly-once** via an LSN-keyed dedup ledger.
3. **At-most-once** (advance the slot on read).

## Decision Outcome

Chosen: **option 1**, stated normatively on the source itself
(`internal/source/postgres/source.go:182`: "Delivery contract: at-least-once") and as a contract on
the sink interface (`internal/sink/provider.go:10`: "It should handle deduplication and ensures
idempotency").

Databend satisfies it with `REPLACE INTO … ON (pk)` (`internal/sink/databend/sink.go:795`), so a
replayed LSN rewrites the same row. Ack-side, `AckManager.Confirm` for an LSN at or below the
watermark is a no-op, making redelivered `RecordAck`s idempotent
(`internal/source/postgres/ack.go:291`).

### Consequences

* Good: a crash replays the unconfirmed batch rather than dropping it.
* Good: no dedup ledger, no extra datastore, no distributed transaction.
* Bad: **idempotency is only as good as the resolved primary key.** If PK resolution fails, the
  sink falls back to `["id"]` and `REPLACE INTO … ON ("id")` **merges distinct rows** — the failure
  mode degrades to *corruption*, not duplication. See [0001](0001-canonical-table-identity.md) for
  the identity bug that caused exactly this, and `MULTI_SCHEMA_PLAN.md` §12.4 for the open gap:
  Databend cannot express `PRIMARY KEY` in DDL, so a sink-created table cannot report its own PK.
* Bad: sinks without an upsert primitive cannot be added without solving dedup themselves.
* Neutral: snapshot rows are outside the LSN watermark entirely — their durability rests on
  JetStream plus chunk-job state (`internal/engine/consumer.go:571-578`).

## More Information

`docs/GRACEFUL_SHUTDOWN_INVESTIGATION.md` refers to "exactly-once delivery semantics on restart".
That phrasing describes the *observable* result of at-least-once plus an idempotent sink, not a
different guarantee. Prefer the wording in `source.go:182`.

Related: [0009](0009-replication-slot-is-the-resume-authority.md), [0010](0010-embedder-owns-slot-advancement.md).

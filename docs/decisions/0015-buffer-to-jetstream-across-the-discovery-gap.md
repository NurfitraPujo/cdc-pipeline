---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/engine/producer.go:1302-1619, docs/requirements/schema-evolution-plan.md:95
---

# Rows arriving during snapshot are buffered to a durable JetStream stream, not memory

## Context and Problem Statement

`CREATE TABLE` produces no CDC event. Between a table appearing and CDC becoming authoritative for
it there is a window — the "Gap of Uncertainty" — where neither source is complete: rows already
present were never in the WAL, and rows inserted immediately after creation are invisible until
`ALTER PUBLICATION ADD TABLE` lands.

## Decision Outcome

Three parts, sequenced by explicit per-table state:

1. **Add to publication, then snapshot.** `Snapshotting → Draining → CDC`, with the state persisted
   in KV so a restart resumes rather than restarts (`producer.go:1302-1388`).
2. **Paginated keyset snapshot.** `WHERE (pk) > ($1,…) ORDER BY pk LIMIT n`, with `LastPK`
   checkpointed per chunk so a crash resumes mid-table (`producer.go:1464-1509`). A table with no
   primary key is a hard error.
3. **Buffer concurrent CDC to a per-table JetStream stream**, then drain it into the ingest topic
   before flipping to `CDC` (`producer.go:554-567`, `:803-840`).

Buffering is **durable, not in-memory**: *"Using a local file WAL causes data loss if a Kubernetes
pod crashes while buffering. Buffering MUST be distributed"* (`schema-evolution-plan.md:95`).

### Consequences

* Good: a pod crash mid-snapshot loses nothing; `recoverEvoStates` restarts the drain.
* Good: the drainer uses a **stable, non-UUID durable name**, so an interrupted drain resumes the
  same durable "instead of a fresh UUID-named consumer silently stranding those buffered messages
  behind an abandoned durable" (`producer.go:715-722`).
* Bad: a per-table stream per in-flight table, which must be drained to empty before the table goes
  live.
* Bad: snapshot rows carry no meaningful LSN, so they are excluded from egress checkpoints — writing
  them "would poison the pipeline's resume floor with a value that has nothing to do with
  replication progress" (`consumer.go:834-837`).

## More Information

**Rejected: a client-side idle timeout to decide the buffer is empty.** *"JetStream redelivery lag
after a NATS restart or under load can easily exceed any reasonable fixed idle window, and treating
that as 'done' strands buffered rows behind the table's flip to CDC — silent data loss"*
(`producer.go:792-802`). Replaced by server-side truth: `PendingCount` counts **both** `NumPending`
and `NumAckPending`, because "treating NumPending==0 alone as 'empty' can declare a drain complete
while up to MaxAckPending messages are still in flight" (`internal/stream/nats/subscriber.go:130-142`).

**Lock discipline.** The verify loop must not hold `muTableStates`, since the producer's hot path
takes that read lock per message and "a sustained JetStream outage would hold that lock indefinitely
and deadlock the producer's main publish path" (`producer.go:842-853`). The residual race is
absorbed by a bounded retry; on exhaustion the table is deliberately left in `Draining` for a later
trigger rather than force-flipped.

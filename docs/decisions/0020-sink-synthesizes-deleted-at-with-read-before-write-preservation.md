---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers
consulted: internal/sink/databend/sink.go (deletedAtColumn, ApplySchema, uploadTableBatch, fetchCurrentDeletedAt), internal/sink/databend/sink_ws4_pk_durability_test.go, docs/decisions/0008-at-least-once-with-sink-side-idempotency.md
---

# The Databend sink synthesizes `deleted_at` on every synced table, and preserves it with a read-before-write on every upsert

## Context and Problem Statement

Soft delete ("WS-4: soft delete everywhere") requires every synced table to have a `deleted_at`
column `deleteTableBatch` can unconditionally `UPDATE ... SET deleted_at = ?`. Several real
satellite tables (e.g. `business_entity_addresses`, `business_entity_contacts`,
`visitation_contacts`, `business_entity_industry`) have no such column at the Postgres source —
so a source-schema-driven `ApplySchema` never declares it for those tables, and an unconditional
delete UPDATE against them fails with an unknown-column error, forever, on every redelivery
(0008's at-least-once contract means it *will* be redelivered).

Separately, once `deleted_at` exists on a table (synthesized or real), `uploadTableBatch`'s
`REPLACE INTO` derives its column list from the upsert payload's own keys. For a synthesized
column, no upsert payload from Postgres ever mentions `deleted_at` (it isn't a source column) — so
a `REPLACE INTO` naively built from the payload's keys omits it, and Databend's `REPLACE INTO`
semantics default any omitted column to NULL on the replace. A row soft-deleted, then later
receiving an at-least-once-redelivered or logically-superseded upsert for the same primary key,
would have its tombstone silently erased and reappear as live data — a correctness regression
worse than the missing-column crash it would otherwise be compared against, because it fails
silently instead of loudly.

## Decision Drivers

* A table missing `deleted_at` at the source must not make `deleteTableBatch` fail forever — that
  is an unrecoverable hot loop under the existing at-least-once redelivery contract (0008).
* A tombstoned row must stay tombstoned across every subsequent upsert for that primary key,
  including a redelivered or logically-stale one — resurrecting soft-deleted data is a correctness
  bug indistinguishable from data loss to anyone querying the sink.
* Whatever guarantees this must not require a schema change to the Postgres source (several
  satellite tables are out of this repo's control).

## Considered Options

For "no `deleted_at` at the source":
1. **Synthesize the column in `ApplySchema`** on every table, regardless of whether the source
   schema declares it.
2. **Require the source to add the column**, and treat its absence as a hard configuration error.
3. **Make `deleteTableBatch` conditional** — skip the soft-delete UPDATE entirely for a table
   without the column, effectively disabling soft delete for satellites.

For "a synthesized column gets nulled by a later upsert":
1. **Read-before-write preservation**: before an upsert whose payload omits `deleted_at`, fetch the
   row's current `deleted_at` from Databend and carry it forward explicitly in the write.
2. **Switch soft-deleted tables to partial `UPDATE`** instead of `REPLACE INTO` for every upsert,
   so an omitted column is never touched regardless of reason.
3. **Do nothing** and accept that a redelivered/superseded upsert can resurrect a tombstone,
   documenting it as a known limitation.

## Decision Outcome

Chosen: **option 1** for synthesis, **option 1** (read-before-write) for preservation.

`ApplySchema` (`internal/sink/databend/sink.go`) always adds a `deleted_at TIMESTAMP` column on
table creation and as an ALTER-TABLE backstop for a pre-existing table that lacks it (guarded
against double-ALTER within the same call — `existingCols` is a snapshot taken once, so a table
whose Databend columns lack `deleted_at` but whose `schema.Columns` *does* declare it, i.e. the
source later gains the column, would otherwise get it ALTERed twice in one `ApplySchema` call, and
Databend has no `ADD COLUMN IF NOT EXISTS`). This makes `deleteTableBatch`'s unconditional UPDATE
always land on a real column, on every table, with no dependency on the source schema declaring it.

`uploadTableBatch` fetches (`fetchCurrentDeletedAt`, batched and chunked against the placeholder
budget) the current `deleted_at` for every row in the batch whose payload omits the key, and
carries the fetched value forward explicitly into the write — so the `REPLACE INTO` column list
always includes `deleted_at` with either the payload's own value (a real source column) or the
preserved current value (a synthesized column, or a payload that simply didn't touch it), never a
silent default. A pk with no existing row (a genuine first insert) has nothing to preserve and
correctly comes back with no tombstone.

Option 2 (require the source to add the column) was rejected: several satellite tables are outside
this repo's control, and making the pipeline's correctness depend on an upstream schema change
this repo cannot enforce is not viable.

Option 3 for synthesis (disable soft delete for satellites) was rejected: it means data deleted at
the source stays visibly "live" in the sink indefinitely for exactly the tables where the source
schema happens not to have a delete-tracking column — an inconsistency a consumer of the sink has
no way to discover without already knowing which tables are affected.

Option 2 for preservation (always partial `UPDATE`, never `REPLACE INTO`, on soft-deleted tables)
was rejected as unnecessarily broad: it changes the write strategy for every column on every
upsert against every soft-delete-bearing table (i.e. all of them), not just the one synthesized
column that needs protecting, for a correctness property `fetchCurrentDeletedAt`'s narrower
read-before-write already provides. (WS-7's later, structurally similar TOAST-preservation problem
reused this same read-before-write pattern rather than adopting a blanket partial-UPDATE strategy,
for the same reason — see `fetchCurrentColumns` in `internal/sink/databend/sink.go`.)

Option 3 for preservation (accept the resurrection bug) was rejected outright: it is silent data
corruption with no operator-visible signal, the worst class of failure this pipeline can produce.

### Consequences

* Good: `deleteTableBatch` never fails on a missing `deleted_at`, on any table, regardless of
  source schema.
* Good: a tombstone survives every subsequent upsert for its primary key, including redelivered or
  logically-superseded ones.
* Bad: every upsert whose payload omits `deleted_at` costs an extra batched `SELECT` round trip
  before the write — `fetchCurrentDeletedAt`'s failure mode is non-fatal-but-lossy (logs + a
  `cdc_sink_deleted_at_preservation_failures_total` counter, then proceeds without preservation for
  that one flush) rather than blocking the batch, so a sustained read-path degradation quietly
  reopens the resurrection window for the duration of the degradation — bounded and observable via
  the counter, but not eliminated.
* Bad: `deleted_at` is a Databend-side-only column with no corresponding source-of-truth column for
  satellite tables; querying the sink for "when was this deleted" on those tables answers a
  question the source database itself cannot answer.

## More Information

`internal/sink/databend/sink_ws4_pk_durability_test.go`'s
`TestWS4_UploadTableBatch_PreservesTombstoneAcrossUpsert` and
`TestBatchUpload_IntraBatchDeleteAndSupersededUpsert_DeleteWins` cover the read-before-write
preservation end to end, including the intra-batch race between an upsert and a delete for the
same primary key in a single flush (serialized per-ref in `BatchUpload`, not run as independent
goroutines, specifically to keep this deterministic). Related: [0008](0008-at-least-once-with-sink-side-idempotency.md)
(the at-least-once contract that makes redelivery — and therefore this bug — reachable at all).

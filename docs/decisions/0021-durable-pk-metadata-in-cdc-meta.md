---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers
consulted: internal/sink/databend/sink.go (pkCache, persistPKMetadata, refreshPrimaryKey, ensurePrimaryKey), internal/sink/databend/sink_ws4_pk_durability_test.go, docs/decisions/0004-postgres-schema-maps-to-databend-database.md
---

# A table's resolved primary key is persisted durably in `cdc_meta.pk_columns`, not trusted to survive only in-memory or in Databend's own `SHOW CREATE TABLE`

## Context and Problem Statement

`uploadTableBatch`/`deleteTableBatch` need a table's primary key columns to build a correct
`REPLACE INTO ... ON (pk)` / `WHERE pk IN (...)`. The key normally arrives via a `schema_change`
message's `PKColumns` and is cached in-memory (`pkCache`). That cache is process-local: it does not
survive a sink restart. Before this fix, a restarted sink with no `schema_change` yet replayed
resolved a missing PK by calling Databend's `SHOW CREATE TABLE` — which never emits a `PRIMARY KEY`
clause, because this sink's own `ApplySchema` never declares one (Databend's `REPLACE INTO`
doesn't require a declared PK, it takes the key list positionally in the `ON (...)` clause) — and,
finding nothing, fell back to a hardcoded default of `["id"]`.

For a table actually keyed on `id`, that fallback is silently correct by coincidence. For a
`custom_objects` sidecar table keyed on `record_id` (not `id`), it is silently wrong:
`REPLACE INTO ... ON ("id")` deduplicates against the wrong column, so every update after a restart
inserts a *new* row instead of replacing the existing one — unbounded row duplication with no error
anywhere in the path, discoverable only by a row count that keeps growing.

## Decision Drivers

* A sink process restart must not be able to silently corrupt a table's dedup key — a fallback that
  is sometimes right and sometimes silently wrong by table is worse than a loud failure, because it
  looks correct until someone notices row counts are off.
* Databend's own catalog (`SHOW CREATE TABLE` / `information_schema`) cannot be the source of truth
  for the PK, because this sink deliberately never declares one there (see "More Information").
* Re-deriving the PK requires a `schema_change` replay, which is not guaranteed to happen before the
  first post-restart write — JetStream redelivery order is not schema-message-first by construction.

## Considered Options

1. **Persist the resolved PK durably** in a dedicated Databend metadata table
   (`cdc_meta.pk_columns`), read back on first use after a restart, authoritative over any
   `SHOW CREATE TABLE` guess.
2. **Make `ApplySchema` declare an actual Databend `PRIMARY KEY`** on table creation, so
   `SHOW CREATE TABLE` becomes a truthful source of the key after a restart.
3. **Keep the `["id"]` fallback**, and document that every table needing a different key must be
   configured with it explicitly out of band.
4. **Treat a missing PK after restart as a hard error**, refusing to write until a fresh
   `schema_change` is observed.

## Decision Outcome

Chosen: **option 1**. `persistPKMetadata` (`internal/sink/databend/sink.go`) writes a table's
resolved PK column list to a `cdc_meta.pk_columns` table keyed by the table's qualified reference,
alongside setting the in-memory `pkCache`/`pkLoaded` on every `ApplySchema` call that carries a
non-empty `PKColumns` (best-effort: a persistence failure is logged loudly but does not fail the
whole `ApplySchema` call, since the in-memory cache already makes *this* process instance correct —
persistence is what makes the *next* instance correct). `refreshPrimaryKey`'s post-restart
resolution path reads `cdc_meta.pk_columns` first and treats it as authoritative over whatever
(nothing, in practice) `SHOW CREATE TABLE` reports. For a `custom_objects` table specifically, a
restart that finds neither a persisted PK nor a fresh `schema_change` is a **hard error** rather
than a fallback to `["id"]` — refusing to write and forcing a loud retry/DLQ is preferable to
silently duplicating rows for a table known to key on something other than `id`. Non-custom-objects
tables keep the `["id"]` fallback, since it is correct for the large majority of this pipeline's
actual tables and a hard error there would be a regression with no compensating safety benefit.

Option 2 (declare a real Databend PK) was rejected: Databend's `REPLACE INTO ... ON (...)` does not
require a declared `PRIMARY KEY` to function, and introducing one would be a second, redundant
source of truth for exactly the same information now durably tracked in `cdc_meta.pk_columns` —
two places that could drift relative to each other, which is the class of bug 0006 and 0018 both
exist to avoid elsewhere in this codebase.

Option 3 (keep the coincidentally-correct fallback) was rejected: it is exactly the bug being
fixed, retained as documentation rather than closed — silent, table-dependent correctness that
depends on operators independently knowing which tables are exceptions.

Option 4 (hard error universally) was rejected as too broad: it would turn a restart of a pipeline
with **many** ordinary `id`-keyed tables into a write outage for all of them until every table's
`schema_change` replays, for a correctness risk that in practice is specific to non-`id`-keyed
tables (concretely, `custom_objects` sidecars). The hard-error behavior is retained, narrowed to
exactly the case where the coincidence the fallback relies on does not hold.

### Consequences

* Good: a sink restart cannot silently duplicate rows in a `custom_objects` sidecar table — either
  the durable PK is found, or the write fails loudly instead of guessing.
* Good: ordinary `id`-keyed tables are unaffected — no new failure mode, no new latency, for the
  common case.
* Bad: `cdc_meta.pk_columns` is a second piece of durable state this sink now owns and must keep
  consistent (created once per process via `pkMetaEnsured`), outside the tables it is actually
  syncing — an operational surface that didn't exist before.
* Bad: persistence is best-effort logged-not-failed; a sink that successfully resolves a PK
  in-memory but fails to persist it is correct until its next restart, at which point the
  durability gap this ADR exists to close reopens for that one table, silently, unless the log line
  was seen.

## More Information

Verified live against Databend's actual `SHOW CREATE TABLE` output (it genuinely never emits a
`PRIMARY KEY` clause for a table this sink creates) rather than assumed from documentation — see
the `pkMu`/`pkCache`/`pkLoaded` field comments in `internal/sink/databend/sink.go` for the keying
requirement (`TableRef.String()`, matched identically between the write path and `ApplySchema`,
since a mismatched key silently reintroduces the same fallback this ADR closes).
`internal/sink/databend/sink_ws4_pk_durability_test.go`'s `persistentFakeDB` specifically models
"restart" as handing the same durable fake store to a second, freshly-constructed sink instance
with empty `pkCache`/`pkLoaded` — `TestWS4_6_PKDurability_SurvivesRestart_Sidecar` and
`TestWS4_6_MissingDurablePK_CustomObjects_IsHardError` cover the persisted-recovery and
hard-error paths respectively.

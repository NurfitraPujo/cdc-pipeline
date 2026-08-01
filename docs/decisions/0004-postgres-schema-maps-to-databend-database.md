---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: MULTI_SCHEMA_PLAN.md §6, §7.4
---

# A PostgreSQL schema maps to a Databend database

## Context and Problem Statement

Databend is the primary sink. Once the pipeline can read from several PostgreSQL schemas, the sink
needs somewhere to put them — and Databend has **no schema layer**. Its namespacing is
`catalog.database.table`.

## Decision Drivers

* Two source tables of the same name in different schemas must land in different targets.
* The sink previously wrote to whatever database the DSN selected, using the bare table name.
* This deployment is a **test/PoC** with no production data to preserve (`MULTI_SCHEMA_PLAN.md` §0).

## Considered Options

1. **Schema → Databend database.** `public.orders` → database `public`, table `orders`.
2. **Keep the DSN's database; fold the schema into the table name** (`sales_orders`).
3. **Bare for `public`, qualified otherwise** — mirroring `TableRef.KeyToken()`.

## Decision Outcome

Chosen: **option 1**.

This means the target database is **no longer the one the DSN selects** — an accepted break,
justified only because there is no production data. See "More Information" for what changes if this
system is ever promoted.

### Consequences

* Good: the destination mirrors the source namespacing exactly; no synthetic names.
* Good: collisions are impossible at the target.
* Bad: **every pre-existing target is stranded.** Rows previously written to `<dsn_db>.orders` stay
  there while new rows go to `public.orders`.
* Bad: `CREATE TABLE IF NOT EXISTS` means the split is **silent** — no error, surfacing later as
  missing history. This is why provisioning and startup validation exist ([0005](0005-databend-schema-provisioning.md)).
* Bad: the e2e harness had to be updated too. Assertions querying bare names resolve against the
  DSN's default database and fail with error 1025 even when rows synced correctly — see
  `qualifyTarget()` in `internal/test/e2e/env.go`. *Changing a target's address requires updating
  everything that reads from it, not just everything that writes to it.*

## More Information

Verified empirically against `datafuselabs/databend:latest`, not from documentation:

| Question | Result |
|---|---|
| Is `information_schema.columns.table_schema` the database name? | **Yes** — qualified existence checks work |
| Does an unqualified lookup leak across databases? | **Yes** — returns the union of both |
| Can `RENAME TABLE` move a table across databases? | **No** — error 1006 |
| Does `CREATE TABLE ... AS SELECT` work cross-database? | **Yes** — a full data copy |
| Is a quoted 3-part name accepted? | **Yes** — read as `catalog.database.table` |

**If this system is promoted to production**, migrating existing targets requires a resumable CTAS
copy (rename is unavailable), verification before any drop, and downtime proportional to data
volume. Those requirements were removed from the plan only because §0 applies.

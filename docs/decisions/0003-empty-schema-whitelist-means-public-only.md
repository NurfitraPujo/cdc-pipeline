---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: MULTI_SCHEMA_PLAN.md §3 Stage 2, §8 item 4
---

# An empty `schemas` whitelist means `public` only, not all schemas

## Context and Problem Statement

`SourceConfig.Schemas` existed before multi-schema support: the UI collected it, the API stored it
in KV, and **no backend code path ever read it**. Its OpenAPI description said
*"Empty/null means all schemas"*.

Making the field live forces a decision about what the empty value means — and because the field
was never read, **every existing stored config has it empty**.

## Decision Drivers

* Honouring the documented "empty means all" would change what gets replicated on upgrade, for
  every existing source, with no operator action.
* CDC replicating an unexpected table is expensive and hard to undo: it creates a target table,
  consumes a snapshot, and may carry data nobody intended to copy.
* The documented semantic was never implemented, so no consumer can depend on it.

## Considered Options

1. **Empty means `public` only** (preserve current effective behaviour).
2. **Empty means all schemas** (honour the documented semantic).
3. **Empty is invalid** — force every config to state its schemas explicitly.

## Decision Outcome

Chosen: **option 1**, and the OpenAPI description was corrected to match.

Discovery always excludes `pg_catalog`, `information_schema`, `pg_toast` and `pg_temp_*`
regardless of the whitelist.

### Consequences

* Good: upgrading changes nothing about what an existing pipeline replicates. Multi-schema is
  strictly opt-in.
* Good: the failure mode of a mistake is "table not replicated" (visible, recoverable) rather than
  "unexpected table replicated" (expensive, invasive).
* Bad: contradicts the previously published API description — a spec change, mitigated by the fact
  that nothing implemented it.
* Bad: an operator wanting every schema must enumerate them, and must revisit the list when a
  schema is added. A future `"*"` sentinel could address this without changing the empty default.

## Pros and Cons of the Options

### Empty means all schemas

* Good, because it matches the published description and needs no config change to adopt.
* Bad, because upgrading silently begins replicating every schema in the database.

### Empty is invalid

* Good, because intent is always explicit.
* Bad, because it breaks every existing stored config at startup — a hard failure for a field that
  was previously ignorable.

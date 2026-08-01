---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: MULTI_SCHEMA_PLAN.md §7.4
---

# Databend target databases are auto-provisioned by default, and validated at startup when they are not

## Context and Problem Statement

Once a PostgreSQL schema maps to a Databend database ([0004](0004-postgres-schema-maps-to-databend-database.md)),
the target database has to exist. It did not: a repo-wide grep for `CREATE DATABASE` / `CREATE SCHEMA`
returned **zero hits**. `ApplySchema` only ever issued `CREATE TABLE IF NOT EXISTS`.

So syncing any non-`public` schema failed 100% — and once targets became qualified, so did
`public`, because no database named `public` existed either.

## Decision Drivers

* DDL privileges in the pipeline credential are a legitimate concern in production.
* A missing database is a **permanent** error. Without a give-up path it redelivers forever.
* The failure must not be discovered per-message, after the pipeline reports itself started.

## Considered Options

1. **Auto-create, default on**, with startup validation when disabled.
2. **Auto-create, opt-in (default off).**
3. **Require pre-provisioning**, validate at startup, never create.

## Decision Outcome

Chosen: **option 1** — sink option `auto_create_schema`, **default `true`**, issuing
`CREATE DATABASE IF NOT EXISTS` before `CREATE TABLE`.

When it is `false`, `PipelineFactory.CreateWorker` calls `ValidateSchemas` via the optional
`sink.SchemaValidator` interface — the same pattern already used for `sink.DebugCapturer` — after
`sink.New` and before the worker is returned. A missing target fails startup with an error naming
it, rather than surfacing later.

This reverses an earlier decision of "opt-in, default off". That rationale was production privilege
hygiene, which `MULTI_SCHEMA_PLAN.md` §0 removes for this deployment; and default-off was measured
to make the **ordinary `public` path broken by default**, failing the e2e suite with
`databend database "public" does not exist and auto-provisioning is disabled` in a hot loop.

### Consequences

* Good: multi-schema works out of the box; the common case is not broken by default.
* Good: the option survives for production, where it should be flipped off and the databases
  pre-created.
* Bad: the sink credential needs create-database rights under the default.
* Bad: two code paths to maintain and test.
* Neutral: `ValidateSchemas` shipped as **dead code** in the first pass because the implementing
  agent's file scope (`internal/sink/**`) excluded the call site (`internal/engine/factory.go`).
  A requirement spanning two ownership boundaries needs an explicit owner.

## More Information

Related: [0006](0006-classify-ddl-errors.md) — without error classification, the `false` path still
loops on the schema path's unbounded `Nack()`.

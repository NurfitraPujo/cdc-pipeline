---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: MULTI_SCHEMA_PLAN.md §2.1, §2.3
---

# Canonical table identity is `protocol.TableRef` with two renderings

## Context and Problem Statement

Before multi-schema support, a table was identified by a bare string (`"orders"`). Config,
messages, KV keys, JetStream names, metric labels and sink targets each built that string by
concatenation, and `"public"` was hardcoded in five places. Two tables of the same name in
different schemas were indistinguishable, and config-shaped names silently diverged from
message-shaped names — causing recovered state, buffer drains and restored stats to address
different keys (see `MULTI_SCHEMA_PLAN.md` §1.1).

We needed one identity type. But the obvious rendering, `schema.table`, is **illegal or unsafe**
in several places it must travel through.

## Decision Drivers

* Same-named tables in different schemas must never collide.
* NATS KV keys are dot-delimited and parsed positionally; JetStream stream names reject `.`.
* Existing deployments should not have their keys rewritten unnecessarily.
* A raw string must not be able to construct a key by accident.

## Considered Options

1. **`TableRef` with two renderings** — `String()` qualified, `KeyToken()` key-safe.
2. **One qualified string everywhere** (`schema.table`).
3. **Keep bare names, add a parallel schema map** keyed by table name.

## Decision Outcome

Chosen: **option 1**.

```go
type TableRef struct{ Schema, Table string }

func (r TableRef) String() string   // "sales.orders" — display, logs, sink targets, metric labels
func (r TableRef) KeyToken() string // "orders" for public; "sales=orders" otherwise
```

`=` is legal in a NATS KV key, is not a subject-token separator, and is not legal in an unquoted
Postgres identifier. `ParseTableRef` rejects `=` and multi-dot names, keeping the encoding
injective.

**Every table-bearing key builder takes a `TableRef`, not a string** — so the compiler prevents a
raw config value from constructing a key. Derive a `TableRef` once at each boundary (config,
message, key-token) and thread it; never re-derive from a raw string mid-function.

### Consequences

* Good: collisions are structurally impossible; the key-builder signature is a compile-time guard.
* Good: `KeyToken()` is byte-identical to the legacy format for `public` tables, so bare-configured
  deployments keep their checkpoints and buffer streams.
* Bad: two renderings must be kept straight. Using `String()` where `KeyToken()` belongs produces
  a *valid but wrong* NATS subject that publishes successfully and drops the message.
* Bad: `KeyToken()` is only injective for refs produced by `ParseTableRef`; constructing a
  `TableRef` literal with a dotted `Table` bypasses the guarantee.

## Pros and Cons of the Options

### One qualified string everywhere

* Good, because there is only one form to remember.
* Bad, because `ParseTableStatsKey` splits positionally on `.` and returns `nil` **silently** for
  an 11-token key — the API returns `{"tables":{}}` with HTTP 200.
* Bad, because `cdc_pipeline_{id}_buffer_{table}` becomes an illegal JetStream stream name.
* Bad, because every existing checkpoint key is orphaned.

### Bare names plus a parallel schema map

* Good, because no existing key changes.
* Bad, because identity is split across two structures that can disagree — exactly the class of bug
  this work was fixing.

## More Information

Verified: `sanitizeDurableComponent` is a 1:1 replacer over `.`, space, `>`, `*` and is strict
identity for ordinary names, so routing stream names through it is safe.

Related: [0002](0002-schema-as-message-sibling-field.md).

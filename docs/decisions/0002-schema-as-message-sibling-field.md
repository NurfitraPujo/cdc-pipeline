---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: MULTI_SCHEMA_PLAN.md §2.2, §9.1
---

# Schema travels as a sibling field on `Message`, not encoded into `Message.Table`

## Context and Problem Statement

`protocol.Message` must carry which schema a row came from. The cheapest option is to qualify the
existing `Table` field (`"sales.orders"`), requiring no wire change.

An earlier attempt did exactly that. It failed, and the post-mortem (`MULTI_SCHEMA_PLAN.md` §11) is
the reason this ADR exists.

## Decision Drivers

* `Message` already holds `Schema *SchemaMetadata`, which itself contains `Table` and `Schema` — so
  qualifying `Table` creates a *second* representation of the same fact.
* Roughly ten call sites compare `m.Table` against **operator-supplied config strings**.
* Failures in those sites are silent: no error, no metric, no log.

## Considered Options

1. **Bare `Table` + new sibling `TableSchema`.**
2. **Qualify `Table` into `"schema.table"`.**
3. **Require `Message.Schema` (the `SchemaMetadata` pointer) to be non-nil on every message.**

## Decision Outcome

Chosen: **option 1**.

```go
type Message struct {
    Table       string `msg:"tbl"`            // "orders"  — always BARE
    TableSchema string `msg:"tsch,omitempty"` // "sales"   — sibling
}
```

Empty `TableSchema` normalises to `"public"` on read, so messages written before this change — and
those already sitting in JetStream buffer streams during an upgrade — decode correctly.
`SchemaDiff` gained the same field.

### Consequences

* Good: an entire class of silent failure **stops existing** rather than needing per-site fixes:
  * `strings.HasPrefix(m.Table, "cdc_snapshot_")` keeps matching — otherwise snapshot-internal
    tables enter schema evolution *and* get persisted into the KV pipeline config.
  * The debug sink's `ExcludeTables` keeps excluding — otherwise tables excluded for PII reasons
    silently start being written in full payload form.
  * Per-table sampling overrides and the transformer allowlist keep matching — the latter passes
    messages through **untransformed rather than erroring**.
* Good: config-shaped and message-shaped names stay the same shape, which is what caused the
  pre-existing state/stats/buffer key divergence.
* Bad: requires an msgp regeneration and a normalise-on-read rule.
* Bad: **the engine must actively populate the field.** Attempt 1 added it and left five readers
  with zero writers, which is worse than not adding it — each layer looks plausible and the seam
  fails silently. Acceptance check: `grep -rn TableSchema internal/engine/ | grep -v _test` must
  show assignments.

## Pros and Cons of the Options

### Qualify `Table`

* Good, because no wire-format change.
* Bad, because ~10 string-matching sites need individual fixes and any missed one fails silently.
* Bad, because the same fact then lives in two places that can disagree.

### Require non-nil `Message.Schema`

* Good, because it reuses existing structure.
* Bad, because `SchemaMetadata` also carries `Columns` and `PKColumns`, so every plain insert would
  inflate with redundant metadata — or be partially populated, inviting nil-field bugs.

## More Information

Related: [0001](0001-canonical-table-identity.md).

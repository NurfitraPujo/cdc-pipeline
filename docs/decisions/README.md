# Architectural Decision Records

Decisions that shaped this codebase, in [MADR](https://adr.github.io/madr/) format.

An ADR records *why* a decision was made, the options rejected, and the consequences accepted —
including the bad ones. They are **immutable**: when a decision changes, add a new ADR that
supersedes the old one rather than editing it. That is the difference between this directory and
`docs/requirements/`, which holds point-in-time design documents.

Write one when a choice is hard to reverse, contradicts an obvious default, or will otherwise be
re-litigated by whoever reads the code next.

## Index

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-canonical-table-identity.md) | Canonical table identity is `protocol.TableRef` with two renderings | accepted |
| [0002](0002-schema-as-message-sibling-field.md) | Schema travels as a sibling field on `Message`, not encoded into `Message.Table` | accepted |
| [0003](0003-empty-schema-whitelist-means-public-only.md) | An empty `schemas` whitelist means `public` only, not all schemas | accepted |
| [0004](0004-postgres-schema-maps-to-databend-database.md) | A PostgreSQL schema maps to a Databend database | accepted |
| [0005](0005-databend-schema-provisioning.md) | Databend target databases are auto-provisioned by default, validated at startup otherwise | accepted |
| [0006](0006-classify-ddl-errors.md) | DDL errors are classified: permanent dead-letter, transient retry | accepted |
| [0007](0007-go-pq-cdc-stays-an-in-tree-fork.md) | `go-pq-cdc` stays a hand-maintained fork in-tree | accepted |

0001–0007 all came out of multi-schema support; the full narrative, including two refuted
assumptions and a failed first implementation, is in [`MULTI_SCHEMA_PLAN.md`](../../MULTI_SCHEMA_PLAN.md).

## Template

Copy [MADR 3.0.0](https://github.com/adr/madr/blob/main/template/adr-template.md):

```markdown
---
status: proposed | accepted | rejected | deprecated | superseded by [NNNN](NNNN-file.md)
date: YYYY-MM-DD
decision-makers: …
---

# Short title in the imperative

## Context and Problem Statement
## Decision Drivers
## Considered Options
## Decision Outcome
### Consequences
## Pros and Cons of the Options
## More Information
```

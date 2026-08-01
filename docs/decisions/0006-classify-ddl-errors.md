---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: MULTI_SCHEMA_PLAN.md §7.4
---

# DDL errors are classified: permanent ones dead-letter, transient ones retry

## Context and Problem Statement

`ApplySchema` errors propagate to the consumer, which `Nack()`s for redelivery. The DLQ was wired
**only for deserialization failures**. So any DDL failure retried forever — and a missing database
can never succeed on retry. One misconfigured schema became an infinite hot loop, observed live in
the e2e suite.

Separately, `ApplySchema` had a path that logged an ALTER failure at warning level and returned
`nil`, reporting success while every later write failed on the missing column.

## Decision Drivers

* A permanently-unsatisfiable error must stop, loudly, with the table marked Failed.
* A transient error (connection, timeout) must still retry — dead-lettering it is data loss.
* Misclassification is dangerous in **both** directions.

## Considered Options

1. **Classify permanent vs transient**, dead-letter the permanent.
2. **Bounded retry then fail the table**, without classifying.
3. **Leave it** and treat the loop as a separate pre-existing issue.

## Decision Outcome

Chosen: **option 1**, in `internal/sink/databend/errors.go`. Permanent: unknown database, unknown
table, syntax, privilege. Transient: everything else. The swallowed ALTER failure now returns
`errors.Join` of the per-column failures instead of `nil`.

Classification matches **code-shaped renderings** (`code: 1003`, `[1003]`), never the bare digits.
A bare `"1003"` substring also matches byte counts, ports, row counts, LSNs and longer codes such
as `21003` — which would dead-letter a retryable failure. That is silent data loss, the mirror
image of the loop this ADR fixes.

### Consequences

* Good: a permanent DDL error fails one table cleanly instead of hot-looping the pipeline.
* Good: transient failures keep their existing retry behaviour.
* Bad: the error taxonomy is provider-specific and must track Databend's codes.
* Bad: classification depends on driver error text. Anchoring to code-shaped forms reduces but does
  not eliminate the risk; a driver that changes its rendering silently reclassifies.

## More Information

Databend codes verified empirically: `1003` unknown database, `1025` unknown table, `1006` rename
restriction, `1005` syntax error.

Related: [0005](0005-databend-schema-provisioning.md).

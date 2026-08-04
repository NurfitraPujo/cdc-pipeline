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
| [0008](0008-at-least-once-with-sink-side-idempotency.md) | At-least-once delivery, idempotency delegated to the sink | accepted |
| [0009](0009-replication-slot-is-the-resume-authority.md) | The replication slot, not the KV checkpoint, is the resume authority | accepted |
| [0010](0010-embedder-owns-slot-advancement.md) | The embedder owns slot advancement (`ManualCommit`) | accepted |
| [0011](0011-nats-as-the-only-control-plane-datastore.md) | NATS is transport, config store and state store; no separate database | accepted |
| [0012](0012-watch-based-config-hot-reload.md) | Config propagates by KV watch, applied via two-phase drain-then-shutdown | accepted |
| [0013](0013-per-sink-consumer-fan-out.md) | Each sink gets its own consumer, durable, checkpoint and DLQ | accepted |
| [0014](0014-schema-evolution-cas-state-machine.md) | Schema evolution is a CAS-fenced state machine with a per-table rate limit | accepted |
| [0015](0015-buffer-to-jetstream-across-the-discovery-gap.md) | Rows arriving during snapshot buffer to durable JetStream, not memory | accepted |
| [0016](0016-credentials-encrypted-at-rest-with-fail-fast-decrypt.md) | Credentials AES-GCM encrypted in KV; decryption failure is fatal | accepted |
| [0017](0017-msgpack-for-state-json-for-config.md) | MessagePack for the data plane, JSON for the control plane | accepted |
| [0018](0018-toast-unchanged-signalled-via-columnkinds.md) | An unchanged TOASTed column is signalled out-of-band via `ColumnKinds`, never inferred from absence in `Data` | accepted |
| [0019](0019-columnkinds-typed-side-channel.md) | Type/routing hints travel in a typed `ColumnKinds` side-channel, not an in-band marker | accepted |
| [0020](0020-sink-synthesizes-deleted-at-with-read-before-write-preservation.md) | The Databend sink synthesizes `deleted_at` on every table, preserved via read-before-write | accepted |
| [0021](0021-durable-pk-metadata-in-cdc-meta.md) | A table's resolved primary key is persisted durably in `cdc_meta.pk_columns` | accepted |
| [0022](0022-ackwait-ceiling-ten-minutes.md) | `deriveAckWait`'s derived worst case is clamped to a configurable ceiling, default 10 minutes | accepted |
| [0023](0023-processor-filter-schemas-tables-or-semantics.md) | A processor's `schemas` and `tables` filters OR together, not AND | accepted |
| [0024](0024-transform-circuit-breaker-and-transport-classification.md) | The transform RPC gets its own circuit breaker; NATS transport errors are classified separately from application errors | accepted |
| [0025](0025-transform-subject-auth-accepted-risk.md) | Subject-level authentication on `custom_object_requests.cdc_transform` is an accepted risk, not implemented | accepted |

**0001–0007** came out of multi-schema support; the full narrative, including two refuted
assumptions and a failed first implementation, is in [`MULTI_SCHEMA_PLAN.md`](../../MULTI_SCHEMA_PLAN.md).

**0008–0017** are retrospective: they record decisions that predate this directory and whose
rationale existed only in code comments and vendored patch notes. They were reconstructed from the
code, and each cites the evidence it rests on. Where a rationale was inferred rather than stated,
the ADR says so. [`rfc/RFC-001-Architecture-and-Design.md`](../../rfc/RFC-001-Architecture-and-Design.md)
remains the broad architecture document; these ADRs complement it rather than replace it.

**0018–0022** came out of `plans/cdc_custom_object_transform_remediation.md`'s WS-5/WS-6/WS-7
remediation pass (see `summaries/ws5_ws6_ws7_implementation.md`). 0020 and 0021 record decisions
made earlier in that same effort (WS-4) but not yet written up as ADRs until this pass. 0022 records
a fix made during Opus validation review of the WS-5/WS-6/WS-7 pass itself. 0023 came out of the
following WS-1B/WS-2B/WS-4C/WS-10 pass (see
`summaries/ws1b_ws2b_ws4c_ws10_implementation.md`). 0024 and 0025 close out WS-5's remaining
resilience gaps (circuit breaker, transport-error classification, health-gated unsubscribe) and
record the deliberate decision to skip subject-level authentication (see
`summaries/ws5_resilience_completion.md`).

Several record a **known deviation between the decision and the current code** (notably 0014's
unimplemented `type_conflict` state and 0017's msgp/JSON boundary violation). That is deliberate —
an ADR describes the decision and honestly notes where reality has drifted from it.

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

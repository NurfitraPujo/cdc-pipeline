---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/engine/producer.go:1025-1139, docs/requirements/schema-evolution-plan.md:100-120
---

# Schema evolution is a CAS-fenced state machine with a per-table rate limit

## Context and Problem Statement

A source `ALTER TABLE` produces no CDC message. When new columns appear, rows carrying them must not
reach a sink whose table lacks them — and the pipeline may be running as multiple workers, so two
of them can observe the same change concurrently.

## Decision Outcome

A per-table state machine — `stable → frozen → draining → stable` — persisted in NATS KV. While
frozen or draining, that table's rows are routed to a buffer stream instead of the ingest topic
(`producer.go:361-371`, `:545-567`). The table unfreezes only when **every** sink has acknowledged
the DDL.

**Compare-and-swap fencing.** Each state write carries the KV revision as a fencing token;
`persistEvoState` uses `kv.Update(key, data, revision)` and refreshes the token on conflict —
"retrying a stale revision unchanged can never converge when another writer has advanced the key"
(`producer.go:1119-1120`). Bounded at 5 attempts.

**Fails closed, not open.** If CAS cannot converge, `pauseTableCDC` marks the table `Error`, which
routes its traffic to the buffer rather than letting unvalidated rows through
(`producer.go:1136-1157`).

**Per-table rate limit.** More than 5 schema changes in a sliding minute moves the table to
`suspended`, requiring manual review (`producer.go:1025-1038`).

### Consequences

* Good: CAS prevents split-brain. Without it, a loser could reset `AcknowledgedSinks` mid-flight
  (permanent freeze) or stamp a frozen table back to `stable` while its buffer still holds rows
  (reordering or loss).
* Good: the rate limit closes a DDL-amplification DoS. Each freeze halts a table, allocates a buffer
  stream, and blocks on an all-sinks round trip, so cheap `ALTER`s become unbounded buffering
  (`schema-evolution-plan.md:118`).
* Bad: `suspended` is terminal and needs an operator. That is the intended trade for a table
  changing shape 5+ times a minute.
* Bad: **`type_conflict` is declared but has no writer or reader in the engine.** It appears only in
  `protocol/state.go:77` and the design docs, which intend narrowing type changes to land there.
  Treat it as reserved / not implemented.
* Bad: the design docs also specify a 60s ack timeout; no such timeout exists in `producer.go`. A
  sink that never acks leaves the table frozen indefinitely.

## More Information

**On `CorrelationID`.** It is minted per evolution round and an ack whose ID does not match is
dropped (`producer.go:661-665`). Its actual function is a **generation/epoch token**: it stops a
stale ack from a *previous* round counting toward the current round's ack set, which would unfreeze
the table before every sink had applied the current DDL. `README.md` describes this as preventing
"spoofed acknowledgments" — that framing overstates it. There is no authentication here; it is a
staleness barrier, and the honest failure mode is the inverse: a systematic mismatch leaves the
table frozen forever, which is why the identity key derivation is commented so carefully at
`producer.go:650-655`.

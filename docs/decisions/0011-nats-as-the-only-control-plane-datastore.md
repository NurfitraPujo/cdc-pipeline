---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: rfc/RFC-001-Architecture-and-Design.md §13, internal/config/manager.go
---

# NATS serves as transport, config store and state store; there is no separate database

## Context and Problem Statement

The control plane needs durable configuration, per-table operational state, and a way to push
config changes to running workers. The data plane needs a durable message backbone. These are
usually separate systems (Kafka + Postgres, or Kafka + etcd).

## Decision Drivers

* JetStream ships with a KV store, so choosing it for transport makes a separate config store
  redundant.
* The API must be stateless and horizontally scalable — "All instances share state through NATS KV"
  (`RFC-001:238`).
* Workers must see config changes without an API→worker RPC path.

## Considered Options

1. **NATS JetStream for everything** — streams, KV, and pub/sub.
2. **Kafka for transport + Postgres/etcd for config and state.**

## Decision Outcome

Chosen: **option 1**. A single KV bucket (`cdc-dp-config`) holds config, checkpoints, table state,
stats, heartbeats and schema-evolution state; the key namespace is centralised in
`internal/protocol/config.go:17-35`.

`RFC-001:323` records the messaging comparison verbatim: *"Existing infra, one binary simplicity,
built-in KV. Lower operational overhead, extremely efficient. Smaller ecosystem vs. Kafka."*
Postgres/etcd/Consul are **not** named as rejected alternatives anywhere — "built-in KV" is the
implicit argument, and this ADR records that as inferred rather than documented.

KV is configured linearizable so "new workers see the absolute latest LSN" (`RFC-001:326`).

### Consequences

* Good: one system to operate, deploy and secure.
* Good: KV watch doubles as the control channel — see [0012](0012-watch-based-config-hot-reload.md).
* Bad: **KV write pressure is a real constraint.** Heartbeats were migrated off KV onto a
  non-persisted pub/sub subject (ticket T1-31) because per-worker KV writes were too expensive;
  the result is a two-cadence design, 2s pub/sub plus a 15s KV write purely so the API can render
  status (`internal/config/manager.go:26-37`, `:608-613`). `heartbeatKVWritesTotal` exists to
  monitor that migration.
* Bad: KV watch can redeliver out of order across restarts, so the manager persists a last-seen
  revision map back into KV to avoid a stale delete terminating a freshly started worker
  (`manager.go:315-332`, T1-27).
* Bad: no relational queries. Anything needing a join or aggregate must be computed in Go, which is
  why `cdc.stats.global_summary` exists as a materialised key.

## More Information

`WORKER_GROUP` namespaces JetStream **durable consumer names** so prod and staging can share a
cluster (`internal/engine/factory.go:152-159`). It does **not** namespace stream names or KV keys,
so groups sharing a cluster still share `cdc_pipeline_<id>_ingest` and all of `cdc.config.*`.
Treat it as a lightweight stopgap; `RFC-001:229-230` records proper `OrgID`-prefixed multi-tenancy
as future work.

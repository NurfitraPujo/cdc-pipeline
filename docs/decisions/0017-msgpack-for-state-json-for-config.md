---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/protocol/{message,config,state}.go (//go:generate msgp), rfc/RFC-001-Architecture-and-Design.md:324
---

# MessagePack for the data plane, JSON for the control plane

## Context and Problem Statement

Every CDC row crosses the wire, and some structs are additionally written to KV, read by the API,
and decoded from YAML. A single serialization format would be simpler, but the data plane is
throughput-sensitive while the control plane is read and edited by humans.

## Decision Drivers

* CDC throughput makes per-message reflection and string keys expensive.
* Config in KV is inspected with `nats kv get` during incidents; unreadable bytes cost debugging time.
* The same struct is often needed in more than one medium.

## Considered Options

1. **MessagePack for the data plane, JSON for the control plane.**
2. **JSON everywhere.**
3. **MessagePack everywhere.**

## Decision Outcome

Chosen: **option 1**.

**MessagePack** (`msgp` code generation) for the data plane: JetStream batches, `RecordAck`, and KV
*state* such as checkpoints. `RFC-001:324` records both the benefit and the cost: "5-10x faster than
JSON, smaller payloads. **Not human-readable without tools.**" Wire tags are aggressively
abbreviated (`msg:"i_lsn"` versus `json:"ingress_lsn"`), which is the size argument made concrete.

**JSON** for the control plane: KV *config* values written by the API and read by `ConfigManager`,
transition state, heartbeats, and all HTTP responses.

Protocol structs therefore carry `msg:`, `json:` and often `yaml:` tags — the same struct is
msgp-encoded on the wire, JSON-encoded for config and API responses, and YAML-decoded when seeding
from `config.example.yaml`. Structs that are API-only carry **no** `msg` tag at all
(`StatsSummary`, `HistoryPoint`), which is the reliable signal that a type is control-plane only.

### Consequences

* Good: the hot path pays no reflection or string-key cost.
* Good: config stays inspectable and hand-editable in KV.
* Bad: **the boundary is a convention, not a type-level guarantee — and it is currently violated.**
  `TableStats` is written to the same KV key as JSON in one place
  (`internal/engine/consumer.go:553`) and as msgp in two others (`:749`, `:874`), and the API
  decodes msgp-written `Checkpoint`/`TableStats` with `json.Unmarshal` under an `err == nil` guard
  (`internal/api/handler.go:698,704,1482,1488`), so it silently skips them. Verified experimentally:
  `json.Unmarshal` on msgp bytes returns an `invalid character` error and leaves the struct
  zero-valued. Per-table stats and checkpoints are therefore absent from the status API. Tracked
  separately — do not read this ADR as blessing the current state.
* Bad: debugging KV state requires a msgpack decoder.
* Bad: two sets of tags per struct is easy to get wrong, and nothing enforces that a new field gains
  both.

## Pros and Cons of the Options

### JSON everywhere

* Good, because everything is inspectable and there is one rule.
* Bad, because the data plane pays reflection and full string keys on every message.

### MessagePack everywhere

* Good, because the boundary problem above disappears.
* Bad, because config becomes opaque exactly when an operator most needs to read it.

## More Information

A durable fix for the violation would make the boundary explicit rather than remembered — for
example distinct `putState`/`getState` (msgp) and `putConfig`/`getConfig` (JSON) helpers, so the
encoding is chosen by the call site's type rather than by the author's memory.

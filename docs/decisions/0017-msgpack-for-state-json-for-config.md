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
* Bad: the boundary is a convention, not a type-level guarantee. It **was** violated: `TableStats`
  was written to the same KV key as JSON in one place (`internal/engine/consumer.go:553`) and as
  msgp in two others, and the API decoded msgp-written `Checkpoint`/`TableStats` with
  `json.Unmarshal` under an `err == nil` guard, silently skipping them — so per-table stats and
  checkpoints were absent from the status API and `TotalSynced` reset on every restart.
  **Fixed:** every state writer and reader now goes through `protocol.MarshalState` /
  `protocol.UnmarshalState`, decode failures are logged instead of swallowed, and
  `internal/engine/state_encoding_test.go` pins writer/reader agreement by piping the bytes the
  real writers `Put` into the real reader's `Get`.
* The boundary is now **checked**, if not type-enforced:
  `internal/protocol/state_boundary_test.go` fails the build on any
  `json.Unmarshal(entry.Value(), &x)` where `x` is a state type, naming the file, line and this
  ADR. It keys on the `.Value()` KV accessor, so JSON-encoding a `TableStats` into an HTTP
  response — which is legitimate and common — stays legal. A blanket ban on JSON for these types
  would have been wrong for exactly that reason.
* The write side is guarded too. `TestStateKeysAreWrittenWithMarshalState` requires any value
  `Put` under a state key (`TableStatsKey`, `IngressCheckpointKey`, `EgressCheckpointKey`,
  `SourceWatermarkKey`) to come from `MarshalState`. It is an allowlist rather than a ban on
  `json.Marshal`, so a raw `MarshalMsg` — correct bytes today, but bypassing the one chokepoint —
  fails too. Adding it surfaced four such writers (three in `producer.go`, one in
  `source/postgres/source.go`), now converted.
* **Fixed:** `TableMetadataKey` was written as `SchemaMetadata` and read as `TableMetadata` — a
  cross-*type* split rather than a cross-encoding one. The two disagree on `columns` (a
  name→type object versus a `[]string`), so the read failed and was silently skipped by the same
  `err == nil` idiom, and the source-tables endpoint returned nothing. The discovery path now
  writes a real `TableMetadata` via `tableMetadataFromSchema`, with columns sorted so `Columns`
  and `Types` stay positionally aligned.
* Still unguarded: cross-type mismatches in general. The write guard checks *how* a value is
  encoded, not *which struct* goes under a key, so a future writer could still put the wrong type
  under the right key. `SchemaEvolutionKey` is the live example — `persistEvoState` writes a
  JSON `tableEvolution` to it while `ConfigManager.UpdateSchemaStateCAS`/`GetSchemaState` use
  msgp `SchemaEvolutionState`. Not currently a bug only because the latter pair has no callers.
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

The fix took the shape this section proposed: `protocol.MarshalState`/`protocol.UnmarshalState`
(`internal/protocol/statecodec.go`) make the encoding a property of the helper rather than of the
author's memory. They are deliberately *not* KV-level `putState`/`getState` wrappers — the KV
handle is `nats.KeyValue` throughout, and wrapping it would have been a much wider change than the
defect warranted.

`UnmarshalState` also accepts JSON, sniffed by leading byte, so state written before the encodings
were unified still decodes. Per §0 of `MULTI_SCHEMA_PLAN.md` this deployment has no production data
to preserve, so that fallback is belt-and-braces rather than a migration requirement; it can be
dropped once no pre-fix KV state remains.

# Plan 01b — Delivery Guarantee, Sink Half: Databend Sink + Transformer Data Correctness

**Scope:** the sink-side portion of Fix Sequence 1 ("Delivery guarantee"). Covers verified findings
Critical 3, 4, 5, 6, 7, 9, 10; High "additive-only schema evolution / empty ddl package"; Mediums
(`Contains("int")`, ADD COLUMN warn-only, `refreshPrimaryKey` scan, `validateIdentifier` poison-pill);
and the round-2 postgresdebug findings (`cleanupByCount` MaxCount==0, sub-hour retention truncation,
float64-only config parse, `filteredIndices` misalignment, `batchInsert` partial commit).

**Companion plan:** `01a` (source half: ack-before-publish, TOAST/replica identity, snapshot
resume). Items here that need source cooperation are marked **[DEP: 01a]**.

**Ground truth files (all paths absolute):**
- `/home/fitrapujo/works/cdc-pipeline/internal/sink/databend/sink.go` (841 lines)
- `/home/fitrapujo/works/cdc-pipeline/internal/sink/databend/db.go`, `dlq.go`, `metrics.go`
- `/home/fitrapujo/works/cdc-pipeline/internal/sink/sink.go` (interface), `factory.go` (registry), `hooks.go`
- `/home/fitrapujo/works/cdc-pipeline/internal/sink/postgresdebug/{sink.go,config.go,hooks.go}`
- `/home/fitrapujo/works/cdc-pipeline/internal/transformer/nats/protobuf.go`
- `/home/fitrapujo/works/cdc-pipeline/internal/transformer/{all.go,provider.go,ddl/ddl.go}`
- `/home/fitrapujo/works/cdc-pipeline/internal/engine/{consumer.go,factory.go}`
- `/home/fitrapujo/works/cdc-pipeline/internal/protocol/{message.go,state.go}`
- `/home/fitrapujo/works/cdc-pipeline/internal/vendor/go-pq-cdc/pq/message/tuple/data.go` (TOAST/bytea decode context)

---

## 1. Objective & correctness invariants

The pipeline promises **at-least-once, ordered, type-faithful CDC delivery into Databend**. Today the
sink violates all four pillars. Every work item below exists to restore one of these invariants:

**I1 — Durability before ack.** `Sink.BatchUpload(ctx, msgs) == nil` MUST mean: every record in
`msgs` is either (a) durably applied to the sink datastore, or (b) durably persisted to a DLQ stream
(JetStream-acked publish). There is no third outcome. Any record that is neither applied nor
dead-lettered forces a non-nil return, so the consumer (`engine/consumer.go:435-446`) nacks instead
of acking. Broken today at `databend/sink.go:434,629-632,656-658,696,711-713` (records silently
dropped, nil returned, consumer acks at `consumer.go:441-446`; `dlqPublisher` is *always* nil in
production because nothing in `engine/factory.go` or `cmd/` ever injects it — `sink.New` receives
only the JSON options map from KV, which cannot carry a Go publisher).

**I2 — Per-key ordering within a batch.** For any primary-key value, the sink must apply the batch's
operations in event order — equivalently, after applying a batch, each PK's state equals the state
implied by that PK's **last** event in the batch (last-write-wins). Broken today twice:
`sink.go:195-211` runs `uploadTableBatch` and `deleteTableBatch` for the same table in *separate
errgroup goroutines* with no ordering (DELETE id=1 + re-INSERT id=1 applies nondeterministically),
and `sink.go:471` iterates column-set groups in random map order with no per-PK dedup, so two updates
to the same PK in different column-set groups apply in random order.

**I3 — Idempotency under redelivery.** Re-applying a batch (JetStream redelivery after a crash
between sink write and ack) must converge to the same state. `REPLACE INTO ... ON (pk)` and
`DELETE ... WHERE pk=` are idempotent *only if* I2 holds and the PK is correct; the `{"id"}` PK
fallback (`sink.go:452-454,681-683`) breaks this for any table whose PK isn't `id`.

**I4 — Correct primary-key knowledge, durable across restarts.** The PK used for REPLACE/DELETE
must be the source table's real PK. Databend cannot express a PK constraint — `CREATE TABLE`
(`sink.go:273-296`) writes no PK, so the `SHOW CREATE TABLE` recovery path
(`sink.go:736-787` + `sinkPKRegex` at `:46`) is **structurally impossible** and always falls back to
`id`. After any worker restart, a table with PK ≠ `id`: deletes silently skip
(`sink.go:702-713` — PK column absent from `data` → `continue` → batch acks) and upserts REPLACE on
the wrong key. PK metadata must be persisted out of band and loaded on restart.

**I5 — Type fidelity end to end.** A value captured from Postgres must arrive in Databend
value-equal: `bytea` as the same bytes (broken: `sink.go:575-591` `[]byte` falls to
`json.Marshal` → quoted base64 string written into a BINARY column), `bigint`/`numeric` at full
precision (broken: `transformer/nats/protobuf.go:196` `structpb.NewStruct` coerces to float64,
`:288-289` `AsMap()` returns float64 — silent corruption above 2^53), `point`/`interval` not
mis-mapped to INT64 (`sink.go:363` `strings.Contains(t, "int")` matches "po**int**" and
"**int**erval"), timestamps with a defined tz convention.

**I6 — Whole-row semantics for updates, or explicit partial-update handling.** `REPLACE INTO` is
whole-row; a message whose `Data` omits columns (unchanged TOAST columns are omitted — vendored
`tuple/data.go:82-91` has no `DataTypeToast` case, so the key never enters the map) NULLs every
omitted column (`sink.go:427-447,479-485`). The sink must never write a partial row as if it were
whole. **[DEP: 01a]**

**I7 — Schema changes never silently diverge.** Column drops, renames, and type changes must either
be applied or fail loudly; today they are silently ignored (`sink.go:299-314` handles ADD only, and
even ADD failure only warns at `:310-312` and returns nil, so the schema message acks).

---

## 2. Target design per problem area

### 2.1 The `BatchUpload` durability contract (I1) — Critical 3

**Interface contract (document on `internal/sink/sink.go`):**

```go
// Sink is the durable write boundary of the pipeline.
//
// CONTRACT: BatchUpload returns nil ONLY when every message has been either
// (a) durably applied to the sink datastore, or (b) durably published to the
// sink DLQ (broker-acknowledged). A nil return authorizes the consumer to ack
// the upstream JetStream messages; a non-nil return guarantees redelivery.
// Errors that cannot succeed on retry (poison records) must be wrapped with
// sink.Terminal so the consumer can isolate instead of retrying forever.
type Sink interface {
	Name() string
	BatchUpload(ctx context.Context, messages []protocol.Message) error
	ApplySchema(ctx context.Context, m protocol.Message) error
	Stop() error
}
```

**Error taxonomy (new, `internal/sink/errors.go`):**

```go
// TerminalError marks a batch failure that will never succeed on redelivery
// (malformed payload, invalid identifier, unknown PK with no schema in flight).
type TerminalError struct{ Err error }
func (e *TerminalError) Error() string { return e.Err.Error() }
func (e *TerminalError) Unwrap() error { return e.Err }
func Terminal(err error) error { if err == nil { return nil }; return &TerminalError{Err: err} }
func IsTerminal(err error) bool { var t *TerminalError; return errors.As(err, &t) }
```

**DLQ becomes load-bearing, not best-effort.** Rewrite `emitDLQ`
(`databend/sink.go:618-662`) as:

```go
// dlqOrFail durably dead-letters a record. Returns non-nil (and therefore
// forces BatchUpload to fail) when no publisher is wired or the publish is
// not broker-acknowledged.
func (s *DatabendSink) dlqOrFail(ctx context.Context, m protocol.Message, table, reason string) error
```

- `s.dlqPublisher == nil` → `return fmt.Errorf("record %s undeliverable (%s) and no DLQ publisher wired", m.UUID, reason)` — **never** `return nil` (today's `:629-632`).
- Publish failure (`:656-658` today logs and returns) → propagate the error.
- The publisher must be the JetStream-backed `stream.Publisher` (watermill NATS JetStream publisher
  used by the engine — `engine/factory.go` `f.Publisher`), publishing to `s.dlqSubject`
  (`dlq.go:12-17`), and a JetStream stream covering `cdc.sink.>` must exist (add to the same infra
  bootstrap that creates the ingest streams; see Work Item S1).
- All three drop sites route through it: decode failure in `uploadTableBatch` (`:431-435`), decode
  failure in `deleteTableBatch` (`:693-697`), and the *new* "DELETE with missing PK value" case
  (replaces the silent `continue` at `:703-708,711-713`).
- Collect per-record DLQ errors; if any DLQ attempt failed, `BatchUpload` returns non-nil (the whole
  wmMsg is redelivered — acceptable: applied rows are idempotent under I2/I3 re-application).

**Wiring (the reason `dlqPublisher` is nil today).** The JSON options map cannot carry a publisher.
Add an optional interface and wire it in the factory:

```go
// internal/sink/sink.go
type DLQAware interface { SetDLQPublisher(pub DLQPublisher, subject string) }
// move DLQPublisher (currently databend/dlq.go:40-42) up to internal/sink so
// the engine can reference it without importing the databend package.
```

`engine/factory.go` after `sink.New(...)` (`:92-95`):

```go
if aware, ok := snk.(sink.DLQAware); ok {
	aware.SetDLQPublisher(f.Publisher, databendDLQSubjectFor(sinkID)) // or let sink keep its default
}
```

Keep the `dlq_subject` option (`sink.go:132-136`); delete the `dlq_publisher` option key
(`:127-131`) — it is untestable dead config in production.

**Consumer-side handling (`engine/consumer.go`):** `flush` (`:415-472`) and `handleSinkError`
(`:538-617`) stay structurally the same, plus: on `sink.IsTerminal(err)` skip the retry counter and
go straight to `isolatePoisonBatch` (`:694-738`), which already splits the batch per wmMsg and
DLQs individual poison messages. This also resolves the `validateIdentifier` poison-pill (matrix
sink #15): a batch that fails identifier validation with `EnableDLQ=false` currently retries
forever; with terminal typing it isolates immediately and the per-message path applies
`MaxRetries` (note `consumer.go:725` still gates final DLQ routing on `retryConfig.EnableDLQ`; the
plan keeps that gate but the bounded-redelivery/`MaxDeliver` question belongs to the engine plan).

### 2.2 Within-batch ordering + last-write-wins compaction (I2, I3) — Critical 4, 5

Replace the upserts/deletes double-map split (`sink.go:181-211`) with a **per-table ordered apply
with per-PK compaction**:

```go
type tableBatch struct {
	table string
	ops   []protocol.Message // original batch order preserved
}

type compactedOp struct {
	pkKey  string          // canonical key: pk values joined per pkCols order
	del    bool            // true => delete, false => upsert with row
	row    map[string]any  // decoded Data (upsert only)
	src    protocol.Message
}
```

`BatchUpload` becomes:

1. Partition messages by table **preserving slice order** (`messages` arrives in WAL order — the
   consumer appends wmMsg batches in receipt order, `consumer.go:284-374`). Skip
   `OpSchemaChange`/`drain_marker` as today (`sink.go:185-187`).
2. Per table (parallel across tables via errgroup is safe — tables are independent — but **one
   goroutine handles both deletes and upserts for a table**, replacing the two racing goroutines at
   `:195-211`):
   a. Resolve PK columns (§2.3). No PK ⇒ terminal error (no `{"id"}` guess).
   b. Decode each payload (`decodePayload`, `:597-612`); decode failure ⇒ `dlqOrFail`.
   c. Extract `pkKey` from decoded data. Missing any PK column value ⇒ for deletes AND upserts this
      is undeliverable ⇒ `dlqOrFail` (replaces silent skip at `:702-713`).
   d. **Compact**: iterate in order into `map[pkKey]compactedOp`, overwriting — the map ends holding
      each PK's *last* operation. Keep a `[]string` of first-seen pkKeys sorted at the end for
      deterministic emission order.
   e. Apply: all compacted deletes as chunked multi-row DELETEs, all compacted upserts grouped by
      column set (existing grouping logic `:427-447`) into `executeReplaceIntoChunks`
      (`:498-567`, unchanged mechanics). **Order between the delete statement and replace
      statements no longer matters**: after compaction there is exactly one op per PK, and ops on
      distinct PKs commute. Still emit deletes first and sort group keys
      (`sort.Strings` over `groups` keys before the loop at `:471`) for determinism and log
      readability.
3. Idempotency follows: re-running the same compacted set of REPLACE/DELETE ops is a no-op.

**Batched delete SQL** (replaces one-DELETE-per-row loop `:692-719`):

- Single-column PK: `DELETE FROM "t" WHERE "pk" IN (?, ?, ...)` chunked by `maxPlaceholders`.
- Composite PK: `DELETE FROM "t" WHERE ("a" = ? AND "b" = ?) OR ("a" = ? AND "b" = ?) ...`
  chunked so `len(pkCols) * rowsPerChunk <= maxPlaceholders`. (Databend's support for row-value
  `(a,b) IN ((?,?),...)` is version-dependent — the OR-of-conjuncts form is safe everywhere; see
  Open Questions.)

**pkKey canonicalization:** `pkKey = strings.Join(canon(vals), "\x1f")` where `canon` renders each
value deterministically (`fmt.Sprintf("%v", normalizeValue(v))`, with `[]byte` hex-encoded and
`time.Time` in RFC3339Nano UTC). Same source row ⇒ same key regardless of msgpack/json decode path.

### 2.3 PK metadata: persist out of band, load on restart (I4) — Critical 6

Databend cannot store a PK constraint, so the sink persists its own metadata **in the sink
database** — a meta table is atomic with the data plane, survives NATS KV loss, and is inspectable
by operators:

```sql
CREATE TABLE IF NOT EXISTS cdc_sink_meta (
    table_name  STRING NOT NULL,
    pk_columns  STRING NOT NULL,   -- JSON array, e.g. ["tenant_id","order_id"]
    source_id   STRING,
    updated_at  TIMESTAMP
);
```

(Name configurable via option `meta_table`, default `cdc_sink_meta`. Written with
`REPLACE INTO cdc_sink_meta ON (table_name) VALUES (?, ?, ?, ?)` — the one place the sink relies on
REPLACE's dedup-on-conflict-key, which is fine because this table is only ever written through this
path.)

**Write path:** `ApplySchema` (`sink.go:249-317`) already receives `schema.PKColumns` — the source
emits it on every table prime and discovery
(`internal/source/postgres/source.go:552,941`) — and today caches it only in memory (`:261-264`).
Add: after updating `pkCache`, upsert the meta row. Meta-write failure ⇒ `ApplySchema` returns
error (schema wmMsg is nacked by `consumer.go:333-339`, redelivered — correct).

**Read path:** rewrite `ensurePrimaryKey`/`refreshPrimaryKey` (`:723-787`):

```go
// resolvePK returns the PK columns for table, in priority order:
// 1. in-memory pkCache (populated by ApplySchema this process lifetime)
// 2. cdc_sink_meta lookup (persisted by a previous process) — cached on success
// 3. error (never guess "id")
func (s *DatabendSink) resolvePK(ctx context.Context, table string) ([]string, error)
```

- **Delete** `sinkPKRegex` (`:46`), `parsePKFromDDL` (`:801-833`), `ensureFallbackPK` (`:789-799`),
  the `SHOW CREATE TABLE` query + single-string `QueryRowScan` (`:756-771` — this also disposes of
  Medium N7, the 2-column scan bug), and the `{"id"}` fallbacks at `:452-454` and `:681-683`.
- Meta lookup failure that is transient (network) ⇒ plain error (retry). Meta row absent ⇒
  `sink.Terminal` error naming the table ("no PK metadata; will resolve when the source's schema
  message arrives") — but see the retry note: since the source re-primes schemas on every
  `startConnector` run (`source.go:540-556`), a plain (retryable) error is actually preferable for
  the *absent* case during rollout, because the schema message will arrive shortly after any
  restart. Decision: **absent ⇒ retryable error**, so ordering of schema-vs-data messages after a
  restart self-heals; only malformed meta content is terminal.
- Keep `SinkPKResolved` metric (`metrics.go`) with values: 1 = cache/meta hit, 0 = unresolved.
- The `pkLoaded` map and its double-checked locking (`:739-754`) simplify to a `singleflight`-style
  per-table lookup or plain mutex around the meta query; the "at most once" property is no longer
  load-bearing since the meta query is cheap and cached on success.

**CREATE TABLE change (`:273-296`):** unchanged structurally (Databend has no PK clause), but
optionally append `CLUSTER BY (<pk cols>)` when PKColumns is known — cheap and improves REPLACE and
DELETE performance. Gate behind option `cluster_by_pk` (default true).

### 2.4 Partial updates / TOAST (I6) — Critical 7 **[DEP: 01a]**

Three candidate designs, one recommendation:

| Option | Mechanics | Pros | Cons |
|---|---|---|---|
| **A. Full row images at source** (REPLICA IDENTITY FULL + old/new tuple merge in the source, per plan 01a) | Source merges the update's old tuple over TOAST-omitted columns (vendored `update.go` already decodes the old tuple; `tuple/data.go` needs a `DataTypeToast` case that pulls from the old image); sink keeps whole-row REPLACE | Sink stays simple and idempotent; snapshot/insert/update all uniform; no new SQL shapes | WAL volume grows on wide tables; requires `ALTER TABLE ... REPLICA IDENTITY FULL` on TOAST-bearing tables (source-side migration) |
| B. Partial-update marker + column-level MERGE in sink | New protocol fields `Partial bool` / `OmittedColumns []string`; sink emits `MERGE INTO t USING (...) ON pk WHEN MATCHED THEN UPDATE SET <present cols> WHEN NOT MATCHED THEN INSERT ...` for partial rows | No source WAL cost | Per-column-set MERGE statement explosion; MERGE + compaction interaction is subtle (a partial update compacting over an insert must merge, not overwrite); Databend MERGE requires enterprise/later versions in some deployments |
| C. UPDATE-instead-of-REPLACE for `OpUpdate` | Partial updates set only present columns | Minimal protocol change | Breaks idempotent replay ordering with inserts; an UPDATE arriving before its row exists (snapshot races, redelivery) is lost; violates I2's single-statement-per-PK model |

**Recommendation: A**, with a **sink-side guard as defense in depth**: add `Partial bool` to
`protocol.Message` (`protocol/message.go:41-55`, msgp regen required) which 01a's source sets
whenever it could **not** reconstruct a full row (e.g. RI FULL not yet applied to that table). The
sink, on `Partial == true`, refuses to REPLACE: `dlqOrFail(..., "partial row image; enable REPLICA
IDENTITY FULL on <table>")`. This converts today's *silent data corruption* (NULLed columns) into a
visible, replayable DLQ event during the migration window. Until 01a lands, the sink change alone
already stops corruption for any message so marked; messages from the pre-01a source (field absent
⇒ false) behave as today, so the two plans can land in either order but only together close I6.

### 2.5 Type fidelity (I5) — Critical 9, 10 + Mediums

**bytea (Critical 10).** `normalizeValue` (`sink.go:575-591`): add `[]byte` to the passthrough
switch:

```go
case string, []byte,
	int, int8, ..., bool, time.Time:
	return v
```

The databend-go driver binds `[]byte` as a binary literal. Verify with an integration round-trip
test (§4); if the driver stringifies, fall back to emitting `unhex('<hex>')`/`TO_BINARY` via a
per-column rewrite — but driver binding is expected to work. Note the decode path: msgpack
preserves `[]byte` (bin family) through `decodePayload` (`:597-612`); the JSON *fallback* branch
(`:607`) yields base64 strings with no type info — document that JSON payloads cannot carry bytea
faithfully and log a warning when the JSON branch is taken for a table whose schema has a BINARY
column (schema is available via `pkCache`'s sibling — add a `colTypes` cache populated in
`ApplySchema`).

**timestamptz.** Same switch: `case time.Time: return v.UTC()`. Databend TIMESTAMP is
timezone-less; the convention is **store UTC** — encode it once in `normalizeValue`, document on
`mapPgTypeToDatabend`.

**Type mapping table (Medium, `sink.go:350-409`).** Replace the substring cascade (whose
`strings.Contains(t, "int")` at `:363` maps `point` and `interval` to INT64) with an exact-match
table plus a small, ordered prefix list:

```go
var pgTypeMap = map[string]string{
	"bool": "BOOLEAN", "boolean": "BOOLEAN",
	"int2": "SMALLINT", "smallint": "SMALLINT",
	"int4": "INT", "int": "INT", "integer": "INT", "serial": "INT",
	"int8": "BIGINT", "bigint": "BIGINT", "bigserial": "BIGINT", "oid": "BIGINT",
	"float4": "FLOAT32", "real": "FLOAT32",
	"float8": "FLOAT64", "double precision": "FLOAT64",
	"numeric": decimal, "decimal": decimal, // computed from precision/scale opts
	"money": decimal,
	"date": "DATE",
	"timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMP",
	"timestamp without time zone": "TIMESTAMP", "timestamp with time zone": "TIMESTAMP",
	"time": "STRING", "timetz": "STRING",
	"interval": "STRING",
	"point": "STRING", "line": "STRING", "lseg": "STRING", "box": "STRING",
	"path": "STRING", "polygon": "STRING", "circle": "STRING",
	"inet": "STRING", "cidr": "STRING", "macaddr": "STRING",
	"uuid": "STRING", "text": "STRING", "citext": "STRING", "name": "STRING",
	"json": "VARIANT", "jsonb": "VARIANT", "xml": "STRING",
	"bytea": "BINARY",
	"bit": "STRING", "varbit": "STRING",
	"tsvector": "STRING", "tsquery": "STRING",
	// OID spellings (kept from :393-407)
	"16": "BOOLEAN", "20": "BIGINT", "21": "SMALLINT", "23": "INT",
	"700": "FLOAT32", "701": "FLOAT64", "1700": decimal,
	"25": "STRING", "1043": "STRING", "1082": "DATE",
	"1114": "TIMESTAMP", "1184": "TIMESTAMP", "17": "BINARY",
	"114": "VARIANT", "3802": "VARIANT", "2950": "STRING",
}
```

Lookup order: strip trailing `[]`/leading `_`/`array` ⇒ VARIANT (keep `:356-358`); exact match on
lowercased name; prefix match only for parameterized forms (`varchar(255)`, `numeric(10,2)`,
`char(...)`, `bit(...)`, `timestamp(3)` — match on the identifier before `(`); default STRING with
a **warn log naming the unmapped type** (today's silent default at `:406` hides drift).

**bigint/numeric precision through the NATS transformer (Critical 9,
`transformer/nats/protobuf.go`).** `structpb.Struct` values are float64-only — this is a wire
format limitation, not a bug in the marshalling code. Two-stage fix:

*Stage 1 (this repo, no contract change): lossless string encoding + response re-typing.*
- `sanitizeValueForStructPB` (`:327-381`): add explicit cases *before* the reflect fallback:
  - `int64`, `uint64`, `int`, `uint`: if the magnitude exceeds 2^53, return
    `strconv.FormatInt/FormatUint` (string); else return as-is (structpb takes it to float64
    losslessly). Always-string is simpler and safer — **recommend always-string for
    int64/uint64/uint** (matches proto3 JSON convention for 64-bit ints).
  - `pgtype.Numeric`, `big.Int`, `big.Float`, and any `fmt.Stringer` numeric the pgx codec
    produces: render to canonical decimal string. (Today these fall to the reflect `Struct` branch
    at `:373-377` and are stringified via `%v` — verify the pgx v5 numeric text output is the plain
    decimal, and pin it with a test.)
- `parseResponseWithOrder` (`:270-303`): after `original.Data = res.TransformedData.AsMap()`
  (`:288-289`), run a **re-typing pass**: for each key, if the *original* `m.Data[key]` was an
  integer type and the response value is (a) a float64 with zero fractional part, or (b) a numeric
  string, coerce back to int64/string-decimal accordingly. Databend's driver accepts decimal
  strings for DECIMAL/BIGINT columns, so string-through-to-the-sink is acceptable for values the
  server didn't touch.
*Stage 2 (contract change, tracked as follow-up): add `bytes data_msgpack = N` to
`cdctransformv1.TransformRecord`/`TransformResult` in `daya-contracts` and pass the raw msgpack
payload through, making structpb advisory. This is the durable fix; Stage 1 makes the current wire
format non-lossy for the common echo-back case.*

### 2.6 Schema evolution & the ddl package — High + Mediums

**Delete the dead ddl package.** `internal/transformer/ddl/ddl.go` is a package clause and nothing
else; `internal/transformer/all.go:4` blank-imports it. Remove both (`all.go` becomes empty ⇒
delete the file; nothing imports `transformer/all` — verify with grep before deleting, and if
something does, keep `all.go` importing `transformer/nats` only). Schema evolution logic belongs in
the sink (it is dialect-specific), not a transformer.

**Make evolution explicit and policy-driven.** `ApplySchema` (`sink.go:249-317`) gets a policy
option:

```go
type EvolutionPolicy struct {
	OnAddColumn  string // "apply" (default)
	OnDropColumn string // "ignore" | "drop" | "error"  (default "error")
	OnTypeChange string // "error" | "recreate_column"   (default "error")
}
```

- ADD COLUMN failure becomes a returned error (fixes `:310-312` warn-and-continue; consumer nacks
  the schema wmMsg at `consumer.go:333-339` and retries — correct, since data messages for the new
  column will otherwise fail or silently drop the column).
- `m.Diff` (`protocol.SchemaDiff`, `message.go:31-39`) carries `Removed` and `TypeChanges` — today
  completely ignored (the consumer only reconstructs `Diff.Added` into a Schema at
  `consumer.go:312-318`). New handling in `ApplySchema`:
  - `Removed`: per policy — `drop` ⇒ `ALTER TABLE t DROP COLUMN c`; `ignore` ⇒ log at WARN with a
    `cdc_sink_schema_drift` counter; `error` (default) ⇒ return `sink.Terminal` with a message
    telling the operator to choose a policy. **Never silent.**
  - `TypeChanges`: default `error` (Databend `ALTER TABLE ... MODIFY COLUMN` support is limited and
    conversion is lossy in general). `recreate_column` (opt-in) ⇒ add `c__new`, leave backfill to
    the operator — document as escape hatch only.
  - Renames arrive as Removed+Added (Postgres logical replication has no rename signal) — the
    policy above covers them; document that a rename under `OnDropColumn: drop` loses history.
- The consumer's diff-to-schema reconstruction (`consumer.go:312-318`) must pass the whole `Diff`
  through instead of flattening to `Added` only: keep `m.Diff` populated on the message it hands to
  `ApplySchema` (it already is — the flattening only affects `m.Schema`; `ApplySchema` should read
  `m.Diff` directly when present).

### 2.7 postgresdebug sink correctness — round-2 Mediums

Not on the delivery path (BatchUpload is a no-op for data ops, `postgresdebug/sink.go:162-171`),
but two total-data-loss paths and one observability-corruption bug live here:

1. **`cleanupByCount` wipes the table when `MaxCount == 0`** (`sink.go:263-276`): guard
   `if s.config.Retention.MaxCount <= 0 { log.Warn(...skip); return }`.
2. **Sub-hour retention truncates to 0 hours ⇒ deletes everything** (`sink.go:250,256`
   `int(MaxAge.Hours())`): switch to seconds — `int64(MaxAge.Seconds())` and
   `($1 || ' seconds')::INTERVAL`; additionally validate `MaxAge > 0` in `ParseOptions`.
3. **float64-only numeric option parse** (`config.go:109,137,151`): reuse the coercion helper —
   move `asInt` from `databend/sink.go:142-160` to a shared `internal/sink/options.go` and use it
   for `max_count`, `sampling.value`, `table_overrides[].value`. A YAML/JSON int decoded as
   `int`/`int64` no longer silently leaves `MaxCount` 0 (which is what arms bug #1).
4. **`batchInsert` commits partial batches** (`hooks.go:241-265`): a failed
   `stmt.ExecContext` is logged and the loop proceeds to `tx.Commit()`. For a *debug* sink,
   partial capture is arguably tolerable, but silent gaps defeat its purpose: count failures and
   if `failed > 0` return an aggregated error after `Commit` (callers already just log,
   `hooks.go:83-85,177-180`) — plus increment a `debug_sink_insert_failures_total` metric.
5. **`filteredIndices` misalignment** (`engine/consumer.go:163,181,199` +
   `postgresdebug/hooks.go:91-182`): `processMessages` appends indices *into the current
   `processed` slice*, but after the first dropping transformer, `processed` indices no longer
   correspond to `msgs`/`correlationIDs` positions (CaptureAfter indexes `correlationIDs[idx]`
   at `hooks.go:100`, and reconstructs the before/after pairing positionally at `:110-135`).
   Fix in the engine, shape-compatible for hooks: carry an origin-index map through the pipeline —

   ```go
   origIdx := make([]int, len(processed)) // origIdx[i] = index into msgs
   for i := range origIdx { origIdx[i] = i }
   // on every drop: filteredIndices = append(filteredIndices, origIdx[i])
   // on every keep into newProcessed: newOrigIdx = append(newOrigIdx, origIdx[i])
   ```

   Both the batch branch (`:157-169`) and scalar branch (`:171-195`) rewrite their drop bookkeeping
   in terms of `origIdx`. `CaptureAfter`'s positional reconstruction then receives original-space
   indices as its docs already assume; also pass `origIdx` (as a new final param or via filtered
   set) so `:110-135` can map `transformed[j] -> originals[origIdx[j]]` exactly instead of
   inferring by skip-counting.

---

## 3. Ordered work items

Legend: **[S#]** = sink plan item. Each lists files, change, and why. Items S1–S4 are the delivery
core and should merge as one reviewed unit if possible (they change one contract together).

**S1. DLQ stream + publisher wiring (I1 preconditions)**
- Files: `internal/sink/sink.go` (add `DLQPublisher`, `DLQAware`), `internal/sink/databend/dlq.go`
  (move `DLQPublisher` iface out, keep event type + `buildDLQMessage`),
  `internal/engine/factory.go:92-110` (wire `SetDLQPublisher(f.Publisher, ...)` after `sink.New`),
  NATS bootstrap (wherever ingest streams are declared — `internal/stream/nats` /
  `internal/infra/nats.go`) to ensure a JetStream stream binds `cdc.sink.>` with sensible retention
  (e.g. limits: 7d / size cap; DLQ must not be a Limits-retention stream that silently drops —
  choose WorkQueue or interest with an operator consumer, decide with ops).
- Why: without a durable, always-wired DLQ, I1 is unimplementable — failure must otherwise always
  nack.

**S2. `BatchUpload` durability + terminal error taxonomy (Critical 3)**
- Files: `internal/sink/errors.go` (new), `internal/sink/sink.go` (contract docs),
  `internal/sink/databend/sink.go`: rewrite `emitDLQ` → `dlqOrFail` (`:618-662`); route `:431-435`,
  `:693-697`, and the missing-PK delete/upsert cases through it; aggregate DLQ failures into the
  return value. `internal/engine/consumer.go`: in `flush`/`handleSinkError`, branch on
  `sink.IsTerminal` → `isolatePoisonBatch` immediately.
- Why: restores at-least-once; nil return again means "durable".

**S3. Per-table ordered apply + per-PK last-write-wins compaction (Critical 4, 5)**
- Files: `internal/sink/databend/sink.go`: rewrite `BatchUpload` (`:176-212`) per §2.2; fold
  `deleteTableBatch` (`:664-721`) into a per-table `applyTableBatch(ctx, table, ops)`; add
  `compactOps`, `pkKeyOf`; add chunked multi-row DELETE builder; sort group keys before the loop at
  `:471`.
- Why: I2/I3 — delete/upsert races and random group order are the two nondeterminism sources.
- Depends: S2 (missing-PK handling uses `dlqOrFail`), S4 (PK resolution).

**S4. PK metadata persistence + resolution (Critical 6, Medium N7)**
- Files: `internal/sink/databend/sink.go`: `ApplySchema` writes `cdc_sink_meta` (`:261-264`
  extended); new `resolvePK`; delete `refreshPrimaryKey`/`ensureFallbackPK`/`parsePKFromDDL`/
  `sinkPKRegex`/`{"id"}` fallbacks (`:46,452-454,681-683,723-833`); create meta table lazily on
  first write (guarded like `pkLoaded`); optional `CLUSTER BY` on CREATE TABLE (`:289-290`).
  `internal/sink/databend/metrics.go`: repoint `SinkPKResolved` semantics.
- Why: I4 — after restart, non-`id`-PK tables currently lose every delete and REPLACE on the wrong
  key. `SHOW CREATE TABLE` can never recover PK on Databend; stop pretending.

**S5. Partial-update guard (Critical 7, sink half) [DEP: 01a]**
- Files: `internal/protocol/message.go` (add `Partial bool \`msg:"part,omitempty"\``; run
  `go generate` for msgp — `message_gen.go` regenerates), `internal/sink/databend/sink.go`
  (`applyTableBatch`: `if m.Partial { dlqOrFail(...) }`).
- Why: converts silent NULLing of TOAST columns into visible DLQ until/unless 01a's RI-FULL merge
  makes rows whole. Coordinate field name + semantics with 01a before merging either side.

**S6. Type fidelity in the sink (Critical 10 + Medium type map + tz)**
- Files: `internal/sink/databend/sink.go`: `normalizeValue` (`:575-591`) — add `[]byte`
  passthrough and `time.Time → .UTC()`; replace `mapPgTypeToDatabend` (`:350-409`) with the table
  in §2.5; add unmapped-type WARN; add `colTypes` cache in `ApplySchema` for the JSON/bytea
  warning.
- Why: I5 — bytea corruption is unconditional today; point/interval become INT64 and then fail (or
  worse, coerce) at insert.

**S7. Numeric precision through the NATS transformer (Critical 9)**
- Files: `internal/transformer/nats/protobuf.go`: `sanitizeValueForStructPB` (`:327-381`) 64-bit →
  string cases + pgtype.Numeric/big.\* handling; `parseResponseWithOrder` (`:270-303`) re-typing
  pass against original value types.
- Follow-up ticket (not this repo): `daya-contracts` msgpack passthrough field (Stage 2).
- Why: I5 — every pipeline using the transform server round-trips all data through float64 today.

**S8. Schema evolution policy (High) + ddl package removal (Low)**
- Files: `internal/sink/databend/sink.go` `ApplySchema` (`:299-316`): ADD failure returns error;
  handle `m.Diff.Removed`/`TypeChanges` per policy; `WithOptions` gains `evolution` block.
  `internal/engine/consumer.go:312-318`: stop discarding Diff (pass through). Delete
  `internal/transformer/ddl/ddl.go` and `internal/transformer/all.go` (after grepping importers).
- Why: I7 — drift is currently invisible; ADD failure currently acks the schema message.

**S9. `validateIdentifier` scope (Medium)**
- Files: `internal/sink/databend/sink.go:236-247`: `quoteIdentifier` (`:225-232`) already makes
  arbitrary names injection-safe by doubling quotes; relax validation to reject only empty names,
  NUL, and backtick/control characters, so legal mixed-case/hyphenated Postgres identifiers stop
  being poison pills. Validation failures that remain are wrapped `sink.Terminal` (S2 routes them
  to isolation/DLQ instead of the forever-retry loop when `EnableDLQ=false`).
- Why: a single table with a quoted name currently wedges the whole consumer.

**S10. postgresdebug fixes (Mediums N4 + retention + config + hooks)**
- Files: `internal/sink/postgresdebug/sink.go:249-277` (MaxCount guard, seconds-based interval),
  `config.go:109,137,151` (shared `asInt`), `hooks.go:241-265` (fail on partial insert),
  `internal/sink/options.go` (new shared helper, moved from `databend/sink.go:142-160`).
- Why: two total-data-loss paths in the debug store; config bug arms them.

**S11. `filteredIndices` origin-index fix (Medium)**
- Files: `internal/engine/consumer.go:102-203` (origIdx threading per §2.7),
  `internal/sink/sink.go` hook signatures if `origIdx` is passed explicitly,
  `internal/sink/postgresdebug/hooks.go:91-182` (use exact mapping).
- Why: debug-sink audit trail mislabels which records were filtered whenever ≥2 transformers drop.

**Cross-plan dependencies:**
- S5 ⇄ 01a (protocol `Partial` field + RI FULL/old-tuple merge). Land protocol field first, in one
  PR both plans reference.
- S2's "nil return authorizes ack" only fixes at-least-once *from the sink back*; the source-side
  ack-before-publish (Critical 1/2) is 01a's job — both must land for end-to-end at-least-once.
- S1's DLQ stream declaration should live next to the engine's ingest-stream bootstrap (owned by
  the engine/infra plan if one exists; otherwise do it here).

---

## 4. Test plan — the missing invariant tests

Existing coverage (`sink_remediation_test.go`, `sink_quoting_test.go`, `sink_test.go`,
`test_helpers_test.go`) exercises chunking, type-map strings, DLQ-on-decode-failure, and PK
regex parsing — none of the invariants. The fake `DBExec`/`DBRows` (`db.go:11-25`) records
executed SQL, which is exactly what the new tests need. New tests, by invariant:

**I1 — durability (unit, databend pkg + engine pkg):**
- `TestBatchUpload_NoDLQPublisher_ReturnsError`: undecodable payload, `dlqPublisher == nil` ⇒
  non-nil error (inverts today's `TestBatchUpload_DeserializationFailure_NoPublisher` at
  `sink_remediation_test.go:677`, which asserts the *bug*: nil return).
- `TestBatchUpload_DLQPublishFails_ReturnsError`: publisher stub returns error ⇒ non-nil.
- `TestBatchUpload_DLQPublishSucceeds_ReturnsNil_AndRecordsRest`: mixed batch, bad record DLQ'd,
  good records still applied, nil return.
- Engine: `TestFlush_SinkError_DoesNotAck` and `TestFlush_TerminalError_Isolates` against a mock
  sink + recorded wmMsg ack/nack (extend `engine/mocks`).

**I2/I3 — ordering & idempotency (unit):**
- `TestApplyTableBatch_DeleteThenInsertSamePK`: ops `[DELETE id=1, INSERT id=1 v=2]` ⇒ recorded SQL
  contains a REPLACE for id=1 and **no** DELETE for id=1 (compacted), 100 iterations to flush out
  map-order nondeterminism.
- `TestApplyTableBatch_InsertThenDeleteSamePK`: inverse ⇒ DELETE only.
- `TestApplyTableBatch_TwoUpdatesDifferentColumnSets_SamePK`: last update wins even when the two
  updates land in different column-set groups (the exact Critical-5 shape).
- `TestApplyTableBatch_Idempotent`: apply the same batch twice against a stateful fake ⇒ identical
  final state.
- `TestBatchUpload_GroupOrderDeterministic`: N runs produce identical SQL sequence.
- `TestApplyTableBatch_CompositePKDeleteChunking`: composite PK, > maxPlaceholders rows ⇒ multiple
  DELETE statements, each within budget, all rows covered.

**I4 — PK metadata (unit + integration):**
- `TestApplySchema_PersistsPKMeta`: ApplySchema ⇒ REPLACE INTO cdc_sink_meta recorded with JSON pks.
- `TestResolvePK_FromMetaAfterRestart`: fresh sink instance (empty caches), fake DB returns meta
  row ⇒ deletes use the real PK. This is the "non-id PK after restart" regression test — the
  headline Critical-6 scenario: restart, then `DELETE` on table with PK `(tenant_id, order_id)` ⇒
  correct WHERE clause, no silent skip.
- `TestResolvePK_MissingMeta_RetryableError_NoIDGuess`: no meta, no cache ⇒ error, and asserted
  **no** SQL containing `"id" = ?` was emitted.
- `TestDelete_MissingPKValueInRow_DLQNotSkip`: row lacks a PK column ⇒ DLQ publish recorded, not a
  silent continue.

**I5 — type fidelity:**
- Unit: `TestNormalizeValue_ByteaPassthrough` (`[]byte` in ⇒ same `[]byte` arg out, not a
  base64-JSON string), `TestNormalizeValue_TimeUTC`.
- Unit: `TestMapPgType_PointIntervalNotInt` (point/interval/inet ⇒ STRING; bigint ⇒ BIGINT;
  timestamptz ⇒ TIMESTAMP), table-driven over the whole §2.5 map.
- Integration (build-tagged, real Databend via docker-compose, alongside `internal/test/e2e`):
  `TestByteaRoundTrip` — insert 256-byte sequence incl. NUL through the full sink path, read back,
  byte-equal. `TestNumericPrecisionRoundTrip` — `numeric(38,9)` value `12345678901234567890.123456789`
  and `bigint` `2^63-1` survive value-equal.
- Transformer unit (`protobuf_test.go`): `TestSanitize_Int64Above2p53_String`,
  `TestParseResponse_RetypesIntegers` (echo server stub returns the request; original int64
  `9007199254740993` survives ≠ float64 rounding), `TestSanitize_PgNumeric_DecimalString`.

**I6 — partial updates [DEP: 01a]:**
- `TestBatchUpload_PartialFlagged_DLQ`: `Partial: true` update ⇒ DLQ'd with reason, not REPLACEd.
- e2e (with 01a): TOAST column > 8KB, update a *different* column ⇒ sink row retains the TOAST
  value (the end-to-end Critical-7 regression).

**I7 — schema evolution:**
- `TestApplySchema_AddColumnFailure_ReturnsError` (inverts current warn-only behavior).
- `TestApplySchema_DropColumn_PolicyError/Drop/Ignore` (three policies, Diff.Removed set).
- `TestApplySchema_TypeChange_DefaultErrors`.

**postgresdebug retention edge cases:**
- `TestCleanupByCount_ZeroMaxCount_Skips` (no DELETE executed — inverts the wipe).
- `TestCleanupByAge_SubHourRetention_UsesSeconds` (`MaxAge: 30m` ⇒ interval `1800 seconds`, not
  `0 hours`).
- `TestParseOptions_IntMaxCount` (`max_count` as Go `int` and `int64`, not just float64).
- `TestBatchInsert_RowFailure_ReturnsError`.
- `TestCaptureAfter_TwoDroppingTransformers_CorrectFilterAttribution` (engine-level: transformer A
  drops index 1, transformer B drops what is now index 1 (originally 2) ⇒ hooks mark originals 1
  and 2, not 1 twice).

**CI note:** none of this matters if nothing runs it (confirmed Critical 20 — no test gate). The
CI plan must add `go test ./...` + the build-tagged integration suite before this plan's fixes can
be considered "kept".

---

## 5. Rollout & migration

**Phase 0 — prerequisites.** Declare the sink DLQ JetStream stream (S1) in all three deployment
universes (docker-compose, helm, `k8s/` raw — they are known to drift; touch all three). Deploy is
a no-op for behavior.

**Phase 1 — sink binary with S1–S4, S6, S9.** On rollout to an existing deployment:
- `cdc_sink_meta` does not exist ⇒ created lazily on first `ApplySchema`.
- **Backfill is automatic on restart**: the source primes a schema message (with `PKColumns`) for
  every configured table synchronously on every `startConnector` run
  (`internal/source/postgres/source.go:540-556`), and discovery does the same for new tables
  (`:920-950`). So the first pipeline (re)start after deploy writes meta rows for every table
  before data flows. Tables therefore never hit the "meta absent" error in steady state; the
  retryable-error path only covers the small window between a data message racing ahead of its
  schema message on a *brand-new* sink database.
- Ordering caveat: the schema message and first data batch travel the same stream in order, so
  ApplySchema lands first in the normal case; the retryable resolvePK error covers redelivery
  reorderings.
- **Existing data is already corrupted** and no code fix repairs it. Per-table audit + re-sync:
  1. Tables with PK ≠ `id` that had deletes while any pre-fix worker restart occurred: rows exist
     in Databend that were deleted in Postgres, and REPLACE-on-wrong-key may have created
     duplicates. ⇒ full re-snapshot (drop + resync) or a reconciliation diff (count + PK anti-join
     via external tooling).
  2. Tables with `bytea` columns: every value is a base64-JSON string ⇒
     `UPDATE t SET c = from_base64(trim_both('"', c))` is *not* reliable (BINARY column already
     holds mangled bytes) ⇒ re-snapshot those tables.
  3. Tables with TOAST columns updated under RI DEFAULT: NULLed columns ⇒ re-snapshot after 01a.
  Recommend a single operator runbook step: after Phase 1 + 01a are live, re-snapshot every
  affected table once (the pipeline supports per-table snapshot; 01a fixes the mid-snapshot-crash
  hole first).
- Feature-flag safety valve: option `pk_fallback_id: true` restores the old `{"id"}` guess per
  sink for an emergency rollback window; default false; delete the flag after one release.

**Phase 2 — S5 protocol field + 01a source.** Deploy order: protocol field (readable by old
binaries — msgp `omitempty` unknown-field tolerance means old sinks ignore it) → new sink (guards
on it) → new source (sets it / RI FULL merge). Enabling RI FULL on large tables is an operator
action with WAL-volume implications; stage per table.

**Phase 3 — S7 transformer + S8 evolution policy + S10/S11.** Independent, low-risk deploys.
Evolution policy defaults to `error` — announce to operators that previously-silent drops/renames
will now stop the pipeline until they set a policy; ship with a clear error string containing the
exact option to set.

---

## 6. Risks, open questions, sequencing

**Risks**
- *Behavioral tightening = new visible failures.* Batches that used to "succeed" (by dropping
  records) will now retry or DLQ. Expect DLQ volume and stalled pipelines on day one wherever data
  was already being eaten. This is the point — but ops needs dashboards on `SinkDLQTotal` and the
  new drift/unresolved-PK metrics before rollout.
- *Compaction changes visible history.* Downstream consumers of Databend that relied on seeing
  intermediate row versions within a batch window lose them (last-write-wins). Analytical sinks
  don't normally care; call it out in the changelog.
- *`resolvePK` retryable-on-absent could loop* if a table's schema message is permanently lost
  (e.g. pre-existing stream pruning). Mitigate with a metric + log on every unresolved retry and a
  documented operator action (touch the source config / restart to re-prime schemas).
- *msgp regeneration* (S5) touches generated files; keep it an isolated commit.
- *Terminal-error isolation still gated on `EnableDLQ`* (`consumer.go:725`): with
  `EnableDLQ=false`, a terminal batch now isolates but individual poison messages still Nack
  forever (no `MaxDeliver` — engine-plan territory). Note the interaction explicitly in the engine
  plan.

**Open questions**
1. Databend row-value `IN ((?,?),...)` support for composite-PK deletes across the deployed
   version range — if confirmed, prefer it over OR-of-conjuncts (fewer parse nodes). Verify against
   the actual cluster version; the plan's default is the portable form.
2. databend-go driver binding of `[]byte` args — assumed binary-safe; the bytea round-trip
   integration test is the gate. Fallback documented in §2.5.
3. DLQ stream retention policy (Limits vs WorkQueue) and who consumes/replays it — needs an ops
   decision; a DLQ nobody drains is a slow-motion drop.
4. `pgtype` numeric decode output shape from the vendored `decodeTextColumnData`
   (`tuple/data.go:97-102`) — pin with a unit test before writing the transformer string-encoding
   (S7) so the type switch covers the real runtime types, not assumed ones.
5. Does anything import `transformer/all.go`? (grep before S8's deletion; if the blank-import chain
   is how `nats/protobuf` gets registered in some binary, keep `all.go` with only the live import.)
6. Should `cdc_sink_meta` also mirror to NATS KV for the dashboard? Optional, out of scope here.

**Sequencing (summary)**
```
S1 (DLQ stream+wiring)
  └─ S2 (durability contract) ── S3 (ordering/compaction) ── S4 (PK meta)   ← one review unit
S6 (types), S9 (identifiers)                                                 ← same release
protocol Partial field ──┬─ S5 (sink guard)      [coordinate with 01a]
                         └─ 01a RI-FULL merge
S7 (transformer), S8 (evolution), S10 (postgresdebug), S11 (filteredIndices) ← independent
re-snapshot runbook                                                          ← after S1–S6 + 01a live
```

# Multi-Schema Support — Remediation Plan

**Status:** IMPLEMENTED on `feat/multi-schema-support` (attempt 2). All 31 e2e tests and 10 unit
packages pass. See §12 for the completion record and remaining known gaps.
**Problem:** the pipeline can only ever sync tables in the `public` schema. `SourceConfig.Schemas`
is collected by the UI, stored in KV, and never read by any backend code path.

This revision incorporates three adversarial audits (message-identity blast radius, Stage 1
no-op verification, Databend migration) **and the post-mortem of a failed first implementation
attempt** (§11). Several assumptions were refuted; those are recorded in §9 rather than quietly
deleted.

## 0. Deployment context — this system is NOT in production

It is a test / proof-of-concept deployment. There is no production data to preserve.

This is load-bearing for the whole plan and simplifies it substantially:

- **No data migration.** No CTAS copy, no legacy-unqualified-table guard, no KV schema-version
  marker, no downtime window. Stage 2.5 collapses to sink correctness plus provisioning.
- **The "byte-identical keys" invariant is no longer load-bearing.** It remains a desirable
  property (it keeps the change reviewable and the diff honest) but orphaned checkpoints are
  recoverable here by wiping state and re-snapshotting.
- **No Prometheus dashboards or alerts to migrate** (§7.5).
- **The Stage 1 preflight KV audit is informational, not blocking.**

If this system is ever promoted to production, the migration requirements removed from §7.4 must
be restored *before* the promotion, not after. They are preserved in git history at
`wip/multi-schema-attempt-1`'s parent revision of this document.

---

## 1. The defect, precisely

Table identity is *de-qualified* at the source boundary and re-qualified with a hardcoded
`public` downstream. Five independent hardcodings make non-`public` schemas unreachable:

| # | Site | Behaviour |
|---|------|-----------|
| 1 | `internal/source/postgres/source.go:1541` | `discoverTables` runs `... WHERE table_schema = 'public'` as a literal; `srcConfig.Schemas` is never consulted. |
| 2 | `internal/source/postgres/source.go:1172`, `:1563` | `getTableMetadata` is schema-parameterised, but every caller passes the literal `"public"`. |
| 3 | `internal/source/postgres/source.go:778` | Publication entries omit `Schema`; vendored `config/config.go:104` defaults it to `public`. Configuring `sales.orders` asks Postgres for `public.orders`. |
| 4 | `internal/source/postgres/source.go:502,517,531,550` | Live filtering does `TrimPrefix(tableName, "public.")` then looks up `knownTables`. `sales.orders` never matches; `sales.users` and `public.users` are indistinguishable. |
| 5 | `internal/engine/producer.go:1192` | `fmt.Sprintf("public.%s", tableName)` re-attaches hardcoded qualification for snapshot bookkeeping. |

Consequence: the only real schema filter (`internal/transformer/nats/protobuf.go:154`) matches
`m.Schema.Schema`, always stamped `"public"` — so any filter value other than `public` drops
every message.

Secondary: `internal/api/handler.go:954` `GetSourceSchema` is a **stub** returning a hardcoded
`["public","inventory","sales"]` regardless of source, so the UI shows custom schemas and implies
discovery works.

### 1.1 Pre-existing bugs found during the audit

These exist **today**, independent of multi-schema work. They share a root cause — config-shaped
names and message-shaped names are used interchangeably as keys — so they are fixed as a
by-product of Stage 1, but they should be understood as bug fixes, not refactors.

- **Recovered table state is silently discarded.** `recoverEvoStates` keys `p.tableStates` and
  `p.evoStates` from `p.config.Tables` (`producer.go:530-562`), while the hot path keys them from
  `m.Table` (`producer.go:320,508`). For any config written as `public.orders` the two never
  meet, so snapshot/evolution state is not recovered on restart and buffering decisions run
  against an empty map. A table stuck mid-evolution silently resumes as STABLE.
- **Buffer writes and buffer drains target different streams.** The write path builds the stream
  name from bare `m.Table` (`producer.go:517`); the recovery drain builds it from the raw config
  string (`producer.go:648`). With a qualified config these are two different streams, so
  buffered rows are written to one and drained from another.
- **Restored stats are orphaned.** `LoadStats` seeds `c.stats[sourceID+"."+table]` from
  `p.config.Tables` (`consumer.go:105`) while the runtime keys on `m.Table` (`consumer.go:703`).
  `TotalSynced` resets to zero on every restart while still reporting ACTIVE.
- **`getCurrentColumns` has no schema predicate** on the unqualified path
  (`internal/sink/databend/sink.go:295-310`). With the same table name in two databases it
  returns the union, so `CREATE TABLE IF NOT EXISTS` is skipped and an ALTER is issued based on
  another database's columns.
- **ALTER failures are swallowed.** `sink.go:283-285` logs a warning and returns `nil`, so
  `ApplySchema` reports success while subsequent writes fail on the missing column indefinitely.
- **Sticky wrong primary key.** `refreshPrimaryKey` marks `pkLoaded` *before* querying and installs
  a fallback PK of `["id"]` on failure (`sink.go:790`); `ensureFallbackPK` then refuses to
  overwrite (`sink.go:791-793`). A transient `SHOW CREATE TABLE` failure installs a bogus PK that
  is sticky for the process lifetime, and every later `REPLACE INTO ... ON ("id")` merges on the
  wrong key — **silent row overwrite for composite-PK tables**.
- **`splitQualified` mis-handles 3+ parts** (`sink.go:217-223`): anything not exactly 2 parts
  falls through to unqualified, while `quoteIdentifier` still emits N-part DDL. A test codifies
  the fallback (`sink_quoting_test.go:36-41`). `validateIdentifier` also accepts `a..b`, `.`, and
  leading/trailing dots.

### 1.2 What is already correct

- **The wire already carries schema structurally.** `SchemaMetadata` has both `Table` and
  `Schema` (`internal/protocol/message.go:20-25`).
- **The Databend sink can already render qualified names.** `splitQualified` / `quoteIdentifier`
  handle the 2-part form (`sink.go:214-232`).
- **One JetStream name is already sanitised.** `sanitizeDurableComponent` (`producer.go:614`) is
  `strings.NewReplacer(".", "_", " ", "_", ">", "_", "*", "_")` — a 1:1 substitution over exactly
  four characters, strict identity for names like `orders` or `order_items`, with no lowercasing
  or truncation.

---

## 2. Design

### 2.1 Canonical identity: `TableRef`

One type in `internal/protocol`, the only way a table is named:

```go
type TableRef struct {
    Schema string // never empty after normalisation
    Table  string
}

func (r TableRef) String() string    // "sales.orders" — display, logs, sink targets
func (r TableRef) KeyToken() string  // KV / JetStream safe, see 2.3
func ParseTableRef(s string) (TableRef, error) // "orders" -> {public, orders}
func NormalizeSchema(s string) string          // "" -> "public"
```

Bare names entering from config or the API normalise to `{public, name}` at the boundary. After
the boundary, no code constructs a table name by string concatenation.

### 2.2 Schema travels as a sibling field, not inside `Table`

**Decision (revised — see §9.1).** `Message.Table` stays **bare**. A new sibling field carries
the schema:

```go
type Message struct {
    // ...
    Table       string `msg:"tbl"`            // "orders"  — unchanged
    TableSchema string `msg:"tsch,omitempty"` // "sales"   — new
}
```

Rationale: `Message` already holds `Schema *SchemaMetadata` containing both `Table` and `Schema`.
Encoding the schema *into* the `Table` string creates a second overlapping representation of the
same data, which the codebase already reads from interchangeably (`producer.go:1081` reads
`m.Schema.Schema` while `:1087` keys on `m.Table`).

Keeping `Table` bare eliminates an entire hazard class outright rather than fixing each site:

| Hazard | Site | Why it disappears |
|---|---|---|
| Snapshot-internal tables leak into evolution *and* get persisted into the KV pipeline config | `producer.go:890`, `:1066` | `HasPrefix(m.Table, "cdc_snapshot_")` still matches |
| Debug-sink `ExcludeTables` silently stops excluding (PII exposure) | `hooks.go:294` | Bare operator config still matches bare `m.Table` |
| Per-table sampling overrides silently revert to global | `hooks.go:333` | Same |
| Transformer allowlist matches nothing, messages pass through **untransformed rather than erroring** | `protobuf.go:175` | Same |

It also largely dissolves the config-vs-message shape divergence behind the §1.1 bugs, since both
sides stay bare.

**Costs, both accepted:**
- `message_gen.go` must be regenerated (`//go:generate msgp`).
- In-flight messages already in buffer streams decode `TableSchema` as `""`. `NormalizeSchema`
  maps `""` → `public` on read. This is the same rule legacy config needs, so it is one rule, not
  two.

**Also required:** `SchemaDiff` (`message.go:33`) has a `Table` but **no schema field at all**, so
schema-change events currently carry no schema. It gets the same sibling treatment.

### 2.3 Key token encoding

Qualified names **cannot** be substituted into KV keys or JetStream names as `schema.table`:

- `ParseTableStatsKey` (`config.go:72`) splits positionally on `.`, requires `len(parts) >= 10`
  and asserts `parts[9] == "stats"`. An extra dot makes it return **`nil` silently**; callers at
  `handler.go:228,703,1410` skip nil, so `/status` returns `{"tables":{},"sinks":{}}` with
  HTTP 200 and the dashboard reads zero. Same for the frontend's `split(".")[8]`
  (`web/src/routes/pipelines/$id/index.tsx:92`).
- `Watch(PipelineStatusPrefix(id) + "*")` (`handler.go:1365`) — NATS `*` matches one token.
- `cdc_pipeline_%s_buffer_%s` (`producer.go:517,648`) — a dot makes a **valid but hierarchical**
  NATS subject; the publish succeeds and the message is dropped if no stream captures it.

**Encoding:**

```
KeyToken() = table                     when Schema == "public"
KeyToken() = schema + "=" + table      otherwise
```

`=` is valid in a NATS KV key, is not a token separator, and is not legal in an unquoted Postgres
identifier. Validation rejects any schema/table containing `=` or `.`, keeping the encoding
injective.

The public branch coincides with today's format for **bare-configured** deployments — see §3
Stage 1 for the preflight that establishes whether that holds, and §9.2 for why it is not a
general no-op guarantee.

### 2.4 Identity by layer

| Layer | Representation |
|-------|----------------|
| Config (`SourceConfig.Tables`, `PipelineConfig.Tables`) | `String()`; bare accepted, normalised on read |
| Wire | `Table` (bare) + `TableSchema` (sibling) |
| KV keys, JetStream streams/durables | `KeyToken()` |
| Prometheus `table` label | `String()` — accepted series break, §7.5 |
| Databend target | `String()`, always qualified — migration, §7.4 |

The asymmetry between `KeyToken()` for KV and `String()` for Databend is deliberate: the KV layer
is internal bookkeeping where silent state loss is the failure mode, so stability wins; the
destination is user-facing schema where an unambiguous model is worth a one-time rename.

---

## 3. Staged rollout

### Stage 0 — verify the T0-3 guard survives

`internal/test/e2e/strict_ack_test.go:258-271` places churn tables in `churn_schema` so they
contribute only physical WAL/keepalive traffic, never decoded events. It is the regression guard
for vendored patch T0-3.

The exclusion is **already expressed declaratively** — the harness sets `Schemas: []string{"public"}`
(`env.go:130`) and the comment opens with "lives in a schema NOT in `env.PgConfig.Schemas`". The
author wrote the correct version; the field was inert. Once Stage 2 enforces `Schemas`, the stated
intent becomes real and the test passes unchanged.

- Add an explicit assertion that `churn_schema.keepalive_churn_table` is never discovered, so the
  exclusion is observable rather than inferred from absence of failure.
- Re-verify the test still fails with T0-3 reverted, both before and after Stage 2.

**Exit:** passes; still fails with T0-3 reverted.

### Stage 1 — `TableRef`, sibling schema field, key plumbing

**Preflight gate (blocking).** Run a read-only audit against each live KV bucket answering:
1. Does any stored `Tables` entry contain a `.`?
2. Do any existing keys contain a `public.` token?

If both are no, Stage 1 is key-identical for that deployment. **If either is yes**, that
pipeline's buffer streams must be drained to empty before cutover — orphaned undrained rows are
unrecoverable data loss, not merely a reset. Ship the audit as a small command so it can be run
per environment, not once by hand.

Then:

- Add `TableRef`, `ParseTableRef`, `NormalizeSchema`, `String`, `KeyToken`.
- Add `Message.TableSchema` and the `SchemaDiff` schema field; regenerate msgp. Apply
  `NormalizeSchema` on read so `""` → `public` for in-flight and legacy messages.
- Rewrite the key builders (`config.go:61-106`, `state.go:90-96`) and the inline metadata key
  (`producer.go:1087`, which has **no builder at all**) to take `TableRef` and use `KeyToken()`.
- **Fix `ParseTableStatsKey`** to parse from both ends — fixed prefix tokens 0..7, terminal
  `stats`, everything between is the token. Robust regardless of encoding. Same for the frontend.
- Route the buffer **stream** name (`producer.go:517,648`) through `sanitizeDurableComponent`, as
  the durable name already is. Verified identity for bare names (§1.2).
- Normalise `p.config.Tables` **on read only** — normalising on write makes `onlyTablesChanged`
  (`config/manager.go:396`) see a full-list change and trigger a restart. This fixes the §1.1
  state-recovery, buffer-drain, and stats-orphaning bugs.
- Add `Validate()` rules for `Schemas`/`Tables` (`config.go:248`, currently none): reject `.` and
  `=` in identifiers so `KeyToken()` stays injective.

**Exit:** full suite green; the §1.1 bugs have regression tests; a KV dump before/after is
identical for bare-configured deployments.

### Stage 2 — source connector becomes schema-aware

- `discoverTables` (`source.go:1540`): parameterise `table_schema = ANY($1)` from
  `srcConfig.Schemas`; add the missing `table_type = 'BASE TABLE'` filter; always exclude
  `pg_catalog`, `information_schema`, `pg_toast`, `pg_temp_*`.

  **Empty `Schemas` means `public` only — not all schemas.** The API doc
  (`internal/api/generated.go:279`) currently says empty means all, but the field has never been
  read, so every existing config has it empty. "Empty means all" would silently begin replicating
  every schema on upgrade. Correct the doc comment to match.
- Pass the real schema to `getTableMetadata` (`:1172`, `:1563`); stamp it on `SchemaMetadata`
  (`:1176`, `:1571`) and on `Message.TableSchema`.
- `knownTables` (`:751`) keyed by `TableRef`, dropping the dual bare/qualified entries. Replace
  the `TrimPrefix` filtering (`:502,517,531,550`) with a `TableRef` lookup.

  **Highest-risk edit in the plan.** A partial change here returns events as
  `handlerKindFiltered` — self-acked, watermark advanced, row dropped forever, no error. It is
  invisible to any public-only test. Land it with the §5 cross-schema test in the same change.
- Set `Schema` explicitly on every `publication.Table` (`:778`) so the vendored default never
  fires. Quote `"schema"."table"` in `AlterPublication` (`:1443`).
- Remove `fmt.Sprintf("public.%s", ...)` (`producer.go:1192`) — with any qualification it yields
  `public.sales.orders` and the `snapshotInProgress` guard stops matching, permitting two
  concurrent chunked snapshots of one table.
- Fix `t == m.Schema.Table` against `p.config.Tables` (`producer.go:1072-1083`): a shape mismatch
  makes every discovery tick decide `isNew`, appending duplicates and re-persisting the pipeline
  config to KV — unbounded config growth logged as normal discovery.
- Pin `search_path` explicitly on the replication connection, re-applied on reconnect, *and*
  fully qualify every identifier we emit.

**Exit:** §5 cross-schema tests pass; public-only suite unchanged.

### Stage 2.5 — Databend target migration (downtime)

Runs after Stage 2, which is what first sends a qualified name to the sink. Detail in §7.4.
**The only stage requiring downtime.**

### Stage 3 — API and UI

- Replace the mocked `GetSourceSchema` (`handler.go:954`) with a real `pg_namespace` query.
- `ListSourceTables` (`handler.go:1030`): parameterise the schema predicate and **return schema in
  `TableMetadata`** — today it drops it (`ID: tableName`), so same-named tables collide.
- Wire `SourceConfig.Schemas` through the mappers (`mappers.go:265,286`) — the dead field.
- UI: populate the schema picker from the real endpoint; display tables qualified.

**Exit:** a source created with `schemas: [sales]` syncs `sales.*` and nothing else.

### Stage 4 — vendored fork

In-tree, with `// vendored-patch:` markers and `PATCHES.md` entries per existing convention.

- `pq/publication/table.go:70` `Tables.Diff` keys on `Name+ReplicaIdentity` — schema-blind, so
  `a.t` and `b.t` diff as one table. Include `Schema`.
- `pq/publication/publication.go:117` splits on `.` taking `st[1]`/`st[0]` — misparses or panics
  on unqualified or multi-dot names.
- `pq/snapshot/coordinator.go:1206,1232` hardcode `public` *and* interpolate the table name into
  SQL. Parameterise both.
- Consider requiring non-empty `Schema` in `Table.Validate()` so the `config.go:104` default
  becomes unreachable rather than silently active.

**Exit:** publication diffing is correct across two schemas holding same-named tables.

---

## 4. Ordering constraints

- **Stage 0 before Stage 2** — otherwise the T0-3 guard reddens for the wrong reason and risks
  being "fixed" by weakening it.
- **Stage 1 before Stage 2** — Stage 2 first emits a non-public schema; without token-safe keys
  and a fixed `ParseTableStatsKey`, the first non-public table silently drops its stats.
- **Stage 1's preflight before Stage 1** — blocking, see above.
- **Stage 2.5 after Stage 2, before Stage 3** — until it runs, the §7.4 guard deliberately holds
  pipelines down.
- Stages 3 and 4 are independent; both need Stage 2.
- Only Stage 2.5 requires downtime; 0, 1, 3, 4 are rolling-safe.

---

## 5. Test plan

Harness is testcontainers (`containers.go:25`, `postgres:16-alpine`, `wal_level=logical`); no SQL
fixtures, all DDL inline.

- **Harness:** `SeedPostgres` (`env.go:213`) hardcodes unqualified DDL and takes no schema.
  Generalise to a `TableRef` with `CREATE SCHEMA IF NOT EXISTS`. `env.go:130` hardcodes
  `Schemas: ["public"]`.
- **Non-public sync:** seed `sales.orders`, assert rows reach Databend.
- **Cross-schema collision:** `public.users` *and* `sales.users` with distinguishable rows; assert
  both sync and neither overwrites the other's checkpoint, stats, or target table. This is the
  test that would have caught the original defect class.
- **Schema whitelist:** `Schemas: [sales]`, create a table in `public`, assert it is not
  discovered.
- **Filtering regression (Stage 2's silent failure):** assert a non-public table's events are
  *not* returned as `handlerKindFiltered`. Without this, the highest-risk edit has no guard.
- **Legacy decode:** a message encoded without `TableSchema` decodes to `{public, table}`.
- **Unit — `TableRef`:** round-trip; `KeyToken` injectivity; explicit
  `{public, orders}.KeyToken() == "orders"`.
- **Unit — `ParseTableStatsKey`:** qualified and unqualified both parse; malformed still `nil`.
- **Unit — §1.1 bugs:** state recovery, buffer drain, and stats restore each survive a restart
  with a `public.`-prefixed config.
- **Regression:** the whole existing e2e suite passes untouched after Stage 1.

---

## 6. Databend behaviour — verified empirically

Probed against `datafuselabs/databend:latest` (the image the e2e harness uses), via the v1 HTTP
API. Results are facts, not assumptions.

**Naming model.** Databend is `catalog.database.table`; there is no separate "schema" layer. A
Postgres *schema* maps to a Databend **database**. A 2-part name resolves as `database.table`
inside the `default` catalog — the server's own error text renders `public_target.orders` as
`"default"."public_target".orders`.

| # | Question | Result |
|---|----------|--------|
| 1 | Is `information_schema.columns.table_schema` populated with the database name? | **Yes.** Rows come back as `['default','orders','id']` / `['sales','orders','id']`. The qualified existence-check path is sound. |
| 2 | Does an unqualified lookup leak across databases? | **Yes — hazard confirmed.** `WHERE table_name='orders'` with no schema predicate returned rows from *both* `default` and `sales`. This is the §1.1 `getCurrentColumns` bug, reproduced. |
| 3 | Does `CREATE TABLE` in a missing database fail loudly? | **Yes.** Error code **1003**, `Unknown database 'nosuchdb'`. |
| 4 | Can `RENAME TABLE` move a table across databases? | **No.** Error code **1006**, `Rename table not allow modify catalog or database`. See §7.4. |
| 5 | Does `CREATE TABLE ... AS SELECT` work cross-database? | **Yes**, row-preserving; `SHOW CREATE TABLE` output matched on a simple table. |
| 6 | Is a quoted 3-part identifier accepted? | **Yes** — read as `catalog.database.table`. So `quoteIdentifier`'s N-part output is silently *valid*, which is why the `splitQualified` mismatch never errors. |

**Error codes for the DDL classifier (§7.4):** `1003` unknown database, `1025` unknown table,
`1006` rename restriction. These are stable and specific enough to classify permanent-vs-transient
without string matching.

### Still to verify before implementation

- **Preflight audit results** per environment (Stage 1 gate).
- **Prometheus dashboard/alert inventory** keyed on bare `table` values (§7.5).
- **CTAS fidelity on real tables** — the probe used a two-column table. Confirm cluster keys,
  nullability, and non-trivial types survive before relying on it for production data (§7.4).

---

## 7. Migration and compatibility

### 7.1 KV and JetStream

Under the §2.3 encoding, bare-configured deployments keep byte-identical keys and stream names;
non-public tables are new identities with nothing to migrate. Deployments failing the preflight
need their buffer streams drained first.

### 7.2 Config normalisation

`SourceConfig.Tables` / `PipelineConfig.Tables` hold whatever the user typed. **Normalise on read
only**; comparing normalised forms in `onlyTablesChanged` (`config/manager.go:396`) avoids a
spurious restart.

### 7.3 Debug-sink filters

With `Table` bare (§2.2) operator filters keep matching. Still worth hardening: `matchesWildcard`
(`hooks.go:377-385`) converts `*` to `.*` without escaping `.`, so `order.` matches `orderX`, and
an invalid pattern is swallowed by `matched, _ :=`. Log filters that match nothing.

### 7.4 Databend targets — mapping and provisioning

**Mapping (DECIDED, and the cause of attempt 1's total failure — see §11):**
a Postgres **schema** maps to a Databend **database**. `public.orders` targets Databend database
`public`, table `orders`. Databend is `catalog.database.table` with no schema layer (§6).

This means the target database is **no longer the one the DSN selects**. That is an accepted
break precisely because §0 applies: there is no production data stranded by it. In a production
deployment this mapping would require the full CTAS migration removed from this plan.

**Provisioning: auto-create, DEFAULT ON.** A sink option `auto_create_schema` issues
`CREATE DATABASE IF NOT EXISTS` for the target database before `CREATE TABLE`.

> **Changed from the earlier decision (was: opt-in, default off).** The original rationale was
> keeping DDL privileges out of the pipeline credential in production. §0 removes that rationale,
> and attempt 1 proved the cost of default-off concretely: the e2e suite fails on the ordinary
> **public** path with `databend database "public" does not exist and auto-provisioning is
> disabled`, redelivering in a hot loop. Default-off makes the common case broken-by-default.
> The option is retained so it can be flipped off when this reaches production.

Requirements:
1. `auto_create_schema` defaults to **true**; the e2e harness must exercise both settings.
2. When **false**, validate at startup that every target database exists and refuse to start with
   an error naming what is missing — never fall into per-message retry.
3. **No migration machinery** (guard / version marker / CTAS). Removed per §0.

**Sink correctness fixes — these are pre-existing bugs and are still required (§1.1):**
4. `pkCache` is written by `ApplySchema` as `pkCache[schema.Table]` and read by the upload path as
   `pkCache[m.Table]`. **These must be qualified identically or not at all** — qualify one only and
   the real PK is never found, silently falling back to `["id"]` and merging distinct rows via
   `REPLACE INTO`. Attempt 1 got this wrong in a new way (§11).
5. Sticky wrong-PK: `refreshPrimaryKey` marks `pkLoaded` *before* querying and installs `["id"]` on
   failure (`sink.go:790`); `ensureFallbackPK` then refuses to overwrite (`:791-793`).
   **`ApplySchema` must not set `pkLoaded` when `PKColumns` is empty** — attempt 1 reintroduced the
   bug through exactly that door.
6. Swallowed ALTER failure (`sink.go:283-285`) returns `nil` on error. Audit *every* sink error
   path for the same pattern, not just this one.
7. `getCurrentColumns` (`sink.go:295-310`) has no schema predicate on the unqualified path.
   Verified live: with `orders` in two databases it returns the union of both.
8. `splitQualified` (`:217`) / `validateIdentifier` (`:236`): reject anything that is not exactly
   1 or 2 **non-empty** components. `sink_quoting_test.go:36-41` currently codifies the bad 3-part
   fallback and must be updated honestly, not deleted.
9. DDL error classification: permanent (1003 unknown database, syntax, privilege) vs transient
   (connection, timeout). Permanent → set table Failed and dead-letter. Today DLQ is wired only for
   deserialization failures (`sink.go:437`), so one permanent error loops forever.
10. `internal/sink/postgresdebug/hooks.go:377-385` `matchesWildcard` converts `*` to `.*` without
    escaping `.`, so `order.` matches `orderX`; an invalid pattern is swallowed by `matched, _ :=`.

### 7.5 Prometheus

The `table` label carries qualified `String()`, so every series changes value. Per §0 there are no
production dashboards or alerts to migrate, so this is a free change here. Recorded only so it is
not rediscovered as a surprise later.

### 7.6 DLQ

`SinkDeadLetterEvent.Table` (`dlq.go:26`) switches form with no version field. Replay tooling or
dashboards filtering on bare names break silently.

---

## 8. Resolved decisions

1. **Identity** — `(schema, table)` everywhere, via `TableRef`.
2. **Message shape** — bare `Table` + sibling `TableSchema`; `SchemaDiff` gains a schema field.
   *(Revised — §9.1.)*
3. **Key token** — stable-for-public `orders` / `sales=orders`.
4. **Empty `Schemas`** — `public` only, not all schemas; API doc corrected.
5. **Databend targets** — always qualified, with guard + KV version marker. Migration is a
   resumable **CTAS copy**, not a rename (Databend forbids cross-database rename, §6); the
   downtime and transient storage cost is accepted in exchange for a uniform model.
6. **Schema provisioning** — auto-create, **opt-in per sink**, default off; loud validation when off.
7. **DDL failures** — classify permanent vs transient; dead-letter the permanent.
8. **Vendored dep** — in-tree for Stage 4; fork promotion out of scope.
9. **Prometheus** — qualified label, accepting the break.
10. **`search_path`** — pinned on the replication connection *and* identifiers fully qualified.

## 9. Refuted assumptions from earlier revisions

Kept deliberately, so they are not re-adopted.

### 9.1 "Qualify `Message.Table` as a string"

Originally approved, now **reversed**. It creates a second representation of schema alongside the
existing `Schema *SchemaMetadata`, and requires individually fixing ~10 string-matching sites
where a miss fails *silently* — including debug-sink excludes (a PII exposure) and the
`cdc_snapshot_` guard (durable config corruption). The sibling field eliminates the class. See §2.2.

### 9.2 "Stage 1 is a byte-identical no-op deploy"

**Refuted.** The claim assumed every table string reaching the key builders is already bare. It is
not: message-derived names are bare, but config-derived names are whatever the operator typed, and
`recoverEvoStates` feeds those raw into `SchemaEvolutionKey`, `TableStateKey` and
`IngressCheckpointKey`. For a `public.orders` config, today's keys and stream names differ from the
normalised ones, orphaning state and abandoning a buffer stream that may hold undrained rows.
No-op is a property of *bare-configured deployments*, not of the change — hence the Stage 1
preflight gate.

### 9.3 "A key-format change re-snapshots every table"

**Overstated.** The re-snapshot trigger at `producer.go:581` is commented out ("Avoid triggering
restart for initial tables"); missing checkpoints there only log. The real consequence of key loss
is state-map loss — wrong buffering and drain behaviour — plus `pipeline.go:173-190` finding no
egress checkpoints and treating a long-running pipeline as new.

## 10. Scope boundary

Not in this plan: promoting the vendored fork to a separate repository; any non-Postgres source
(Postgres is the only source connector); rewriting historical Prometheus series or historical
`postgres_debug` rows.

---

## 11. Post-mortem of attempt 1, and mandatory requirements

An automated staged implementation was attempted and **failed completely**. The result is
preserved on branch `wip/multi-schema-attempt-1` for reference. It built, all 10 unit packages
passed, and every stage self-reported success — while the e2e suite was red on the ordinary
**public-schema** path. Read this section before implementing.

### 11.1 Root cause: the design was half-landed

The source connector qualified, the **engine dropped the qualification**, and the sink assumed it
was present. `grep TableSchema internal/engine/*.go` returned **five lines, all readers, zero
writers**. `SchemaDiff.TableSchema` had no producer and no consumer anywhere.

A partially-threaded identity is **worse than no change at all**: each layer is individually
plausible and the seam between them fails silently.

### 11.2 Mandatory requirements

These are not suggestions. Attempt 1 violated each one.

1. **The engine MUST populate `TableSchema`, not just read it.** Every site constructing a
   `protocol.Message` sets it. Specifically: `emitSchemaChange` (`producer.go:999`), the inline
   copy at `:341`, and `performChunkedSnapshot` (`:1466`). `consumer.go:381-386` must populate
   `SchemaMetadata.Schema` when reconstructing metadata from a diff.
   **Acceptance check:** `grep -rn TableSchema internal/engine/ | grep -v _test` shows writers,
   not only readers.
2. **`Message.Table` MUST stay bare, everywhere, with no exceptions.**
   `performChunkedSnapshot` put a config-shaped qualified string into it. Any qualified value in
   `Message.Table` silently breaks the `cdc_snapshot_` guards at `producer.go:890`/`:1066`.
3. **One `TableRef` per table, derived once at each boundary, then threaded.** Never re-derive
   from a raw string mid-function. Attempt 1's worst bug: `handleDynamicTables` parsed a correct
   `TableRef` and then used the **raw string** six lines later for the state key, `setTableState`,
   the ingress checkpoint and `flushBuffer`. Writes and reads both succeeded against different
   keys.
4. **Buffer topic and durable name MUST derive from the same `TableRef` on the write and drain
   sides.** This is §1.1 bug 2; attempt 1 reintroduced it.
5. **KV keys and in-memory map keys MUST use the same identity.** Attempt 1 produced three
   different key shapes for the same table (`orders`, `sales=orders`, `sales.orders`), and kept
   `evoStates`/`tableStates` keyed bare so `public.orders` and `sales.orders` shared one freeze
   state.
6. **Every `information_schema` query needs a schema predicate.** `getPrimaryKey`
   (`producer.go:1532`) filters only on `table_name`. Grep for `information_schema` and check each.
7. **Cross-stage interaction:** once Stage 2 pins `search_path`, the vendored snapshot
   coordinator's hardcoded `'public'` (`coordinator.go:1205`) becomes actively wrong — it creates
   `cdc_snapshot_*` tables unqualified in the whitelisted schema but checks for them in `public`,
   so `initTables` re-runs `CREATE TABLE` and errors on every restart. Stage 4's original
   justification for leaving it expired when Stage 2 landed.

### 11.3 Test gate — the process failure

The gate was `go test -short ./internal/...`, which the implementers ran as a subset that
**excluded `internal/test/e2e`**. The branch was red there the whole time.

- **The gate MUST include `go test ./internal/test/e2e/`.** It needs containers and is slow. Run
  it anyway. A stage is not complete until it passes.
- **No tautological tests.** `multi_schema_key_plumbing_test.go:87` recomputed topic strings
  inline, never invoked `publishBufferBatch` or `flushBuffer`, and asserted `NotEqual` — it
  *documented* the bug rather than guarding it, and would pass with the fix reverted. Every
  regression test must call the real production function and must fail if the fix is reverted.
  **State explicitly how you verified that.**
- **Report test results honestly, including which packages were and were not run.**

### 11.4 What attempt 1 got right

Not everything was wrong; these are worth reusing rather than redoing:
- The `TableRef` protocol type and its unit tests.
- The `ParseTableStatsKey` both-ends rewrite.
- The vendored-dep fixes (`Tables.Diff` schema-awareness, publication parsing).
- Stage 2 fixing `source_remediation_test.go` fixtures — that legitimately uncovered
  `TestCoordinator_AckIngestion_NoLossUnderBurst` passing while every event was wrongly filtered
  and self-acked. A real catch, honestly reported.

---

## 12. Attempt 2 — completion record

**Implemented and verified.** All 31 e2e tests pass (30 pass, 1 pre-existing skip), all 10 unit
packages pass. Every §11.2 mandatory requirement was checked against code by independent
adversarial review and holds.

### 12.1 Consequence of the schema→database mapping that the plan missed

§7.4 decided a Postgres schema maps to a Databend *database*, but did not follow that through to
the **test harness**. Every e2e assertion queried `SELECT ... FROM <bare_table>`, which resolves
against the DSN's default database, while the sink now correctly writes to `public.<table>`.
Result: Databend error 1025 (unknown table) and 5 failing tests, despite the rows syncing
correctly.

Fixed by `qualifyTarget()` in `internal/test/e2e/env.go`, applied to every Databend assertion.
**Generalisable lesson:** changing a target's *address* requires updating everything that reads
from that address, not just everything that writes to it.

### 12.2 `ValidateSchemas` wiring

Stage 2.5 correctly implemented `ValidateSchemas` but could not wire it — the agent's file scope
was `internal/sink/**` and the call site is `internal/engine/factory.go`. It shipped as dead code
with an honest comment. Now wired via an optional-interface check after `sink.New`, matching the
existing `sink.DebugCapturer` pattern. Without it, a missing database with
`auto_create_schema=false` hits the schema path's unbounded `Nack()` — an infinite loop, the exact
failure that killed attempt 1, merely relocated behind a non-default flag.

**Orchestration lesson:** file-scoping parallel agents prevents merge conflicts but silently
creates cross-boundary gaps. Any requirement spanning two scopes needs an explicit owner.

### 12.3 E2E harness flakiness (pre-existing, NOT caused by this change)

Tests fail at ~31s with
`run postgres: ... wait until ready: "database system is ready to accept connections" matched 1
times, expected 2` when several run sequentially in one process. Every affected test passes in
isolation. Independent of this change — the testcontainers Postgres module expects the readiness
line twice (initdb starts, stops, restarts) and times out under container churn.

Consequence for anyone reading a red suite here: **check for that string before assuming a code
defect.** A killed run also leaves stray containers that make it far worse — 50 had accumulated at
one point, and clearing them changed nothing about the code but everything about the result.

Worth fixing separately: raise the startup timeout, or reuse one Postgres container across tests.

### 12.4 Known gaps deliberately not closed

- **Composite-PK fallback.** Databend cannot express `PRIMARY KEY` in DDL (verified: it is a
  reserved-word syntax error), so `SHOW CREATE TABLE` on a sink-created table never reports one.
  A restart mid-stream with no schema replay falls back to `["id"]`, and `REPLACE INTO ... ON
  ("id")` then merges distinct rows. Pre-existing; needs a design decision (persist resolved PKs
  in KV), not a local patch.
- **Debug-sink filters are schema-blind.** `IncludeTables`/`ExcludeTables`/`TableOverrides` match
  bare `m.Table`, so `exclude_tables: ["orders"]` excludes it in *every* schema. Correct per §2.2
  and §7.3, but ambiguous once two schemas hold same-named tables.
- **Misleading discovery log.** `source.go:1674` logs "New table discovered" *before* the
  `cdc_snapshot` filter on the next line, so bookkeeping tables appear in logs as discovered when
  they are correctly skipped. Cosmetic.

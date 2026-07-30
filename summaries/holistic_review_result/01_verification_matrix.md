# Verification Matrix — Per-Claim Verdicts

Each round-1 finding was independently re-checked against the code by a skeptical verifier that did not inherit the original reviewer's assumptions. Verdicts: **CONFIRMED** / **REFUTED** / **PARTIAL** (real but corrected) / **UNVERIFIABLE** (needs runtime).

## Source layer (`internal/source/`, vendored go-pq-cdc)

| # | Claim | File:line (verified) | Verdict | Note |
|---|---|---|---|---|
| 1 | `lc.Ack()` advances slot before publish; AckManager decorative | source.go:313; stream.go:397-402,448-458 | CONFIRMED | Critical stands |
| 2 | Confirm immediate; engine ackChan drained-and-discarded | source.go:505-509,292-295,309-319 | CONFIRMED | Critical stands |
| 3 | Keepalive confirms undecoded WAL | stream.go:299-302 | CONFIRMED | line drift (299-302) |
| 4 | Mid-snapshot crash permanently disables snapshot | source.go:412; producer.go:310-316 | CONFIRMED | Critical stands |
| 5 | Dropped lsnChan send stalls watermark + growth | source.go:292-295; ack.go:89-98 | **PARTIAL** | Mechanism real; **MEDIUM** (memory leak, not data integrity — slot rides on lc.Ack) |
| 6 | RestartWithNewTables never respawns ack coordinator | source.go:465-469,675-811 | CONFIRMED | Leak, not correctness (High defensible) |
| 7 | Reallocated msgChan unreachable; batches dropped | source.go:743,754-769 | CONFIRMED | Real post-restart data loss |
| 8 | ReplicaIdentity DEFAULT drops unchanged TOAST cols | source.go:401; update.go:100-107; data.go | CONFIRMED | High stands |
| 9 | Panic recover leaves mu held forever | source.go:185-191 | CONFIRMED | High stands |
| 10 | primeOIDCache wrong catalog | source.go:861,209,226,259 | **PARTIAL** | Wrong catalog real but **LOW** — OIDs globally unique, entries inert |
| 11 | Data race on s.lastCheckpoint | source.go:801,807,641-645 | CONFIRMED | Medium |
| 12 | sslmode=disable hardcoded; PassEncrypted as password | source.go:356,353,405 | CONFIRMED* | sslmode real; PassEncrypted naming is cosmetic (factory decrypts in place) |
| 13 | ack.go duplicate-check return is dead code | ack.go:65-67 | CONFIRMED | line drift; harmless |

## Engine / supervisor / stream (`internal/engine/`, `config/manager.go`, `stream/`, `infra/`)

| # | Claim | File:line (verified) | Verdict | Note |
|---|---|---|---|---|
| 1 | KV conn default MaxReconnects → terminal CLOSE, watchers die | infra/nats.go:21; manager.go:208-212,289-293 | CONFIRMED | Critical stands |
| 2 | flushBuffer fresh durable per drain → dup + leak | producer.go:519,524; subscriber.go:53-56,68,88-90 | CONFIRMED | Critical stands |
| 3 | Producer early-return → zombie pipeline | pipeline.go:73-87,155-156; manager.go:799 | CONFIRMED | Critical stands |
| 4 | attemptRestart ignores attempt; backoff never escalates | manager.go:658,574,584,827 | CONFIRMED | High stands |
| 5 | Any transient KV error treated as crash → dueling starts | manager.go:810-812,815-819,546 | CONFIRMED | High stands |
| 6 | No per-pipeline transition serialization | manager.go:512-547,489 | CONFIRMED | line drift |
| 7 | Snapshot goroutines on context.Background() | producer.go:933,959,979,1136 | CONFIRMED | High stands |
| 8 | Ack-before-durable-write for mixed schema+data wmMsg | consumer.go:280,349,373; producer.go:263 | CONFIRMED | Latent (single-msg batches); **fix helper is dead code** |
| 9 | 1s idle window as buffer-empty proof | producer.go:571-593,43 | CONFIRMED | High stands |
| 10 | Supervisor ctx cancelled before Drain | manager.go:946-948,963; factory.go:198; pipeline.go:45 | CONFIRMED | Also affects process Stop (1020-1024) |
| 11 | KV I/O under muEvo; muTableStates across verify | producer.go:490-513,730-780,596-614 | CONFIRMED | Medium |
| 12 | Malformed payload Nack, no MaxDeliver → infinite loop | consumer.go:254-257; subscriber.go:53-56 | CONFIRMED | **Upgrade to HIGH** |
| 13 | SetNatsConn never called → fast heartbeat dead | manager.go:94,648-651,895-905 | CONFIRMED | Medium |
| 14 | dynamicTablesChan never closed → goroutine leak | producer.go:893-902; pipeline.go:35 | CONFIRMED | Medium |
| 15 | get-then-Create KV bucket race | infra/nats.go:32-41 | CONFIRMED | Medium (startup) |
| 16 | Vestigial applyErr; unquoted snapshot table name | consumer.go:347-348; producer.go:1061,1068,1072 | CONFIRMED | unquoted identifier understated → MEDIUM |

## Sink / transformer (`internal/sink/`, `internal/transformer/`)

| # | Claim | File:line (verified) | Verdict | Note |
|---|---|---|---|---|
| 1 | Concurrent delete/upsert race per table | databend/sink.go:195-211 | CONFIRMED | Critical stands |
| 2 | No per-PK dedup; random group order | databend/sink.go:471,539-546 | CONFIRMED | Critical stands |
| 3 | DELETE silently skipped when PK absent; id fallback | databend/sink.go:702-713,452-454,681-683 | CONFIRMED | Critical stands |
| 4 | CREATE TABLE omits PK; recovery falls back to id | databend/sink.go:273-296,757,774-778 | CONFIRMED | Databend can't express PK → recovery structurally impossible |
| 5 | []byte → json.Marshal → base64 into BINARY | databend/sink.go:579-590,389-390 | CONFIRMED | High stands |
| 6 | Partial-column REPLACE nulls unlisted columns | databend/sink.go:427-447,479-485 | CONFIRMED | High stands |
| 7 | DLQ best-effort; dropped records acked | databend/sink.go:629-632,656-658; consumer.go:441-446 | CONFIRMED | line drift on consumer |
| 8 | structpb float64 precision loss | protobuf.go:196,288-289 | CONFIRMED | Loss written back when server echoes data (typical) |
| 9 | Empty ddl package; additive-only evolution | ddl.go; all.go:4; sink.go:249-317 | CONFIRMED | **Split**: dead import LOW, additive-only HIGH |
| 10 | Contains("int") matches point/interval | databend/sink.go:363 | CONFIRMED | Medium |
| 11 | ADD COLUMN failure only warns, returns nil | databend/sink.go:310-312,316 | CONFIRMED | Medium |
| 12 | filteredIndices misaligned across ≥2 dropping transformers | consumer.go:163,181,199; hooks.go:91-182 | CONFIRMED | Debug-sink only (Medium/Low) |
| 13 | Sub-hour retention truncates → deletes all | postgresdebug/sink.go:250,256 | CONFIRMED | Medium |
| 14 | Numeric options accept only float64 | postgresdebug/config.go:109,137,151 | CONFIRMED | Impact conditional on decoder |
| 15 | validateIdentifier rejects legal-quoted names → poison pill | databend/sink.go:236-247; consumer.go:725 | **PARTIAL** | "Retries forever" only when EnableDLQ=false; else routes to DLQ |
| 16 | LoadPlugin discards factory; 16-byte []byte → UUID | provider.go:62-83; protobuf.go:347-353 | CONFIRMED | Low |

## API / crypto / web / infra

| # | Claim | Verdict | Note |
|---|---|---|---|
| API-1 | SSRF TOCTOU + fail-open + unguarded ListSourceTables | CONFIRMED | Critical |
| API-2 | Rate-limit bypass via X-Forwarded-For | CONFIRMED | High |
| API-3 | isPrivateHost misses fc00::/7 | CONFIRMED | +missed `::` and IPv4-mapped IPv6 |
| API-4 | No CAS on config writes | CONFIRMED | Medium |
| API-5 | Insecure default DSNs | CONFIRMED | Medium |
| API-6 | Shipped ENCRYPTION_KEY usable | **REFUTED** | 34 bytes, **rejected** by crypto → startup-break, Medium config |
| API-7 | Write paths echo encrypted secrets | CONFIRMED | Low |
| API-8 | maskDSN fails open | CONFIRMED | Low |
| API-9 | /metrics, /swagger unauthenticated | CONFIRMED | Low |
| API-10 | AES-GCM correct | CONFIRMED GOOD | — |
| API-11 | JWT hardening correct | CONFIRMED GOOD | — |
| API-12 | CORS not wildcard | CONFIRMED GOOD | — |
| WEB-13 | SSE JWT query-param → 401 loop | CONFIRMED | Critical |
| WEB-14 | SSE snake/camel mismatch | CONFIRMED | reads span index.tsx:94-114 |
| WEB-15 | globalConfig.update casing | CONFIRMED | Critical/High |
| WEB-16 | VITE build-time bake vs runtime inject | CONFIRMED | Critical |
| WEB-17 | edit.tsx snake reads on camel object | CONFIRMED | High |
| WEB-18 | Recursive key-transform corrupts map keys | CONFIRMED | High |
| WEB-19 | MSW mocks camelCase-shaped | CONFIRMED | Medium (root cause of casing bugs shipping) |
| INF-20 | k8s NATS no persistence | CONFIRMED | Critical |
| INF-21 | CI runs no tests/lint/scan; no main/PR pipeline | CONFIRMED | Critical |
| INF-22 | Root/unpinned images; empty securityContext | CONFIRMED | High |
| INF-23 | wrangler dev as prod server | CONFIRMED | High |

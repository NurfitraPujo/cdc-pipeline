# CDC Pipeline — Holistic Review (Verified Results)

**Date:** 2026-07-30
**Method:** Two rounds. Round 1: six parallel deep reviews (source, engine/stream, sink/transformer, API/crypto, web, infra). Round 2: four independent skeptical verifiers re-read the actual code for every Critical/High claim (confirm / refute / correct + severity re-check), plus one fresh-eyes sweep of under-covered areas. Line numbers below were re-verified in round 2.

**Verdict at a glance:** Of ~70 round-1 findings, verification **CONFIRMED the large majority**, **REFUTED 1** (the "shipped encryption key" — see below), **downgraded 2 severities**, **upgraded 2**, and **added ~15 new findings**. The headline conclusion is unchanged and now cross-checked: **the pipeline's core promise — at-least-once, ordered CDC delivery — is broken in at least four independent places.**

---

## The one correction that matters most

**REFUTED — "functional 32-byte ENCRYPTION_KEY shipped in `.env.example`" (was Critical #13).**
The example value `my-super-secret-key-32-chars-long!` is **34 bytes**, and `crypto/aes.go:GetEncryptionKey` (lines 19–39) accepts only raw 16/24/32 bytes or base64 decoding to those. 34 raw bytes fails the length check and it contains `!` so base64 decode fails too — the key is **rejected**, not usable. This is therefore **not** a secret-exposure vulnerability. The real (lesser) defect: `cp .env.example .env` (the documented flow) produces a key that **breaks API/worker startup**, while docker-compose's inline 32-char default works — a config drift. **Reclassified: MEDIUM (config/dev-experience), not a security breach.**

Everything else in the security set held up, including the SSRF, rate-limit bypass, and the *good* verdicts on AES-GCM, JWT, and CORS.

---

## Severity re-classifications from verification

| Finding | Round 1 | Verified | Why |
|---|---|---|---|
| Shipped ENCRYPTION_KEY | Critical (security) | **MEDIUM (config)** | Key is rejected by crypto; breaks startup, not a breach |
| Source `lsnChan` drop stalls watermark | High (data integrity) | **MEDIUM (memory leak)** | Slot advancement rides on `lc.Ack()`, so a stalled watermark leaks `AckManager` memory but does not cause replay/data loss |
| `primeOIDCache` wrong catalog | High | **LOW** | Postgres OIDs are globally unique across `pg_class`/`pg_type`, so the mis-keyed entries are inert, never matched |
| Poison-payload infinite redelivery (consumer) | Medium | **HIGH (defensible)** | No `MaxDeliver`, no DLQ on this path; one bad message can wedge a partition's throughput forever |
| Unquoted table name in snapshot SQL | Low | **LOW→MEDIUM** | Correctness bug for mixed-case/special names + latent injection since names flow from discovery |
| Empty `ddl` package | (bundled High) | **split** | Dead blank-import = LOW; additive-only schema evolution = HIGH |

---

## Confirmed CRITICAL findings (data loss / corruption / outage / breach)

Delivery & correctness — **all CONFIRMED**:
1. **Source acks Postgres before publish** — `internal/source/postgres/source.go:313` (`lc.Ack()` per event) → vendored `stream.go:397-402,448-458` sends standby status update immediately. Slot advances before downstream publish; AckManager/watermark machinery is decorative. Crash after ack, before NATS publish = permanent loss.
2. **Vendored keepalive fast-forwards the slot** — `vendor/go-pq-cdc/pq/replication/stream.go:299-302` confirms `ServerWALEnd` (undecoded WAL) on every keepalive.
3. **Sink drops records but acks upstream** — `internal/sink/databend/sink.go:629-632,656-658`; nil `dlqPublisher` (the default) or failed publish → record dropped, `BatchUpload` returns nil, consumer acks (`consumer.go:441-446`). At-least-once → at-most-once.
4. **Concurrent delete/upsert races per table** — `databend/sink.go:195-211`; separate errgroup goroutines, no ordering. `DELETE id=1` + `INSERT id=1` in one batch apply nondeterministically.
5. **No per-PK dedup + random group order** — `databend/sink.go:471,539-546`; not last-write-wins within a batch.
6. **DELETE silently skipped when PK ≠ `id`** — `databend/sink.go:702-713` + `"id"` fallback (`452-454,681-683`) + CREATE TABLE omits PK (`273-296`; Databend can't express a PK constraint, so `SHOW CREATE TABLE` recovery is structurally impossible). After restart, deletes for non-`id`-PK tables vanish and the batch acks.
7. **Partial-column updates null out omitted columns** — `databend/sink.go:479-485` (REPLACE is whole-row) + source drops unchanged TOAST columns (`source.go:401`; vendored `update.go:100-107`, `data.go` has no `DataTypeToast` case).
8. **Buffer drainer re-reads entire stream history** — `producer.go:519,524`; fresh `drainer-<uuid>` durable, no `DeliverPolicy` → `DeliverAll` on a default-Limits-retention stream → duplicates grow each freeze/drain cycle; server-side durables leak.
9. **`bigint`/`numeric` precision loss through NATS transformer** — `transformer/nats/protobuf.go:196,289` (structpb coerces to float64). Applies whenever the transform server echoes data back (typical).
10. **bytea corrupted** — `databend/sink.go:579-590`; `[]byte` falls to `json.Marshal` → quoted base64 into a BINARY column.
11. **Snapshot→streaming crash skips rows** — `source.go:412` disables snapshot when `IngressLSN>0`, but snapshot rows carry LSN and are checkpointed (`producer.go:310-316`). Crash mid-snapshot → remaining rows never emitted.

Availability — **CONFIRMED**:
12. **KV/control-plane NATS uses default `MaxReconnects=60`** — `infra/nats.go:21`; terminal CLOSE after ~2 min downtime; watchers exit permanently (`manager.go:208-212,289-293`) and never restart. Data-plane correctly uses `MaxReconnects(-1)`.
13. **Zombie pipeline on transient config error** — `pipeline.go:73-87`; producer goroutine returns without `p.cancel()`, consumers keep running, `finished` never closes, supervisor heartbeats "Running" forever.

Security — **CONFIRMED**:
14. **SSRF via DNS-rebinding TOCTOU + fail-open + unguarded discovery** — `api/handler.go:60-72` (validate resolves, connect re-resolves; fails open on DNS error, `62-64`); `ListSourceTables` (`1015-1024`) builds the URL with no `validateHost` and `sslmode=disable`.

Web (dashboard substantially non-functional in prod) — **CONFIRMED**:
15. **SSE auth impossible** — `useSSE.ts:68-71` sends JWT as `?token=`; backend reads only the `Authorization` header (`auth.go:87`); endpoint is in the authorized group (`main.go:128`) →永 401 + 3s reconnect loop; JWT leaked into URLs/logs.
16. **SSE field names never match** — `pipelines/$id/index.tsx:94-114` reads camelCase; backend emits `sink_id`/`is_debug`/`total_synced`/`lag_ms`; no snakeToCamel on the SSE path.
17. **Global config can never be saved** — `api/globalConfig.ts:26` casts camelCase straight to the snake_case wire type.
18. **Web bundle points at `localhost` in prod** — `web/Dockerfile` bakes `VITE_*` at build time; helm/compose inject at runtime (ignored); `constants.ts:6` falls back to `http://localhost:8080`.

Infra — **CONFIRMED**:
19. **k8s NATS JetStream has no persistence** — `k8s/nats.yaml:33` (`-js`, no `-sd`, no PVC, single replica). Commit `0ab8d40` fixed docker-compose only.
20. **CI runs zero tests/lint/typecheck/scan; no main/PR pipeline** — `bitbucket-pipelines.yml` triggers only on `release/*` and tags; builds + pushes + ArgoCD-syncs, nothing verifies.

---

## Confirmed HIGH findings (patterns worth fixing at the root)

- **Web camelCase↔snake_case is systematically wrong** — beyond #16/#17: pipeline edit form drops retry config + processor operation types (`edit.tsx:58-68`, object already camelCased by `pipelines.ts:58`); recursive key mapper with no allowlist corrupts user map keys, `Authorization`→`_authorization` (`mappers.ts:9-11,14-43`). MSW mocks are camelCase-shaped (`test/mocks/data.ts`), which is *why* all three drift bugs shipped.
- **No CAS on any config write** — `api/handler.go` CreatePipeline `:539`, UpdatePipeline `:601`, sources `:823/887`, sinks `:1188/1259`; also `producer.go:831` discovery. Concurrent edits + discovery clobber each other.
- **Supervisor backoff never escalates + dueling restarts** — `manager.go:658` (ignores `attempt`), `574/584` (always 0/1); `810-812` (any transient KV error = crash); `512-547` (no per-pipeline transition serialization). Crash-loops restart at constant base delay; `Shutdown` skipped on the losing worker (sink `Stop`/transformer `Close` leak).
- **Stop cancels worker context before Drain** — `manager.go:946-948` before `963`; supCtx is the parent of the consumer ctx, so every "graceful" stop hard-kills in-flight batches. Same at process `Stop` (`1020-1024`).
- **Every replica runs every pipeline** — `config/manager.go:150`; no sharding/leader election; HPA 3–20 replicas fight over the single-active Postgres replication slot.
- **Additive-only schema evolution + empty ddl package** — `databend/sink.go:299-314` only CREATE + ADD COLUMN; drops/renames/type-changes silently diverge. `transformer/ddl/ddl.go` is an empty ~12-byte file, dead-blank-imported by `all.go`.
- **Dynamic-snapshot goroutines on `context.Background()`** — `producer.go:933,959,979,1136`; survive worker shutdown → two writers snapshotting the same table after restart.
- **Poison payload → infinite redelivery** — `consumer.go:254-257`; no `MaxDeliver` (`subscriber.go:53-56`), no DLQ on this path. *(Upgraded to HIGH.)*
- **`sslmode=disable` hardcoded** — `source.go:356`, `handler.go:1020,1515`; source DB credentials in cleartext.
- **Rate-limit bypass** — `ratelimit.go:79` uses `c.ClientIP()`; no `SetTrustedProxies` → `X-Forwarded-For` spoof defeats `/login` limiter.
- **Containers root on `alpine:latest`/`node:24-slim`; empty helm securityContext; `wrangler dev` as prod server** — `Dockerfile:37`, `Dockerfile.swr:1`, `web/Dockerfile:17,31`; helm `securityContext: {}` for all three components.
- **Restart data loss/leaks** — `RestartWithNewTables` never respawns the ack coordinator (`source.go:465-469` only in Start) and orphans the reallocated `msgChan` (`743`, caller can't get it, new-session `triggerFlush` drops on cap-1 `default:`, `754-769`).
- **Handler panic deadlocks the source** — `source.go:185-191`; recover without `defer mu.Unlock()`, panic between Lock and Unlock holds `mu` forever.
- **1s idle window as "buffer empty" proof** — `producer.go:571-593`; redelivery lag >1s strands buffered rows when the table flips to CDC.
- **Ack-before-durable-write for mixed schema+data wrapper** — `consumer.go:280,349,373`; latent only because the producer sends schema changes as single-message batches (`producer.go:263`). **The intended fix (`flushWithFilter`, `consumer.go:476-536`) is dead code — never called.**

---

## NEW findings from verification round (not in round 1)

**HIGH**
- **Unbounded Prometheus label cardinality on `worker_id`** — `manager.go:774` mints a new id per restart/reload; no `DeleteLabelValues`/`Reset` anywhere. A crash-looping pipeline grows `/metrics` time series without bound → worker memory + Prometheus TSDB blowup.
- **SSE stream killed every 30s by server `WriteTimeout`** — `cmd/api/main.go:169` (`WriteTimeout: 30s`) vs infinite SSE loop in `handler.go:1348-1379`. Go enforces the deadline across the whole response, so every live-metrics client is torn down on a 30s cadence. (Independent of the frontend SSE bugs.)

**MEDIUM**
- **`checkDrained` is dead code → drain-by-LSN is non-functional** — `consumer.go:682-692` never called; `Drain(targetLSN)` stores `targetLSN` but nothing reads it. Draining depends entirely on a single `drain_marker` message arriving; if lost, a draining consumer can't self-terminate.
- **`cleanupByCount` deletes all rows when `MaxCount==0`** — `postgresdebug/sink.go:263-276`; combined with the float64-only config parse (`config.go:109`), a YAML int `max_count` is ignored, `MaxCount` stays 0, `OFFSET 0` deletes everything. Second total-data-loss path in the debug sink.
- **Known-CVE dependencies** — `go.mod`: `pgx/v5 v5.6.0` (GO-2026-5004, SQL-injection via placeholder confusion; actively used in snapshot queries), `x/text v0.35.0` (GO-2026-5970), `x/crypto v0.49.0` (SSH advisories). `govulncheck` shows none currently *reached*, but bump pgx/x-text/x-crypto.
- **`dynamicTablesChan` goroutine leak** — `producer.go:893-902` ranges a never-closed channel (`pipeline.go:35`), not tracked by `wg`; one leaked goroutine per pipeline instance.
- **get-then-Create KV bucket TOCTOU** — `infra/nats.go:32-41`; two processes starting together, the loser fails `InitNATS`.
- **`SetNatsConn` never called** — `manager.go:94`; the entire fast pub/sub heartbeat path is dead; status freshness is the 15s KV write only.
- **`refreshPrimaryKey` scans `SHOW CREATE TABLE` into one string** — `databend/sink.go:757-765`; Databend typically returns 2 columns, so the scan may error, clear `pkLoaded`, and re-issue every call (defeats the "once per table" doc). *(Needs driver-adapter confirmation.)*

**LOW**
- **`log.Fatal` in health-server goroutine skips graceful shutdown** — `cmd/pipeline/main.go:144`, `cmd/api/main.go:177`; a late `ListenAndServe` failure `os.Exit(1)`s past deferred `nc.Close()`/`mgr.Stop()`, risking in-flight loss.
- **`workerID` not unique across restarts** — `cmd/pipeline/main.go:73`, `manager.go:774` use `Format("05.000")` (sec+ms only); restarts a minute apart collide.
- **Makefile `cce-seal-string` leaks secret via `ps`** — plaintext interpolated into a `curl -d` argument.
- **Hook-install conflict** — `Makefile setup-hooks` (core.hooksPath, both hooks) vs `scripts/install-hooks.sh` (copies only pre-commit); running the script silently drops pre-push.
- **Source `ackChan` (cap 1000) can block the engine** if acks aren't 1:1 with produced events; drain is opportunistic only (`source.go:309-312`).
- **Non-monotonic `UpdateXLogPos`** — vendored `stream.go:448-451` sets position unconditionally; keepalive vs per-message paths can move the reported flush position backwards.
- **`RestartWithNewTables` appends tables with no dedup** — `source.go:715`; duplicate publication entries grow unbounded across restarts.
- **`snapshotDoneChan` write-only** — `producer.go:81/962`; dead coordination signal.
- **`batchInsert` commits partial data on per-row failure** — `postgresdebug/hooks.go:241-265`; failed row logged, loop continues to `Commit()`.
- **`isPrivateHost` also misses IPv6 `::` and IPv4-mapped IPv6** — `handler.go:42-48` (in addition to the confirmed `fc00::/7` gap).

---

## Confirmed GOOD (do not spend effort here)

- **AES-GCM crypto** — `crypto/aes.go`: fresh `crypto/rand` nonce per encryption (`59-61`), key length enforced (`45-46`), weak/passphrase keys rejected (`32-38`), no hardcoded fallback. Correct.
- **JWT** — `validateJWTSecret` fatals if <32 bytes and runs first (`main.go:35-44`); `AuthMiddleware` pins HMAC (`auth.go:104-108`). Correct.
- **CORS** — fixed allowlist, origin reflected only on match (`cors.go:11-15`), not wildcard.
- **`PassEncrypted` field name is misleading but functionally correct** — `SourceConfig.Decrypt()` (`protocol/config.go:276`) overwrites the field in place with plaintext and the factory decrypts before constructing the producer (`factory.go:162-164`). The real issue on that path is `sslmode=disable`, not a missing decrypt.
- **`factory.go` error paths** close all subscribers and stop sinks — no leak on failure.
- **Logger** does not log DSNs/passwords (row-data leakage only if debug/trace enabled — a config choice).
- **`protocol` msgp/JSON tags** are in sync between write and read paths; duration parsing works.
- **`internal/test/e2e`** CDC suite is genuinely strong (snapshot, DLQ, duplication, recovery, schema evolution) — the only problem is nothing runs it (#20).
- **k8s secrets** are kubeseal-encrypted; no plaintext secrets in git except the (rejected) `.env.example` key.

---

## Cross-cutting root causes

1. **Ack/checkpoint has no end-to-end contract.** Acks are anonymous tokens, carry no LSN, dropped with `default:`; the per-event `lc.Ack()` and the vendored keepalive advance the slot independently. Needs an explicit design: downstream durable-write → LSN-carrying ack → slot advance, with per-event `lc.Ack()` removed. Note the two dead helpers (`flushWithFilter`, `checkDrained`) show a fix was *started* and never wired.
2. **No CAS discipline on the KV that is the system of record.** Every writer races.
3. **The web↔API wire contract is unenforced and the tests mock the wrong shape**, so casing drift ships every release.
4. **No CI quality gate**, so all of the above ships untested despite good suites existing.
5. **Manifest drift** — `k8s/` raw manifests, the helm chart, and docker-compose are three divergent, partially-wrong universes.

---

## Suggested fix sequencing

1. **Delivery guarantee** (Critical 1–11) — the reason the system exists; all broken.
2. **NATS persistence + KV reconnect** (12, 19) — losing the KV loses everything.
3. **Wire up CI** (20) so fixes stay fixed.
4. **SSRF + config-drift key** (14, ENCRYPTION_KEY).
5. **Web casing/config epidemic** (15–18 + High web items) — one contract fix + rewrite MSW mocks to wire format.
6. **Operational** — worker_id cardinality, SSE WriteTimeout, backoff/restart correctness.

See `01_verification_matrix.md` for the per-claim verdict table and `02_new_findings.md` for the round-2 additions in isolation.

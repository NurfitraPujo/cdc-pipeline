# Plan 05 — Web Casing/Config Epidemic (Seq 5) + Operational Correctness (Seq 6)

Status: PLAN ONLY. No code modified. All line numbers re-verified against the working tree on 2026-07-30 (some differ slightly from the review docs because the tree has moved since round 2; deltas are called out inline).

Covers verified findings: Critical 15–18, High web items (edit-form field wipe, recursive mapper corruption, camelCase MSW mocks), new finding N2 (SSE WriteTimeout — backend half of the SSE outage), N1 (worker_id metric cardinality), the operational faces of supervisor backoff/dueling-restart/stop-ordering, N6 (dynamicTablesChan leak), SetNatsConn dead path, no-sharding/slot-contention, N8/N9/N10 (log.Fatal, workerID collision, Makefile secret leak), and the container/helm hardening set.

Explicitly out of scope (owned by other sequences): `checkDrained`/`flushWithFilter` dead code (seq 1, delivery), CAS on KV writes (seq 1/2), CI wiring (seq 3), SSRF (seq 4).

---

## 1. Objective

**Part A (Sequence 5 — Web):** Make the dashboard functional in production by (a) collapsing the camelCase↔snake_case chaos into one compile-time-enforced wire contract, (b) making SSE actually authenticate, deliver correctly-shaped data, and stay connected past 30 seconds, (c) making the deployed web image respect runtime configuration instead of build-time `VITE_*` baking, and (d) rewriting the MSW test fixtures to wire format so this class of bug cannot ship silently again.

**Part B (Sequence 6 — Operational):** Make the worker fleet operable: bounded Prometheus cardinality with cleanup on teardown, a supervisor whose backoff actually escalates and whose restart/stop/update transitions are serialized per pipeline, graceful stop that drains *before* killing contexts, a worker-assignment (sharding/lease) design that stops N replicas fighting over one replication slot, and hardened containers/helm (non-root, pinned bases, real server, probes, rollback history, sane grace periods).

---

## 2. Part A — Web casing/config epidemic

### 2.1 Inventory of the defects (verified anchors)

| # | Defect | Where |
|---|---|---|
| A-1 | SSE auth impossible: JWT sent as `?token=` query param | `web/src/hooks/useSSE.ts:68-71`; backend only reads `Authorization` header (`internal/api/auth.go:87`, `AuthMiddleware`); SSE route inside authorized group (`cmd/api/main.go:128` — `pipelines.GET("/:id/metrics", h.StreamMetrics)`) → permanent 401 + 3s reconnect loop (`useSSE.ts:114-116`); JWT leaks into server/proxy access logs via URL |
| A-2 | SSE field shape never matches: FE reads `msg.sinkId`/`msg.isDebug`/`data.totalSynced`/`data.lagMs`/`data.tableName` (`web/src/routes/pipelines/$id/index.tsx:94-124`); backend emits `sink_id`/`is_debug` (`internal/api/handler.go:1424-1429`) and `protocol.TableStats` JSON is `total_synced`/`lag_ms`/`error_count`/... (`internal/protocol/state.go:33-40`). Extra: `TableStats` has **no** `table_name` field at all — `data.tableName` at `index.tsx:94` can never exist; the table name only lives in the KV key. No `snakeToCamel` runs on the SSE path (raw `JSON.parse` at `useSSE.ts:88`) |
| A-3 | Global config save silently drops everything: `web/src/api/globalConfig.ts:26` — `const body = input as unknown as WireGlobalConfig;` casts camelCase runtime object to the snake_case wire type. `batch_size`/`batch_wait`/`retry.max_retries` etc. arrive as `undefined` server-side |
| A-4 | Edit form wipes retry + processor operation types: `web/src/routes/pipelines/$id/edit.tsx:58-68` reads `p.retry.max_retries`, `pp.operation_types` — but `pipelinesApi.get` already camelCased the object (`web/src/api/pipelines.ts:58`), so these are `undefined`; saving persists the loss |
| A-5 | Recursive blind key transform corrupts user data: `web/src/api/mappers.ts:9-11` (`camelToSnakeKey`) + `:14-43` (`transform`, no allowlist, recurses into every nested object). User-authored map keys inside processor `options` / HTTP header maps get mangled (`Authorization` → `_authorization`, `maxRetries` → `max_retries` inside a value payload). The `key === value` guard at `:34-37` is a coincidence-based band-aid |
| A-6 | Prod bundle points at localhost: `web/Dockerfile` bakes `import.meta.env.VITE_*` at `pnpm run build` (line 14) with no build args; helm injects `VITE_API_BASE_URL` via configMap `envFrom` at **runtime** (`deploy/helm-chart/templates/web/deployments.yaml:41-43`, values.production.yml:231-232) — ignored by an already-built bundle; `web/src/lib/constants.ts:6` falls back to `http://localhost:8080/api/v1` in every browser |
| A-7 | MSW mocks are camelCase (client-shaped, not wire-shaped): `web/src/test/mocks/data.ts` (e.g. `totalSynced`/`lagMs` at :49-53, `totalRowsSynchronized` at :19) — so tests exercise the app against a fake API that speaks the *client* dialect, which is exactly why A-2/A-3/A-4 shipped |
| A-8 | Bonus drift found while planning: `web/src/api/stats.ts:9` declares `totalRowsSynchronized` but the wire field is `total_rows_synced` (`internal/protocol/state.go:55`) → `snakeToCamel` produces `totalRowsSynced`; the summary tile reads a field that never exists |
| A-9 | Backend half of the SSE outage (N2): `cmd/api/main.go:169` `WriteTimeout: 30 * time.Second` applies to the whole response; `StreamMetrics`' infinite loop (`internal/api/handler.go:1348, 1379-1432`) is hard-killed every 30s even with correct auth |

### 2.2 Target design

#### D1 — One wire dialect, enforced by the compiler: **the web client speaks snake_case wire types end-to-end**

Recommendation: **abolish the camelCase client-model layer entirely** rather than perfecting the mapper.

Rationale (why not "fix the mapper" or "generate mappers from OpenAPI"):
- The repo already generates `web/src/api/schema.d.ts` from the swagger spec and calls the API through typed `openapi-fetch` (`web/src/api/schema-client.ts:6-8`). The type system already knows the exact wire shape of every request/response. Every bug in this sequence is a place where that knowledge was *thrown away* by `snakeToCamel<T>(...)`/`as unknown as Wire...` casts.
- A reflective transform can never be made safe for this API: pipeline/processor `options` and future user-authored maps are open `Record<string, unknown>` — no allowlist derived from the schema can distinguish "our field" from "user key" inside `additionalProperties` objects without schema-walking machinery that is far more code than it saves.
- Using wire types directly means: `apiClient.PUT("/global", { body })` only compiles when `body` is `{ batch_size, batch_wait, retry: { max_retries, ... } }`. A-3, A-4, A-8 become compile errors, not runtime data loss.

Concrete shape:
- `web/src/api/*.ts` re-export the generated types: `export type Pipeline = components["schemas"]["PipelineConfig"];` etc. Hand-written camelCase interfaces (`Pipeline` in `pipelines.ts:9-21`, `GlobalConfig` in `globalConfig.ts:7-17`, `StatsSummary` in `stats.ts`, `TableStats`/`SSEMessage` in `types.ts`) are deleted or redefined as aliases of wire types.
- `mappers.ts` (`snakeToCamel`/`camelToSnake` and the recursive `transform`) is **deleted**, along with `mappers.test.ts`. All call sites (`pipelines.ts:51,58,65,69,71,75,80`, `globalConfig.ts:22,26,28`, `sources.ts`, `sinks.ts`, `stats.ts`, `edit.tsx:5`) return `unwrap<Wire...>(result)` directly.
- UI components use snake_case property access (`pipeline.batch_size`). This is a mechanical rename; TypeScript flags every site after the type change (`rtk tsc` drives the migration to zero errors).
- Where a form model genuinely wants a different shape (`AdvancedConfig` in `web/src/components/pipelines/advancedConfig.ts`), keep it — but the conversion functions (`pipelineToAdvancedConfig`, `advancedConfigToPayload`) become **explicit field-by-field typed mappers between two named types**, which the compiler checks. No reflection anywhere.
- Guardrail so the pattern can't regrow: add a Biome/ESLint `no-restricted-imports` rule (or a grep check in CI) banning `snakeToCamel|camelToSnake` and banning `as unknown as` in `web/src/api/**`.
- Schema freshness: add a `pnpm run gen:api` script (`openapi-typescript docs/swagger.json -o src/api/schema.d.ts`) and a CI step that regenerates and fails on diff, so `schema.d.ts` can't drift from the Go handlers' swagger annotations (ties into seq-3 CI work; the script itself belongs here).

#### D2 — SSE auth: **fetch-based SSE with the Authorization header** (recommended), not query-param tokens, not cookies

Options considered:
1. **Fetch + ReadableStream SSE client with `Authorization: Bearer` header** — RECOMMENDED. No backend auth change at all (`AuthMiddleware` already accepts the header); no token in URLs/logs; `useSSE` already owns its reconnect loop so losing EventSource's built-in retry costs nothing; works under the existing CORS config (Authorization already an allowed header for the REST calls).
2. Short-lived one-time SSE ticket endpoint (`POST /sse-ticket` → 30s single-use token in query). Works, but adds a backend endpoint, ticket store, and a second auth code path to audit; still puts a (short-lived) credential in the URL.
3. HttpOnly cookie set at login. Largest blast radius: changes the auth model for the whole app (CSRF posture, CORS `credentials: include`, logout semantics) to fix one endpoint. Rejected for now.

Concrete change (`web/src/hooks/useSSE.ts`):
- Replace `new EventSource(url)` (`:74`) with `fetch(url, { headers: { Authorization: \`Bearer ${token}\`, Accept: "text/event-stream" }, signal })` + an incremental parser over `res.body.getReader()`: accumulate a text buffer, split on `\n\n`, parse `event:`/`data:` lines, dispatch on complete frames. ~50 lines, no new dependency (or use `@microsoft/fetch-event-source` if a dependency is acceptable — hand-rolled preferred to keep the surface auditable).
- Delete the `?token=` URL construction (`:68-71`). Keep the 3s reconnect (`:114-116`) but add exponential backoff with cap (3s → 6s → 12s → 30s max, reset on a successful open) so a down API isn't hammered.
- Abort via `AbortController` in the cleanup path (`:140-147`) instead of `es.close()`.
- Treat HTTP 401 on the stream request as terminal (call the existing `handleUnauthorized()` from `schema-client.ts:36`) instead of reconnect-looping.

#### D3 — SSE payload contract + keepalive (backend)

- Keep the wire event as-is (`handler.go:1424-1429`: `key`, `data`, `sink_id`, `is_debug`) and fix the **frontend** to read wire names (falls out of D1: define `SSEMessage = { key: string; data: TableStats | Checkpoint | PipelineTransitionState | string; sink_id: string; is_debug: boolean }` in `types.ts` using wire field names; `index.tsx:94-124` reads `msg.sink_id`, `msg.is_debug`, `data.total_synced`, `data.lag_ms`).
- Add `table_name` (and `pipeline_id`) as top-level SSE envelope fields in `StreamMetrics` — the handler already computes `info := protocol.ParseTableStatsKey(key)` (`handler.go:1410-1413`); emit `"table_name": info.Table` so the frontend stops parsing `key.split(".")[8]` (`index.tsx:93-94`, brittle positional parsing). Frontend uses `msg.table_name` with the key-parse kept only as fallback during rollout.
- Add the envelope to the swagger annotations (define an `SSEMetricEvent` schema) so it lands in `schema.d.ts` and the MSW mock can be typed against it.
- **Keepalive:** in the `StreamMetrics` loop add a `time.Ticker` (15s) case that writes a comment frame `c.Writer.WriteString(": keepalive\n\n"); c.Writer.Flush()` — keeps intermediaries (ingress-nginx default `proxy_read_timeout 60s`) from reaping idle streams and gives the client liveness signal.

#### D4 — WriteTimeout vs infinite stream (backend, N2)

Options:
1. `http.ResponseController` per-route: in `StreamMetrics`, `rc := http.NewResponseController(c.Writer); _ = rc.SetWriteDeadline(time.Time{})` before the loop. Requires the gin `ResponseWriter` chain to expose `Unwrap()` — gin ≥ v1.10 implements `Unwrap` on its writer; verify with a small integration test (see §4). RECOMMENDED as primary.
2. Fallback if `SetWriteDeadline` errors (`feature not supported`): move `/pipelines/:id/metrics` onto a second `http.Server` (same handler tree, `WriteTimeout: 0`) on a separate internal port fronted by the same ingress path — clunky; only if (1) fails.
3. Set global `WriteTimeout: 0` and add a gin timeout middleware for the JSON routes — weakens the defense-in-depth for every route; rejected.

Also re-arm a **read** deadline correctly: with `ReadTimeout: 15s` (`main.go:168`) the request body deadline is already past by stream time; no change needed there. Keep `IdleTimeout` as is (applies between requests, not mid-response).

#### D5 — Runtime configuration instead of build-time `VITE_*`

The web app is TanStack Start SSR (Cloudflare Worker build served by `wrangler dev` in prod — `web/Dockerfile:31`). Two coupled fixes:

1. **Serve with a real Node server, not `wrangler dev`.** `wrangler dev` is a development simulator (miniflare): unhardened, single-process, not meant to take production traffic, and it is the reason env plumbing is weird. Switch the TanStack Start build target from the `cloudflare` plugin to the **node** preset for the container image (`vite.config.ts:22` — `cloudflare({ viteEnvironment: { name: "ssr" } })` becomes conditional on `BUILD_TARGET`, defaulting to node for the Docker build; keep the cloudflare target available for anyone actually deploying to Workers via `pnpm run deploy`). Runner stage becomes `CMD ["node", "dist/server/index.mjs"]` (exact entry per TanStack Start node output; confirm at implementation time).
2. **Runtime config injection.** With a Node SSR server, `process.env` is finally readable at request time:
   - `web/src/lib/constants.ts` is rewritten: server side reads `process.env.API_BASE_URL` / `process.env.INTERNAL_API_BASE_URL` (drop the `VITE_` prefix — these are no longer build-time vars; keep `VITE_*` as dev-mode fallback so `pnpm dev` still works).
   - The **client** gets config via an injected `window.__APP_CONFIG__`: the root route's server loader (or the TanStack Start `server function` / document head injection in `__root.tsx`) serializes `{ apiBaseUrl }` into a `<script>` tag. `API_BASE_URL` on the client becomes `window.__APP_CONFIG__?.apiBaseUrl ?? import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080/api/v1"` (localhost only ever reachable in dev).
   - Helm: rename configmap keys `VITE_API_BASE_URL`→`API_BASE_URL`, `VITE_INTERNAL_API_BASE_URL`→`INTERNAL_API_BASE_URL` (`values.production.yml:231-232`, `values.staging.yml:241-242`, `templates/web/configmaps.yaml`); docker-compose likewise.
   - Alternative considered (static `config.js` written by an entrypoint script): appropriate for pure-SPA nginx serving; inferior here because the app is SSR and the server itself also needs `INTERNAL_API_BASE_URL` for server-side fetches. Rejected.

#### D6 — Wire-format MSW fixtures

- `web/src/test/mocks/data.ts` payloads are rewritten to **wire shape and typed as the generated schema types**: `const mockPipelines: components["schemas"]["PipelineConfig"][] = [...]` with `batch_size`, `retry: { max_retries, initial_interval, max_interval, enable_dlq }`, `total_synced`, `lag_ms`, `total_rows_synced`, etc. Because they're typed against `schema.d.ts`, a camelCase fixture no longer compiles.
- `handlers.ts` must serve exactly those objects (no re-mapping helper allowed in mocks).
- Add an SSE mock: an MSW handler for `GET /api/v1/pipelines/:id/metrics` returning a `text/event-stream` `ReadableStream` that (a) asserts the `Authorization` header is present (401 otherwise — this single assertion would have caught A-1), and (b) emits frames shaped exactly like `handler.go:1424-1429` (`sink_id`, `is_debug`, wire `TableStats`).

### 2.3 Ordered work items — Part A

Each item lists files, change, and why. Order minimizes broken intermediate states.

**A-W1. Regenerate + pin the wire schema.**
Files: `web/src/api/schema.d.ts`, new `web/package.json` script `gen:api`, swagger annotations in `internal/api/handler.go` (add `SSEMetricEvent` model near `StreamMetrics`, `handler.go:1341-1347`).
Change: add `openapi-typescript` generation script; regenerate; add SSE envelope schema.
Why: everything downstream types against it.

**A-W2. Backend SSE fixes (independent of FE, deploy first).**
Files: `internal/api/handler.go` (`StreamMetrics`, `:1348-1433`), `cmd/api/main.go` (`:163-171` server config untouched).
Change: (a) `http.NewResponseController(c.Writer).SetWriteDeadline(time.Time{})` at stream start with error log + degrade note (D4); (b) 15s keepalive comment ticker (D3); (c) add `table_name` (+ `pipeline_id`) to the SSE envelope (D3).
Why: N2/A-9; makes streams survive past 30s; backward compatible (adds fields, removes none).

**A-W3. Replace `useSSE` transport (fetch-stream + header auth).**
Files: `web/src/hooks/useSSE.ts` (all of `:47-126`), `web/src/test/unit/useSSE.test.ts`.
Change: per D2 — fetch + ReadableStream parser, `Authorization` header, AbortController cleanup, capped exponential reconnect, terminal 401 handling. Delete `?token=` (`:68-71`).
Why: A-1. Deployable before or after A-W2 (header auth already works server-side); do it after so testing hits a stable stream.

**A-W4. Kill the mapper layer; adopt wire types.**
Files: `web/src/api/mappers.ts` (delete), `web/src/api/mappers.test.ts` (delete), `web/src/api/pipelines.ts`, `globalConfig.ts`, `sources.ts`, `sinks.ts`, `stats.ts`, `session.ts`, `auth.ts`, `types.ts`, `enums.ts` (audit each).
Change: per D1 — type aliases to `components["schemas"][...]`, remove all `snakeToCamel`/`camelToSnake`/`as unknown as` (specifically `globalConfig.ts:26`, `pipelines.ts:69,75`). `unwrap` return values keep wire shape.
Why: A-3, A-5, A-8 fixed at the root; A-4 becomes fixable next.

**A-W5. Migrate UI property access to wire names.**
Files (driven by `rtk tsc` after A-W4): `web/src/routes/pipelines/$id/index.tsx` (`:94-124` — `sink_id`, `is_debug`, `total_synced`, `lag_ms`, new `table_name`), `edit.tsx` (`:32-71` — `pipelineToJson` no longer hand-converts `batch_size`; `pipelineToAdvancedConfig` reads `p.retry.max_retries` which is now *correct* because `p` is wire-shaped — A-4 disappears), `create.tsx`, `web/src/routes/settings*` (global config form), `web/src/components/pipelines/advancedConfig.ts` (`advancedConfigToPayload` emits wire fields explicitly), stats/dashboard components (`total_rows_synced`), `web/src/lib/jsonToUpdateRequest.ts`, `web/src/lib/pipelineMerge.ts`.
Why: A-2, A-4, A-8. Mechanical; compiler-enforced completeness.

**A-W6. Rewrite MSW fixtures to wire format + SSE mock.**
Files: `web/src/test/mocks/data.ts`, `handlers.ts`, `server.ts`; new `web/src/test/mocks/sse.ts`.
Change: per D6. Fixtures typed against `schema.d.ts`; SSE handler asserts `Authorization` and streams wire frames.
Why: A-7; regression net for the whole sequence.

**A-W7. Runtime config + real server + web image.**
Files: `web/vite.config.ts` (conditional cloudflare plugin), `web/src/lib/constants.ts`, `web/src/routes/__root.tsx` (config injection), `web/Dockerfile` (node runner CMD, drop wrangler from prod path; also non-root + pinned base — coordinated with B-W10), `deploy/helm-chart/templates/web/configmaps.yaml`, `values.production.yml:225-233`, `values.staging.yml:235-243`, `docker-compose*.yml` web service env.
Change: per D5.
Why: A-6; also removes `wrangler dev` (part of the hardening finding).

**A-W8. Lint guardrails.**
Files: `web/biome.json` (or eslint config), CI script.
Change: ban `as unknown as` under `src/api/`, ban reintroduction of case-transform helpers, add `gen:api` drift check.
Why: keeps the epidemic from recurring.

---

## 3. Part B — Operational correctness

### 3.1 Inventory (verified anchors, current tree)

| # | Defect | Where (current lines) |
|---|---|---|
| B-1 | Unbounded `worker_id` metric cardinality: `metrics.WorkerHeartbeat` is a GaugeVec labeled `worker_id` (`internal/metrics/prometheus.go:29-32`); labels minted per monitor run — `workerID := fmt.Sprintf("%s-%s", pid, startTime.Format("05.000"))` (`internal/config/manager.go:774`) plus `"%s-retry"` variants (`:709,749,765,835,...`); emitted at `manager.go:640` (writeHeartbeatKV), `:905`, `:916` (fast/slow tickers) and `cmd/pipeline/main.go:110`. Zero `DeleteLabelValues`/`Reset` calls in the repo | manager.go, metrics/prometheus.go, cmd/pipeline/main.go |
| B-2 | Backoff never escalates: `attemptRestart(ctx, pid, cfg, attempt)` receives `attempt` (`manager.go:655`) but ends in `m.startNewWorker(ctx, pid, latestCfg)` (`:698`) which hardcodes monitor attempt to `0` on success (`:585`) / `1` on factory error (`:578`). Crash path computes `nextAttempt = attempt + 1` (`:833`) from that always-0 base → the delay ladder tops out at `getBackoffDelay(1)` forever for fast crash loops. (`getBackoffDelay` itself, `:605-627`, is now correct: exp + cap + jitter — the *propagation* is the bug.) |
| B-3 | Dueling restarts / no per-pipeline transition serialization: `handlePipelineUpdate` spawns an unsupervised transition goroutine (`:512-548`) while `monitorWorker`'s crash branch (`:806-812`: **any** KV error on the transition key is treated as "not transitioning" → restart) can concurrently `attemptRestart`; `startNewWorker` cancels whatever supervisor exists (`:563-566`) without waiting for the old worker's `Shutdown`, so the loser skips cleanup (sink `Stop`/transformer `Close` leak) |
| B-4 | Stop cancels worker context before Drain: `stopWorker` (`:940-993`) calls `m.supervisors[id]` `cancel()` at `:946-948` **before** `w.Drain()` at `:963`; the worker was built from `supCtx` (`m.factory(supCtx, id, cfg)`, `:569`), so every "graceful" stop hard-kills in-flight batches first, then "drains" a corpse. Same ordering at process `Stop` (`:1020-1026`: cancel all supervisors, then fan out `stopWorker`) |
| B-5 | Fast heartbeat path dead: `SetNatsConn` (`manager.go:94`) has no callers — `cmd/pipeline/main.go` never calls it, so `publishHeartbeatPS` (`:645-655`) is a permanent no-op; status freshness is the 15s KV write only, while the code pretends 2s |
| B-6 | `dynamicTablesChan` goroutine leak: `Producer.SetDynamicTablesChan` (`internal/engine/producer.go:893-902`) ranges a channel created in `NewPipeline` (`pipeline.go:35`) that is never closed; goroutine is not in any WaitGroup, not ctx-aware → one leaked goroutine + captured Producer per pipeline instance, accumulating across restarts |
| B-7 | Every replica runs every pipeline: `ConfigManager.Watch` (`manager.go:150` region) primes and starts *all* pipeline configs on *every* worker process; no sharding/lease → with HPA `minReplicas`>1 all replicas open the same Postgres replication slot (single-active) and fight; also duplicate consumers/sinks |
| B-8 | `log.Fatal` in health-server goroutines: `cmd/pipeline/main.go:144`, `cmd/api/main.go:177-178` — late `ListenAndServe` failure `os.Exit(1)`s past deferred `nc.Close()`/`mgr.Stop()`. Pipeline health server also never `Shutdown()` on `ctx.Done()` |
| B-9 | `workerID` collides across restarts: `cmd/pipeline/main.go:73` and `manager.go:774` use `Format("05.000")` (seconds+ms only) |
| B-10 | Containers/helm: root user on `alpine:latest` (`Dockerfile:37`, `Dockerfile.swr:1`), `node:24-slim` unpinned + `wrangler dev` (`web/Dockerfile:2,17,31`); `securityContext: {}` for api/worker/web in both values files (`values.production.yml:61,142,200`); `revisionHistoryLimit: 0` on all three deployments (`templates/{api,worker}/deployments.yaml:12`, `web/deployments.yaml:10`) → no rollback; web `replicas: 1` and **no probes** (`web/deployments.yaml:9`, container spec `:32-52` has none); `tolerantions` typo in all templates (`api:78`, `worker:78`, `web:61`) and values (silently never applied); `terminationGracePeriodSeconds: 30` equals the app's own 30s shutdown budget (`cmd/pipeline/main.go:157` — `context.WithTimeout(..., 30*time.Second)`), so kubelet SIGKILLs mid-drain |
| B-11 | Makefile `cce-seal-string` interpolates the plaintext secret into a `curl -d "...$$VALUE..."` argv → visible in `ps` (N10) |

### 3.2 Target design

#### D7 — Bounded metric labels + lifecycle cleanup

- **Relabel to bounded dimensions.** `WorkerHeartbeat` becomes `[]string{"pipeline_id"}` (bounded by configured pipelines), not `worker_id` (unbounded instance-mint). The per-instance identity moves to an **info-style gauge** `cdc_pipeline_worker_info{worker_id="..."} 1` at *process* scope (one series per process lifetime, set once in `cmd/pipeline/main.go`), or simply into logs — Prometheus label sets must be bounded, identities belong in logs/exemplars.
- **Status as a value or bounded label**: replace the `"-retry"` label suffix trick with either `cdc_pipeline_worker_state{pipeline_id, state="running|retrying"}` (state ∈ small fixed set, both series maintained with 0/1) or a numeric state gauge. Recommend the 0/1 state gauge pair — keeps PromQL simple (`max by (pipeline_id) (...)`).
- **Cleanup on teardown**: `stopWorker` (and the crash branch that deletes from `m.workers`) calls `metrics.WorkerHeartbeat.DeleteLabelValues(pid)` / `WorkerState.DeletePartialMatch(prometheus.Labels{"pipeline_id": pid})` so deleted pipelines don't leave stale series. Add a `metrics.CleanupPipeline(pid string)` helper in `internal/metrics/prometheus.go` so call sites stay one-liners; audit `PipelineLag`/`CircuitBreakerState` (`prometheus.go:19-27`) for the same treatment.

#### D8 — Supervisor rework: per-pipeline serialized state machine with real backoff

Replace the current "goroutines + shared maps + KV-transition-flag" choreography with one **`pipelineSupervisor` struct per pipeline** owning all transitions:

```
type pipelineSupervisor struct {
    pid      string
    cmds     chan supCmd        // {update cfg} | {stop} | {crashRestart}
    attempt  int                // escalating, reset after stable uptime
    worker   engine.PipelineWorker
    runCtx   context.CancelFunc // cancels the *worker*, distinct from monitor ctx
}
```

Key properties:
1. **Single goroutine per pipeline** consumes `cmds` and worker `Finished()` — update, crash-restart, and stop can no longer interleave (fixes B-3). `ConfigManager` keeps only `map[pid]*pipelineSupervisor` and forwards watch events as commands; `handlePipelineUpdate`'s detached goroutine (`manager.go:512-548`) is deleted.
2. **Attempt propagation** (fixes B-2): `attempt` lives in the supervisor struct; crash → `attempt++` → `getBackoffDelay(attempt)` → restart; reset to 0 only after the worker survives the stabilization window (keep the existing 10s rule at `:826-831`, but make the threshold configurable and measured properly). `attemptRestart`'s trailing `startNewWorker` call passes `s.attempt` through to the monitor instead of dropping it (`:698` → the concept disappears in the rework; if a minimal patch is preferred instead of the rework, thread `attempt` through `startNewWorker(ctx, id, cfg, attempt)` and use it at `:585`).
3. **Transition state is advisory, not a lock**: keep writing `TransitionStateKey` to KV for API display, but the supervisor no longer *reads* KV to decide crash-vs-intentional (fixes the `:806-812` "any KV error ⇒ restart" hazard). Intent is local state (`stopping`/`updating` flags inside the supervisor goroutine); the KV flag becomes observability only.
4. **Two contexts** (fixes B-4): `workerCtx` (passed to `m.factory`) is cancelled **only** inside the stop/shutdown sequence *after* Drain completes or times out; the supervisor's own loop ctx is what `ConfigManager.Stop` cancels. Ordering on stop: send `stop` cmd → supervisor: mark stopping → `w.Drain()` → wait `Finished()`/drainTimeout → `w.Shutdown(shutdownCtx)` → cancel `workerCtx` (belt-and-braces) → delete metrics (D7) → clear KV transition key → exit goroutine. `ConfigManager.Stop` (`:996-1056`) fans out `stop` cmds and waits with the caller's deadline; it no longer pre-cancels all supervisor ctxs at `:1020-1026`.
5. **Heartbeats**: move ticker heartbeat emission into the same supervisor goroutine (states: Running/Retrying/Stopping), using bounded metric labels per D7 and calling `publishHeartbeatPS` which becomes live via D9.

#### D9 — Wire the fast heartbeat (B-5)

`cmd/pipeline/main.go`: after `mgr := config.NewConfigManager(...)` (`:84`) add `mgr.SetNatsConn(nc)` (the `go_nats.Conn` from `:47-55`). One line; also add a unit/integration assertion (§4) so the wiring can't silently regress. Verify the API side actually subscribes to `heartbeats.worker.*` (if nothing consumes it, either wire the API subscriber or delete the pub/sub path — don't keep dead cadence code; check `internal/api` for a subscriber before deciding; as of this review no subscriber exists, so the *plan* is: wire `SetNatsConn` AND add the API-side subscription that feeds `GetWorkerHeartbeat`/status freshness, or consciously delete `publishHeartbeatPS`. Recommend wiring both — the 15s KV cadence is why dashboards lag).

#### D10 — `dynamicTablesChan` lifecycle (B-6)

`internal/engine/producer.go:893-902` + `pipeline.go`: give `SetDynamicTablesChan` a ctx (`p.ctx` of the producer/pipeline): `for { select { case <-ctx.Done(): return; case tables, ok := <-ch: if !ok { return }; ... } }`; register the goroutine in `Pipeline.wg`; close `dynamicTablesChan` in `Pipeline.Stop`. Also make `handleDynamicTables` respect ctx.

#### D11 — Worker assignment: per-pipeline lease (leader election) over NATS KV

Problem: `manager.go` Watch starts every pipeline on every replica (B-7); Postgres logical replication slots are single-consumer, so replicas >1 currently produce connect-fail crash loops (and, worse, interleaved consumption windows during failover races).

Design — **per-pipeline lease in a dedicated KV bucket** (`cdc_leases`, bucket TTL = lease TTL, e.g. 15s):
- Key `lease.pipeline.<id>`, value `{holder: workerID, acquired_at, epoch}`.
- **Acquire**: `Create()` (fails if key exists) — natural CAS; on success, this replica's ConfigManager starts the pipeline supervisor. On failure, it registers a watch on the key and stays passive for that pipeline.
- **Renew**: holder re-`Update(key, val, lastRevision)` every TTL/3. A missed renewal (NATS partition, process death) lets the bucket TTL expire the key; watchers see the delete and race to `Create()` — winner takes over. Add jitter (0–2s) on takeover attempts so replicas don't thundering-herd.
- **Release**: supervisor stop path deletes the lease before exiting (clean handoff without waiting for TTL).
- **Fencing**: `epoch` increments on every acquisition. The worker passes its epoch down to the source; before *slot-affecting* operations the producer can compare against the current lease (best-effort — true fencing at Postgres is the slot's own single-active property, which is exactly what makes stale holders fail fast: a new holder's `START_REPLICATION` kicks/blocks the old one). Document that the Postgres slot is the ultimate arbiter; the lease exists to make the *normal* case contention-free, not to be a perfect distributed lock.
- **Distribution**: with per-pipeline granularity and jittered acquisition, pipelines spread across replicas statistically. Good enough for v1; if skew matters later, bias the acquisition delay by current load (`delay ∝ #held leases`). Rejected alternatives: k8s Lease API (couples engine to k8s; docker-compose deploys exist), consistent-hash ring over the worker registry (needs a membership protocol we'd have to build anyway; the KV lease *is* that protocol at pipeline granularity).
- **Note bucket TTL support**: per-key TTL requires nats-server ≥ 2.11 (`LimitMarkerTTL`); with older servers use bucket `MaxAge`-free design where staleness = `time.Since(acquired_at) > TTL` checked by contenders (with CAS `Update` on takeover using the observed revision — safe without server TTL). Plan for the timestamp+CAS variant since it works everywhere; server TTL is an optimization.
- Integration point: `ConfigManager` gains a `leaseManager` collaborator; `handlePipelineUpdate`/reconcile consults `leaseManager.Held(pid)` before spawning a supervisor; lease loss event → supervisor receives `stop` cmd (graceful drain — the new holder's slot takeover will forcibly end streaming anyway).
- HPA sanity: worker HPA on CPU is now meaningful (replicas share pipelines); keep `minReplicas: 2` for failover, and document that max useful replicas = number of pipelines.

#### D12 — Process lifecycle hygiene (B-8, B-9, B-11)

- `cmd/pipeline/main.go:141-146` and `cmd/api/main.go:174-179`: replace `log.Fatal` in the serve goroutine with `errCh <- err` / `stop()` (the signal-context cancel func) so the normal graceful path (`mgr.Stop`, deferred closes) runs; log at Error level. Add `healthSrv.Shutdown(shutdownCtx)` after `<-ctx.Done()` in the pipeline main (API already shuts its main server; also shut its health/metrics server if separate).
- workerID: `fmt.Sprintf("%s-%s", hostname, uuid.NewString()[:8])` (or `time.Now().UnixNano()` + 4 random hex) at `cmd/pipeline/main.go:73`; `manager.go:774`'s per-monitor ID disappears with D7/D8 (identity = pipeline_id + process worker ID in logs).
- Makefile `cce-seal-string`: pipe the value via stdin — `jq -n --arg v "$$VALUE" '{value:$v}' | curl ... -d @-` so the secret never enters argv.

#### D13 — Container & helm hardening (B-10)

- `Dockerfile`: builder stays `golang:1.26-alpine` (pin digest); runtime → `alpine:3.22@sha256:<digest>` (or `gcr.io/distroless/static` since binaries are pure Go — requires `CGO_ENABLED=0`, check the `gcc musl-dev` build deps at `Dockerfile:7` imply cgo; if cgo is truly needed use distroless/base) + `RUN adduser -D -u 10001 app` + `USER 10001`. Same for `Dockerfile.swr`.
- `web/Dockerfile`: pin `node:24-slim` to minor+digest; runner runs `USER node`; CMD per D5 (node server, not wrangler). Drop `node_modules` copy if the node build is self-contained (TanStack Start node output bundles deps; verify — smaller image, smaller CVE surface).
- Helm values (`values.production.yml`, `values.staging.yml`, all three components):
  - `securityContext` (pod): `runAsNonRoot: true, runAsUser: 10001, seccompProfile: {type: RuntimeDefault}`; (container): `allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}`.
  - Fix `tolerantions` → `tolerations` in `templates/api/deployments.yaml:78`, `templates/worker/deployments.yaml:78`, `templates/web/deployments.yaml:61` **and** both values files (`values.production.yml:59,140,198`, staging equivalents). Backward-compat shim not needed — the key was always dead.
  - `revisionHistoryLimit: 0` → `3` on all three deployments.
  - Web: `replicas: 2` (or an hpa block matching api's), add `livenessProbe`/`readinessProbe` hitting `/` (or add a `/healthz` route to the node server in D5 and probe that).
  - `terminationGracePeriodSeconds`: worker → `60` (> the 30s `mgr.Stop` budget at `cmd/pipeline/main.go:157` + drain time); api → `45` (> the 5s server shutdown + SSE client disconnect). Optionally add a worker `preStop` sleep 5s so endpoints deprogram before SIGTERM.

### 3.3 Ordered work items — Part B

**B-W1. Metrics relabel + cleanup (D7).**
Files: `internal/metrics/prometheus.go:29-32` (+ audit `:19-27`), `internal/config/manager.go` (`:640,:709,:749,:765,:835,:905,:916` call sites), `cmd/pipeline/main.go:110`, `internal/metrics/prometheus_test.go`.
Why: N1; do first — small, independent, stops the active TSDB bleed.

**B-W2. One-line `SetNatsConn` wiring + API-side heartbeat subscriber decision (D9).**
Files: `cmd/pipeline/main.go` (~`:84`), `internal/api/handler.go` (`GetWorkerHeartbeat` freshness path) or deletion of `publishHeartbeatPS`.
Why: B-5; trivial, unblocks status freshness.

**B-W3. Lifecycle hygiene batch (D12): log.Fatal → graceful, health server Shutdown, workerID uniqueness, Makefile secret.**
Files: `cmd/pipeline/main.go:73,141-159`, `cmd/api/main.go:174-179`, `Makefile`.
Why: B-8/B-9/B-11; independent small fixes.

**B-W4. `dynamicTablesChan` lifecycle (D10).**
Files: `internal/engine/producer.go:893-902`, `internal/engine/pipeline.go:35` + Stop path.
Why: B-6.

**B-W5. Supervisor rework (D8): per-pipeline serialized state machine, attempt escalation, drain-before-cancel.**
Files: `internal/config/manager.go` (major: `handlePipelineUpdate :480-556`, `startNewWorker :557-587`, `attemptRestart :655-699`, `monitorWorker :701-938`, `stopWorker :940-993`, `Stop :996-1056`), possibly a new `internal/config/supervisor.go`.
Why: B-2/B-3/B-4 — the heart of Part B. Largest item; land after the small ones so bisection stays possible. Coordinate with seq-1 owners (they touch Drain semantics downstream of `w.Drain()`); this item owns the *ordering and serialization*, seq-1 owns what Drain means.

**B-W6. Per-pipeline lease / sharding (D11).**
Files: new `internal/config/lease.go` (+ tests), `internal/config/manager.go` (reconcile gate + lease-loss → stop cmd), `internal/infra/nats.go` (lease bucket init), `cmd/pipeline/main.go` (workerID into lease manager), helm worker values (`minReplicas: 2` note).
Why: B-7. Depends on B-W5 (needs the stop cmd path and serialized supervisor).

**B-W7. Container hardening (D13 images).**
Files: `Dockerfile`, `Dockerfile.swr`, `web/Dockerfile` (jointly with A-W7).
Why: B-10.

**B-W8. Helm hardening (D13 chart).**
Files: `deploy/helm-chart/templates/{api,worker,web}/deployments.yaml`, `values.production.yml`, `values.staging.yml`.
Why: B-10; ship with B-W7 in one release so securityContext matches the non-root images.

---

## 4. Test plan

### Part A

1. **Wire-format MSW as the regression net (the test that would have caught all of this):**
   - Fixtures typed `components["schemas"][...]` — camelCase fixture = compile error (A-W6).
   - Route tests: render pipeline detail against wire fixtures; assert the numbers actually display (`total_synced` value appears in DOM). Before this plan, such a test fails — proving it detects A-2.
   - Global-config round-trip test: fill the settings form, submit, and have the MSW `PUT /global` handler **validate the body against required wire keys** (`batch_size` present, `batchSize` absent) — returns 422 on violation. Catches A-3 style casts forever.
   - Edit round-trip: load pipeline with `retry` + processor `operation_types`, save without touching those fields, assert MSW receives them unchanged (catches A-4).
   - Processor-options preservation: pipeline with `options: {"Authorization": "Bearer x", "maxRetries": "3"}` survives a load→save round trip byte-identical (catches A-5; this is the test the deleted `mappers.test.ts` should have been).
2. **SSE client tests** (`useSSE.test.ts` rewrite): MSW streaming handler asserts `Authorization` header (401 without → hook surfaces terminal auth error, no reconnect); frames split across chunk boundaries parse correctly; abort on unmount cancels fetch; backoff schedule 3/6/12/30s verified with fake timers.
3. **Backend SSE integration test** (Go, `internal/api`): httptest server with `WriteTimeout: 2s` on a real `http.Server` + gin; open the stream, feed KV updates for >2s, assert the connection survives (proves `ResponseController.SetWriteDeadline` works through gin's writer — this is the D4 feasibility gate; if it fails, trigger the fallback option before building more). Assert keepalive frames every tick and envelope contains `sink_id`, `is_debug`, `table_name`.
4. **Runtime config e2e**: build the Docker image once, run it twice with different `API_BASE_URL` env, curl the served HTML, assert `window.__APP_CONFIG__` differs (proves no build-time baking). Add as a compose-based smoke script; wire into CI when seq-3 lands.
5. `rtk tsc` + `rtk vitest` clean across the A-W4/A-W5 migration; grep-guard (A-W8) green.

### Part B

1. **Metric cardinality test** (`internal/metrics` or `internal/config`): using `prometheus.NewPedanticRegistry` + `testutil.CollectAndCount`, run start→crash→restart×5→stop of a fake worker through the supervisor; assert series count for the heartbeat/state family is O(1) per pipeline (== expected fixed number), and 0 after `stopWorker` (cleanup verified). This test fails loudly against today's code.
2. **Backoff escalation test**: fake factory whose worker crashes instantly; assert observed delays follow `base*2^(n-1)` (capped 60s, jitter-tolerant bounds) across ≥4 consecutive crashes, and that a worker surviving the stabilization window resets attempt to 0. Today's code fails at crash #2 (delay stays at base).
3. **Transition serialization test**: fire `update` + simulated crash + `stop` concurrently at one supervisor; assert exactly one worker instance exists at any time (factory counts live workers), every superseded worker got `Shutdown` called (no leak), and final state honors the last command. Run with `-race`.
4. **Graceful-stop ordering test**: fake worker records event order; assert `Drain` precedes any context cancellation of the worker ctx, and `Shutdown` precedes cancel; assert in-flight batch (a Drain that takes 500ms) completes rather than seeing ctx.Done. Today's code fails (ctx cancelled first).
5. **Lease tests** (needs embedded NATS — `nats-server/v2/test` in-process): two managers, one pipeline → exactly one starts it; kill holder's renewals → other acquires within TTL+jitter; clean stop → immediate handoff; epoch strictly increases; partition (pause renewals without death) → old holder observes lease loss and stops.
6. **dynamicTablesChan leak test**: `goleak.VerifyNone` around pipeline start/stop.
7. **Lifecycle**: kill health port binding after startup → process exits via graceful path (mgr.Stop observed), not `os.Exit`; verified by an e2e harness assertion on the fake sink's Stop having run.
8. **Helm**: `helm template` snapshot tests asserting rendered `tolerations` (post-typo-fix), `revisionHistoryLimit: 3`, non-empty securityContext, web probes present; `helm lint` in CI. Container: CI step runs `docker run --rm <img> id -u` ≠ 0.
9. Existing e2e suite (`internal/test/e2e`) re-run after B-W5/B-W6 — supervisor semantics changed; drain/restart e2e cases are the acceptance gate.

---

## 5. Rollout / migration

Order of deployment (each step independently shippable and reversible):

1. **API first (backward compatible):** A-W2 (SSE keepalive + WriteTimeout fix + `table_name` envelope). Old frontends keep failing exactly as before (401 loop) — no regression. B-W1/B-W2/B-W3 ride the same worker/api release; metric **series names/labels change** — update any Grafana dashboards/alerts referencing `cdc_pipeline_worker_heartbeat_timestamp{worker_id=...}` in the same change (grep `deploy/` and any dashboard repo; if none exist in-repo, note in release notes).
2. **Web release (the big one):** A-W3..A-W8 + A-W7's new image + B-W7 web image changes ship together — this release simultaneously changes transport (fetch-SSE), data shape handling, and the serving stack (node instead of wrangler). Helm configmap key rename (`VITE_*` → unprefixed) must land in the **same** chart release as the image; a mixed pair (new image + old configmap or vice versa) falls back to localhost defaults. Guard: the new server logs a startup error and fails readiness if `API_BASE_URL` is unset in production mode — turns misconfig into an obvious failed rollout instead of a silently broken UI. Rollback: `revisionHistoryLimit: 3` (B-W8) must merge **before** or with this so rollback is even possible.
3. **Worker supervisor release:** B-W4 + B-W5. Rolling update: old and new workers overlap briefly — both generations still race on slots exactly as today (no worse). Recommend `strategy: Recreate` for the worker Deployment until B-W6 lands (overlap of old+new during RollingUpdate is today's slot-fight; Recreate gives a clean cut). Verify drain behavior in staging with a live pipeline before prod.
4. **Lease/sharding release:** B-W6. Migration: on first deploy the lease bucket is empty; all replicas race, winners take pipelines — no data migration needed. Raise worker `minReplicas` to 2 only **after** this release (today >1 replica is actively harmful). Feature-flag via env (`ENABLE_PIPELINE_LEASES`, default on in staging first) for one release cycle.
5. **Helm hardening (B-W8):** with B-W7 images. `readOnlyRootFilesystem: true` needs verification that nothing writes to disk (Go binaries: check temp usage; node server: may need an emptyDir for `/tmp`). Termination-grace bump is safe anytime.

SSE auth interplay: no backend auth change is made (header auth already exists), so there is **no** FE/BE lockstep requirement for auth — old FE was already broken; new FE works against both old and new API (only keepalive/30s-cutoff behavior differs). The only true lockstep pair is web-image ⟷ web-configmap (step 2).

---

## 6. Risks, open questions, sequencing

**Risks**
- *R1 — gin + `ResponseController`:* if gin's writer chain doesn't `Unwrap()` in the pinned version, `SetWriteDeadline` returns `ErrNotSupported`. Mitigation: the §4 A-3 integration test is written first (it's the feasibility gate); fallback design (second server) already specified.
- *R2 — TanStack Start node target:* switching the build target off the cloudflare plugin may change SSR entry paths/behavior (`vite.config.ts:22`). Mitigation: keep the cloudflare target as a parallel build script; smoke-test SSR routes in the compose stack before helm rollout.
- *R3 — supervisor rework regression risk:* B-W5 rewrites the most stateful file in the repo while seq-1 touches Drain semantics underneath. Mitigation: land the §4 supervisor test suite against the *current* behavior first (characterization where behavior is correct), then refactor; sequence after seq-1's Drain contract is merged, or agree the interface (`Drain()/Finished()/Shutdown(ctx)`) is frozen.
- *R4 — wire-type migration churn:* A-W4/A-W5 touch most of `web/src`. Mitigation: purely mechanical, compiler-driven; do it in one PR (half-migrated states are the worst state); MSW wire fixtures merged first so tests are meaningful during the migration.
- *R5 — lease correctness under partitions:* KV lease is advisory; a paused-then-resumed holder could briefly overlap with the new one. The Postgres slot's single-active property bounds the damage (old holder's stream dies), but sink-side double-consumption for NATS consumers must be tolerated by at-least-once semantics (seq-1's domain). Document explicitly; add the partition test (§4 B-5).
- *R6 — metric rename breaks dashboards/alerts* — handled in rollout step 1; do not "fix" by keeping the old unbounded family alive in parallel.

**Open questions**
- Q1: Does anything consume `heartbeats.worker.*` today? (Grep says no.) Decide in B-W2: wire an API subscriber for fresh status vs delete `publishHeartbeatPS`. Plan recommends wiring it; needs product confirmation that sub-15s status freshness matters.
- Q2: Is cgo actually required (Dockerfile installs `gcc musl-dev`)? If not, `CGO_ENABLED=0` + distroless/static is the better runtime base.
- Q3: Cloudflare Workers deployment (`pnpm run deploy`) — is it a real target anyone uses? If yes, D5's runtime-config needs a Workers-side equivalent (env bindings via `wrangler.json`); if no, delete the cloudflare plugin entirely and simplify.
- Q4: `is_debug`/debug-sink scan in `StreamMetrics` re-lists **all** KV keys per connection (`handler.go:1352-1364`) — O(keys) per SSE connect. Acceptable now; flag for seq-1/2 KV-usage cleanup if connect volume grows.
- Q5: Lease TTL value (proposed 15s) vs Postgres slot takeover latency — tune in staging; too short = flapping under GC pauses, too long = slow failover.
- Q6: Should `table_name` addition to the SSE envelope also backfill `PipelineStatusResponse` so initial load and stream use identical shapes? (Recommended yes; small handler change, fold into A-W2.)

**Sequencing summary (dependency order)**
```
A-W1 ─▶ A-W2(+Q6) ─▶ A-W3 ─▶ A-W4 ─▶ A-W5 ─▶ A-W6 ─▶ A-W7(+B-W7 web) ─▶ A-W8
B-W1, B-W2, B-W3, B-W4 (parallel, anytime)
B-W5 (after seq-1 Drain contract freeze) ─▶ B-W6
B-W7 ─▶ B-W8 (one release)
```
Estimated effort: Part A ≈ 6–9 dev-days (A-W4/A-W5 dominate); Part B ≈ 8–12 dev-days (B-W5 ≈ 4, B-W6 ≈ 3–4, rest ≈ 1–3 total).

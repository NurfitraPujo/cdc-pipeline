# Remaining Frontend Control-Plane Gaps

## Context

A full audit of `web/` against the control-plane API (2026-08-03) found ~70% of
the API surface unreachable or broken from the UI. The destructive bugs and the
missing UI have since been fixed, and the Playwright suite was repaired and
extended. The items below were deliberately deferred — none of them block a
working control plane, but each is a real gap.

The long-form audit lives at `summaries/frontend_control_plane_audit.md`, which
is a local working note: `summaries/` is gitignored, so that file is not in the
repository. Everything needed to act on the items below is restated here.

## The Problem

### 1. The SSE stream authenticates via a query-string JWT

`web/src/hooks/useSSE.ts` appends `?token=<jwt>` because the browser
`EventSource` API cannot set an `Authorization` header. The bearer token
therefore lands in server access logs, proxy logs, and any `Referer` header the
page emits. `StreamMetrics` in `internal/api/handler.go` reads it from there.

This cannot be fixed on the frontend alone. Options, roughly in order of
preference:

- Issue a short-lived (say 60s), single-purpose stream token from a new
  endpoint, and accept only that token on `/pipelines/{id}/metrics`.
- Authenticate the stream with an `HttpOnly`, `SameSite=Strict` cookie set at
  login, and drop the query parameter.
- Replace SSE with a WebSocket, which can authenticate during the handshake.

### 2. `GET /stats/history` is deprecated but still routed

`GetStatsHistory` is hardcoded to `return []protocol.HistoryPoint{}` and its
doc comment marks it "Deprecated/Ditched", yet it is still registered in
`cmd/api/main.go` and still described in `docs/openapi.yaml` as returning a
populated array.

The dashboard now derives its throughput series by sampling `/stats/summary`
client-side, which only covers the current session. Either remove the endpoint
and its spec entry, or implement a real server-side time series and have the
dashboard consume it.

### 3. `internal/api/generated.go` is dead code

`RegisterHandlers` has no caller — `cmd/api/main.go` registers all 27 routes by
hand, and the handlers bind and emit `internal/protocol` structs directly. The
oapi-codegen request/response types and nearly all of `internal/api/mappers.go`
are therefore unused (only `SourceConfigFromProtocol` is called, at
`handler.go:941`).

This is the root cause of the whole divergence class: the spec and the wire had
drifted on ten duration fields, and nothing could detect it. The duration bug
itself is fixed (`internal/protocol/duration.go`), but the *structural* problem
remains — the next divergence will be just as invisible.

Either wire `RegisterHandlers` up so the typed layer is load-bearing, or delete
`generated.go` and `mappers.go` and treat `docs/openapi.yaml` as hand-maintained
documentation with a contract test to keep it honest.

### 4. `docs/openapi.yaml` omits the operational endpoints

`/healthz`, `/readyz`, `/metrics` and `/swagger/*any` are registered in
`cmd/api/main.go` and referenced by the `k8s/` manifests, but appear nowhere in
the spec. The spec's `servers:` is `/api/v1`, so they are technically out of
scope — but they are the endpoints an operator and every k8s probe actually
depend on, and nothing in the repo documents them.

### 5. Scaffolding routes still ship

`/about` and `/demo/tanstack-query`, `/demo/table`, `/demo/store` are TanStack
project-template leftovers. They are no longer linked from the navigation, but
the routes still exist and `web/src/routes/__root.tsx` still imports
`lib/demo-store-devtools`. Removing them means unpicking that import and
deleting `web/src/lib/demo-store.ts`, `web/src/lib/demo-store-devtools.tsx` and
`web/src/data/demo-table-data.ts`.

### 6. `GET /pipelines/{id}/status`'s `status` map is still unused

The detail page consumes `.tables` and `.sinks`, and now renders checkpoints
and transitions from the SSE stream. The `status` map returned by the status
endpoint — which carries per-table circuit state and schema-evolution status —
is still fetched and discarded. The `SCHEMA_STATUS`, `PER_TABLE_STATUS` and
`TABLE_STATE` enums in `web/src/api/enums.ts` remain dead code.

Rendering it needs a decision about what schema-evolution state an operator
should see, and whether `protocol.SchemaEvolutionState`
(`internal/protocol/state.go`) should be modelled in the spec rather than
returned as an opaque value.

### 7. Sink DSN masking has no UI affordance

The backend masks the DSN password as `***` on read and reconstructs it on
update (`reconstructDSN`, `internal/api/handler.go`). The round-trip works, but
`web/src/routes/sinks/$id/edit.tsx` loads the masked string straight into an
editable input with a placeholder showing an *unmasked* example, and nothing
tells the user not to touch the `***`. Pasting a fresh DSN without a password
silently stores a password-less DSN.

### 8. No e2e coverage requires a running pipeline worker

The Playwright harness starts NATS and the API, but not `cmd/pipeline`. So no
test can assert on live metrics, checkpoint rows, per-sink tables, pipeline
transitions, or a healthy worker heartbeat — the specs assert the
no-worker states instead. Covering those paths means bringing up a worker plus
a real Postgres source and Databend sink, which is closer to the Go e2e suite's
testcontainers approach than to a browser test.

## Action Items

- [ ] Replace the query-string SSE token with a short-lived stream token or an
      `HttpOnly` cookie (item 1) — this is the only security-relevant item.
- [ ] Decide the fate of `/stats/history`: implement or remove (item 2).
- [ ] Decide the fate of `generated.go`/`mappers.go`: wire up or delete, and add
      a contract test either way (item 3).
- [ ] Document `/healthz`, `/readyz`, `/metrics` (item 4).
- [ ] Delete the demo/about scaffolding and its supporting `lib/` files (item 5).
- [ ] Design and render the schema-evolution / circuit state (item 6).
- [ ] Add a "leave `***` to keep the existing password" hint, and warn when a
      submitted DSN has no password (item 7).
- [ ] Decide whether browser e2e should ever cover live-worker paths (item 8).

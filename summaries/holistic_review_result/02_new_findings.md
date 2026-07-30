# New Findings — Round 2 (Verification + Fresh-Eyes)

Defects surfaced during verification and the fresh-eyes sweep that were **not** in round 1. All verified by reading the code.

## HIGH

### N1. Unbounded Prometheus label cardinality on `worker_id`
`internal/config/manager.go:774` — `workerID := fmt.Sprintf("%s-%s", pid, startTime.Format("05.000"))` mints a new id on every `monitorWorker` run (every pipeline (re)start, config reload, crash-restart). It becomes the `worker_id` label of `cdc_pipeline_worker_heartbeat_timestamp` (emitted at :640, :905, :916). There is **no `DeleteLabelValues`/`Reset` anywhere** in the repo. The worker process is long-lived, so a crash-looping or frequently-reconfigured pipeline accumulates a new time series roughly per restart, forever.
**Failure:** a pipeline stuck in a backoff crash-loop grows `/metrics` without bound over hours → worker memory bloat and Prometheus scrape/TSDB overload.

### N2. SSE metrics stream torn down every 30s by server `WriteTimeout`
`cmd/api/main.go:169` sets `WriteTimeout: 30 * time.Second`; `internal/api/handler.go:1348` (`StreamMetrics`) writes `text/event-stream` in an infinite `for {}` (`:1379`). Go enforces `WriteTimeout` across the entire response, so the long-lived stream is killed at 30s.
**Failure:** every dashboard client streaming live metrics is disconnected on a fixed 30s cadence → constant reconnect churn and periodic gaps. (Independent of the frontend SSE bugs in the main report; even once those are fixed, this caps stream life at 30s.)

## MEDIUM

### N3. `checkDrained` is dead code → drain-by-LSN never runs
`internal/engine/consumer.go:682-692` has no callers (grep-confirmed). `Consumer.Drain(targetLSN)` (`:675-680`) stores `targetLSN`/`isDraining`, but only `isDraining` is consumed at runtime (via the `drain_marker` path `:286-299`); `targetLSN` is written and never read. So the LSN-bounded drain the design implies does not exist — draining depends entirely on a single `drain_marker` message arriving. If that marker is lost or redelivered oddly, a draining consumer cannot self-terminate.

### N4. `cleanupByCount` deletes all rows when `MaxCount==0`
`internal/sink/postgresdebug/sink.go:263-276`. `RetentionConfig.MaxCount` defaults to zero and is only set via the float64-only assertion at `config.go:109`. With `mode:"count"` and a YAML-int `max_count` (ignored — see matrix sink #14), `MaxCount` stays 0, so `... ORDER BY captured_at DESC OFFSET 0` selects every row and the DELETE wipes the table. Second total-data-loss path in the debug sink (companion to the sub-hour-retention bug).

### N5. Known-CVE dependencies
`go.mod` — `govulncheck` reports (none currently *reached* by call analysis, so informational, but worth bumping):
- `github.com/jackc/pgx/v5 v5.6.0` — GO-2026-5004 (SQL injection via dollar-quoted placeholder confusion). **Directly used** by the postgres source snapshot queries.
- `golang.org/x/text v0.35.0` — GO-2026-5970 (infinite loop on invalid input).
- `golang.org/x/crypto v0.49.0` — multiple SSH advisories.
Action: `go get -u` on pgx/x-text/x-crypto and re-run `govulncheck`.

### N6. `dynamicTablesChan` goroutine leak
`internal/engine/producer.go:893-902` ranges `p.dynamicTablesChan` (created `pipeline.go:35`, never closed) in a goroutine not tracked by `Pipeline.wg` and not ctx-aware. One leaked goroutine (plus captured `Producer`) per pipeline instance; accumulates across restarts. (Overlaps engine matrix #14; listed here as a confirmed leak.)

### N7. `refreshPrimaryKey` scans `SHOW CREATE TABLE` into a single string
`internal/sink/databend/sink.go:757-765` — `QueryRowScan(ctx, query, nil, &ddl)`. Databend's `SHOW CREATE TABLE` typically returns two columns; scanning into one destination may error, which clears the `pkLoaded` marker (`:763-765`) and re-issues the query on every subsequent call, defeating the "at most once per table" guarantee. *(Confirm against the `QueryRowScan`/`DBExec` adapter; risk MEDIUM if confirmed.)*

## LOW

### N8. `log.Fatal` in health-server goroutine skips graceful shutdown
`cmd/pipeline/main.go:144` (and `cmd/api/main.go:177`). If `healthSrv.ListenAndServe()` fails after workers are running (e.g. port already bound), the goroutine `log.Fatal` → `os.Exit(1)`, skipping deferred `nc.Close()`, `sharedPub.Close()`, `mgr.Stop(shutdownCtx)` → ungraceful exit, risking in-flight batch loss. The pipeline health server is also never `Shutdown()` on `ctx.Done()`.

### N9. `workerID` not unique across restarts
`cmd/pipeline/main.go:73`, `manager.go:774` use `time.Now().Format("05.000")` (sec+ms only, no minute/hour/date). Two restarts exactly a minute apart collide, contradicting the "unique ID per instance" comment (heartbeat-key overwrite).

### N10. Makefile `cce-seal-string` leaks secret via process table
The read-in `$$VALUE` is interpolated into `curl ... -d "{... \"value\": \"$$VALUE\" ...}"`, exposing the plaintext secret to `ps` while the request runs.

### N11. Hook-install mechanisms conflict
`Makefile setup-hooks` sets `git config core.hooksPath .git-hooks` (both pre-commit and pre-push); `scripts/install-hooks.sh` copies only `pre-commit` into `.git/hooks`. If a dev runs the script, git honors any later `core.hooksPath` and ignores `.git/hooks`, and the script never installs `pre-push` — silently dropping that safety net.

### N12. Source `ackChan` (cap 1000) can block the engine
`internal/source/postgres/source.go:332` creates `ackChan` cap 1000; the handler only drains it opportunistically (one non-blocking recv per event, `:309-312`) and discards the value. If engine acks aren't 1:1 with produced events, or arrive during idle periods, the buffer fills and the engine's send blocks. Filtered-event early returns (`:215-217,232-234,247-250,264-267`) also skip the drain, accumulating faster.

### N13. Non-monotonic `UpdateXLogPos` (vendored)
`internal/vendor/go-pq-cdc/pq/replication/stream.go:448-451` sets `lastXLogPos = lsn` unconditionally. Because `lc.Ack()` passes a per-message `walStart` while keepalives pass `ServerWALEnd`, the reported flush position can move backwards — an independent slot-integrity concern beyond the ack-before-publish bug.

### N14. `RestartWithNewTables` appends tables with no dedup
`internal/source/postgres/source.go:715` — `s.config.Tables = append(...)` with no membership check; a table added twice yields duplicate `publication.Table` entries and unbounded `Tables` growth across restarts.

### N15. `snapshotDoneChan` is write-only
`internal/engine/producer.go:81/962` — declared, buffered(10), only a non-blocking send; no receiver. Dead coordination signal (an intended snapshot-completion handshake that was dropped).

### N16. `batchInsert` commits partial data on per-row failure
`internal/sink/postgresdebug/hooks.go:241-265` — a failed `stmt.ExecContext` is only logged (:261) and the loop proceeds to `tx.Commit()` (:265); the failed debug record is silently missing while the rest commit. (Note: if the failure aborts the Postgres tx, later execs fail too — round 1 flagged the abort variant; this is the partial-commit variant.)

### N17. `isPrivateHost` also misses IPv6 `::` and IPv4-mapped IPv6
`internal/api/handler.go:42-48` — beyond the `fc00::/7` gap, the unspecified `::` and IPv4-mapped forms like `::ffff:10.0.0.1` are not rejected, widening the SSRF surface.

## Notable NON-issues confirmed during round 2

- **`PassEncrypted` used as password is NOT a missing-decrypt bug** — `SourceConfig.Decrypt()` (`protocol/config.go:276`) overwrites the field in place with plaintext; the factory decrypts before constructing the producer (`factory.go:162-164`). Misleading name only; the real path issue is `sslmode=disable`.
- **`factory.go` error paths** correctly close all subscribers (incl. producer ack sub) and stop sinks — no leak on failure.
- **Logger** does not log secrets; **protocol** msgp/JSON tags are in sync; **schema-client/authStore 401 handling** is solid; **`time.After`** usages are context-guarded one-shots; no `sync.Map` misuse; `bin/` is gitignored.

# CDC source-ack runbook (WI-5a)

## Status as of this commit -- read this first

> **The `strict_ack` flag exists and gates `ManualCommit`.**
> `internal/source/postgres/source.go` (`resolveStrictAck`, used from `Start`
> and `Restart`) reads `CDC_STRICT_ACK` and sets
> `cfg.ManualCommit` from it, together with the handler's ack behaviour (see
> below). This is exactly the flag plan `01a_delivery_source_ack` §6
> describes; an earlier revision of this document said it did not exist --
> that is no longer true.
>
> **Default, absent an explicit `CDC_STRICT_ACK`:**
>
> | `ENV` | strict_ack default | meaning |
> |---|---|---|
> | `production` | **OFF** | legacy per-event `lc.Ack` |
> | `staging` | **ON** | the new contract |
> | unset / anything else (dev, test) | **ON** | the new contract |
>
> An explicit `CDC_STRICT_ACK=true`/`false` always overrides the default in
> either direction, on top of whatever `ENV` is set to.
>
> Staging is deliberately grouped with dev, **not** with production: it is the
> intended bake environment and ships with these alerts enabled. Note this is
> *not* the same split `logger.Init` uses -- that one groups staging with
> production. The two encode different intents; do not "align" them.
>
> - **`CDC_STRICT_ACK=true` (or the dev/test default): the new contract.**
>   The handler never calls `lc.Ack()`; `runAckCoordinator` is the sole
>   slot-advancer, gated on every configured sink durably writing each LSN.
>   This is the slot-freezing behaviour described below.
> - **`CDC_STRICT_ACK=false` (or the prod default today): the legacy
>   contract.** `cfg.ManualCommit` is `false`, and the handler calls
>   `lc.Ack()` per event again, exactly as before this plan -- the vendored
>   library advances the slot itself, per event, regardless of whether any
>   sink has durably written it yet.
>
> **Rollback at any step is flipping `CDC_STRICT_ACK` to `false` (a config
> change, no code revert, no data-format change to unwind).** Be precise
> about what that buys you: it is an **availability** escape hatch, not a
> **correctness** one. Flipping it off un-freezes the slot and stops WAL
> retention, but it also **re-opens the data-loss window this plan closed**
> -- a dead or slow sink can silently lose events again, exactly as it could
> before plan `01a_delivery_source_ack`. Use it to relieve WAL-retention
> pressure on the source primary, not as a routine toggle.
>
> The AckManager/coordinator/`runSlotLagProbe` plumbing (and therefore every
> metric in this document) stays live in **both** modes -- see
> [Metrics](#metrics). Under `strict_ack=false` the coordinator's
> `UpdateXLogPos` calls are redundant with the legacy `lc.Ack()` calls, not
> harmful: the vendored `stream.UpdateXLogPos` (`pq/replication/stream.go`)
> only ever *stores* `max(lsn, lastXLogPos)` and always *reports* that same
> monotonic value back to PostgreSQL, so a coordinator call carrying a
> same-or-lower LSN than what `lc.Ack()` already advanced to is a verified
> no-op, never a regression. This is what makes it safe to keep
> `cdc_source_ack_watermark` observable *before* the flag is ever flipped --
> the whole point of the bake period below.
>
> **Current per-environment state:**
>
> - **Staging (`ENV=staging`, `values.staging.yml`): `strict_ack` defaults ON.**
>   The new contract, and therefore the WAL-retention risk described below, is
>   **live in staging today**. This is intended -- staging is the bake
>   environment and has `worker.alerts.enabled: true`. If you are paged on
>   staging WAL growth, do **not** assume strict_ack is off; it is on.
> - **Production (`ENV=production`, `values.production.yml`): `strict_ack`
>   defaults OFF.** Production still runs legacy per-event acking, so the
>   WAL-retention risk is not yet live there -- but neither is the data-loss
>   fix. Deploying this branch to production does **not** enable the fix;
>   someone must set `CDC_STRICT_ACK=true`. See
>   [Deploy order](#deploy-order). The alerting in
> `deploy/helm-chart/templates/worker/prometheusrule.yaml` is gated by
> `worker.alerts.enabled`, which currently defaults to `false` in
> `values.production.yml`. Per the deploy order below, `worker.alerts.enabled: true`
> for production is a **precondition** for flipping `strict_ack` on there,
> not something to defer until after.

## Why this exists

Before this plan, a dead sink silently lost data. Now, with `ManualCommit`
live, a dead sink **freezes the replication slot**, and PostgreSQL retains WAL
without bound on the **source primary**. That is the correct at-least-once
trade -- loss becomes visible backpressure -- but it converts a silent
data-loss problem into a potential source-database disk-pressure outage. This
document is the operator-facing half of that trade: how to see it coming, and
what to do about it.

## Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `cdc_source_slot_lag_bytes` | `pipeline, source, slot` | `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` for the slot, probed every ~15s. Growing = WAL accumulating on the source primary. |
| `cdc_source_pending_lsns` | `pipeline, source, slot` | Count of LSNs observed-or-confirmed but not yet folded into the AckManager watermark. Non-zero-and-flat is the shared symptom of failure modes (b) and (c) below. Shares the SAME label set as the other two gauges deliberately -- see [Probe health](#probe-health) and the alert rule comments for why that matters. |
| `cdc_source_ack_watermark` | `pipeline, source, slot` | The AckManager's current watermark -- the highest LSN confirmed durably written by every required sink. Used by the bake-period check below. |
| `cdc_source_slot_lag_probe_last_success_timestamp_seconds` | `pipeline, source, slot` | Unix timestamp of the last successful slot-lag probe query. See [Probe health](#probe-health) -- this exists because the lag gauge above silently goes stale on a probe failure. |

`Config.Print()` also logs `"manualCommit":true` on every connector construction
-- a free signal that `ManualCommit` actually took effect for a given process.

## Probe health

`cdc_source_slot_lag_bytes` is a plain Prometheus gauge: when the probe query
fails (slot missing, or a query/connection error), the code deliberately
leaves the gauge at its **last successfully-observed value** rather than
clearing it (see `runSlotLagProbe` in `internal/source/postgres/source.go`).
That is correct for the "value is real but old" case, but it means a
**degraded source-DB connection** -- exactly the kind of problem most likely
to co-occur with a genuine slot-lag incident -- can leave the lag gauge
reporting a stale, healthy-looking number indefinitely, silently disarming
`CDCSourceSlotLagWarning`/`CDCSourceSlotLagCritical` during the incident they
exist for.

`cdc_source_slot_lag_probe_last_success_timestamp_seconds` exists to make
that staleness independently observable. The `CDCSourceSlotLagProbeStale`
alert fires when `time() - cdc_source_slot_lag_probe_last_success_timestamp_seconds`
exceeds `worker.alerts.probeStale.staleAfterSeconds` (default 120s, 8x the
15s probe interval). **If this alert is firing, do not trust the current
value of `cdc_source_slot_lag_bytes` -- it may be stale.** Investigate source
database connectivity directly (can the pipeline process still reach the
source host? is `s.db` erroring on every query?) rather than reasoning from
the possibly-frozen lag number.

## Failure modes

There are three distinct failure modes. Do not collapse them -- they have
different symptoms and different responses.

### (a) Sink down -> WAL growth

**Symptom:** `cdc_source_slot_lag_bytes` growing steadily (the `CDCSourceSlotLagWarning`
/ `CDCSourceSlotLagCritical` alerts) -- and `CDCSourceSlotLagProbeStale` is
**not** firing (if it is, see [Probe health](#probe-health) first; the lag
reading may not be current).

**Cause:** a downstream sink is down, slow, or misconfigured. The replication
slot's `confirmed_flush_lsn` cannot advance past LSNs that sink has not durably
written, so PostgreSQL retains the WAL those LSNs live in.

**Response:** fix the sink. Slot lag should start shrinking once it catches up
or comes back.

**Disaster floor:** set `max_slot_wal_keep_size` on the source database. This is
the operator-chosen ceiling on how much WAL PostgreSQL will retain for this
slot; past it, PostgreSQL invalidates the slot rather than filling the primary's
disk. Recovering from an invalidated slot requires a forced re-snapshot
(`Resnapshot` config) -- accept that outcome as the deliberate backstop, not a
bug, when a sink outage runs long enough to hit it.

### (b) Wedged connector needing a process restart

**Symptom:** `cdc_source_pending_lsns > 0` **and the slot position (`cdc_source_ack_watermark`
/ `confirmed_flush_lsn`) is not moving** -- i.e. `CDCSourcePendingLSNsStuck` fires
and `cdc_source_slot_lag_bytes` is roughly **flat**, not growing (contrast with
(a), where it grows because new WAL keeps arriving behind a frozen position; here
ingestion itself has stalled so there is little or no new WAL either).

**Cause:** if the source ever goes long enough without sending a standby status
update while LSNs are pending, PostgreSQL's `wal_sender_timeout` kills the
walsender. The vendored `connector.Start` parks on shutdown with **no
reconnect** in that case -- ingestion stalls permanently and does **not**
self-heal even after the sink recovers. WI-7 closed the known trigger (seeding
`lastXLogPos` from the slot's own `confirmed_flush_lsn` at session start), but
the residual case -- that seed query failing on both the pre-Start and
post-`WaitUntilReady` attempts -- remains possible.

**Response:** restart the pipeline process. A restart rebuilds the AckManager
and replays from the slot's `confirmed_flush_lsn`, which re-establishes the
standby-status heartbeat.

### (c) Pinned LSN from an unparseable ingest payload

**Symptom:** `cdc_source_pending_lsns` **flat and non-zero** while
`cdc_source_slot_lag_bytes` **grows** (the inverse of (b): here new WAL keeps
being generated normally, but one LSN can never be confirmed).

**Cause:** a wmMsg whose payload fails to unmarshal has no recoverable LSN
anywhere -- not in a message header, not in JetStream redelivery metadata, not
in the AckManager's own bookkeeping. DLQ-routing that message is the only
correct thing to do with it, but doing so pins its LSN in `AckManager.pending`
forever, since nothing will ever confirm it. Reachability is low (the same
payload must already have parsed once to reach DLQ routing in the first place),
but it is not zero.

**Response:** restart the pipeline process. Same self-healing mechanism as (b):
a fresh AckManager and a slot-anchored replay clears the pin.

### OPS-2: IdleAdvance-refused (T0-3 regression canary)

**Symptom:** `CDCSourceIdleAdvanceRefused` fires. Every other gauge in
[Metrics](#metrics) can look completely healthy while this fires:
`cdc_source_pending_lsns` reads 0, `cdc_source_ack_watermark` is advancing,
`cdc_source_slot_lag_bytes` is flat or low. **This alert is the only signal
for this failure mode** -- do not wait for, or expect, corroborating symptoms
elsewhere before treating it as real.

**Cause:** commit `f192fe3` (T0-3) fixed a bug where `IdleAdvance` could
fast-forward the AckManager's watermark past a replay backlog that had been
handed to a sink but never re-`Observe()`d -- i.e. the watermark moved past
data that was never actually confirmed, and nothing detected it: the same
0-pending-LSNs, healthily-moving-watermark signature described above. The fix
added an ordering guarantee (`sink` is the sole writer of `messageCH`,
`process` the sole reader, and `buf.flush()` is required to precede the
marker enqueue -- see the vendored-patch notes in
`internal/vendor/go-pq-cdc/PATCHES.md`, entry T0-3) plus a guard in the
AckManager that refuses an `IdleAdvance` call it cannot prove safe.

That guard is intentionally **log-only and latching**: it logs one Error the
first time it refuses, then sets an internal `idleTrusted` flag and stops
re-checking for the lifetime of that AckManager. This keeps the guard cheap
and non-blocking, but it also means: (1) a single refusal is not a fluke to
shrug off -- the guard does not refuse repeatedly to "confirm" the problem,
it fires once and goes quiet, so treat one occurrence with full severity; and
(2) `cdc_source_idle_advance_refused_total` (incremented via the
`SetIdleAdvanceRefusedHook` callback wired from `internal/source/postgres`)
is the only way this refusal becomes visible outside the log stream. Without
this counter and this alert, the exact invisible-data-loss signature T0-3 was
written to eliminate would simply reappear, unnoticed, if the vendored
ordering guarantee it depends on (the `buf.flush()`-before-marker-enqueue
line -- see the re-sync risk callout in `PATCHES.md`) ever regressed during a
future upstream re-sync.

**Response:**
1. Treat as a possible active data-loss regression, not routine noise. Page
   on-call immediately (this is why the alert is `severity: page`, unlike
   the `warning`-level gauges above).
2. Do not restart the pipeline as a first response the way you would for
   failure modes (b)/(c) -- a restart clears the AckManager's `idleTrusted`
   latch and log line, destroying the only evidence of what happened,
   without fixing anything if the root cause is the vendored ordering
   guarantee.
3. Pull the AckManager/source logs around the refusal timestamp for the
   `IdleAdvance refused` Error line (see `internal/source/postgres/ack.go`)
   to get the specific LSNs involved.
4. Check whether `internal/vendor/go-pq-cdc/pq/replication/stream.go`'s
   `buf.flush()`-before-marker-enqueue ordering (T0-3) is intact -- especially
   if this fired shortly after a vendored dependency re-sync. If that
   ordering has regressed, this counter is confirming exactly the class of
   bug commit `f192fe3` fixed.
5. Escalate to whoever owns the vendored `go-pq-cdc` patches
   (`internal/vendor/go-pq-cdc/PATCHES.md`) before resuming normal operation
   on the affected pipeline.

### Telling (b) and (c) apart from (a)

All three can show `cdc_source_pending_lsns > 0`. The distinguishing signal is
`cdc_source_slot_lag_bytes`:

- **(a):** lag growing, `pending_lsns` may be low/normal -- a healthy sink
  gating on a slow one, or fully caught up in-flight but the sink itself is down.
- **(b)/(c):** lag flat-to-slowly-growing while `pending_lsns` is stuck
  non-zero and `cdc_source_ack_watermark` has stopped advancing entirely.
  Distinguish (b) from (c) by checking whether ingestion itself has stalled
  (no new events flowing at all -> (b)) versus continuing normally around the
  one pinned LSN (-> (c)); either way the response is the same restart.

`CDCSourcePendingLSNsStuck` (`cdc_source_pending_lsns > 0 and
delta(cdc_source_ack_watermark[10m]) == 0`) is the alert covering both (b) and
(c) -- it cannot tell them apart on its own, which is why an operator paged by
it should read this section, not just the alert text.

## Alert rules

`deploy/helm-chart/templates/worker/prometheusrule.yaml` defines
`CDCSourceSlotLagWarning`, `CDCSourceSlotLagCritical`,
`CDCSourcePendingLSNsStuck`, `CDCSourceSlotLagProbeStale`, and
`CDCSourceIdleAdvanceRefused` (see
[OPS-2: IdleAdvance-refused](#ops-2-idle-advance-refused-t0-3-regression-canary)),
gated by
`worker.alerts.enabled` (see [Status as of this commit](#status-as-of-this-commit----read-this-first)
for current per-environment state). Thresholds live in `worker.alerts.*` in
`values.staging.yml` / `values.production.yml`.

`deploy/helm-chart/tests/` contains a `promtool test rules` unit test
(`rules_test.yml`, evaluated against `rendered_rules.yml`, a hand-kept mirror
of the templated rule expressions with the staging thresholds substituted
in) proving:

- `CDCSourcePendingLSNsStuck` fires when `cdc_source_pending_lsns` and
  `cdc_source_ack_watermark` share a matching `{pipeline, source, slot}`
  label tuple (the fixed, current exporter behaviour), and
- it does **not** fire when the two series carry non-matching label tuples
  (e.g. different `slot` values) -- proving the `and` join is doing real
  work rather than vacuously matching, which is the exact bug class that
  originally made this alert dead-on-arrival when `cdc_source_pending_lsns`
  was exported with only a `{"source"}` label.
- `CDCSourceSlotLagWarning` fires on sustained growth past its threshold and
  stays silent on a flat-but-large value.
- `CDCSourceSlotLagProbeStale` fires when the probe's success timestamp has
  not advanced.
- `CDCSourceIdleAdvanceRefused` fires when `cdc_source_idle_advance_refused_total`
  increases at all in a 15m window, and stays silent when it does not.

Run it with:

```sh
cd deploy/helm-chart/tests
docker run --rm --entrypoint promtool -v "$PWD:/rules" -w /rules \
  docker.io/prom/prometheus:latest test rules rules_test.yml
```

If the templated alert expressions or thresholds in
`templates/worker/prometheusrule.yaml` change, update `rendered_rules.yml` to
match by hand and re-run the test -- it is not generated automatically from
the Helm template.

## Deploy order

The `strict_ack` flag (`CDC_STRICT_ACK`, see
[Status as of this commit](#status-as-of-this-commit----read-this-first))
exists and is live today. This section describes the actual rollout
procedure for turning it on in production, pipeline by pipeline.

**Preconditions before any prod flag flip (all mandatory):**

- [ ] Fix Sequence 3 (CI) landed and green, running the e2e invariant tests
      (`TestSlotNeverAdvancesBeforeSinkAck` and siblings). This is not something
      to verify by hand for a change that can retain unbounded WAL on the
      source primary.
- [ ] This WI-5a metric + `PrometheusRule` (`deploy/helm-chart/templates/worker/prometheusrule.yaml`,
      `worker.alerts.enabled: true`) live for the target pipeline's source,
      including `CDCSourceSlotLagProbeStale` (so a degraded probe doesn't
      silently blind the other two alerts). This means flipping
      `worker.alerts.enabled` to `true` in `values.production.yml` before
      the first production `CDC_STRICT_ACK=true` flip -- it currently
      defaults to `false` there.
- [ ] Bake period completed: `cdc_source_ack_watermark` tracks
      `confirmed_flush_lsn` closely under production load with the flag
      still off. With `strict_ack=false`, `confirmed_flush_lsn >
      cdc_source_ack_watermark` is expected (the legacy `lc.Ack()` path
      advances the slot without waiting for the watermark); the gap should
      be small and stable, not growing. The AckManager/coordinator/metrics
      plumbing runs regardless of the flag (see the status note above), so
      this bake period needs no code change to start -- only time and
      observation.

**Then, pipeline by pipeline:**

1. Release N: all WIs shipped, `strict_ack` off in prod (today's state),
   metrics observed (watermark plumbing already live regardless of the
   flag).
2. Release N (config change): once every precondition above is checked for a
   given pipeline, set `CDC_STRICT_ACK=true` for that pipeline's worker
   process only. Watch `cdc_source_slot_lag_bytes` and sink lag closely
   after the flip. Stop and investigate any pipeline whose slot lag grows
   without a corresponding sink outage before proceeding to the next
   pipeline.
3. Release N+1: once every pipeline has been flipped and observed healthy,
   remove legacy ack parsing (the dual-read described in plan 01a §6) and
   make `strict_ack` on-by-default; flag removal in N+2.

**Rollback at any step:** set `CDC_STRICT_ACK=false` for the affected
pipeline (a config change, redeploy the worker; no code revert, no
data-format change to unwind). This is an **availability** escape hatch,
not a **correctness** one -- it un-freezes the slot and stops WAL retention,
but it also re-opens the data-loss window this plan closed (a dead/slow sink
can silently lose events again, exactly as before plan `01a_delivery_source_ack`).
Use it to relieve WAL-retention pressure, not as a routine toggle.

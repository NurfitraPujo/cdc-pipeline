---
status: proposed
date: 2026-08-05
decision-makers: cdc-pipeline maintainers
consulted: internal/config/manager.go, internal/engine/pipeline.go, internal/engine/factory.go, internal/vendor/go-pq-cdc/connector.go, cmd/pipeline/main.go, rfc/RFC-001-Architecture-and-Design.md, docs/decisions/0011-nats-as-the-only-control-plane-datastore.md
---

# Worker HA comes from a per-pipeline NATS KV lease, not from replica count

## Context and Problem Statement

The worker is a **stateful singleton per pipeline**. [RFC-001](../../rfc/RFC-001-Architecture-and-Design.md)
records this explicitly ("Current Model: Stateful Singletons per pipeline… Not natively HA
(requires K8s restart)"). Nothing in the code enforces it.

Every pod runs its own `ConfigManager` watching the same `PrefixPipelineConfig` prefix
(`internal/config/manager.go:209`), and `handlePipelineUpdates` (`:301`) → `transitionWorker`
(`:511`) → `startNewWorker` (`:580`) starts a producer for **every pipeline it sees**. The only
gate at that chokepoint is `cfg.Validate()` (`:594`). There is no ownership, sharding, claim, or
leader-election step anywhere in the path.

Meanwhile the Helm chart shipped worker `hpa.enabled: true, minReplicas: 3, maxReplicas: 20`
(`deploy/helm-chart/values.production.yml`). The chart's posture directly contradicted the code's
single-writer assumption, and production ran that way.

### Why nobody noticed

The multi-writer collision is **real but silent**, and the silence is the reason this survived.

`internal/engine/pipeline.go:152-153` derives the replication slot name from the *pipeline ID*:

```go
srcCfg.SlotName = fmt.Sprintf("%s_%s", srcCfg.SlotName, strings.ReplaceAll(p.id, "-", "_"))
```

The comment above it claims uniqueness "for every worker instance" — that is wrong. The pipeline ID
is identical across pods by construction. `WORKER_GROUP` does not reach the slot; it only namespaces
NATS durable names (`internal/engine/factory.go:293`), and every production pod shares the same
value anyway.

Postgres permits exactly one active walsender per slot, so pods 2..N lose at `START_REPLICATION`.
What happens next is the crux. `internal/vendor/go-pq-cdc/connector.go` mapped PgError 55006 to
`ErrorSlotInUse` and appeared to handle it:

```go
if goerrors.Is(err, replication.ErrorSlotInUse) {
    logger.Info("capture failed, slot in use. Retrying in 1s...")
    time.Sleep(1 * time.Second)
    c.Start(ctx)
    return
}
```

**That branch never executed.** `ErrorSlotInUse` was constructed with
`github.com/go-playground/errors.New`, which returns an `errors.Chain` — a slice, therefore
non-comparable. `errors.Is` matches a target by `==` and only attempts that for comparable targets,
and `Chain` has no `Is` method, so `errors.Is(err, ErrorSlotInUse)` was always false — even against
the sentinel itself. The code fell through to `logger.Error("postgres stream open")` and returned.
(Had the branch ever run it would have hung on the second attempt: `Open` pushes to the cap-1
`sinkEnd` channel before returning, and only `Close` drains it.)

So the failure *was* logged at Error. What made it invisible was everything else:

* `Start` has no error return, so the failure never propagated to `runProducer`. The worker never
  "finishes" and the supervisor's crash branch in `monitorWorker` never fires. No CrashLoopBackOff.
  `WaitUntilReady` simply blocked until shutdown, making "failed and gave up" indistinguishable from
  "still starting up".
* `/readyz` only checks NATS connectivity (`cmd/pipeline/main.go:120-133`); `/healthz` is an
  unconditional 200. A pod that never captured stays **Ready and Healthy indefinitely** — and keeps
  earning HPA credit.
* The heartbeat writes `Status: "Running"` on every pod regardless (`manager.go:844-849`), so the
  API and dashboard show all replicas as healthy.
* All five worker alerts (`deploy/helm-chart/templates/worker/prometheusrule.yaml`) key on `{pipeline, source, slot}`,
  not on pods. The winning pod streams normally, slot lag stays healthy, nothing fires.

Net effect: one pod per pipeline does the work; the other 2-19 log one error at startup and then sit
idle forever, Ready and reporting Running. **Scaling the worker adds zero throughput.**

The reporting half of this — sentinel comparison, `startErrCh`, and a
`cdc_source_capture_setup_failures_total` metric to alert on — is fixed as patch HA-1
(`internal/vendor/go-pq-cdc/PATCHES.md`), independently of the lease work below.

## Decision Drivers

* Correctness first: never two producers on one replication slot, and never a stale writer
  advancing a checkpoint past a live one.
* Stay within [ADR-0011](0011-nats-as-the-only-control-plane-datastore.md) — NATS is the only
  control-plane datastore. A design pulling in etcd, Redis, or Postgres for coordination is
  disqualified.
* Do not require a nats.go client or nats-server upgrade as a prerequisite.
* Keep the worker deployable outside Kubernetes (the docker-compose dev flow must retain the same
  semantics).
* Failures must be *loud*. The current silent-idle mode is the deeper defect; whatever we build
  must make "this pod is not the owner" an observable, intentional state rather than an accident.

## Considered Options

* **NATS KV lease keyed per pipeline, using `Create` + `Update`-with-revision CAS**
* Kubernetes `coordination.k8s.io/Lease` leader election
* Upgrading to the `jetstream` KV package for per-key TTL
* Per-pod slot names (give each pod its own replication slot)
* Do nothing; keep `replicas: 1` permanently

## Decision Outcome

Chosen option: **NATS KV lease keyed per pipeline**, because it is the only option that provides
mutual exclusion without a new datastore (ADR-0011), without a client/server upgrade, and without
binding the worker to Kubernetes.

### The TTL constraint that shapes the design

`go.mod:26` pins the NATS Go client at v1.37.0, and the codebase uses its legacy `nats.KeyValue` API.
That interface provides exactly the two primitives needed:

* `Create(key, value)` — insert-iff-absent. The atomic first claim.
* `Update(key, value, last uint64)` — CAS on revision. Atomic renew, and atomic steal of an expired
  lease.

But `KeyValueConfig` exposes **bucket-level TTL only**. Per-key TTL exists solely in the newer
`jetstream` package (`KeyValueConfig.LimitMarkerTTL`, a per-op `ttl` threaded through
`updateRevision`) and is server-gated behind nats-server 2.11+ (`ErrLimitMarkerTTLNotSupported`).

**Therefore lease expiry must not depend on KV TTL.** Expiry is an `expires_at` timestamp *inside*
the lease value, enforced by readers via CAS. Bucket TTL serves only as a GC backstop, which also
means leases cannot live in `cdc-dp-config` — a TTL on that bucket would delete pipeline configs.

### Design

A new bucket `cdc-dp-leases` (`History: 1`, `Replicas: 3`, bucket TTL = 10× lease duration as
janitor only) and a new `internal/lease` package.

Key `cdc.lease.pipeline.<id>`, value (msgpack): `{pipeline_id, owner_id, epoch, acquired_at,
expires_at, hostname}`.

* **Acquire** — `Create`. On `ErrKeyExists`, `Get`; if `expires_at` is past, `Update` at the
  observed revision. A CAS conflict means a peer won: back off with jitter, do not spin.
* **Renew** — `Update` at the held revision every `leaseTTL/3`, on a dedicated goroutine that
  touches nothing but KV (renewal starvation behind a blocked goroutine is a failure mode).
  Conflict or `ErrKeyNotFound` ⇒ lease lost.
* **Release** — on graceful stop, verify `owner_id` is us, then `Delete`. This is what makes
  rollout handover sub-second rather than TTL-bound.

**Epoch is the KV revision at acquisition.** JetStream KV revisions are stream sequence numbers,
strictly increasing per bucket, which makes epoch a valid monotonic fencing token. Epoch changes
only on *acquisition* and is carried unchanged through renewals, so a holder's token is stable for
as long as it holds. This property is what the fencing story rests on.

**`leaseTTL = 15s`, renew every 5s.** Crash failover is bounded by TTL; graceful shutdown is
immediate via explicit release. 5s was rejected as too sensitive to NATS latency and clock skew —
spurious lease loss causes real producer churn.

The claim belongs **inside `startNewWorker`** (`manager.go:594`, after `Validate()`, before
`m.factory(...)`), because all three entry paths — config update, `reloadAllWorkers` (`:267`), and
the crash-restart path via `attemptRestart` (`:717`) — funnel through it. The code already treats it
as the single chokepoint.

A lost lease must **`Shutdown`, not `Drain`**. `Drain` (`pipeline.go:289`) is a graceful flush that
waits for the walsender; that is precisely wrong when you may already have been fenced out.
`Shutdown` (`:298`) calls `p.cancel()`, which actually kills `runProducer` and the pg connection.
Bound it with a timeout well under the 30s `ShutdownTimeout`; if the producer will not die while a
peer is claiming the slot, crashing the pod is the safer outcome.

### Consequences

* Good: `replicas > 1` becomes safe, and pod failure recovers in ≤15s instead of waiting on a
  Kubernetes restart.
* Good: ownership becomes explicit and observable rather than an emergent property of who won a
  race.
* Good: no new infrastructure, no client upgrade, no Kubernetes coupling.
* **Bad, and accepted: a NATS outage stops all pipelines.** Leases expire, nobody can renew,
  everyone stands down. Today's single pod would stream straight through a control-plane blip. We
  accept this deliberately — correctness over availability. A "grace mode" that keeps producing past
  expiry when KV is unreachable but Postgres is healthy was considered and **rejected**: it trades a
  bounded outage for an unbounded split-brain window, which is the exact failure this ADR exists to
  prevent. Clustered NATS mitigates the outage risk; grace mode would not mitigate split-brain.
* Bad: clock skew between pods can cause premature expiry or steal. Mitigated by a generous TTL
  relative to plausible skew.
* Neutral: the worker gains a hard dependency on the lease bucket existing. Bootstrap must create it.

### Confirmation

The single invariant that catches nearly every way this design can be gotten wrong:
**egress checkpoint LSN per `(pipeline, sink, table)` is monotonically non-decreasing for an entire
run.** Every e2e scenario below asserts it.

* Unit (`internal/lease`): N concurrent `Acquire` ⇒ exactly one winner; expired-lease steal;
  renew-after-loss closes `Lost()`; epoch strictly increases across acquisitions. Use the existing
  testcontainers NATS module — mocking JetStream CAS would test the mock, not the design.
* Unit (`internal/config`): `startNewWorker` does not call the factory when the lease is held;
  lease loss triggers `Shutdown` (not `Drain`) within the bound; two `ConfigManager`s against one
  bucket start exactly one worker per pipeline.
* E2E: 3 workers × M pipelines ⇒ exactly M producers, no duplicate sink rows; SIGKILL the owner ⇒
  takeover within TTL + margin, no loss or duplication; rolling restart under write load; NATS
  partition of one pod ⇒ it stands its producer down and writes no stale checkpoints.

## Pros and Cons of the Options

### Kubernetes `coordination.k8s.io/Lease`

* Good: purpose-built for leader election; battle-tested; server-side TTL semantics.
* Bad: requires an in-cluster client, ServiceAccount, and RBAC — new deployment surface.
* Bad: makes the worker undeployable outside Kubernetes, breaking the docker-compose dev flow's
  fidelity.
* Bad: splits control-plane state across two systems, against ADR-0011.

### Upgrading to the `jetstream` KV package for per-key TTL

* Good: native per-key TTL; lease expiry becomes the server's problem.
* Bad: requires nats-server 2.11+ — an operational prerequisite, not just a code change.
* Bad: wide migration; `manager.go` alone has ~15 KV call sites with context-less signatures.
* Bad: buys only what an `expires_at` field already provides.

### Per-pod slot names

* Good: eliminates slot contention outright; no coordination needed.
* Bad: N pods × N slots each retaining WAL on the source primary — the failure mode the
  `CDCSourceSlotLagCritical` alert exists to catch, deliberately induced.
* Bad: every pod captures every change ⇒ full duplicate publication, and producer msg-ids are fresh
  UUIDs (`internal/engine/producer.go:365`), so JetStream `TrackMsgId` dedups none of it.
* Bad: slots are orphaned when pods are rescheduled.

### Do nothing (`replicas: 1` forever)

* Good: correct today, zero work. This is the current state after the accompanying chart change.
* Bad: capture stops until Kubernetes reschedules the pod — no HA, and the RTO is whatever the
  scheduler decides.
* Neutral: acceptable while capture volume fits one pod. This ADR is what we do when it does not,
  or when the restart gap becomes unacceptable.

## More Information

### Phasing

The design is deliberately splittable. Phase 2 is the point at which `replicas > 1` becomes safe.

| Phase | Scope |
|---|---|
| 1 | `internal/lease` + `cdc-dp-leases` bucket + `bootstrapKV` CAS. Not yet wired in. |
| 2 | Wire claim/renew/release into `startNewWorker`/`stopWorker`/`Stop`; lease-loss teardown; slot-busy error classification; metrics. **`replicas: 3` becomes safe.** |
| 3 | `Epoch` on `protocol.Checkpoint` + monotonic fenced checkpoint writes. |
| 4 | `PipelineRole` split so consumers run on non-owner pods. |
| 5 | Member registry + rendezvous-hash assignment for even distribution. |

Phases 3-5 are independently shippable after 2 and can be reordered by whether correctness margin
(3), throughput (4), or distribution (5) is wanted first.

A prerequisite phase for clustering NATS was dropped: NATS already runs in cluster mode. Note that
`k8s/nats.yaml` (a `replicas: 1` Deployment with no JetStream or route configuration) does **not**
describe the running deployment — those manifests are dev/reference only. Worth confirming
separately that the KV buckets are created with `Replicas: 3`; cluster mode alone does not give R3
buckets, and R3 is what makes the CAS operations linearizable across a node failure — which
ADR-0011 already assumes.

### Related work this surfaced

Independent of leases, and arguably more urgent than the lease work itself:

1. **Slot-in-use must not be silent.** The Info-level infinite recursion in
   `internal/vendor/go-pq-cdc/connector.go:324-331` should be a bounded, loudly-logged, metric-bearing
   retry. A worker that cannot capture should not report `Status: "Running"` and pass readiness.
   Note the recursion also leaks a stack frame per second.
2. **Readiness should reflect pipeline health**, not just NATS connectivity — with the caveat that
   under leases a legitimately-idle standby must stay Ready, or the Service will drop it.
3. **`bootstrapKV`** (`cmd/pipeline/main.go:158`) is check-then-write; concurrent pods each seed a
   different bcrypt hash. Needs `Create`-based seeding behind a marker key.
4. **`terminationGracePeriodSeconds: 30`** is shorter than `DrainTimeout + ShutdownTimeout` (30+30),
   so SIGKILL can strand a lease for its full TTL. Raise to 90 when leases land.
5. The stale comment at `pipeline.go:151` claiming per-worker slot uniqueness should be corrected —
   it is the sentence most likely to reproduce this bug.

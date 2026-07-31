# Fix Sequence 2 — NATS Persistence + KV Reconnect

**Scope:** Critical 12 (KV/control-plane reconnect + dead watchers), Critical 19 (JetStream persistence in k8s), Medium (bucket-create TOCTOU), and the manifest-drift root cause (k8s/ vs helm vs compose).

**Why this sequence matters:** NATS KV (`cdc-dp-config`, `internal/protocol/config.go:19`) is the *system of record* for every pipeline, source, sink, global config, auth config, schema-evolution state, transition state, heartbeat, and checkpoint. Today it is (a) unreachable-forever after a 2-minute NATS outage, and (b) stored in RAM on a single-replica k8s Deployment. Either failure loses or freezes the entire control plane; the second one *silently re-seeds example config pointed at production databases* (see §1.4).

---

## 1. Objective & failure modes today

### 1.1 Control-plane connection dies terminally after ~2 min of NATS downtime

- `internal/infra/nats.go:21` — `nc, err := nats.Connect(cfg.URL)` with **zero options**. nats.go defaults: `MaxReconnects=60`, `ReconnectWait=2s`. 60 × 2s ≈ 2 minutes of downtime → the client transitions to `CLOSED`, which is **terminal**: no code path ever reconnects.
- Both binaries use this connection for everything control-plane:
  - `cmd/api/main.go:69` — the API's *only* NATS connection. After CLOSE: every `h.kv.Get/Put/Keys` in `internal/api/handler.go` errors; `/readyz` (`cmd/api/main.go:93`) returns 503 forever; `/healthz` returns 200 forever → Kubernetes pulls the pod from the Service but **never restarts it** (liveness passes). Permanent zombie until manual delete.
  - `cmd/pipeline/main.go:52` — the worker's KV connection. Same zombie pattern (`/readyz` at `cmd/pipeline/main.go:124-132`, `/healthz` at `120-123`). Running pipelines keep flowing data (the data plane at `internal/stream/nats/subscriber.go:20` and `publisher.go:40,56` correctly uses `MaxReconnects(-1)`), but the worker can never again receive config changes, deletes, or heartbeat, and checkpoints written via KV fail.

### 1.2 Config watchers die permanently — even before the terminal close

- `internal/config/manager.go:208-212` (`handleGlobalUpdates`) and `manager.go:289-293` (`handlePipelineUpdates`): when `watcher.Updates()` closes (`!ok`), the goroutine logs `"watcher closed, exiting ..."` and **returns**. Nothing recreates it. The watch channel closes when the connection closes, when the server deletes the consumer's interest (e.g. KV stream recreated after a memory-store NATS restart — exactly the k8s scenario), or on any server-side consumer teardown.
- Consequence: a worker that looks healthy runs stale pipelines forever — new pipelines never start, deleted pipelines never stop, config edits never apply. This is strictly worse than a crash because nothing alerts.
- Same class of bug in the API SSE endpoint: `internal/api/handler.go:1366` `h.kv.Watch(pattern)` inside `StreamMetrics` — a closed watcher silently ends updates for connected dashboards (lesser severity: the HTTP client reconnects).
- Aggravator: the supervisor treats *any* KV error as "crash" — `manager.go:810-812`: `if err == nats.ErrKeyNotFound || err != nil { // treat as crash }`. During a NATS blip, a worker that finished a legitimate transition is misdiagnosed as crashed and restarted; during a longer outage `attemptRestart`'s `kv.Get` (`manager.go:662`) also fails and it restarts from cached config.

### 1.3 k8s NATS has no persistence, no PVC, wrong workload kind

- `k8s/nats.yaml:33` — `args: ["-js"]` only. No `-sd <dir>` → JetStream **memory store**. `k8s/nats.yaml:17-21` — a `Deployment`, `replicas: 1`, no volume at all (not even `emptyDir`). Any pod restart, eviction, node drain, or OOM (limit is 512Mi at `nats.yaml:45` — trivially exceeded by JetStream data plane streams held in RAM) wipes:
  - the entire `cdc-dp-config` KV bucket (all configs, checkpoints, schema-evolution state),
  - all JetStream data streams auto-provisioned by watermill (`subscriber.go:67` `AutoProvision: true`) — in-flight CDC events gone.
- Commit `0ab8d40` fixed only docker-compose (`docker-compose.yaml:6` `command: ["-js", "-sd", "/data"]` + `nats_data` volume at `:172`). The k8s path was never touched.

### 1.4 Loss amplifier: silent re-bootstrap from the embedded example config

- `cmd/pipeline/main.go:157-167` (`bootstrapKV`): if `kv.Keys()` returns empty, the worker **re-seeds the KV from the embedded `config.example.yaml`**, with env overrides (`POSTGRES_SOURCE_HOST`, `DATABEND_HOST`, …). After a NATS wipe in production, the first worker to reconnect resurrects a control plane made of *example pipelines pointed at whatever the env vars say* — in the helm chart those env vars point at the real production RDS (`values.production.yml:27-31`). A wipe therefore doesn't just lose config; it can start unintended snapshots/replication against production databases.
- The bootstrap is also racy under concurrent worker startup: N fresh workers all see empty KV and all seed (last-write-wins `kv.Put`s at `main.go:290-316`, no CAS).

### 1.5 Bucket creation TOCTOU

- `internal/infra/nats.go:32-41` — `js.KeyValue(bucket)` then, on any error, `js.CreateKeyValue(...)`. Two processes starting concurrently (API + worker, or 3 worker replicas — helm HPA min is 3, `values.production.yml:149`): both fail the Get, both Create, the loser receives `stream name already in use`, `InitNATS` fails, `log.Fatal` (`cmd/pipeline/main.go:54`, `cmd/api/main.go:71`) → crashloop at cold start until timing luck resolves it. Also: the created bucket has no explicit `Storage`, `Replicas`, or `History` — it inherits server defaults (memory store on the current k8s server; R1 everywhere).

### 1.6 Manifest drift — three divergent universes

| Universe | NATS? | Persistence? | Who uses it |
|---|---|---|---|
| `docker-compose.yaml` | in-compose `nats:2.10-alpine` | ✅ `-sd /data` + volume (0ab8d40) | local dev |
| `k8s/*.yaml` (`nats.yaml`, `api.yaml`, `pipeline.yaml`) | in-cluster Deployment | ❌ memory, single replica | **nobody, apparently** — `NATS_URL=nats://nats:4222` hardcoded, no ArgoCD app points here; drifted (no ENCRYPTION_KEY/JWT_SECRET, different resources) |
| `deploy/helm-chart/` | **none deployed** — `NATS_URL` is a SealedSecret (`values.production.yml:22`, decrypted into `shared-secrets`, injected via `envFrom` in `templates/worker/deployments.yaml`) | unknown/unverifiable | **the real deploy path** — ArgoCD `Application` at `deploy/helm-chart/argocd-app/production.yml` sources `deploy/helm-chart` with `values.production.yml`; `deploy/helm-chart/argocd-init.yml` bootstraps the app-of-apps from `deploy/helm-chart/argocd-app` |

The production NATS is therefore an opaque external endpoint. Its persistence and replication settings are not in git, not reviewable, and not guaranteed. The `k8s/` directory is dead weight that *looks* authoritative and encodes the memory-store trap.

**Objective:** (1) the KV connection reconnects forever and the watchers resurrect with no missed state; (2) bucket/bootstrap is idempotent and safe under N concurrent replicas; (3) JetStream state lives on disk with explicit replication, deployed from a single GitOps source of truth (the helm path), and the other two universes are reconciled.

---

## 2. Target design

### 2.1 (a) Connection resilience + watcher resurrection

#### 2.1.1 Connection options (`internal/infra/nats.go`)

Mirror the data-plane options (`subscriber.go:18-42`) into a shared, exported helper so all four connection sites converge on one policy. New function in `internal/infra/nats.go`:

```go
// ControlPlaneOpts returns the standard resilient connection options for
// control-plane (KV) NATS connections. MaxReconnects(-1) means the client
// never enters terminal CLOSED state due to outage; only an explicit
// Close() does.
func ControlPlaneOpts(component string) []nats.Option {
    return []nats.Option{
        nats.Name("cdc-control-plane-" + component), // "api" | "worker"
        nats.MaxReconnects(-1),
        nats.ReconnectWait(2 * time.Second),
        nats.ReconnectJitter(500*time.Millisecond, 2*time.Second), // jitter for thundering herd (HPA min 3 workers + 3 APIs reconnect together)
        nats.Timeout(5 * time.Second),
        nats.PingInterval(20 * time.Second),
        nats.MaxPingsOutstanding(2),
        nats.ReconnectBufSize(8 * 1024 * 1024), // explicit; buffers KV Puts made while disconnected
        nats.RetryOnFailedConnect(true),        // survive NATS-not-yet-up at pod start instead of log.Fatal crashloop
        nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
            metrics.NatsControlPlaneDisconnects.Inc()
            log.Warn().Err(err).Str("component", component).Msg("control-plane NATS disconnected")
        }),
        nats.ReconnectHandler(func(nc *nats.Conn) {
            metrics.NatsControlPlaneReconnects.Inc()
            log.Info().Str("component", component).Str("url", nc.ConnectedUrl()).Msg("control-plane NATS reconnected")
        }),
        nats.ClosedHandler(func(_ *nats.Conn) {
            // With MaxReconnects(-1) this fires only on deliberate Close().
            log.Warn().Str("component", component).Msg("control-plane NATS connection closed")
        }),
        nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
            log.Error().Err(err).Bool("has_subscription", sub != nil).Str("component", component).Msg("control-plane NATS async error")
        }),
    }
}
```

`InitNATS` signature gains the component name (or a full `NATSConfig.Component` field) and calls `nats.Connect(cfg.URL, ControlPlaneOpts(cfg.Component)...)`.

Notes:
- `RetryOnFailedConnect(true)` changes startup semantics: `nats.Connect` returns a conn in `RECONNECTING` state instead of an error when NATS is down. The subsequent `js.KeyValue`/`CreateKeyValue` will then error until connected — wrap bucket acquisition in the bounded retry loop of §2.2 so pod startup rides out a NATS restart instead of crashlooping. `/readyz` already gates on `nc.Status() == CONNECTED`, so an unconnected pod correctly stays out of rotation.
- Two new counters in `internal/metrics` (`NatsControlPlaneDisconnects`, `NatsControlPlaneReconnects`) — no labels, no cardinality risk.
- Optional hardening: add a liveness dimension — if you want k8s to eventually recycle a pod that has been disconnected for a very long time (e.g. > 15 min), have `/healthz` in both `cmd/api/main.go:89` and `cmd/pipeline/main.go:120` return 503 when `nc.Status() == nats.CLOSED` (terminal only — never for RECONNECTING). With `MaxReconnects(-1)` CLOSED shouldn't happen, but this converts "impossible state" into "self-healing state" for free.

#### 2.1.2 Watcher resurrection (`internal/config/manager.go`) — the load-bearing half

The invariant to build on: **NATS KV `Watch` replays the latest revision of every matching key on (re)creation** (initial-values delivery, terminated by the `nil` sentinel already handled at `manager.go:218` and `:294`), and the manager already has full idempotency machinery for redelivery — `m.revisions` staleness check (`manager.go:344`), `reflect.DeepEqual` config comparison (`:349`), and stale-delete-marker suppression (`:308-319`, T1-27). Therefore *recreating a watcher and replaying everything is safe by construction*; what's missing is only the loop that recreates it, plus one reconciliation for the case KV history no longer carries a delete marker.

Restructure `Watch`/`handleGlobalUpdates`/`handlePipelineUpdates` into supervised watch loops:

```go
// Watch keeps the same signature. Instead of spawning the handlers directly,
// it spawns two supervision loops.
func (m *ConfigManager) Watch(ctx context.Context) error {
    m.ctx, m.cancel = context.WithCancel(ctx)
    m.primeGlobalConfig()          // existing steps 1 & 1b, extracted
    m.restoreSupervisorRevisions()

    // Fail fast on first-ever watch so misconfiguration is still loud at startup.
    gw, err := m.kv.Watch(protocol.KeyGlobalConfig)
    if err != nil { return fmt.Errorf("failed to watch global config: %w", err) }
    pw, err := m.kv.Watch(protocol.PrefixPipelineConfig + "*")
    if err != nil { gw.Stop(); return fmt.Errorf("failed to watch pipeline configs: %w", err) }

    go m.superviseWatcher(m.ctx, "global", gw,
        func(ctx context.Context, w nats.KeyWatcher) { m.consumeGlobalUpdates(ctx, w) },
        func() (nats.KeyWatcher, error) { return m.kv.Watch(protocol.KeyGlobalConfig) },
        nil)
    go m.superviseWatcher(m.ctx, "pipeline", pw,
        func(ctx context.Context, w nats.KeyWatcher) { m.consumePipelineUpdates(ctx, w) },
        func() (nats.KeyWatcher, error) { return m.kv.Watch(protocol.PrefixPipelineConfig + "*") },
        m.reconcilePipelines) // extra resync only needed for pipelines (deletes)
    return nil
}

// superviseWatcher consumes a watcher until its channel closes, then loops:
// backoff -> recreate watcher -> post-resurrection reconcile -> consume again.
// It exits only on ctx.Done().
func (m *ConfigManager) superviseWatcher(
    ctx context.Context,
    name string,
    first nats.KeyWatcher,
    consume func(context.Context, nats.KeyWatcher),
    recreate func() (nats.KeyWatcher, error),
    reconcile func(context.Context),
) {
    w := first
    backoff := time.Second
    for {
        consume(ctx, w) // returns when Updates() closes OR ctx done
        _ = w.Stop()
        if ctx.Err() != nil { return }

        metrics.ConfigWatcherRestarts.WithLabelValues(name).Inc()
        log.Warn().Str("watcher", name).Dur("backoff", backoff).
            Msg("KV watcher closed unexpectedly; resurrecting")

        for {
            select {
            case <-ctx.Done():
                return
            case <-time.After(jitter(backoff)): // +/-20% jitter
            }
            nw, err := recreate()
            if err != nil {
                log.Warn().Err(err).Str("watcher", name).Msg("failed to re-establish KV watcher; retrying")
                backoff = min(backoff*2, 30*time.Second)
                continue
            }
            w = nw
            backoff = time.Second
            break
        }
        if reconcile != nil {
            reconcile(ctx) // see below
        }
        // loop back to consume(w): the new watcher replays latest values of all
        // keys, so updates missed during the gap are re-delivered here and
        // filtered by the existing revision/DeepEqual idempotency checks.
    }
}
```

`consumeGlobalUpdates` / `consumePipelineUpdates` are the existing bodies of `handleGlobalUpdates` (`manager.go:202-247`) and `handlePipelineUpdates` (`manager.go:283-394`) with exactly one change: on `!ok` they **return** instead of being the top of a dead goroutine (the `return` stays, but the caller now resurrects). The nil-sentinel `continue`, revision checks, DeepEqual checks, smart-reload path, and delete handling are untouched.

**`reconcilePipelines` — closing the missed-delete hole.** Watcher replay covers missed Puts (latest value redelivered) and usually missed Deletes (KV delete writes a tombstone which is the latest revision and is delivered to a fresh watcher). The one gap: a key that was **purged** (`nats.KeyValuePurge` history removal) or whose tombstone was compacted while the watch was down would never be replayed, leaving a running worker for a deleted pipeline. The API's DeletePipeline path and the KV bucket config decide whether that's reachable, but a defensive reconcile is cheap and also covers "KV stream was recreated empty" (§1.4 scenario):

```go
func (m *ConfigManager) reconcilePipelines(ctx context.Context) {
    keys, err := m.kv.Keys() // tolerate error: skip reconcile, watcher replay still ran
    if err != nil && err != nats.ErrNoKeysFound { ... log & return }
    live := map[string]bool{}
    for _, k := range keys {
        if id := extractPipelineID(k); id != "" { live[id] = true }
    }
    m.workersMu.RLock()
    var stale []string
    for id := range m.workers { if !live[id] { stale = append(stale, id) } }
    m.workersMu.RUnlock()
    for _, id := range stale {
        log.Warn().Str("pipeline_id", id).Msg("reconcile: pipeline absent from KV after watcher resurrection; stopping worker")
        m.stopWorker(ctx, id)
    }
}
```

Guard: if `len(keys)==0` **and** we currently have >0 workers, do *not* mass-stop — log an error and skip (an empty KV after we had config is far more likely to be the §1.4 wipe than a legitimate delete-all; stopping everything would compound the disaster). Emit a dedicated metric/alert (`cdc_kv_suspected_wipe`).

**Supervisor KV-error hardening** (small but same failure family): `manager.go:810-812` — split `err != nil` into `err == nats.ErrKeyNotFound` (→ genuine "no transition, treat as crash") vs other errors (→ retry the `kv.Get(TransitionStateKey)` up to ~3 times with 1s backoff before concluding crash). Same treatment for `attemptRestart`'s config fetch (`manager.go:662`) — it already falls back to cache, which is acceptable; just keep it.

**API SSE watcher** (`internal/api/handler.go:1366`): no resurrection loop needed server-side — on watcher close, `return` from the handler so the SSE request ends and the client reconnects (today it may spin or hang). One-line check where the updates channel is ranged.

**Wire `SetNatsConn`** (`manager.go:94`, currently never called — noted in findings): `cmd/pipeline/main.go` should call `mgr.SetNatsConn(nc)` after `NewConfigManager`. It both activates the intended fast heartbeat path and gives the manager access to `nc.Status()` if we later want to gate resurrection retries on connection state (not required — `kv.Watch` errors are an equivalent signal).

#### 2.1.3 What we deliberately do NOT do

- No re-`Connect()` logic anywhere: with `MaxReconnects(-1)` the *connection object* heals itself; only *watchers/consumers* need resurrection. The `nats.KeyValue` handle and `JetStreamContext` remain valid across reconnects.
- No event-sourced replay of intermediate revisions: the manager is level-triggered (latest config wins), so replaying only latest values is correct. Intermediate states missed during an outage were never contractually observable.

### 2.2 (b) Idempotent bucket creation & bootstrap

#### 2.2.1 Create-first bucket acquisition (`internal/infra/nats.go:32-41`)

Invert the order and make it a bounded loop; also pin the bucket's durability properties explicitly instead of inheriting server defaults:

```go
func getOrCreateKV(js nats.JetStreamContext, cfg NATSConfig) (nats.KeyValue, error) {
    kvCfg := &nats.KeyValueConfig{
        Bucket:   cfg.BucketName,
        History:  5,                    // keeps delete tombstones + short audit trail
        Storage:  nats.FileStorage,     // explicit: never depend on server default
        Replicas: cfg.KVReplicas,       // from KV_REPLICAS env; 1 dev, 3 prod
    }
    var lastErr error
    for attempt := 0; attempt < 5; attempt++ {
        kv, err := js.CreateKeyValue(kvCfg)
        if err == nil { return kv, nil }
        if errors.Is(err, nats.ErrStreamNameAlreadyInUse) || strings.Contains(err.Error(), "already in use") {
            if kv, err2 := js.KeyValue(cfg.BucketName); err2 == nil { return kv, nil }
            // bucket vanished between Create-conflict and Get (or config mismatch) -> retry
        }
        lastErr = err
        time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond) // linear + implicit jitter from scheduling
    }
    return nil, fmt.Errorf("failed to get or create KV bucket %q: %w", cfg.BucketName, lastErr)
}
```

Details:
- `CreateKeyValue` against an **existing identical** bucket succeeds idempotently in nats.go; the conflict branch only triggers on genuine races or config mismatch. On config mismatch (e.g. existing R1 bucket vs requested R3) `js.KeyValue` (pure lookup) still succeeds — startup must not be blocked by a replication mismatch; log a warning comparing `kv.Status()` replicas vs desired, and leave the actual scale-up to the migration step (§5.3), since changing replicas is a `js.UpdateStream("KV_cdc-dp-config")` administrative operation, not something every worker should attempt at boot.
- `KVReplicas` and `BucketName` come from env (`KV_REPLICAS`, default `1`) so docker-compose (single node) keeps working. Helm sets `KV_REPLICAS: "3"` in the shared configmap.
- With `RetryOnFailedConnect(true)` (§2.1.1) the very first `CreateKeyValue` may fail with "not connected"; the 5-attempt loop with sleeps covers a short NATS start lag. If NATS is down longer, `InitNATS` still errors and the pod crashloops with backoff — acceptable and visible at cold start.

#### 2.2.2 Concurrent-safe, opt-in bootstrap seeding (`cmd/pipeline/main.go:157-320`)

Two changes:

1. **Single-seeder election via CAS.** Replace the "keys empty → everyone seeds" check with a KV `Create` (create-only, fails if the key exists — the primitive already used in `UpdateSchemaStateCAS`, `manager.go:121`):
   ```go
   if _, err := kv.Create("cdc.bootstrap.lock", []byte(workerID)); err != nil {
       if errors.Is(err, nats.ErrKeyExists) { return nil } // someone else seeded / is seeding
       return err
   }
   // ... perform seeding, then write "cdc.bootstrap.done" with a timestamp
   ```
   All the `kv.Put`s in the seed body stay as-is; only one worker reaches them.
2. **Opt-in in production.** Gate the entire `bootstrapKV` call behind `KV_BOOTSTRAP=true` (set in `docker-compose.yaml` worker env; **absent** in helm values). In production, an empty KV must surface as an incident (readiness stays green, but a startup log at Error level + the `cdc_kv_suspected_wipe` metric from §2.1.2), never as a silent re-seed of `config.example.yaml` against production hosts (§1.4). This is the cheapest, highest-value data-loss firebreak in the whole sequence.

### 2.3 (c) Persistence / HA topology + single source of truth

#### 2.3.1 Decision: helm chart is the single source of truth; NATS moves in-cluster under GitOps; `k8s/` is deleted

Justification:
- ArgoCD demonstrably deploys `deploy/helm-chart` (`argocd-app/production.yml` → path `deploy/helm-chart`, `values.production.yml`; `argocd-init.yml` is the app-of-apps bootstrap). Nothing references `k8s/`. Keeping `k8s/` invites exactly the drift that produced Critical 19 — its NATS manifest is the memory-store trap, and its `NATS_URL=nats://nats:4222` contradicts the sealed-secret model. **Delete `k8s/` in the same PR that lands the helm NATS** (or, if someone does use it out-of-band, move it to `docs/examples/` with a loud README — but deletion is the recommendation; git history preserves it).
- The current external-NATS-via-SealedSecret model makes the system of record **unauditable**: nobody can verify from this repo whether the production NATS has file storage, what its retention is, or how it's backed up. For a KV that holds checkpoints and all configs, durability settings must be reviewable in git. Running NATS in-cluster with the official `nats` helm chart, deployed by the same ArgoCD app-of-apps, restores that property. (If the external NATS is a shared org-wide cluster that must stay — see Open Questions §6; the code changes in §2.1/§2.2 are topology-agnostic and land either way.)

#### 2.3.2 Concrete deployment: official NATS helm chart as a sibling ArgoCD Application

Add `deploy/helm-chart/argocd-app/nats-production.yml` (and `nats-staging.yml`):

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: cdc-nats-production
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://nats-io.github.io/k8s/helm/charts/
    chart: nats
    targetRevision: 1.2.x        # pin exact version
    helm:
      valueFiles: []
      values: |
        config:
          cluster:
            enabled: true
            replicas: 3
          jetstream:
            enabled: true
            fileStore:
              enabled: true
              dir: /data
              pvc:
                enabled: true
                size: 20Gi
                storageClassName: <cluster-default-or-ssd-class>   # decide per cluster
              maxSize: 18Gi        # < PVC size, leave headroom
            memoryStore:
              enabled: false
        podTemplate:
          topologySpreadConstraints:
            kubernetes.io/hostname:
              maxSkew: 1
              whenUnsatisfiable: DoNotSchedule
          # NOTE: no node_type: spot selector — the system of record must NOT
          # run on spot/preemptible nodes (app pods do, values.production.yml:130).
        container:
          env:
            GOMEMLIMIT: 1GiB
          merge:
            resources:
              requests: { cpu: 500m, memory: 1Gi }
              limits:   { cpu: "1",  memory: 1536Mi }
        natsBox:
          enabled: true            # ships the `nats` CLI for backup/inspection
  destination:
    server: https://kubernetes.default.svc
    namespace: cdc-pipeline-production
  syncPolicy:
    syncOptions: [CreateNamespace=true]
    # No automated prune for the stateful app; sync manually or with prune disabled.
```

The chart renders a **StatefulSet** with one PVC per replica, headless service `nats-headless`, client service `nats` — giving the URL `nats://nats.cdc-pipeline-production.svc.cluster.local:4222`.

Then in `values.production.yml` / `values.staging.yml`:
- Remove `NATS_URL` from `sharedConfigs.secrets` (line 22) and add to `sharedConfigs.configs`: `NATS_URL: "nats://nats:4222"` (same namespace; no credentials → no secret needed. If NATS auth is added later, it returns to a SealedSecret).
- Add `KV_REPLICAS: "3"` (production) / `"1"` or `"3"` (staging, per budget) to `sharedConfigs.configs`.

#### 2.3.3 R3 clustering vs R1 + PV — decision: R3 in production, R1 in staging/dev

| | R1 + PVC (single server) | R3 cluster |
|---|---|---|
| Pod restart / reschedule | survives (PVC reattach) but **full outage** during reschedule — control plane + data plane down minutes; watchers now resurrect (§2.1) so recovery is automatic, but CDC delivery stalls | zero-downtime for R3 assets; rolling server upgrades possible |
| Node loss | outage until PVC can attach to a new node; on zonal PV, potentially stuck until node returns | quorum survives one node loss |
| Volume loss/corruption | **total state loss** (back to §1.4) | survives one volume loss; replica repaired from peers |
| Cost/complexity | 1 pod, 1 PVC | 3 pods, 3 PVCs, RAFT traffic; must pin `Replicas: 3` per asset (server clustering alone does NOT replicate an R1 stream) |
| Split-brain/ops | none | needs 3 healthy peers for asset placement; monitor `/healthz?js-enabled=true` and RAFT lag |

For a KV that is the system of record for checkpoints, R1's "volume loss = lose everything" is disqualifying in production. **Production: 3-server cluster, KV bucket `Replicas: 3` (§2.2.1).** Staging: 3 servers if cheap, else R1 accepted with documented risk. Docker-compose: unchanged (single server, R1).

Data-plane streams: watermill `AutoProvision` (`subscriber.go:67`, `publisher.go:36`) creates streams with client defaults (**R1**, file storage once the server has a file store). Bumping data streams to R3 requires passing stream config through watermill or pre-provisioning streams — flag this as a follow-up work item (§3, WI-9); it is a durability upgrade, not a blocker for this sequence (data-plane loss is bounded by Postgres slot retention; the *checkpoint/config* KV is the unrecoverable part and is covered).

#### 2.3.4 Reconciling the three universes — end state

- `docker-compose.yaml`: already correct; add `KV_BOOTSTRAP=true` to the `pipeline` service env (§2.2.2).
- `k8s/`: **deleted**.
- `deploy/helm-chart/`: app chart unchanged in shape; NATS as sibling ArgoCD Application (pinned upstream chart); `NATS_URL` demoted from SealedSecret to plain config; `KV_REPLICAS` added. One paragraph in `deploy/README.md` stating "helm-chart + argocd-app is the only supported k8s deploy path".

---

## 3. Ordered work items

Each item is independently landable in the order given; code items (1–6) are topology-agnostic and safe against both the current external NATS and the future in-cluster one.

**WI-1 — Control-plane connection options.**
Files: `internal/infra/nats.go`, `internal/metrics/` (new counters), `cmd/api/main.go:69`, `cmd/pipeline/main.go:52`.
Change: add `ControlPlaneOpts(component string)` (§2.1.1); `InitNATS` takes/uses component name; both mains pass `"api"` / `"worker"`. Add `NatsControlPlaneDisconnects/Reconnects` counters.
Why: kills the terminal-CLOSE (Critical 12, connection half). Dependencies: none.

**WI-2 — Idempotent bucket creation with explicit durability.**
Files: `internal/infra/nats.go:32-41`.
Change: create-first bounded loop (§2.2.1); `NATSConfig` gains `KVReplicas` (read from `KV_REPLICAS` env by the mains, default 1); `KeyValueConfig` gains `History: 5`, `Storage: FileStorage`, `Replicas`. Log-warn on live-bucket config mismatch, never fail on it.
Why: fixes TOCTOU crashloop; makes bucket durability an explicit reviewed property. Dependencies: WI-1 (RetryOnFailedConnect interaction).

**WI-3 — Watcher resurrection loop + reconcile.**
Files: `internal/config/manager.go` (`Watch` :150-200 split into prime/restore/supervise; `handleGlobalUpdates` :202-247 and `handlePipelineUpdates` :283-394 become `consume*`; new `superviseWatcher`, `reconcilePipelines`, `jitter`), `internal/metrics/` (`ConfigWatcherRestarts` counter vec by watcher name — fixed 2-value cardinality).
Change: §2.1.2 in full, including the empty-KV mass-stop guard + `cdc_kv_suspected_wipe` metric.
Why: the harder half of Critical 12 — resilient conn is useless while watchers stay dead. Dependencies: none (works with current conn too; strictly better with WI-1).

**WI-4 — Supervisor transient-KV-error hardening.**
Files: `internal/config/manager.go:810-812` (and touch `:662` fallback comment).
Change: retry `kv.Get(TransitionStateKey)` ≤3× with 1s backoff; only `ErrKeyNotFound` (or exhausted retries) ⇒ crash path.
Why: stops NATS blips from being misdiagnosed as worker crashes → spurious restarts mid-outage. Dependencies: none.

**WI-5 — Bootstrap seeding: CAS election + prod opt-out.**
Files: `cmd/pipeline/main.go:57-60,157-320`, `docker-compose.yaml` (add `KV_BOOTSTRAP=true` to `pipeline` env).
Change: §2.2.2 — `kv.Create("cdc.bootstrap.lock", ...)` election; whole call gated on `KV_BOOTSTRAP=true`; empty-KV-in-prod logs Error + metric.
Why: removes the data-loss amplifier (§1.4) and the N-replica seed race. Dependencies: none.

**WI-6 — API SSE watcher termination + `SetNatsConn` wiring.**
Files: `internal/api/handler.go` (~:1366 loop: return on channel close), `cmd/pipeline/main.go` (call `mgr.SetNatsConn(nc)` after `NewConfigManager` at :84).
Why: closes the remaining watcher-death instance; activates the intended fast-heartbeat path (dead code today). Dependencies: none.

**WI-7 — In-cluster NATS via official chart + ArgoCD app.**
Files (new): `deploy/helm-chart/argocd-app/nats-production.yml`, `.../nats-staging.yml` (§2.3.2).
Files (edit): `deploy/helm-chart/values.production.yml` (drop `NATS_URL` sealed secret :22, add `NATS_URL` + `KV_REPLICAS: "3"` to `sharedConfigs.configs`), same for `values.staging.yml`.
Why: Critical 19 — durable, replicated, git-reviewable JetStream. Dependencies: WI-2 (KV_REPLICAS consumed), migration plan §5.

**WI-8 — Delete `k8s/`; document the single deploy path.**
Files: delete `k8s/nats.yaml`, `k8s/api.yaml`, `k8s/pipeline.yaml`; add a short `deploy/README.md`.
Why: removes the drifted universe that encodes the memory-store trap. Dependencies: WI-7 merged (so no window with zero k8s NATS definition in-repo).

**WI-9 (follow-up, tracked not blocking) — Data-plane stream replication.**
Files: `internal/stream/nats/subscriber.go`, `publisher.go` (or a pre-provisioning step in `cmd/pipeline`).
Change: stop relying on watermill `AutoProvision` defaults; provision streams explicitly with `Replicas: 3` in production.
Why: R1 data streams on an R3 cluster still lose in-flight events on one volume loss. Dependencies: WI-7 deployed.

---

## 4. Test plan

Existing harness: `internal/config/manager_test.go` uses testcontainers (`testcontainers-go/modules/nats`) — reuse it; testcontainers can `Stop`/`Start` a container to simulate outage, and start with fixed host port mapping to reconnect to "the same" server.

**T1 — Reconnect past the old 60-attempt limit (unit/integration).**
Start NATS container with a fixed host port; `InitNATS`; `container.Stop()`; wait longer than `60 × ReconnectWait` would have allowed (compress by overriding `ReconnectWait` to 50ms in a test hook → wait > 3s ≙ >2min real); `container.Start()`; assert `nc.Status()==CONNECTED` and a `kv.Put`/`Get` round-trips. Assert `NatsControlPlaneReconnects ≥ 1`.

**T2 — Watcher resurrection with missed update (the core scenario).**
1. `NewConfigManager(kv, factory).Watch(ctx)` with a spy factory; put pipeline config A; assert worker A starts.
2. Stop the NATS container. While down: nothing (KV unreachable).
3. Restart container; **from a second connection**, write config A′ (changed batch size) and a brand-new pipeline B, and delete pipeline… (see T3).
4. Assert within a deadline: watcher-restart metric incremented; worker A transitions to A′ exactly once (revision idempotency respected — no double restart from replay); worker B starts.
Variant: kill only the watcher (server-side consumer delete via `nats consumer rm` on the KV stream's watch consumer) without killing the connection — assert resurrection also fires.

**T3 — Missed delete + reconcile.**
With watcher down (container stopped, or watch consumer deleted), delete pipeline A from a side channel after restart, ensure its tombstone is compacted (or simulate purge), bring the watcher back; assert `reconcilePipelines` stops worker A. Counter-case: wipe the whole bucket while workers exist → assert **no** mass-stop, `cdc_kv_suspected_wipe` raised.

**T4 — Concurrent-startup bucket race.**
Fresh NATS server; run 10 goroutines calling `InitNATS` simultaneously; assert all 10 succeed and `kv.Status()` shows one bucket with the expected History/Storage/Replicas. Repeat 50× (race is timing-sensitive). Run with `-race`.

**T5 — Bootstrap election.**
Fresh bucket; 5 goroutines run `bootstrapKV` concurrently; assert seed keys written once, `cdc.bootstrap.lock` holds one workerID, no error from losers. Then: `KV_BOOTSTRAP` unset + empty bucket → assert no seeding and the error log/metric fires.

**T6 — Supervisor transient-error hardening.**
Mock KV (existing `internal/api/mocks/nats_mock.go` pattern) returning `nats.ErrConnectionClosed` twice then the transition key → assert worker is *not* treated as crashed. `ErrKeyNotFound` immediately → crash path as before.

**T7 — Chaos in a real cluster (staging), after WI-7.**
- `kubectl delete pod nats-0` → assert: KV intact (`nats kv ls cdc-dp-config` via nats-box), zero pipeline worker restarts, watcher-restart metric may tick, CDC rows keep flowing (R3) or resume ≤ 2 min (R1 staging).
- `kubectl drain` the node hosting nats-1 → same assertions; verify PVC reattach.
- Network partition > 2 min: NetworkPolicy (or `iptables` in a debug pod) blocking 4222 from one worker pod for 3 minutes → assert the worker recovers *without pod restart*, applies a config change made during the partition, and its `/readyz` flapped 503→200.
- Full NATS scale-to-zero for 5 min → scale back → assert workers reconnect, watchers resurrect, **no re-seed occurs** (KV_BOOTSTRAP unset), all pipelines resume from stored checkpoints.

**T8 — Persistence across reschedule (the Critical 19 acceptance test).**
Staging: create a pipeline via API; `kubectl delete pod nats-0 --force`; after the pod returns, assert the pipeline config and checkpoint keys have identical revisions/values. Then simulate the pre-fix behavior once against a memory-store NATS in kind to keep a regression demo.

**CI:** T1–T6 run under the (to-be-created, Fix Sequence 3) test job with testcontainers; mark `testing.Short()`-skipped like the existing manager tests.

---

## 5. Rollout / migration

Order of operations is designed so **no step can lose KV data** and each step is individually revertible.

**Phase 0 — Land code (WI-1…WI-6), ship images.** Safe against the current external NATS; behavior only improves. Note `RetryOnFailedConnect` changes cold-start semantics (pod waits instead of crashlooping) — mention in release notes for on-call.

**Phase 1 — Snapshot current state (before touching topology).**
From any pod with the `nats` CLI (or a one-off nats-box against the sealed `NATS_URL`):
`nats stream backup KV_cdc-dp-config /backup/kv-$(date +%F).tgz` — the KV bucket is stream `KV_cdc-dp-config`. Also `nats stream ls` to inventory data-plane streams. Store the backup off-cluster. **If the current external NATS turns out to be memory-store, treat this backup as the only durable copy and do Phase 2 immediately.**

**Phase 2 — Deploy in-cluster NATS (WI-7, NATS app only).**
Commit `nats-production.yml`/`nats-staging.yml` ArgoCD Applications; sync **staging first**. Do not change app `NATS_URL` yet — the new cluster runs empty alongside the old. Verify: 3/3 pods ready, `nats server report jetstream` shows file store + cluster formed, PVCs bound on non-spot nodes.

**Phase 3 — Migrate KV data.**
Freeze control-plane writes (short maintenance window; announce; optionally scale the API to 0 so no config edits land mid-copy — data-plane keeps flowing against the old NATS):
`nats stream restore /backup/kv-<date>.tgz` against the new cluster (restores `KV_cdc-dp-config` with contents + revisions), then `nats stream edit KV_cdc-dp-config --replicas 3` (or restore then update) to lift it to R3. Verify key count and spot-check a pipeline config + checkpoint key against the old cluster.
Data-plane streams: prefer **draining over migrating** — let consumers on the old NATS finish in-flight batches (workers keep running), and accept that unconsumed events at cutover are re-fetched from Postgres (the replication slot is the true source; note interplay with Fix Sequence 1's ack correctness). Migrating JetStream data streams with live consumers is not worth the complexity.

**Phase 4 — Cut over `NATS_URL`.**
One commit to `values.production.yml`: remove the `NATS_URL` sealed secret entry, add plain `NATS_URL: nats://nats:4222` + `KV_REPLICAS: "3"` to `sharedConfigs.configs`. ArgoCD sync → rolling restart of api/worker (envFrom change forces new pods only if the configmap checksum is annotated — the current templates don't checksum-annotate; do a manual `kubectl rollout restart` of both Deployments after sync, and note this as a template gap). Watch: `/readyz` green, `cdc_config_watcher_restarts` stable, pipelines resume from checkpoints, `nats kv ls` shows heartbeat keys updating.
SealedSecret implications: the `shared-secrets` SealedSecret still carries the other keys; removing one key from `encryptedData` re-renders fine. Keep the old encrypted `NATS_URL` value in git history in case of rollback. If your sealed-secrets controller key rotated since sealing, re-seal the whole block — verify with a staging dry run.

**Phase 5 — Decommission + cleanup.**
After ≥1 week of stable operation: stop pointing anything at the old NATS; if it was dedicated to this system, tear it down; land WI-8 (`k8s/` deletion). ArgoCD `PruneLast`/manual prune only — never auto-prune the NATS Application (guard against a bad sync deleting the StatefulSet; PVCs survive StatefulSet deletion by default, which is the last-ditch safety net).

**Rollback points:** Phase 2 — delete the NATS app, nothing referenced it. Phase 3 — old cluster untouched; discard new bucket, retry. Phase 4 — revert the values commit + rollout restart; old NATS still has pre-freeze state; anything written to the new KV after cutover is lost on rollback (keep the window between cutover and "rollback no longer allowed" explicit, e.g. 24h, and take a post-cutover backup before declaring it closed).

**Ongoing:** add a CronJob (or external job) running `nats stream backup KV_cdc-dp-config` daily to object storage — R3 protects against hardware loss, not against operator error / bad deploys wiping the bucket.

---

## 6. Risks, open questions, sequencing

**Risks**
1. *Watcher replay double-apply.* Resurrection replays the latest revision of every key; the revision map + DeepEqual checks (`manager.go:344,349`) should suppress restarts, but `applyHierarchy` (`manager.go:472`) mutates the compared config — if global config changed during the outage, replayed pipeline configs will legitimately differ and restart workers. That is *correct* behavior, just potentially a restart storm after a long outage on a many-pipeline deployment. Mitigation: the existing `GlobalReloadDelay`/`StabilizationDelay` pacing; consider jittering `transitionWorker` calls during a reconcile burst (note in WI-3).
2. *`reconcilePipelines` vs `kv.Keys()` cost* — `Keys()` materializes all keys of the bucket (heartbeats, checkpoints, everything). Fine at current scale; if the bucket grows, switch to `kv.ListKeys()`/`WatchAll(IgnoreDeletes, MetaOnly)` filtering. Also it runs only after resurrection, not on a timer.
3. *Every-replica-runs-every-pipeline* (High finding, out of scope here): 3–20 worker replicas all watch and all start workers. WI-3 multiplies nothing (each replica resurrects its own watchers), but the restart-storm risk in Risk 1 is per-replica. The sharding/leader-election fix belongs to a later sequence; sequencing note: land it *after* this one — it will build on the same supervised-watch structure.
4. *Legacy JetStream API.* `nc.JetStream()` / `nats.KeyValue` is the deprecated API surface; the new `jetstream` package has better-behaved ordered watchers. Migrating now would balloon the diff across manager/API/publisher; explicitly deferred. The resurrection loop is designed so a later API swap only replaces the `recreate` closure.
5. *Chart-values drift for the NATS app* — pin the upstream chart version in the Application and treat bumps as reviewed PRs; the upstream `nats` chart has had breaking values reshuffles between majors.
6. *Spot nodes.* App pods select `node_type: spot` (`values.production.yml:130`). Ensure the NATS values **do not** inherit any such selector; verify scheduling landed on on-demand nodes before Phase 3. An R3 quorum on spot nodes can lose 2 members simultaneously during a reclamation wave.

**Open questions (need answers before Phase 2)**
1. **What is the current production NATS behind the sealed `NATS_URL`?** Managed service? Shared org cluster? Does it have file storage today? If it is a *shared* cluster that must remain, the alternative plan is: keep external, but (a) obtain/commit its JetStream config as documentation, (b) still land WI-1…WI-6 unchanged, (c) run `nats stream edit KV_cdc-dp-config --replicas 3` there, (d) skip WI-7/Phases 2–4. Decision owner: whoever holds the sealing key / infra.
2. **Storage class + zone topology** of the CCE cluster (values reference Huawei CCE): are there ≥3 nodes across failure domains for the anti-affinity/spread constraint, and which storageClass gives non-preemptible SSD PVs?
3. **KV `History: 5` retroactivity**: changing history on the existing bucket is a stream-config update; confirm the restore in Phase 3 carries the new limit, else run `nats stream edit` post-restore.
4. **Is anything actually deployed from `k8s/`?** Grep of ArgoCD apps says no, but confirm with the cluster (`argocd app list`) before deleting.
5. **Bucket TTL/size limits**: heartbeat keys are written every 2–15s per pipeline (`manager.go:635-641`) — with `History: 5` the KV stream grows 5 revisions per key; confirm `MaxBytes` for `KV_cdc-dp-config` (suggest 1Gi cap) so heartbeat churn can't fill the PVC.

**Sequencing within the overall remediation**
- This sequence is #2 after delivery-guarantee fixes, per `00_SUMMARY.md`. WI-1…WI-6 have no dependency on Fix Sequence 1 and can land in parallel branches.
- WI-7/8 (topology) should wait for CI (Fix Sequence 3) only if possible — but do **not** block on it if the answer to Open Question 1 is "the external NATS is memory-backed": in that case Phase 1's backup and Phase 2 become the most urgent action items in the entire remediation program.
- The CAS-on-config-writes finding (High, `api/handler.go`) and the sharding finding both compose with this design; neither blocks it.

# Remediation Plans — Index

Six implementation-ready plans, one per fix sequence in [`../00_SUMMARY.md`](../00_SUMMARY.md). Each plan was written after re-reading the actual code, so where review line numbers had drifted the plans cite re-verified anchors. Plans are documents only — no source files were modified.

| Plan | Sequence | Scope |
|---|---|---|
| [01a_delivery_source_ack.md](01a_delivery_source_ack.md) | 1 (source half) | End-to-end ack/checkpoint contract; vendored-lib slot-advance paths; snapshot resume; reviving the dead ack helpers |
| [01b_delivery_sink_correctness.md](01b_delivery_sink_correctness.md) | 1 (sink half) | Durable-write-before-ack; per-PK ordering/idempotency; PK metadata persistence; TOAST; type fidelity; schema evolution |
| [02_nats_persistence_reconnect.md](02_nats_persistence_reconnect.md) | 2 | KV connection resilience + watcher resurrection; idempotent bootstrap; JetStream persistence/HA; manifest-drift reconciliation |
| [03_ci_quality_gate.md](03_ci_quality_gate.md) | 3 | PR/main triggers; test+lint+vuln stages; testcontainers & Playwright in CI; unified build; hook alignment |
| [04_ssrf_and_secrets.md](04_ssrf_and_secrets.md) | 4 | TOCTOU-closing dialer; complete IP blocklist; fail-closed validation; TLS config; secret hygiene; key-example drift |
| [05_web_and_operational.md](05_web_and_operational.md) | 5 + 6 | Wire-contract enforcement; SSE auth + WriteTimeout; runtime config; metric cardinality; supervisor state machine; sharding; container hardening |

---

## Discoveries during planning that change the picture

These were found while reading code for the plans and are **not** in the original review documents. Two of them alter priority.

### 1. Prod can reboot into example pipelines pointed at production databases — *raises Sequence 2's urgency*
`cmd/pipeline/main.go:157-320` — `bootstrapKV` silently re-seeds an empty KV from the embedded `config.example.yaml` with env overrides. Combined with the missing JetStream persistence (Critical 19) and helm env pointing at real RDS (`values.production.yml:27-31`), a NATS wipe doesn't merely lose config — the system boots **example pipeline definitions against production databases**. Gating this behind `KV_BOOTSTRAP=true` plus a CAS seeder election is the cheapest, highest-value firebreak in the entire remediation and should land before anything else in Sequence 2.

### 2. The DLQ is provably dead, not merely default-nil — *confirms Critical 3 is always live*
Nothing in the engine or `cmd/` tree ever injects `dlq_publisher`. `sink.New` receives only the JSON options map from KV, which structurally cannot carry a Go publisher. So the drop-record-then-ack path is not an edge case behind a config flag — it is the only behavior that exists in production.

### 3. An existing test asserts the bug
`internal/sink/databend/sink_remediation_test.go:677` — `TestBatchUpload_DeserializationFailure_NoPublisher` asserts that `BatchUpload` returns nil when no DLQ publisher is wired, i.e. it encodes the data-loss behavior as expected. It must be inverted as part of the fix, not merely extended.

### 4. A third slot-advance path
Beyond per-event `lc.Ack` and the keepalive fast-forward, vendored `stream.go:334` also advances the position on undecodable messages. Any fix that neutralizes only the first two remains incorrect.

### 5. The ack fix is plumbing, not invention — *reduces Sequence 1 scope*
`internal/engine/consumer.go:448-465` already publishes LSN-carrying acks (`Op:"ack"` with the real `LSN`). The LSN is discarded at `producer.go:182-191`, which forwards a bare `struct{}{}` with a `default:` drop into an untyped `chan<- struct{}` (`provider.go:11`). Similarly, the vendored snapshotter already implements full resumable chunk state (`cdc_snapshot_job`/`cdc_snapshot_chunks`, `LoadJob`, per-chunk claims) — Critical 11 exists only because `source.go:412` gates `Snapshot.Enabled` on `IngressLSN==0` while snapshot rows leak LSNs into checkpoints. Both findings mean materially less new machinery than the review implied.

### 6. `ListSourceTables` needs no rebinding to exploit — *do this SSRF path first*
The other SSRF paths require winning a DNS-rebinding race. `ListSourceTables` (`handler.go:1015-1024`) has **zero** host validation, so `host=169.254.169.254` plus a normal GET reaches cloud metadata directly. Cheapest exploit, cheapest fix.

### 7. `k8s/` is dead weight
ArgoCD (`deploy/helm-chart/argocd-app/production.yml`) sources `deploy/helm-chart` only. Nothing deploys `k8s/`. Recommendation is deletion rather than repair — which removes the "three divergent universes" drift finding by subtraction.

### 8. Additional web casing bug + an impossible field
`web/src/api/stats.ts:9` reads `totalRowsSynchronized` against wire `total_rows_synced` — a summary tile that never populates. And `protocol.TableStats` has **no** `table_name` field at all, so the frontend's `data.tableName` read can never work regardless of casing; the SSE envelope has to add it.

### 9. The pre-push hook may never have run successfully
The hook's e2e path uses the 34-byte `ENCRYPTION_KEY` that `crypto.GetEncryptionKey` rejects — worth confirming whether that gate has ever actually executed.

---

## Cross-plan dependencies and sequencing

**Hard ordering constraints:**
- **Dependency bumps before CI tightening.** The pgx/x-text/x-crypto upgrades plus a one-time local `go mod tidy` must merge *before* `go mod tidy` is removed from the `&goBuild` anchor, or the tag build breaks under `-mod=readonly` (plan 03).
- **01a ⟷ 01b on TOAST.** The recommended fix is REPLICA IDENTITY FULL with old-tuple merge at the source (01a) plus a `Partial bool` protocol guard that DLQs partial rows at the sink (01b). Either can land first; both are needed for correctness.
- **Web image ⟷ helm configmap rename** is the only true lockstep deploy pair (plan 05).
- **Sequence 1 depends on Sequence 2's connection work** in practice: an ack contract that must reach KV is only as reliable as the KV connection.

**Independent, landable immediately:** the `KV_BOOTSTRAP` gate, the `ListSourceTables` validation, the CI PR gate in warn-mode, the metric relabel, and every container/helm hardening item.

**Breaking changes flagged for coordination:** the `sslmode` default flip from `disable` to `require` (needs an env-flag grace period), `/metrics` relocation (needs a scrape-config update), and the SSE auth change (touches frontend and backend together).

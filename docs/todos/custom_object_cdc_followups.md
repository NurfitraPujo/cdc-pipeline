# Follow-ups surfaced by the CDC custom-object transform review

Out of scope for `plans/cdc_custom_object_transform_remediation.md` — recorded
here rather than fixed as part of that change. Each is independent.

See also `docs/todos/holistic_review_remediation.md`, which overlaps in places.
(`docs/todos/lossy_type_mappings.md` covered the decimal/array half of item 1 <!-- hygiene:ignore: file deliberately deleted in fd76c0f; historical note, not a live pointer -->
below; it was fixed and the TODO deleted on 2026-08-04.)

---

## 1. `mapPgTypeToDatabend` matches type names by substring

**Severity:** medium — a latent silent-corruption hazard for the whole type map.

`internal/sink/databend/sink.go:611-655` dispatches on
`strings.Contains(t, "int")` **before** the float branch. `"double precision"`
happens not to contain `"int"`, so it correctly reaches `FLOAT64` — by luck, not
by design. Any future pg type name containing `int` (an `interval`, say) would be
silently mapped to `INT64`. `"point"` would too.

**Action:** replace the substring chain with an explicit map keyed on normalised
type names, with a documented fallback. Add a test asserting the exact mapping for
every type the custom-object sync declares.

---

## 2. No compaction, `OPTIMIZE TABLE` or snapshot retention anywhere

**Severity:** low at current volume (~100 rows/s peak), rising with growth.

Every batch emits `REPLACE INTO`, which in Databend is a `MERGE INTO` —
copy-on-write at block granularity, appending a snapshot version per statement.
A grep for `OPTIMIZE TABLE`, `VACUUM` and `compact` across the repo returns
nothing.

The CDC plan's WS-4B decides to keep merge-on-write on the basis of current
volume, and includes compaction scheduling as part of that decision. This entry
records the standing gap for any *other* pipeline using the Databend sink.

**Action:** a compaction/retention policy at the sink level, not per-pipeline.

---

## 3. `AckWait` is not derived from batch wall-clock

**Severity:** medium under degradation.

`internal/stream/nats/subscriber.go:63-65` defaults `AckWait` to 30s.
`MaxAckPending` is `BatchSize * 2` (`internal/engine/factory.go:146`). A batch
that is chunked into several serial requests, or held by a circuit breaker, is
**redelivered while still being worked** — duplicate in-flight work exactly when
the system is already degraded.

The CDC plan raises this for its own pipeline (WS-5.6), but the default applies to
every pipeline.

**Action:** derive `AckWait` from `batch_size × timeout_ms` plus sink latency, or
at minimum document the relationship and validate it at config load.

---

## 4. Retry backoff intervals default to zero

**Severity:** medium.

`RetryConfig{MaxRetries: 3}` (`internal/engine/factory.go:166`) leaves
`InitialInterval` and `MaxInterval` unset, so the doubling loop at
`internal/engine/consumer.go:786-793` keeps `backoff = 0` and falls through to a
flat 5s (`:800-810`) — effectively a tight retry loop against an already-degraded
dependency.

The CDC plan fixes this for its pipeline (WS-5.3); the defaults themselves should
change.

**Action:** set sane `InitialInterval`/`MaxInterval` defaults with jitter, and
validate that a zero interval never silently degrades to the flat path.

---

## 5. `SnapshotChunkSize` is not wired to the snapshot path

**Severity:** low, until someone needs to tune a backfill.

`SourceConfig.SnapshotChunkSize` (`internal/protocol/config.go:262`) only affects
the producer's dynamic-table path (`internal/engine/producer.go:1456-1459`). The
actual snapshot chunk size is **hardcoded 8000** at
`internal/source/postgres/source.go:871-884`. There is also no snapshot
worker-count knob — `SetSnapshotActiveWorkers`
(`internal/vendor/go-pq-cdc/internal/metric/metric.go:35`) is a metric, not a
setting, and `workerProcess`
(`internal/vendor/go-pq-cdc/pq/snapshot/worker.go:124-158`) is a single
sequential loop.

**Action:** wire `SnapshotChunkSize` through to the vendored `SnapshotConfig`, and
expose a worker count.

---

## 6. Dead code and contract drift in the NATS transformer

**Severity:** low.

- `internal/transformer/nats/protobuf.go:143-152` — `filterMessages` is dead;
  `TransformBatch` uses the index-based path.
- `protobuf.go:82-91` — `Transform` (single-message) returns `(m, true, err)`,
  keep-on-error, contradicting the batch path's fail-closed behaviour.
  Unreachable today because the consumer prefers `TransformBatch`.
- `protobuf.go:238` — `PipelineId` is always `""`; the pipeline ID is never
  plumbed through the factory, so a responder cannot attribute requests.
- `internal/transformer/AGENT.md` documents error handling as "return the original
  message if the transformation is non-critical", the opposite of actual batch
  behaviour.

The CDC plan's WS-10 covers these for that workstream; recorded here in case it
lands first.

---

## 7. A processor that fails to construct leaves the pipeline silently untransformed

**Severity:** medium.

`internal/engine/factory.go:208-217` — an unregistered processor type, or a
factory that errors, only logs and `continue`s. The pipeline then reports
`Running` while transforming nothing. Similarly `internal/engine/consumer.go:134-137`
skips a processor with empty `operation_types` entirely, with no warning and no
match-all default.

The CDC plan's WS-8 fixes both for that pipeline; the underlying behaviour is
repo-wide.

**Action:** make construction failure fatal for the pipeline, or reflect it in the
heartbeat status so a degraded pipeline is never reported healthy.

---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers (user-ratified during Opus validation review)
consulted: internal/engine/factory.go (deriveAckWait, defaultMaxAckWait, PipelineFactory.AckWaitCeiling), internal/engine/consumer.go (handleSinkError, MaxRetries), docs/decisions/0008-at-least-once-with-sink-side-idempotency.md, summaries/ws5_ws6_ws7_implementation.md
---

# `deriveAckWait`'s derived worst case is clamped to a configurable ceiling, default 10 minutes

## Context and Problem Statement

`deriveAckWait` (`internal/engine/factory.go`) replaced a flat 30s NATS `AckWait` with
`BatchSize * per-chunk-timeout + sinkSubscriberLatencyMargin`, sized against WS-3 chunking's
stated pathological worst case: one oversized record per chunk, splitting a batch into up to
`BatchSize` serial requests against the nats/protobuf transformer. That derivation shipped with no
upper bound. Its own tests, asserting the unbounded value as intended behavior, exposed the
consequence directly: `BatchSize:1000, timeout_ms:20000` computed to **~5.6 hours**;
`BatchSize:200` with the WS-5-item-4 default timeout computed to **~50 minutes**. Both are
realistic-looking configs, not edge cases.

An AckWait that long makes the fix a regression in the exact failure mode it was meant to improve.
`MaxAckPending = BatchSize * 2` means every message in a sink's subscriber queues behind one
in-flight, unacked batch — so a consumer that hard-crashes, gets OOM-killed, or is
network-partitioned mid-batch stalls the *entire* pipeline for however long AckWait is, with
nothing redelivering the stuck batch to a healthy consumer until it expires. Combined with WS-5's
own declared gap (no circuit breaker yet), a daya-core outage during that window compounds the
stall rather than being contained by it.

## Decision Drivers

* Duplicate delivery in this system is **safe by design** — [0008](0008-at-least-once-with-sink-side-idempotency.md)
  establishes at-least-once delivery with sink-side idempotency: `REPLACE INTO ... ON (pk)`
  rewrites the same row on a replayed LSN, and `AckManager.Confirm` at or below the already-observed
  watermark is a no-op. A long AckWait therefore buys **no correctness** — only avoided redundant
  work — which makes it a pure availability/efficiency trade-off, not a safety one.
* The realistic worst case is not `BatchSize` chunks: WS-3's chunker bounds chunk count by encoded
  payload bytes (`chunkSafetyFraction * maxPayload`, `internal/transformer/nats/protobuf.go`), so a
  real batch typically produces a handful of chunks (1–5), not `BatchSize` of them. K chunks ×
  `timeout_ms` is closer to ~75–150s including sink latency for realistic K, nowhere near the
  unclamped derivation's output.
* Going too short is a distinct, opposite failure: `handleSinkError`
  (`internal/engine/consumer.go:804`) counts a redelivered-and-failed attempt toward `MaxRetries`
  and eventually the DLQ. During a degradation where the dependency is slow but not down, a
  premature redelivery's own attempt also times out — a **genuine** failure, not a false alarm —
  so an AckWait too close to the realistic worst case amplifies failure exactly when retries should
  be patient.
* An operator's actual workload (record size distribution, transformer latency) may not match this
  repo's assumptions, so the bound should be tunable without a code change.

## Considered Options

1. **No ceiling** (the initial WS-5 implementation) — derive purely from `BatchSize * timeout_ms`.
2. **A short, fixed ceiling** (e.g. 2 minutes) close to the realistic worst case.
3. **A generous, fixed ceiling** (e.g. 1 hour) that only guards against truly pathological configs.
4. **A moderate, configurable ceiling**, defaulting to 10 minutes, overridable per-factory.
5. **Derive the realistic chunk count directly** (`ceil(estimatedBatchBytes / maxPayload)`) instead
   of `BatchSize`, eliminating the need for a ceiling at all.

## Decision Outcome

Chosen: **option 4**. `defaultMaxAckWait = 10 * time.Minute` (`internal/engine/factory.go`),
applied as the ceiling in `deriveAckWait(cfg, ceiling)` when `ceiling <= 0`; `PipelineFactory` gained
an `AckWaitCeiling time.Duration` field threading an explicit override through to every pipeline it
creates. `deriveAckWait`'s clamp is `[defaultAckWait (30s floor), ceiling]`.

Ten minutes is roughly a 4–8× margin over the realistic worst case (~75–150s), which absorbs
reasonable variance in chunk count and per-chunk latency without being so long that a stalled
consumer becomes an extended, on-call-worthy outage before JetStream even attempts redelivery. It
also comfortably exceeds the "too short causes premature-redelivery amplification" floor implied by
the realistic worst case — two minutes would not.

Option 1 was rejected as the finding that prompted this ADR: mathematically defensible against the
stated pathological case, but that case is not the realistic one, and the unbounded output is a
regression in the availability failure mode the derivation exists to improve.

Option 2 (short, ~2 minutes) was rejected: it sits too close to the realistic worst case itself,
leaving little margin for legitimate variance (a slightly slower daya-core response, a slightly
larger batch than typical) before a *healthy* batch starts getting prematurely redelivered —
reintroducing the amplification failure mode above for ordinary operation, not just degradation.

Option 3 (generous, ~1 hour) was rejected: it reintroduces most of the original problem at a smaller
scale — an hour-long pipeline stall on a hard consumer failure is still a severe availability
regression relative to the pre-WS-5 flat 30s, just less severe than 5.6 hours.

Option 5 (derive real chunk count from bytes) was rejected for this pass, not permanently: it would
be the more precise fix, but `deriveAckWait` runs at pipeline-config-build time
(`PipelineFactory.CreateWorker`), before any message has been seen, so there is no batch-byte-size
estimate available without adding new instrumentation (e.g. tracking observed average record size
per table and feeding it back into config derivation) — a larger change than this fix's scope. A
fixed, configurable ceiling was the change that fit the moment; deriving from actual observed
payload sizes remains worth doing later if the 10-minute default proves wrong for a real workload.

### Consequences

* Good: closes the multi-hour-stall regression while preserving the original derivation's intent
  (scale with batch size and timeout, don't stay flat at 30s).
* Good: configurable without a code change (`PipelineFactory.AckWaitCeiling`), so a workload this
  default doesn't fit can be tuned in place.
* Bad: 10 minutes is still a judgment call, not a value derived from this pipeline's actual observed
  chunk/latency distribution — see option 5 above. If a real workload's realistic worst case turns
  out to exceed the margin this ADR assumes, the ceiling (or the derivation itself) needs revisiting,
  not just retuning.
* Bad: a pipeline that legitimately needs more than 10 minutes to process one chunk (e.g. an
  unusually slow but not-broken downstream) will now see premature redelivery where the unclamped
  derivation would have waited it out — the same "too short amplifies failure" mechanism this ADR
  itself argues against, just at the tail of the distribution instead of the middle. `AckWaitCeiling`
  exists specifically so that tail case has a supported way out.

## More Information

`internal/engine/factory_ws5_test.go`'s `TestDeriveAckWait_ClampsAtDefaultCeiling` pins the
BatchSize:1000/timeout_ms:20000 case to exactly 10 minutes (not the ~5.6h unclamped value);
`TestDeriveAckWait_CustomCeilingOverridesDefault` proves the override is honoured in both
directions (tighter and looser than the default), not silently ignored in favour of the constant.

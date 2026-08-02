---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers (health-gated unsubscribe rejected during Opus validation review)
consulted: internal/transformer/nats/protobuf.go (sendRequest, transformCircuitBreaker), internal/engine/consumer.go (handleSinkError), internal/engine/producer.go (gobreakerCircuitBreaker, the existing publish-path breaker this reuses the shape of), internal/engine/pipeline.go (Start, the Critical-13 heartbeat comment), plans/cdc_custom_object_transform_remediation.md WS-5, summaries/ws5_ws6_ws7_implementation.md
---

# The transform RPC gets its own circuit breaker; NATS transport errors are classified separately from application errors

## Context and Problem Statement

Before this change, `internal/engine/consumer.go`'s `handleSinkError` treated every transform or
sink failure identically: increment a per-message retry counter, and once it exceeds
`RetryConfig.MaxRetries`, isolate the batch and route it to the DLQ. A daya-core outage (deploy,
crash, network partition) produces `nats.ErrNoResponders` or a request timeout on every single
batch, which under that logic **DLQs live CDC traffic within `MaxRetries` batches of a routine
daya-core deploy** — the exact failure this WS-5 pass exists to close (plan §WS-5, "Today daya-core
being down or slow → `ErrNoResponders` or a 5s timeout → batch error → 3 retries → DLQ").

Two things compound this:

* `internal/transformer/nats/protobuf.go`'s `sendRequest` had no circuit breaker: every batch during
  an outage paid the full per-request timeout (WS-5 item 4 already raised this to
  `max(15s, 5ms×batch_size)`) before failing, so a sustained outage was also a sustained *latency*
  problem, not just an eventual-DLQ problem.
* Nothing distinguished "daya-core is unreachable" (a NATS transport-layer fact, true regardless of
  which records were in the batch) from "daya-core rejected these specific records" (an
  application-layer fact about the batch's content). Both incremented the same retry counter toward
  the same DLQ.

`internal/engine/producer.go` already solved a structurally similar problem for the **publish** path
(NATS publish failures) with a `gobreaker.CircuitBreaker` (`gobreakerCircuitBreaker`,
`producerCircuitBreaker` interface, `publishWithRetry`). That pattern — the same dependency, the same
`Execute`/`IsOpen` seam, the same trip ratio — is the one this decision reuses for the transform path,
per the plan's explicit steer ("reuse that dependency and follow its shape rather than inventing a
second pattern").

## Decision Drivers

* A daya-core outage must never DLQ live traffic (plan §WS-5 acceptance criterion).
* The fix must not fight [0022](0022-ackwait-ceiling-ten-minutes.md)'s AckWait/backoff timeline: an
  open breaker failing fast, combined with `handleSinkError`'s existing blocking
  `time.Sleep(backoff)` before the next pull, must not turn into a tight Nack→redeliver loop, and
  must not need a *second*, independent backoff mechanism.
* `nats.ErrNoResponders` specifically means "nobody is subscribed to this subject" — an unambiguous,
  fast signal that daya-core is entirely absent, not merely slow. It deserves to be treated
  identically to a timeout (both are "unreachable"), not as a special case requiring its own handling
  path.
* Reuse over reinvention: the publish-path breaker is an existing, working pattern; a second,
  differently-shaped breaker for the transform path would cost future readers a second mental model
  for no behavioural benefit.
* **Whatever this pass does must remain recoverable without human intervention.** Anything that can
  silently stop a consumer from making progress is disqualified unless something else in the system
  is actively watching for and correcting that state — see the rejected option below, which violates
  this driver.

## Considered Options

1. **Do nothing beyond WS-5's already-shipped backoff/AckWait work** — rely on the retry backoff
   alone and accept that a sustained outage still eventually DLQs.
2. **A circuit breaker on the transform RPC + explicit transport-error classification.**
3. **A circuit breaker only**, without transport classification — trip fast, but still count every
   open-breaker failure toward `MaxRetries`/DLQ.
4. **Transport classification only**, without a circuit breaker — never DLQ on
   `ErrNoResponders`/timeout/`ErrConnectionClosed`, but still pay the full per-request timeout on
   every batch during an outage.
5. **Hold-and-retry-later at the batch level** (the plan's originally-floated idea) — freeze the
   batch until daya-core returns, bounded by slot lag. Rejected in this pass; see "Gaps" below.
6. **Circuit breaker + transport classification + health-gated unsubscribe** (plan item 7: once a
   consumer accumulates enough consecutive transport failures, unsubscribe its NATS connection so it
   stops pulling work it cannot complete). Implemented in an earlier draft of this pass, then reverted
   during Opus validation review — see "Rejected: health-gated unsubscribe" below.

## Decision Outcome

Chosen: **option 2**. The two pieces compose rather than substitute for each other:

* **Circuit breaker** (`internal/transformer/nats/protobuf.go`): `NatsProtoTransformer.cb`, a
  `transformCircuitBreaker` interface wrapping `*gobreaker.CircuitBreaker`
  (`gobreakerTransformCircuitBreaker`), constructed per-transformer-instance in
  `NewNatsProtoTransformer` via `transformCircuitSettings` — `MaxRequests: 3`,
  `Interval: 5s`, `Timeout: transformCircuitCoolDown (10s)`, trip at `Requests >= 3 && failureRatio
  >= 60%`, matching `internal/engine/producer.go`'s publish breaker's own settings so an operator who
  knows one breaker's behaviour already knows the other's. It wraps **only**
  `conn.RequestWithContext` inside `sendRequest`, not the whole batch/chunk pipeline, so it trips
  purely on transport failures — an individual record daya-core validly rejects comes back as a
  *successful* RPC carrying `Success:false`, never as an `Execute` error, so it can never trip this
  breaker. `sendRequest` checks `cb.IsOpen()` before doing any work (marshal, network), so an open
  breaker fails in microseconds, not `t.timeout`.
* **Transport classification**: `ErrTransportFailure` (sentinel, `internal/transformer/nats/protobuf.go`)
  wraps any error from `conn.RequestWithContext` that `isTransportErr` recognises as one of the
  plan's three named shapes (`nats.ErrNoResponders`, `nats.ErrTimeout`/`context.DeadlineExceeded`,
  `nats.ErrConnectionClosed`), or an open breaker (`ErrCircuitOpen`, also wrapped in
  `ErrTransportFailure` — from the caller's point of view an open breaker *is* a transport failure,
  standing in for the request that would otherwise have failed the same way).
  `internal/engine/consumer.go`'s `handleSinkError` checks `errors.Is(err, transformernats.ErrTransportFailure)`
  and, when true, **never** lets `entry.count > c.retryConfig.MaxRetries` isolate the batch — it
  still Nacks and still waits out the existing jittered exponential backoff (unchanged, so this
  composes with [0022] rather than adding a second timeline), but it retries forever instead of
  eventually reaching `isolatePoisonBatch`/DLQ.

Why the interaction is safe (the concern this ADR's context section and the plan both raise): an
open breaker turns a network round-trip into an immediate, synchronous error return from
`sendRequest`. That error still flows through the *same* `handleSinkError` path as any other sink
failure: Nack, then block on `time.After(backoff)` before the consumer's single-goroutine loop reads
its next message. Nothing about an open breaker skips that blocking wait — it just makes the
*failure* fast, not the *retry cadence*. A tight loop would require the backoff to also be skipped,
which it is not; this was verified directly (`TestHandleSinkError_TransportFailure_NeverIsolatesOrDLQs`
drives the real `flushWithFilter → processMessages → handleSinkError` path six times in a row and
observes each Nack still going through the existing backoff/jitter code, unchanged).

Option 1 was rejected because it is the status quo the plan opens by describing as broken. Option 3
was rejected because it fails fast but still eventually DLQs — a shorter road to the same regression.
Option 4 was rejected because it never DLQs but pays full latency on every batch during an outage,
which is strictly worse operationally than paying it once (the breaker's `MaxRequests:3` probe) and
then failing fast. Option 5 (hold-and-retry, fail-open to a quarantine table bounded by slot lag) is
the plan's own stated `[BLOCKER]` framing for the *ideal* end state, but implementing the slot-lag
bound correctly (`querySlotLagBytes`/`slotLagBytesGauge` wiring, a quarantine-table-or-DLQ-plus-page
decision) is materially larger in scope than this pass and was not attempted — see "Gaps" below.
Option 6 is covered in its own section below since it was implemented and then deliberately removed,
which is a different kind of rejection than "never built."

### Consequences

* Good: a daya-core outage no longer DLQs traffic — verified by
  `TestHandleSinkError_TransportFailure_NeverIsolatesOrDLQs`, which drives `MaxRetries:1` (a config
  that would isolate almost immediately under the pre-fix logic) through six consecutive
  transport-classified failures with a `Times(0)` expectation on the DLQ publish.
* Good: an outage is noticed and failed-fast within `MaxRequests:3` probe requests instead of paying
  `t.timeout` on every subsequent batch.
* Good: a genuinely malformed record — an application-level rejection, not a transport failure —
  still isolates and DLQs exactly as before
  (`TestHandleSinkError_ApplicationFailure_StillIsolatesAndDLQs`, the explicit "doesn't use the
  feature" control).
* Good: composes with the existing backoff/AckWait timeline rather than adding a competing one — no
  new sleep/wait primitive was introduced anywhere in the consumer.
* Bad: **retrying forever on a transport-classified error means unbounded WAL retention for the
  duration of any daya-core outage.** Nothing bounds this: the jittered backoff caps at
  `defaultRetryMaxInterval` (30s) and then retries indefinitely, and every retry that fails is still a
  failure to `publishRecordAck`, so the replication slot's watermark never advances for that table
  while the outage lasts. A sufficiently long outage grows WAL on the source database exactly as
  described in the plan's item 2 framing — this pass narrows the failure mode (no more DLQ) but does
  not add the slot-lag bound that would cap it. See "Gaps" below; this is the single most important
  thing a reader should take from this ADR before assuming "retries forever" is unconditionally safe.

## Rejected: health-gated unsubscribe

An earlier draft of this pass also implemented plan item 7: `Consumer` tracked consecutive
transport-classified failures and, past a threshold (5), called a new
`stream.HealthUnsubscriber.Unsubscribe()` — implemented in `internal/stream/nats/subscriber.go` as
`Conn.Drain()` on the subscriber's dedicated NATS connection — so a sustained-unhealthy consumer would
stop pulling work. It was removed during Opus validation review for three compounding, verified
reasons, none of which is fixable by tuning the threshold:

1. **It is genuinely unrecoverable.** `Conn.Drain()` (`nats.go`) closes the connection; nothing in
   this codebase resubscribes it. `Consumer.Run` sees the resulting closed `msgChan` and returns
   `nil`, which `internal/engine/pipeline.go`'s `Start` goroutine treats as a normal, clean exit — no
   error is logged, nothing retries.
2. **The pipeline keeps reporting healthy while doing nothing.** `Pipeline.Start`'s `wg.Wait()` only
   returns once every consumer goroutine *and* the producer goroutine have exited; the producer keeps
   running, so `finished` never closes and the config manager keeps writing `Status: "Running"` for a
   pipeline that has silently stopped ingesting for the affected sink. `pipeline.go:94-96`'s own
   comment names this exact failure mode as "Critical 13," already fixed once by a previous pass —
   the unsubscribe mechanism reintroduced it through a different door.
3. **The replication slot freezes permanently, not just for the outage's duration.** The only path
   that advances the slot's watermark is `flushWithFilter → publishRecordAck`. A consumer that has
   unsubscribed never flushes again, so `confirmed_flush_lsn` for that consumer's tables freezes while
   the producer keeps reading WAL — unbounded WAL growth on the *source* database that requires a
   human to notice the silently-dead consumer and restart the pipeline. This is strictly worse than
   the pre-WS-5 DLQ behaviour it was meant to improve on.

A fourth, independent problem: the threshold was too eager for the risk it carried. `ErrNoResponders`
returns immediately (no timeout wait), the breaker fails fast after its 3-request probe, and the only
pacing between attempts is the backoff schedule (500ms → 1s → 2s → 4s) — five consecutive failures,
enough to trip the old threshold, elapse in roughly 7.5 seconds. Any routine rolling deploy of
daya-core would trip it, not just a genuine sustained outage.

The mechanism's stated justification — stop a crash-looping daya-core replica from winning its share
of queue-group deliveries and dropping every one — assumes a queue group exists.
`internal/stream/nats/subscriber.go`'s `nats.SubscriberConfig` sets no explicit `QueueGroup`; the
companion plan's queue-group work (WS-5.1) is not implemented in this repo. So the mechanism bought
nothing today and cost a permanent, silent wedge in exchange. **The breaker alone already provides the
useful part** — failing fast instead of paying `t.timeout` — and `handleSinkError`'s existing
Nack-plus-backoff already prevents in-flight batch accumulation without needing the consumer to stop
pulling entirely. If a queue-group deployment is built later (WS-5.1), health-gating should be
revisited then, designed around an explicit, supervised resubscribe path from the start rather than a
one-way `Drain()` — not reintroduced by relaxing this version's threshold.

## Gaps — explicitly not implemented in this pass

* **Slot-lag-bounded fail-open/quarantine** (plan item 2's full `[BLOCKER]` shape: hold for at most
  `T` minutes AND `L` bytes of slot lag, then fail open to a quarantine table or DLQ-and-page). What
  shipped is fail-fast-and-keep-retrying-forever, not hold-then-fail-open — see the "Consequences"
  entry above: this means **unbounded WAL retention for the duration of any daya-core outage**, with
  nothing in this pass capping it. `querySlotLagBytes`/`slotLagBytesGauge`
  (`internal/source/postgres/source.go`) already exist and are the documented starting point for that
  follow-up.
* **Health-gated unsubscribe / queue-group-aware gating** — considered, implemented, and reverted; see
  the dedicated section above for the full reasoning, kept for the next person who considers
  re-adding it.

## More Information

Tests (all real-path, each regression-verified by disabling the fix and confirming the corresponding
test fails, then restoring):

* `internal/transformer/nats/protobuf_ws5_circuit_test.go` — `TestIsTransportErr`,
  `TestClassifyTransportErrKind` (pure classification), `TestSendRequest_NoResponders_WrapsErrTransportFailure`
  (real NATS container, genuinely no responder subscribed — not a mock), `TestSendRequest_OpenBreaker_FailsFastWithoutNetworkCall`
  (a nil `t.conn` proves the network is never touched once the breaker is open),
  `TestSendRequest_BreakerExecuteRejects_ClassifiedAsTransport`,
  `TestSendRequest_ApplicationError_NotClassifiedAsTransport` (the "doesn't use the feature" control).
* `internal/engine/consumer_ws5_test.go` — `TestHandleSinkError_TransportFailure_NeverIsolatesOrDLQs`,
  `TestHandleSinkError_ApplicationFailure_StillIsolatesAndDLQs` (control). Both drive `Consumer.Run`
  end to end (real `flushWithFilter`/`processMessages`/`handleSinkError`), not `handleSinkError`
  called in isolation.

---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers (user-scoped decision; vector correction and posture statement added during Opus validation review)
consulted: internal/transformer/nats/protobuf.go (NewNatsProtoTransformer's nats.Connect call, sendRequest), internal/stream/nats/subscriber.go, plans/cdc_custom_object_transform_remediation.md WS-5 item 8/item 6, summaries/ws5_ws6_ws7_implementation.md
---

# Subject-level authentication on `custom_object_requests.cdc_transform` is an accepted risk, not implemented

## Context and Problem Statement

The plan (WS-5 item 6, called out as `[BLOCKER]`) states: "daya-core's NATS client configures no
credentials at all, so today any process that can reach the cluster may publish to
`custom_object_requests.cdc_transform` and enumerate any company's custom-object schema. The
pipeline must hold a dedicated credential scoped to publish on that subject only, matching the
daya-core-side permission work (companion plan WS-5.10)."

That framing implies the fix is scoping *this* subject specifically. It is not, for a reason the
plan itself doesn't state: `nats.Connect` calls across this entire codebase —
`internal/transformer/nats/protobuf.go`'s transform connection, `internal/stream/nats/subscriber.go`'s
data-plane connection, and every other NATS client in this repo — pass none of `nats.UserCredentials`,
`nats.Token`, `nats.UserInfo`, or `nats.Nkey` (confirmed by grep: zero hits for all four across
`internal/`). The entire NATS connection, for every subject this pipeline publishes or subscribes to,
is unauthenticated. Scoping credentials to one subject on one connection while every other subject
on every other connection remains open to anything that can reach the cluster changes nothing about
what such a caller can already do — reach every other subject instead.

## The actual exposure (corrected)

An earlier draft of this ADR characterized the exposure as "schema metadata disclosure only,"
reasoning that the requester (this pipeline) supplies the row data in the transform request, so an
unauthenticated publisher on the subject can at most learn a company's custom-object schema shape by
crafting a request, and that "the reply is consumed by this pipeline's own sink logic, not executed as
commands from the responder" rules out a write path. That second claim conflated two different things:
it correctly rules out **command injection** (the reply cannot make this pipeline execute arbitrary
SQL/DDL), but it does not rule out **data injection**. `internal/transformer/nats/protobuf.go`'s
`sendRequest` performs a NATS *request*, meaning it accepts a reply from **whichever process answers
first** on the subject — NATS request/reply has no concept of "the real responder." Nothing about this
pipeline's connection distinguishes daya-core's reply from anyone else's. A rogue process that
subscribes to `custom_object_requests.cdc_transform` as a **responder** (not merely a publisher, the
vector the earlier draft considered) can return a `TransformResponse` with `Success: true, Keep: true`
and arbitrary `TransformedData` of its own choosing, which `parseResponseWithOrder`
(`internal/transformer/nats/protobuf.go`) decodes and hands to the sink's `BatchUpload` exactly as if
it were daya-core's own answer — **written straight into Databend as real warehouse rows.**

So the exposure is not merely metadata disclosure. It is **warehouse data integrity**: anything that
can reach the NATS cluster and subscribe to this subject can write arbitrary row data into Databend,
for any table this pipeline transforms, indistinguishable at the sink layer from a legitimate
daya-core response. The original publish-side vector (crafting a request to learn schema shape) is
still real and still narrower than row-data exfiltration for the reasons the earlier draft gave — but
it is the smaller of the two risks this subject's lack of authentication carries, not the whole of it.

## Decision Drivers

* **False assurance is worse than no assurance.** A reader who sees "the transform subject is
  authenticated" reasonably infers the transform subject is protected. Scoping credentials to one
  subject while every other subject on the same unauthenticated connection stays open would not
  actually close the rogue-responder vector above unless the credential model also prevents
  subscribing as a responder, not just publishing requests — a materially different (and larger)
  access-control problem than "add a token to one connection."
* **The real fix is a trust-boundary change, not a per-subject patch.** Authenticating the whole NATS
  connection (credentials, TLS, or both) is the fix that actually closes the exposure. That is a
  cluster-wide change spanning every producer and consumer on this NATS deployment (this repo's
  engine, transformer, and stream packages; daya-core's own NATS client; potentially other consumers
  of the same cluster not in either repo) — well outside a single workstream's scope, and not a
  decision one repository can make unilaterally since the companion plan's WS-5.10 (daya-core-side
  permission work) is a coordinated cross-repo change.
* **The user-confirmed network trust boundary is the actual control in place today.** The NATS cluster
  is network-isolated: it is reachable only by pods running inside the cluster, not from the open
  network. That boundary — not application-layer authentication — is what currently stands between an
  external attacker and the rogue-responder vector above. It is a real control, and it is why this
  decision is accepted rather than deferred as unacceptable. It is also the *only* control: there is no
  defence in depth behind it. Anything that already runs inside the cluster is, by construction, not
  stopped by this boundary.

## Considered Options

1. **Implement per-subject credentials now**, as WS-5 item 6/8 literally asks, scoping only
   `custom_object_requests.cdc_transform`.
2. **Implement connection-wide NATS authentication now** (credentials or TLS across every subject
   this pipeline touches), coordinated with daya-core's WS-5.10.
3. **Skip subject authentication entirely, rely on the existing network-isolation boundary, and record
   the posture explicitly** — including the corrected rogue-responder vector and the "no defence in
   depth" residual — so it resurfaces if that boundary ever changes.

## Decision Outcome

Chosen: **option 3**, per explicit user direction, on the stated basis that the NATS cluster's network
isolation (reachable only by in-cluster pods) is the trust boundary this decision relies on, not "the
exposure is narrow enough not to matter." Recorded here specifically so this does not get silently
re-approved as "already handled" by a future reader skimming WS-5's item list — the plan marks item 6
(its authentication proper) and item 8 (implied by item 6's cross-repo framing) as unresolved, and this
ADR is the record of *why* that is a deliberate, network-boundary-dependent skip, not an oversight, and
what would have to be true for it to become worth revisiting.

Option 1 was rejected: per-subject credentials on an otherwise-open connection would not close the
rogue-responder vector (subscribing as a responder is a different capability than publishing a
request, and nothing in this pass scopes who may subscribe), so it would cost real implementation
effort for a security property that does not actually hold — the false-assurance problem named above.

Option 2 is very likely the eventual correct application-layer fix if the network boundary is ever
weakened, but is out of scope for this pass: it requires coordinated work in daya-core (WS-5.10) and is
a decision affecting every subject on this NATS cluster, not a call this repository's WS-5 resilience
pass can make alone.

### Consequences

* Good: no wasted effort implementing a control that wouldn't hold the property it claims to.
* Good: the actual exposure is named precisely and completely — **warehouse data integrity** via a
  rogue NATS responder impersonating daya-core, not merely schema-metadata disclosure via a crafted
  request (the narrower, publish-side vector, which remains real but secondary) — rather than left
  understated as the earlier draft of this ADR had it.
* Bad — **stated as a posture, not left implicit**: any compromised or malicious pod inside the
  cluster can write arbitrary rows into the warehouse by answering transform requests on
  `custom_object_requests.cdc_transform` before daya-core does, or by publishing crafted requests to
  learn a company's custom-object schema shape. The network boundary is the *entire* control — there is
  no authentication, authorization, or other application-layer defence behind it. This is an accepted
  posture for a cluster whose only occupants are this deployment's own trusted workloads; it stops
  being acceptable the moment that assumption changes (see "When to revisit").
* Bad: this decision is scoped to *this* pass's judgment that a network-isolated cluster makes the
  application-layer gap tolerable for now. It is not a judgment that the underlying exposure is
  acceptable indefinitely, or in a cluster with a different trust profile.

## When to revisit

This decision should be treated as **stale, not merely worth re-checking**, the moment any of the
following becomes true, since each removes the network-isolation basis this ADR relies on:

* The NATS cluster ever becomes reachable by workloads this deployment does not fully trust —
  multi-tenant infrastructure, a shared cluster with other teams' pods, exposure beyond the current
  pod network, or any relaxation of "in-cluster pods only." At that point the rogue-responder vector
  stops being a compromised-pod-only concern and the posture above is no longer acceptable.
* The NATS cluster gains connection-wide authentication (TLS, credentials, or both) for any other
  reason — at that point, subject- and role-scoping the transform request/responder pair stops being a
  false-assurance patch and becomes a real, additive access control worth implementing properly
  (including restricting who may subscribe as a responder, not just who may publish).
* Daya-core's companion WS-5.10 permission work ships on its side, making this repo the only
  remaining unauthenticated leg of that specific request/reply pair.

## More Information

Grep evidence this decision rests on (re-verifiable, not a one-time claim): `nats.UserCredentials`,
`nats.Token`, `nats.UserInfo`, and `nats.Nkey` all return zero matches across `internal/` as of this
pass. `internal/transformer/nats/protobuf.go`'s `nats.Connect` call
(`NewNatsProtoTransformer`) passes only `nats.Name`, `nats.MaxReconnects`, and `nats.ReconnectWait`
(WS-5 item 5, [0024](0024-transform-circuit-breaker-and-transport-classification.md)'s companion
change) — no auth options of any kind. The rogue-responder mechanics above are read directly from
`sendRequest`'s use of `conn.RequestWithContext` (accepts the first reply on the subject, from
whichever process answers) and `parseResponseWithOrder`'s unconditional trust of
`TransformResponse.TransformedData` for any result with `Success: true` — neither performs any check
that the reply came from a specific, known responder identity.

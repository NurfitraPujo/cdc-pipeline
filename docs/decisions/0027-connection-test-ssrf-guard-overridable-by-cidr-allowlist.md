---
status: accepted
date: 2026-08-06
decision-makers: cdc-pipeline maintainers
consulted: internal/api/handler.go:28-118 (validateHost / isPrivateHost), internal/api/cors.go (allowlist precedent), deploy/helm-chart/values.staging.yml, deploy/helm-chart/values.production.yml
---

# The connection-test SSRF guard is overridable by a CIDR allowlist

## Context and Problem Statement

The `POST /sources/test` and `POST /sinks/test` handlers run `validateHost` before dialling a
database (`internal/api/handler.go:1644`, `:1744`). That guard (T2-1, added in `dd3e3a6`'s SSRF
hardening) resolves the target hostname and rejects any resolution to a private/reserved IP —
loopback, link-local, RFC-1918, CGNAT (`handler.go:30-56`).

Every database this pipeline connects to is private. Staging's `rds-postgres-main` resolves to
`10.200.38.64`; production's to `10.200.53.141` / `10.200.60.184`
(`deploy/helm-chart/values.*.yml`). All sit in the `10.200.0.0/16` VPC. The guard therefore
returns `400 host … resolved to private IP … not allowed` for **every legitimate target**, making
the connection-test feature unusable in exactly the environments it exists for. This was reported as
a spurious 400 on the staging source.

The endpoints are already behind the `authorized` group (`cmd/api/main.go:131`), so the only actor
who can trigger `validateHost` is an authenticated operator pointing at an internal database — which
is the intended workflow, not an attack.

## Decision Drivers

* The feature must work against private VPC databases (staging and production).
* Some SSRF protection is still worth keeping: loopback, link-local and cloud metadata endpoints
  (`169.254.169.254`) should stay unreachable even to an authenticated operator.
* The mechanism should match an existing convention rather than invent a new config style.

## Considered Options

1. **Remove the guard from the two DB-test paths.** Simplest; leans entirely on the auth boundary.
2. **Env-gate the whole guard** (`ALLOW_PRIVATE_DB_HOSTS`, off in production). Since production DBs
   are *also* private, the flag would have to be on in production too — so it protects nothing while
   pretending to.
3. **CIDR allowlist** (`DB_HOST_ALLOWED_CIDRS`): private IPs inside listed ranges are exempt; every
   other private/reserved IP stays blocked.

## Decision Outcome

Chosen: **option 3, the CIDR allowlist.** It restores the feature for the operator's real targets
while still blocking loopback, link-local, metadata and unrelated private ranges — a meaningfully
smaller blast radius than removing the guard, at negligible complexity.

`DB_HOST_ALLOWED_CIDRS` is a comma-separated list of CIDR blocks (`handler.go:allowedHostCIDRs`).
`validateHost` rejects a resolved IP only when it is private **and** not contained in any listed
block. Deployments set it to their VPC range: `10.200.0.0/16` in staging and production, the Docker
bridge + loopback locally (`docker-compose.yaml`). This deliberately mirrors the
`CORS_ALLOWED_ORIGINS` allowlist precedent in `internal/api/cors.go`.

The list is read per request via `os.Getenv`. These endpoints are low-frequency and
operator-triggered, so a fresh read keeps `validateHost` a pure function of its input plus env — and
therefore trivially testable with `t.Setenv` — rather than caching process state. Malformed entries
are skipped, not fatal: a typo in one CIDR must not take down connection testing entirely.

### Consequences

* Good: staging and production connection tests reach their real databases again.
* Good: loopback and link-local stay blocked regardless of the allowlist, because a VPC CIDR such as
  `10.200.0.0/16` does not contain `127.x` or `169.254.x`. Covered by
  `TestValidateHost_AllowlistNeverPermitsLoopback`.
* Good: allowlisting one private range does not widen to others —
  `TestValidateHost_AllowlistDoesNotWidenBeyondListedRange` pins this.
* Good: an empty/unset var preserves the original guard behaviour exactly
  (`TestValidateHost_EmptyAllowlistPreservesGuard`), so nothing changes for deployments that do not
  opt in.
* Bad: the allowlist is only as tight as whoever configures it. A lazy `10.0.0.0/8` — or worse,
  `0.0.0.0/0` — re-opens most of what the guard defends. This is an operational discipline the
  config comments call out but cannot enforce.
* Bad: a too-broad VPC CIDR could expose an unrelated internal service on the same range to a
  connection probe. Scope the CIDR to the database subnet where practical.
* Neutral: the guard still only inspects the host at the *test* endpoints. `CreateSource` /
  `UpdateSource` never called `validateHost`; that is unchanged and out of scope here.

## More Information

The guard originated in `docs/todos/holistic_review_remediation.md` (T2-1) and
`summaries/holistic_review_result/plans/04_ssrf_and_secrets.md`. Behaviour is verified by the
`DB_HOST_ALLOWED_CIDRS` tests in `internal/api/api_test.go`. Deployment wiring lives in
`deploy/helm-chart/values.staging.yml`, `deploy/helm-chart/values.production.yml`, and
`docker-compose.yaml`.

# Fix Sequence 3 — Wire Up CI (Quality Gate)

**Scope:** Critical #20 (CI runs zero verification; no main/PR pipeline), New-Finding N5 (known-CVE deps, no govulncheck), N11 (hook-install conflict), plus the three CI-adjacent defects found in review: `go mod tidy` inside the CI build, divergent backend build paths, and the kube-linter step that can never fail.

**Plan only. No files are modified by this document.**

---

## 1. Objective & current CI gaps

Objective: every merge to `main` and every shipped image passes gofmt/golangci-lint, `go vet`, `go build`, unit tests, govulncheck, web biome+tsc+vitest — and the existing (strong, currently orphaned) integration and Playwright suites actually run on a defined cadence. The image that ships must be the image (or at minimum the exact Dockerfile+flags) that was tested.

### Gap inventory (verified against files)

| # | Gap | Evidence |
|---|-----|----------|
| G1 | No pipeline for `main` or PRs at all. Triggers are only `branches: release/*` (`bitbucket-pipelines.yml:152-153`), `tags: v*-staging` (`:192`), `tags: v*` (`:255`). Code merges to `main` completely unverified. | `bitbucket-pipelines.yml:151-317` |
| G2 | Zero tests/lint anywhere in CI. The `release/*` "Test Build" steps (`:155-189`) only compile (`goBuild build-all`) and `docker build`. No `go test`, no `golangci-lint`, no `go vet`, no biome, no `tsc`, no vitest, no Playwright, no vuln scan. | `&goBuild` anchor `:30-62` |
| G3 | `go mod tidy` runs inside the CI build (`&goBuild` anchor, `bitbucket-pipelines.yml:52`). CI can silently rewrite `go.mod`/`go.sum` before compiling — the shipped binary may embed a dependency graph that is not the committed one, and `go mod verify` (`:53`) then verifies the *rewritten* module set. |
| G4 | kube-linter can never fail the build. `executeLint` (`bitbucket-pipelines.yml:134-142`): `lint_output=$(kube-linter lint ./tmp/... >> lint-$1.log \|\| true)`. Stdout is redirected into the log file, so `$lint_output` is **always empty**; `\|\| true` swallows the exit code; the `if [ -z "$lint_output" ]` branch then merely `cat`s the log and the function returns 0 unconditionally. The step is decoration. |
| G5 | Two divergent backend build paths: (a) CI: `make build-all` on the host runner (`CGO_ENABLED=0 GOOS=linux -ldflags="-w -s"`, `Makefile:11-15`) → binaries copied into `Dockerfile.swr` (bare `alpine:latest` + `COPY api/worker`). (b) Local/pre-push hook: `Dockerfile` (multi-stage `golang:1.26-alpine`, gcc installed, **no `CGO_ENABLED=0`** → cgo-enabled build, and no config-embed parity — `Dockerfile:31` copies `config.example.yaml` into `cmd/pipeline/` which the Makefile path never does). What developers test locally is not the binary that ships. |
| G6 | 19 testcontainers integration test files in `internal/test/e2e/` (postgres:16 `wal_level=logical`, nats:2.10 `-js`, `datafuselabs/databend:latest`; provider switchable to podman via `TESTCONTAINER_PROVIDER`, `containers.go:19-23`) — nothing in CI runs them. Every file self-skips under `-short`. |
| G7 | Full Playwright suite in `e2e/` (chromium, `workers: 1`, `webServer` boots `cd ../web && npm run dev` on :3000; assumes API on :8080 + NATS + Postgres started externally, per `Makefile:32-57` and `playwright.config.ts`) — nothing in CI runs it. |
| G8 | Quality gating exists only in **opt-in** local hooks (`.git-hooks/pre-commit`: vet, golangci-lint, biome, short unit tests; `.git-hooks/pre-push`: unit + vitest + container builds + full Playwright run). Two conflicting install mechanisms: `Makefile:130-134 setup-hooks` sets `core.hooksPath .git-hooks` (both hooks); `scripts/install-hooks.sh` copies **only pre-commit** into `.git/hooks` — running the script silently drops pre-push, and `core.hooksPath` (if later set) silently shadows `.git/hooks`. |
| G9 | Known-CVE deps with no scanner: `go.mod:19` `pgx/v5 v5.6.0` (GO-2026-5004, SQL injection via dollar-quoted placeholder confusion — pgx is directly used by snapshot queries), `x/text v0.35.0` (GO-2026-5970), `x/crypto v0.49.0` (SSH advisories). No `govulncheck` anywhere. |
| G10 | ArgoCD sync (`&deployK8s`, `:71-118`) fires immediately after push with zero verification upstream — staging deploys are fully automatic on `v*-staging` tags. |

Toolchain facts to build on: Go `1.26.3` (`.go-version`, `go.mod:3`), Node `v24.16.0` (`web/.node-version`), pnpm (web + e2e), `.golangci.yml` present (errcheck, gosec, gocyclo, revive, depguard, govet — good config, just unenforced), private module `bitbucket.org/daya-engineering/daya-contracts` requires the `&setupNetrc` + `GOPRIVATE` dance in every Go step, vendored `go-pq-cdc` via `replace` directive.

---

## 2. Target pipeline design

### 2.1 Stage/step diagram

```
PULL REQUESTS (pull-requests: '**')                 ~8-12 min wall clock
─────────────────────────────────────────────────────────────────────────
  parallel  ┌ [B] Go verify        gofmt-diff → tidy-diff → vet →
  (all      │                      golangci-lint → build-all → unit tests   BLOCKING
  blocking) ├ [W] Web verify       pnpm install → biome check → tsc →
            │                      vitest run                               BLOCKING
            ├ [V] govulncheck      go mod download → govulncheck ./...      BLOCKING (see §6 rollout)
            └ [K] kube-linter      (condition: deploy/helm-chart/** changed)BLOCKING (fixed, §3.4)
  then      [I] Integration (testcontainers)  trigger: manual on PR         OPT-IN
            [P] Playwright e2e                 trigger: manual on PR         OPT-IN

MAIN (branches: main) — post-merge                  ~25-35 min wall clock
─────────────────────────────────────────────────────────────────────────
  parallel  [B] [W] [V] [K]  (same as PR)                                   BLOCKING
  then      [I] Integration suite (docker service, testcontainers)          BLOCKING
  then      [P] Playwright e2e (services: postgres+nats, api artifact)      BLOCKING
  then      [D] Docker build smoke: build Dockerfile (unified, §3.5)
             for both backend + web, no push                                BLOCKING

RELEASE (branches: release/*) — pre-tag rehearsal
─────────────────────────────────────────────────────────────────────────
  Same as MAIN, plus: build images exactly as the tag pipeline will
  (today's "Test Build" steps, kept, but building the unified Dockerfile).

TAGS v*-staging / v* — ship
─────────────────────────────────────────────────────────────────────────
  [K] kube-linter (fixed) → parallel([B],[W],[V]) fast gate               BLOCKING
  → parallel(Build&Push backend, Build&Push web)                           (unchanged shape, fixed goBuild)
  → Deploy staging (auto) / production (manual)                            (unchanged)
  NOTE: integration+e2e are NOT re-run on tags — they gated the commit on
  main/release already; tags must point at a commit whose main pipeline
  is green (enforced by branch protection + release flow, §6).

NIGHTLY (custom: nightly-full, Bitbucket schedule on main)
─────────────────────────────────────────────────────────────────────────
  Full: [B][W][V] + Integration + Playwright + govulncheck (fresh DB) +
  docker build of both images. Catches: new CVEs published against pinned
  deps, upstream image drift (databend:latest until pinned), flake trends.
```

### 2.2 Gating rationale

- **Blocking on PR = fast + deterministic only.** Lint/build/unit/vitest/tsc/biome are hermetic and quick (<10 min with caches). govulncheck is deterministic per-DB-snapshot; it's blocking, but with the §6 escape hatch during rollout.
- **Integration (testcontainers) blocking on main, opt-in (manual step) on PR.** The suite spins postgres+nats+databend per test file; `databend:latest` alone has a 2-minute startup wait (`containers.go:63`) and 19 files run serially — expect 20-30+ min and nonzero flake rate on shared runners. Making it PR-blocking on day 1 would train the team to bypass CI. Post-merge blocking on main means a red main is loud, bisectable to one merge, and blocks tagging. Promote to PR-blocking per §6 once 2 weeks of main runs show <2% flake.
- **Playwright blocking on main, opt-in on PR.** Same reasoning plus a heavier environment (API + NATS + PG + Vite dev server + chromium). Note honestly: per the review, several dashboard flows are currently broken in prod-shaped builds (SSE auth, casing) — the suite passes against the dev server today; keep it against the dev server initially (that's what `playwright.config.ts` encodes) and revisit after Fix Sequence 5.
- **Tags do not re-run slow suites.** A tag is an immutable pointer; re-running e2e there only delays shipping and can flake a hotfix. The guarantee instead comes from: tags may only be cut from `main`/`release/*` commits with green pipelines (Makefile `release` already checks out main; add the branch-protection merge check in §6).
- **Nightly** exists because govulncheck results change without code changes, and because slow suites need a flake-trend baseline independent of merge traffic.

---

## 3. Concrete `bitbucket-pipelines.yml` changes

### 3.1 New definitions: caches, services, shared anchors

```yaml
definitions:
  services:
    docker:
      memory: 8096                      # keep — testcontainers needs headroom
    postgres-e2e:                       # for the Playwright step (NOT testcontainers)
      image: postgres:16-alpine
      variables:
        POSTGRES_USER: postgres
        POSTGRES_PASSWORD: postgres
        POSTGRES_DB: cdc_e2e
    nats-e2e:
      image: nats:2.10-alpine
      # nats image entrypoint takes args; Bitbucket services can't pass args,
      # so use a tiny wrapper image OR run nats via docker in-step (see 3.3b).
  caches:
    external-install: /usr/local/bin/
    gomod:                              # module download cache, keyed on go.sum
      key:
        files: [go.sum]
      path: /root/go/pkg/mod            # golang:1.26-alpine GOPATH=/go → also add /go/pkg/mod if needed
    gobuild: ~/.cache/go-build          # replaces the ad-hoc $(pwd)/go-build-cache
    golangci: ~/.cache/golangci-lint
    pnpm:
      key:
        files: [web/pnpm-lock.yaml, e2e/pnpm-lock.yaml]
      path: ~/.pnpm-store
```

Notes:
- Keep the existing `go-build-cache` name during transition if you don't want to lose the warm cache; otherwise switch `GOCACHE` to the default `~/.cache/go-build` and cache that.
- Bitbucket keyed ("smart") caches rebuild when `go.sum`/lockfiles change — this is what makes the testcontainers/unit steps tolerable (~30-60 s restore vs multi-minute `go mod download` of a large graph).
- Testcontainers **image pulls are not cacheable** by Bitbucket's `docker` cache (that cache only covers layers created by `docker build` through the service daemon). Accept the pull cost (~1-2 min for pg/nats, more for databend) and pin digests so pulls are at least stable (§7).

### 3.2 Fixed `&goBuild` — remove `tidy`, enforce readonly

Replace lines 50-53 of the anchor:

```bash
export GOFLAGS="-mod=readonly"          # any build/test that would need go.mod edits FAILS
export GOCACHE="$HOME/.cache/go-build"
go mod download
go mod verify
```

`go mod tidy` is **deleted from CI entirely**. Tidiness is instead *checked*, not applied, in the Go verify step (Go ≥1.23):

```bash
go mod tidy -diff   # exits non-zero and prints the diff if go.mod/go.sum are untidy
```

(Fallback if `-diff` misbehaves with the `replace ./internal/vendor/go-pq-cdc` directive: `go mod tidy && git diff --exit-code go.mod go.sum` — but that requires dropping `-mod=readonly` for that one command.)

### 3.3 New PR/main steps

Add step anchors and wire them:

```yaml
  yaml-anchors:
    # ... existing anchors ...
    - &goVerify
      name: Go verify (lint + build + unit tests)
      image: golang:1.26-alpine
      size: 2x
      max-time: 20
      caches: [gomod, gobuild, golangci]
      script:
        - apk add --no-cache git make gcc musl-dev curl
        - *setupNetrc
        - export GOPRIVATE="bitbucket.org/daya-engineering/*"
        - export GOFLAGS="-mod=readonly"
        - go mod download && go mod verify
        - go mod tidy -diff                                # G3 gate
        - test -z "$(gofmt -l $(find . -name '*.go' -not -path './internal/vendor/*'))"
        - go vet ./...
        - go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.x   # or curl installer; pin version
        - golangci-lint run ./...                          # uses committed .golangci.yml
        - make build-all
        - go test -short $(go list ./... | grep -v /test/e2e)   # exact pre-commit hook command
    - &webVerify
      name: Web verify (biome + tsc + vitest)
      image: node:24-slim                                  # matches web/.node-version v24.16.0
      size: 2x
      max-time: 15
      caches: [pnpm]
      script:
        - corepack enable && corepack prepare pnpm@latest-9 --activate   # pin exact pnpm version
        - cd web
        - pnpm config set store-dir ~/.pnpm-store
        - pnpm install --frozen-lockfile
        - pnpm check                                       # biome check && tsc --noEmit (package.json)
        - pnpm test                                        # vitest run
    - &goVulncheck
      name: govulncheck
      image: golang:1.26-alpine
      max-time: 10
      caches: [gomod, gobuild]
      script:
        - apk add --no-cache git
        - *setupNetrc
        - export GOPRIVATE="bitbucket.org/daya-engineering/*" GOFLAGS="-mod=readonly"
        - go install golang.org/x/vuln/cmd/govulncheck@latest
        - govulncheck ./...                                # symbol-reachability mode; exits 3 on findings
    - &integrationTests
      name: Integration tests (testcontainers)
      image: golang:1.26-alpine
      size: 4x
      max-time: 60
      services: [docker]
      caches: [gomod, gobuild]
      script:
        - apk add --no-cache git make gcc musl-dev docker-cli
        - *setupNetrc
        - export GOPRIVATE="bitbucket.org/daya-engineering/*" GOFLAGS="-mod=readonly"
        # Bitbucket's docker service is a DinD daemon on tcp://localhost:2375.
        # testcontainers-go honors DOCKER_HOST; Ryuk needs /var/run/docker.sock
        # which Bitbucket's restricted daemon does not expose → disable Ryuk,
        # cleanup is the ephemeral runner itself.
        - export DOCKER_HOST=tcp://localhost:2375
        - export TESTCONTAINERS_RYUK_DISABLED=true
        - docker pull postgres:16-alpine & docker pull nats:2.10-alpine &
          docker pull datafuselabs/databend:latest & wait   # parallel pre-pull (pin digests, §7)
        - go test -v -timeout 45m -count=1 ./internal/test/e2e/...
```

Key testcontainers-on-Bitbucket facts baked in above:
- `services: [docker]` + `options: docker: true` gives an in-step Docker daemon; testcontainers-go v0.41 auto-detects it via `DOCKER_HOST` (export it explicitly to be safe).
- Bitbucket's DinD forbids privileged containers and host-path mounts → **Ryuk cannot start**; `TESTCONTAINERS_RYUK_DISABLED=true` is mandatory. Leaked containers die with the step VM, so this is safe here (do *not* copy this env to self-hosted runners without adding cleanup).
- `size: 4x` + `definitions.services.docker.memory: 8096` (already present): postgres + nats + databend + the test binary need it; databend especially.
- `-count=1` defeats test-result caching (the containers are external state).

**Playwright step:**

```yaml
    - &playwrightE2e
      name: Playwright e2e
      image: mcr.microsoft.com/playwright:v1.50.1-noble    # MUST match @playwright/test in e2e/package.json (^1.50.0 → pin both)
      size: 4x
      max-time: 40
      services: [docker]                                    # for NATS+PG (see below)
      caches: [pnpm, gomod, gobuild]
      artifacts:
        download: true                                      # gets bin/api from goVerify (declare artifacts: bin/** there)
      script:
        # 1. Infra: run NATS+PG via the docker service (mirrors `make e2e-up`,
        #    which needs args like `-js` that Bitbucket service defs can't pass)
        - export DOCKER_HOST=tcp://localhost:2375
        - docker run -d --name e2e-nats -p 4222:4222 nats:2.10-alpine -js
        - docker run -d --name e2e-pg  -p 5432:5432 -e POSTGRES_USER=postgres
            -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=cdc_e2e postgres:16-alpine
        - for i in $(seq 1 60); do nc -z localhost 4222 && nc -z localhost 5432 && break; sleep 1; done
        # 2. API: the linux/amd64 bin/api artifact from the Go step (CGO_ENABLED=0 → runs on noble)
        - chmod +x bin/api
        - JWT_SECRET="$E2E_JWT_SECRET" ENCRYPTION_KEY="$E2E_ENCRYPTION_KEY"
            NATS_URL="nats://localhost:4222" ./bin/api > api.log 2>&1 &
        - for i in $(seq 1 60); do curl -sf http://localhost:8080/healthz | grep -q OK && break; sleep 1; done
        # 3. Web deps (playwright.config.ts webServer boots `cd ../web && npm run dev` itself)
        - corepack enable && corepack prepare pnpm@latest-9 --activate
        - (cd web && pnpm config set store-dir ~/.pnpm-store && pnpm install --frozen-lockfile)
        - (cd e2e && pnpm config set store-dir ~/.pnpm-store && pnpm install --frozen-lockfile)
        # 4. Browsers are baked into the MS image → skip `playwright install`
        - cd e2e && pnpm exec playwright test
      after-script:
        - tail -n 200 api.log || true
      artifacts:
        - e2e/test-results/**
        - api.log
```

Notes: `E2E_JWT_SECRET`/`E2E_ENCRYPTION_KEY` are repository variables (do **not** copy the pre-push hook's inline `my-super-secret-key-32-chars-long!` — per the review that 34-byte key is *rejected* by `GetEncryptionKey`; the hook works only because... verify it, and set a valid 32-byte value in repo variables). The `webServer.env` in `playwright.config.ts` already injects `VITE_API_BASE_URL=http://localhost:8080/api/v1`, so no web config change is needed.

**Wiring the triggers:**

```yaml
pipelines:
  pull-requests:
    '**':
      - parallel:
          - step: *goVerify           # add `artifacts: [bin/**]` to goVerify
          - step: *webVerify
          - step: *goVulncheck
          - step:
              <<: *lintKubeYaml       # fixed version, §3.4
              condition: { changesets: { includePaths: [deploy/helm-chart/**] } }
      - step:
          <<: *integrationTests
          trigger: manual             # opt-in on PRs (promote later, §6)
      - step:
          <<: *playwrightE2e
          trigger: manual
  branches:
    main:
      - parallel:
          - step: *goVerify
          - step: *webVerify
          - step: *goVulncheck
          - step: { <<: *lintKubeYaml, condition: { changesets: { includePaths: [deploy/helm-chart/**] } } }
      - step: *integrationTests       # blocking post-merge
      - step: *playwrightE2e          # blocking post-merge
      - step: *dockerBuildSmoke       # build both Dockerfiles, no push (§3.5)
    release/*:
      # keep existing test-build steps, but prepend the same fast gate:
      - parallel: [ *goVerify, *webVerify, *goVulncheck ]
      - step: *integrationTests
      - step: *playwrightE2e
      - parallel:
          # existing "Test Build Go Backend / Web Dashboard (no push)" steps,
          # rebuilt on the unified Dockerfile (§3.5)
  custom:
    nightly-full:                     # attach a Bitbucket schedule (daily, main)
      - parallel: [ *goVerify, *webVerify, *goVulncheck ]
      - step: *integrationTests
      - step: *playwrightE2e
      - step: *dockerBuildSmoke
  tags:
    "v*-staging": # prepend: - parallel: [ *goVerify, *webVerify, *goVulncheck ]  before Build & Push
    "v*":         # same; keep Deploy steps unchanged
```

Also apply the §3.2 `&goBuild` fix (drop `tidy`, add `GOFLAGS=-mod=readonly`) so the tag Build & Push steps compile the committed module graph.

### 3.4 Fix kube-linter so it fails the build

Replace `executeLint` (`bitbucket-pipelines.yml:134-142`) — capture the exit code, don't hide stdout, and pin/checksum the linter:

```bash
executeLint() {
  kube-linter lint ./tmp/$1-$timestamp.yaml | tee lint-$1.log
  status=${PIPESTATUS[0]}                       # sh on alpine: use `set -o pipefail` + $? instead
  if [ "$status" -ne 0 ]; then
    echo "kube-linter FAILED for $1"; exit "$status"
  fi
}
```

(Simplest robust form for alpine `sh`: `set -e; kube-linter lint ./tmp/... 2>&1 | tee lint-$1.log; test "${PIPESTATUS...}"` is bashism — prefer `kube-linter lint file > lint-$1.log 2>&1; status=$?; cat lint-$1.log; [ $status -eq 0 ] || exit $status`.) Add `artifacts: [lint-*.log]` to the step. Expect this to **immediately fail** on the known helm findings (empty `securityContext`, root containers — High finding from the review); see §6 rollout for how to land it without freezing releases: start with a curated `--exclude` list or a `.kube-linter.yaml` config that ignores the pre-existing checks, tracked as debt, then tighten.

### 3.5 Reconcile the two build paths (tested image == shipped image)

Decision: **one canonical multi-stage `Dockerfile`, byte-identical build flags to today's shipped artifact; delete the split.**

1. Edit `Dockerfile` build stage to match the Makefile exactly:
   - `RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -mod=readonly -o /app/bin/api ./cmd/api/main.go` (same for worker). This removes the cgo divergence (G5) — today's shipped binaries are already `CGO_ENABLED=0` via `make build-all`, so runtime behavior is unchanged; it's the *local* Dockerfile that was drifting.
   - Keep the `cp config.example.yaml cmd/pipeline/` embed step **and add the same step to `make build-worker`** (or move the embed to `//go:embed` of a path that exists in both) — today the Makefile path ships a worker built *without* that copy while the Dockerfile path builds *with* it; whichever is correct, make both identical. **Open question for the team: which artifact is the intended one?** (Flagged as an action item, §7.)
   - Pin the base: `FROM alpine:3.21` (or distroless/static) instead of `alpine:latest` in both stages — `alpine:latest` in `Dockerfile:37` and `Dockerfile.swr:1` is unpinned drift.
2. CI Build & Push steps switch from `make build-all` + `Dockerfile.swr` to `docker build -f Dockerfile` with `--ssh`/netrc build secret for the private module (`--secret id=netrc,src=$HOME/.netrc` + `RUN --mount=type=secret,id=netrc,target=/root/.netrc go mod download`). Then the image the pre-push hook builds, the image `main`'s docker-build-smoke step builds, and the image the tag pipeline pushes are all products of the same file and flags.
3. Delete `Dockerfile.swr` (grep first: `Makefile`, `bitbucket-pipelines.yml:166,219,282` are the only users found). If build speed on 4x runners regresses badly, the fallback is the inverse unification: keep host-build+`Dockerfile.swr` but change local `Dockerfile` usage (pre-push hook) to the same host-build path — less clean, second choice.
4. Stronger guarantee (phase 2, optional): on `main`, push `cdc-data-pipeline:main-<shortsha>` to SWR after the full suite passes; the tag pipeline then does `docker pull main-<sha> && docker tag $TAG && docker push` instead of rebuilding — the *literal* tested image ships. Requires SWR creds on main pipeline; defer until the basics are in.

### 3.6 Caching summary (what/why)

| Cache | Path | Key | Benefit |
|---|---|---|---|
| gomod | `/root/go/pkg/mod` (+`/go/pkg/mod` on golang images) | `go.sum` | avoids multi-minute module download in 4 steps |
| gobuild | `~/.cache/go-build` | (unkeyed, LRU) | incremental compile + test cache; replaces ad-hoc `go-build-cache` dir |
| golangci | `~/.cache/golangci-lint` | (unkeyed) | lint drops from minutes to seconds |
| pnpm | `~/.pnpm-store` | `web/pnpm-lock.yaml`,`e2e/pnpm-lock.yaml` | web + e2e installs |
| docker (predefined) | service layer cache | n/a | `docker build` layer reuse in build steps only |
| playwright browsers | n/a | n/a | **not needed** — browsers ship in the `mcr.microsoft.com/playwright` image; if you drop that image, cache `~/.cache/ms-playwright` keyed on `e2e/pnpm-lock.yaml` |
| testcontainers pulls | **not cacheable** | — | mitigate with parallel pre-pull + pinned digests; consider a mirrored registry (SWR) copy of postgres/nats/databend if pull time dominates |

---

## 4. Makefile + git-hook alignment

Principle: **CI and hooks call the same Makefile targets** so they cannot drift.

1. Add canonical targets to `Makefile`:
   ```make
   lint:            ## gofmt-check + go vet + golangci-lint (mirrors CI goVerify)
   test-unit:       go test -short $$(go list ./... | grep -v /test/e2e)
   test-integration: go test -v -timeout 45m -count=1 ./internal/test/e2e/...
   vuln:            govulncheck ./...
   web-check:       cd web && pnpm check
   web-test:        cd web && pnpm test
   verify-fast:     lint test-unit web-check web-test   # what PR CI runs
   ```
2. Rewrite `.git-hooks/pre-commit` to call `make lint test-unit web-check` (identical commands to CI, single source of truth). Rewrite the CI `goVerify` script to `make lint test-unit` likewise.
3. Fix the install conflict (N11): **delete `scripts/install-hooks.sh`** (or reduce it to `#!/bin/sh\nexec make setup-hooks` for backward compatibility — grep README/LOCAL_DEVELOPMENT.md/AGENTS.md for references to it and update). `core.hooksPath .git-hooks` (Makefile `setup-hooks`, `Makefile:130-134`) is the surviving mechanism because it (a) installs both hooks, (b) tracks hook edits in git without re-install, (c) is what the Makefile documents. `setup-hooks` should additionally warn if stale copies exist in `.git/hooks/` (`test -f .git/hooks/pre-commit && echo "warning: legacy hook in .git/hooks will be ignored (core.hooksPath is set); delete it"`).
4. Pre-push hook: keep its structure but (a) replace inline test commands with `make test-unit` / `make web-test`, (b) replace the hardcoded `ENCRYPTION_KEY="my-super-secret-key-32-chars-long!"` (34 bytes — rejected by `GetEncryptionKey` per the review; if the hook currently "works" it deserves investigation) with a documented valid 32-byte dev key sourced from `.env.e2e.example`, (c) build the **unified** `Dockerfile` from §3.5 so the hook tests the shipped path. Since CI now runs integration+e2e on main, optionally demote pre-push e2e to `make test-unit`-only by default with an opt-in `PREPUSH_FULL=1` — pre-push full-e2e is the reason people run the weaker `install-hooks.sh`; making the default hook fast is the real fix for opt-out behavior.
5. Document one onboarding line in README/LOCAL_DEVELOPMENT.md: `make setup-hooks`.

---

## 5. Dependency / vulnerability remediation

Order matters because CI currently runs `go mod tidy` (G3) — land these in the same PR or in this sequence:

1. **Local, one time:** upgrade the three flagged modules:
   ```
   go get github.com/jackc/pgx/v5@latest        # ≥ fix for GO-2026-5004; pgx is on the snapshot-query path — run integration suite after
   go get golang.org/x/text@latest              # ≥ fix for GO-2026-5970
   go get golang.org/x/crypto@latest
   go mod tidy && go mod verify
   go test -short ./... && go test ./internal/test/e2e/...   # pgx bump touches snapshot SQL behavior
   ```
   Commit `go.mod` + `go.sum` together. Check `govulncheck ./...` locally before pushing.
2. **Same PR:** remove `go mod tidy` from `&goBuild` (§3.2). Caveat/interaction: if the tree is currently *untidy*, today's CI has been silently papering over it at build time — removing tidy without first committing a tidy `go.mod/go.sum` will break the tag build. Hence step 1 (which tidies) must merge before or with the pipeline change; the new `go mod tidy -diff` gate then keeps it tidy forever.
3. **Wire the gate:** `&goVulncheck` step (§3.3) blocking on PR/main/tags + nightly. govulncheck does *reachability* analysis (the review notes none of the three CVEs is currently reached — expect green after the bumps; the gate exists for the future).
4. Watch-outs:
   - The `replace github.com/Trendyol/go-pq-cdc => ./internal/vendor/go-pq-cdc` directive means `go get -u`-style bulk upgrades can churn the vendored module's requirements; upgrade the three modules **individually**, not `go get -u ./...`.
   - `x/crypto`/`x/text` are also indirect deps of testcontainers/watermill — `tidy` may bump more lines than expected; review the diff, don't rubber-stamp.
   - Pin `govulncheck` install (`@latest` is acceptable since its DB is remote anyway, but pin if reproducibility complaints arise).
5. Recurring: nightly govulncheck (§2.1) is the mechanism that surfaces *newly published* CVEs; triage policy: reached=fix within a sprint, unreached=track.

---

## 6. Rollout — introduce gates without blocking the team on day 1

**Week 0 (land the plumbing, nothing newly blocking):**
- PR 1: dependency bumps + local `go mod tidy` (§5.1) — pure code change, no CI semantics.
- PR 2: new `bitbucket-pipelines.yml` with PR/main/nightly sections. Bitbucket pipelines only *block* merges if a merge check requires them — so on day 1 everything runs and reports, but a red build doesn't stop anyone. This IS the warn phase; no `|| true` hacks needed.
- PR 2 also: kube-linter fix with a `.kube-linter.yaml` that `ignore`s the checks the current chart is known to fail (`run-as-non-root`, `no-read-only-root-fs`, etc. — enumerate by running it once); file the exclusions as tracked debt for Fix Sequence infra work.
- PR 3: Dockerfile unification (§3.5) + hook/Makefile alignment (§4) + delete `scripts/install-hooks.sh`.

**Week 1-2 (observe):** watch main-branch integration/Playwright runs; log flakes; fix or quarantine (`t.Skip` with ticket ref) anything <98% pass. Tune `max-time` and sizes from real durations; check the Pipelines minutes burn (§7).

**Week 2+ (enforce):**
- Bitbucket → Repository settings → Branch restrictions / Merge checks on `main`: **"Minimum number of successful builds: 1"** (Premium: make it required rather than advisory) + require PR before merge + no direct pushes. From this point the PR fast gate is truly blocking.
- Add the fast-gate `parallel` block to the `tags:` pipelines (it was safe from day 1 but formally announce it: a red lint now stops a release — the escape hatch is fixing the code or, in a true emergency, an admin `[skip ci]`-free revert of the gate, not force-push).
- Promote integration suite from `trigger: manual` to automatic on PRs **only when** the 2-week flake rate is <2% and wall time <25 min; Playwright likewise, but expect this to wait until Fix Sequence 5 (web contract fixes) lands, since the suite's dev-server dependence is a known crutch.

**Escape hatches (deliberate, visible):** PR-level manual steps stay available for early runs of the slow suites; the pre-push hook keeps the `[E2E NOT CHECKED]` subject-line bypass (already implemented) — CI on main remains the backstop that cannot be bypassed.

---

## 7. Risks, open questions, sequencing

### Risks
- **Flaky slow suites poisoning trust.** Testcontainers + `databend:latest` + shared runners = flakes. Mitigations: post-merge (not PR) blocking first; `-count=1` and generous waits already in the suite; quarantine policy with tickets; nightly trend line. Biggest single fix: **pin `datafuselabs/databend` to a digest** (`containers.go:63` uses `:latest` — an upstream push can break CI overnight; also pin postgres/nats tags to digests).
- **CI minutes / cost.** `size: 4x` steps consume 4x minutes; integration (~30 min × 4x = 120 min-equivalents) + Playwright per main merge plus nightly will dominate the plan's quota. Mitigations: keyed caches (§3.6), right-size steps down after measuring (goVerify likely fine at 2x, maybe 1x with warm caches), consider a self-hosted Linux runner for the two slow suites (also unlocks privileged Docker/Ryuk — but then `TESTCONTAINERS_RYUK_DISABLED` must be revisited to avoid container leaks on a persistent host).
- **Bitbucket DinD limits.** No privileged mode, no host mounts → Ryuk disabled (accepted, ephemeral runners); some testcontainers features (reuse, custom networks edge cases) may misbehave; `docker` service memory (8096 MiB) may still be tight with databend + pg + nats concurrently — the suite runs files serially which helps; raise service memory within the 4x envelope if OOMs appear.
- **Playwright image/npm version skew.** `mcr.microsoft.com/playwright:v1.50.1` must match `@playwright/test` — pin the npm dep to the exact same version and add a comment linking the two locations.
- **Private-module auth on PR builds.** `&setupNetrc` warns-but-continues when `$BITBUCKET_TOKEN` is unset (`bitbucket-pipelines.yml:20`) — in the new world that means a cryptic `go mod download` failure; change the warning to `exit 1` with a clear message. Confirm repository variables are available to PR pipelines (same-repo PRs: yes; forks: secured variables are withheld — likely irrelevant for a private team repo, but verify).
- **kube-linter turning red on day 1.** Guaranteed, given the review's helm findings; handled via the ignore-list rollout (§6). The risk is the ignore list becoming permanent — attach owners/tickets.
- **`go mod tidy` removal breaking tag builds** if sequencing in §5 is violated (untidy tree + readonly flag). Merge order is load-bearing.
- **Vitest radix-stub smell.** `web/vitest.config.ts` stubs `@radix-ui/*` because node_modules were out of sync in some dev environment; under CI's `pnpm install --frozen-lockfile` the real packages will exist but tests still get stubs via alias — meaning component tests exercise stubs, not real primitives. Not a CI blocker, but note it so green vitest isn't over-trusted; candidate cleanup in the web fix sequence.

### Open questions
1. Which worker artifact is correct — with or without the `config.example.yaml` embed copy (Dockerfile does it, Makefile doesn't)? Determines the §3.5 unification detail.
2. Bitbucket plan tier: are required merge checks ("minimum successful builds" as *hard* block) and scheduled pipelines available? (Schedules are standard; enforced merge checks are Premium.)
3. Appetite for a self-hosted runner for the slow suites (cost vs minutes vs Ryuk)?
4. Does the tag flow guarantee tags are cut from green main commits today (Makefile `release` checks out main but doesn't check pipeline status)? Optionally add a `git tag` pre-check via `gh`-equivalent Bitbucket API, or accept branch protection as sufficient.
5. Pre-push hook's `ENCRYPTION_KEY` (34-byte, review says rejected) — does the pre-push e2e path actually work today? Verify before relying on it as the local mirror of CI.
6. SWR registry creds scope: OK to expose to `main` pipeline for the phase-2 "push tested image, retag on release" flow?

### Sequencing (dependency-ordered)
1. §5.1 dep bumps + local tidy (unblocks tidy-removal).
2. New pipeline YAML: PR/main/nightly + fixed `&goBuild` + fixed kube-linter (+ ignore list) + govulncheck. Non-enforcing.
3. Dockerfile unification + `Dockerfile.swr` deletion + tag-pipeline build switch.
4. Makefile targets + hook rewrite + delete `scripts/install-hooks.sh`.
5. Two-week burn-in; fix/quarantine flakes; measure minutes.
6. Enable merge checks on `main`; announce release-gate semantics.
7. Promote integration (then Playwright, post-Fix-Sequence-5) to PR-blocking.
8. Phase 2 (optional): push-tested-image/retag-on-release; mirrored base images in SWR; self-hosted runner evaluation.

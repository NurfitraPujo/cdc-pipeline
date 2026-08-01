# TODO: the pre-push E2E gate is heavy enough that its escape hatch will become the default

**Found:** 2026-08-01, while pushing after the harness repairs.
**Status:** deferred — needs a deliberate decision, not a quick fix.

## What the gate does

`.git-hooks/pre-push` runs, on **every push**:

1. Go unit tests, then frontend unit tests — seconds, uninteresting.
2. `podman build` of `cdc-pipeline-api` **and** `cdc-pipeline-web-dashboard`.
3. `make e2e-up`, boot an API server, run the **Playwright** suite, `make e2e-down`.

Steps 2–3 dominate: two full image builds plus browser tests, on every push including
documentation-only ones.

## The problem

The hook already has an escape hatch — `[E2E NOT CHECKED]` in the commit subject skips steps 2–3
while keeping the fast tests. It exists for good reason, and it was needed on first use: port 8080
was held by an unrelated container on the developer's machine, so the hook's API server could not
bind no matter what.

That is the concern. A gate this expensive, with a documented one-line bypass, converges on the
bypass being used habitually — at which point the gate is theatre, and worse than an honest fast
gate because it implies coverage that is not happening.

The same dynamic already played out here: the harness was broken for so long that ~517 Go lint
findings and 40 web findings accumulated behind it (`golangci_lint_backlog.md`,
`web_biome_backlog.md`).

## Constraints worth weighing

- Port 8080 is hardcoded for the API server. Any conflicting local service blocks the gate
  outright, with no fallback — that alone will drive bypass use.
- The Go e2e suite is separately known to be flaky when run in bulk
  (see `internal/test/e2e/AGENT.md`), so a red run is not reliably a real signal.
- CI has **no test or lint step at all** — `bitbucket-pipelines.yml` is build/deploy only. So this
  hook is currently the only automated gate that exists, which is the strongest argument for
  keeping something here.

## Options

1. **Move E2E to CI, keep pre-push fast.** Push stays quick; e2e runs where flakiness can be
   retried and where a shared machine has no port conflicts. Requires adding a CI test step, which
   is worth doing regardless.
2. **Keep E2E pre-push but make it robust:** pick a free port dynamically instead of hardcoding
   8080, and skip cleanly with a clear message when infrastructure is unavailable rather than
   failing the push.
3. **Status quo**, accepting that the escape hatch is the normal path — in which case it should be
   documented as such, so nobody believes e2e is gating pushes when it is not.

Option 1 is the strongest: it addresses the real gap (no CI testing) rather than compensating for
it locally.

## Action items

- [ ] Decide between the options above.
- [ ] Whatever is decided, stop hardcoding port 8080 in the hook.
- [ ] Add a test step to CI. This is the item with value independent of the decision.

## Related

- `docs/todos/e2e_log_unbounded_growth.md` — surfaced by this gate.
- `docs/todos/golangci_lint_backlog.md`, `docs/todos/web_biome_backlog.md` — what accumulated the
  last time a gate stopped working.

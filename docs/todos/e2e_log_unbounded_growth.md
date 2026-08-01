# TODO: `internal/test/e2e/logs/app.log` grows without bound

**Found:** 2026-08-01, when a container build failed with `no space left on device`.
**Status:** deferred. The build-breaking symptom is fixed; the underlying growth is not.

## What happened

A day of e2e runs grew `internal/test/e2e/logs/app.log` to **38 GB**. Nothing rotates it, caps it,
or truncates it between runs — every run appends.

It first surfaced indirectly: `.dockerignore` listed `*.log`, which matches only root-level files,
so `COPY . .` pulled all 38 GB into the build context and the image build died. The pre-push hook
builds images before running e2e, so this blocked pushing entirely.

## What is already fixed

`.dockerignore` now carries `**/*.log` and `internal/test/e2e/logs/`, so the file can no longer
break an image build.

**That is a containment fix, not a cure.** The log still grows without limit and still fills the
disk — it just no longer takes the build down with it. On the machine where this was found, `/home`
was at 90% with 9.5 GB free.

## Why it is easy to miss

The file is gitignored, so it never appears in `git status`. Nothing warns as it grows. The failure
mode is a confusing, unrelated-looking error somewhere else — a container build, a test needing
scratch space, an editor — long after the cause.

## Action items

- [ ] Cap or rotate the log. Simplest credible option: truncate it in the e2e harness setup, so
      each run starts clean and the file is bounded by one run's output.
- [ ] Consider whether it needs to be a file at all. The Go e2e suite already streams to stdout,
      which the test runner captures; a second unbounded copy on disk may be redundant.
- [ ] Add a size guard to `make e2e-up` (or the harness) that fails loudly if the log is already
      over, say, 1 GB — a bounded, obvious failure beats a mystery disk-full elsewhere.

## Related

- `docs/todos/pre_push_e2e_cost.md` — the pre-push gate that surfaced this.

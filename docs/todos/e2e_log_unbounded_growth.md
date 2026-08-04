# TODO: `internal/test/e2e/logs/app.log` grows without bound

**Found:** 2026-08-01, when a container build failed with `no space left on device`.
**Status:** 2026-08-04 — growth is bounded. The log is truncated once per test binary, so it now
holds one run's output. Two optional follow-ups remain (below).

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

- [x] **Cap or rotate the log.** `truncateLogFile` in `internal/test/e2e/env.go` empties
      `logs/app.log` from the package `init()` — once per test binary, before `logger.Init` opens
      it in append mode. Each `go test` invocation starts clean, so the file is bounded by one
      run's output. It prints a one-line notice to stderr when it discards a non-empty file, and
      degrades to a warning (not a failure) if the truncate errors. Covered by
      `internal/test/e2e/env_log_test.go`.
      Note this is per *test binary*: running the suite in batches means each batch discards the
      previous batch's log, so capture stdout if you need to compare across batches.
- [ ] Consider whether it needs to be a file at all. The Go e2e suite already streams to stdout,
      which the test runner captures; a second copy on disk may be redundant.
- [ ] ~~Add a size guard~~ — largely moot now that every run truncates: the file can only exceed
      1 GB *within* a single run. Worth revisiting only if one run's own output gets that large.

## Related

- `docs/todos/pre_push_e2e_cost.md` — the pre-push gate that surfaced this.

# TODO: Migrate `.golangci.yml` to golangci-lint v2 config format

**Found:** 2026-08-01, while wiring the docs hygiene sensor into the pre-commit harness.
**Status:** deferred — deliberately not fixed at discovery time.

## The problem

The pre-commit harness (`.git-hooks/pre-commit`, active via `core.hooksPath=.git-hooks`) is
**currently failing for anyone with golangci-lint v2 installed**:

```
Error: can't load config: unsupported version of the configuration: ""
```

`.golangci.yml` is in v1 format. golangci-lint v2 (verified against 2.12.2) requires an explicit
`version:` key.

## Why it is not a one-line fix

Adding `version: "2"` makes the file parse, but v2 also renamed `linters-settings` to
`linters.settings`. With the old key name the settings block is ignored, so **depguard's `allow`
list becomes empty and every import is denied**. Verified:

```
internal/protocol/config.go:9:2: import '.../internal/crypto' is not allowed from list 'Main' (depguard)
internal/protocol/config.go:10:2: import 'github.com/go-ozzo/ozzo-validation/v4' is not allowed ...
```

So a naive migration turns a broken hook into a hook that fails on every file — worse.

The depguard rules encode real architectural fencing (api/engine/source/sink boundaries) that must
survive the migration intact. That is the part deserving care.

## Why this matters more than it looks

A blocking pre-commit hook that always fails trains people to run `git commit --no-verify`, which
disables **every** sensor — `go vet`, the unit tests, and the docs hygiene check. The harness is
worth less than nothing in that state, because it creates false confidence that checks are running.

## Action items

- [ ] Migrate `.golangci.yml` to v2 format (`version: "2"`, `linters.settings`, and the v2
      `linters.exclusions` replacement for `issues.exclude-use-default`).
- [ ] Verify each depguard rule still fires: `main` allow-list, `core_architecture`,
      `domain_isolation_sink`, `domain_isolation_source`. A rule that silently stops matching is
      the failure mode to guard against — test with a deliberate bad import.
- [ ] Confirm `golangci-lint run ./...` is clean, or record the remaining findings here.
- [ ] Pin the golangci-lint version in the harness and CI so v1/v2 drift cannot recur.

## Related

- CI has **no lint or test step at all** (`bitbucket-pipelines.yml` is build/deploy only), so the
  pre-commit hook is currently the only thing running `go vet` and the unit tests. That amplifies
  the impact of a broken hook. Tracked separately.

# TODO: Migrate `.golangci.yml` to golangci-lint v2 config format

**Found:** 2026-08-01, while wiring the docs hygiene sensor into the pre-commit harness.
**Status:** config migration DONE 2026-08-01. Follow-up items below remain open.

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

- [x] Migrate `.golangci.yml` to v2 format. Used the official `golangci-lint migrate`, which needed
      `--skip-validation` because the v1 config carried a `listMode` key that golangci-lint's own v1
      schema rejects — so the config had been invalid for longer than the v2 release. Backup left at
      `.golangci.bck.yml`.
- [x] Close the allow-list gaps the working linter exposed: `github.com/ThreeDotsLabs/watermill/message`
      and `golang.org/x/time` were denied. 18 findings, all false alarms from an incomplete list.
- [x] Verify each depguard rule still fires — **they do not**, and did not before the migration
      either. Tracked in `docs/todos/depguard_architecture_rules_never_fire.md`.
- [x] Record the remaining findings — 517, tracked in `docs/todos/golangci_lint_backlog.md`. The
      pre-commit hook is scoped to `--new-from-rev=HEAD` meanwhile.
- [ ] Pin the golangci-lint version in the harness and in CI so v1/v2 drift cannot recur.
- [ ] Delete `.golangci.bck.yml` once the v2 config has proven itself.

## Related

- CI has **no lint or test step at all** (`bitbucket-pipelines.yml` is build/deploy only), so the
  pre-commit hook is currently the only thing running `go vet` and the unit tests. That amplifies
  the impact of a broken hook. Tracked separately.

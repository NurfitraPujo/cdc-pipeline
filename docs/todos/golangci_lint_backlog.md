# TODO: burn down the 517 pre-existing golangci-lint findings

**Found:** 2026-08-01, after migrating `.golangci.yml` to golangci-lint v2 and getting the linter
running again for the first time in a while.
**Status:** deferred — the pre-commit hook is scoped to `--new-from-rev=HEAD` so new code is held to
the standard while this is paid down.

## Baseline

`golangci-lint run ./...` on `main`:

| Linter | Findings |
|---|---|
| revive | 274 |
| errcheck | 174 |
| gosec | 40 |
| gocyclo | 29 |
| depguard | 0 |
| **total** | **517** |

These are not new. They accumulated while the pre-commit hook was failing on a config-format
incompatibility (see `docs/todos/golangci_lint_v2_migration.md`), so nobody saw them.

## Current mitigation, and its cost

`.git-hooks/pre-commit` runs `golangci-lint run --new-from-rev=HEAD ./...`, so only findings a
commit *introduces* block. That keeps the harness usable and stops the backlog growing.

The cost is that `--new-from-rev` diffs against a revision: a finding in a file you touched but did
not cause can still surface, and a genuinely pre-existing finding on a line you moved will look new.
Expect occasional friction.

## Triage order

Not all of these are equal. Suggested order by value:

1. **gosec (40)** — security findings. Some will be false positives worth annotating with
   `#nosec` *and a reason*; the rest deserve a real look first.
2. **errcheck (174)** — unchecked errors. In a CDC pipeline a swallowed error is the exact shape of
   the silent-failure bugs already found in this codebase (see `MULTI_SCHEMA_PLAN.md` §1.1). Highest
   correctness value despite the volume.
3. **gocyclo (29)** — complexity over 15. Treat as a refactor signal, not a mandate.
4. **revive (274)** — mostly missing doc comments on exported symbols. Highest count, lowest risk;
   mechanical, and a reasonable first contribution.

## Action items

- [ ] Burn down in the order above, in separate commits per linter so review stays tractable.
- [ ] Once a linter reaches zero, keep it there — the `--new-from-rev` scoping already prevents
      regression for new code.
- [ ] When all reach zero, drop `--new-from-rev=HEAD` from the hook and lint the whole tree.
- [ ] Add golangci-lint to CI. It currently has **no lint or test step at all**
      (`bitbucket-pipelines.yml` is build/deploy only), so the pre-commit hook is the only gate —
      and it is trivially bypassed with `--no-verify`.

## Related

- `docs/todos/golangci_lint_v2_migration.md` — why the linter was silent.
- `docs/todos/depguard_architecture_rules_never_fire.md` — depguard reports 0, but its
  architectural deny rules are not actually matching. Zero here does not mean the boundaries hold.

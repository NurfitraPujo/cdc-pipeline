# TODO: depguard architectural deny rules never fire

**Found:** 2026-08-01, while migrating `.golangci.yml` to golangci-lint v2.
**Status:** deferred — the config migration was the task; this is a separate defect.
**Severity:** the architectural fencing `AGENTS.md` advertises is not actually enforced.

## The claim vs the reality

`.golangci.yml` defines three depguard rules that encode layer boundaries:

| Rule | Intent |
|---|---|
| `core_architecture` | engine/source/sink/stream/metrics must not import `internal/api` |
| `domain_isolation_sink` | sink must not import `internal/source` |
| `domain_isolation_source` | source must not import `internal/sink` |

**None of them fire.** Verified by planting deliberate violations:

```go
// internal/engine/tmp_fence_probe.go
import _ "github.com/NurfitraPujo/cdc-pipeline/internal/api"      // not reported

// internal/sink/databend/tmp_fence_probe.go
import _ "github.com/NurfitraPujo/cdc-pipeline/internal/source"   // not reported
```

The probe files *are* analysed — `revive` reports `blank-imports` on the same line — so this is
depguard declining to match, not the file being skipped.

## What was ruled out

Each tested in isolation against a planted violation:

- **Not the v2 migration.** Behaviour is identical before and after.
- **Not `list-mode` being dropped.** Adding `list-mode: strict`, `lax` or `original` back to the
  rule changes nothing (0 hits in all three).
- **Not the `files` glob.** Removing the `files` restriction entirely, or rewriting
  `'**/internal/sink/**/*.go'` as `'internal/sink/**'`, changes nothing.
- **Not the catch-all `main` rule shadowing them.** Deleting the `main` rule entirely changes
  nothing.

## Leading hypothesis

depguard appears to apply **one rule per file** rather than all matching rules. A file under
`internal/sink/` matches both `core_architecture` (via its `files` list) and
`domain_isolation_sink`, so only one is evaluated — and the one that wins denies a package the file
does not import. If rule selection is map-iteration order, which is unordered in Go, the effective
rule could vary between runs.

This is a hypothesis, not a conclusion. It needs confirming against depguard's source or a minimal
reproduction outside this repo.

## Why it matters

`AGENTS.md` cites "Depguard Architecture Rules" as enforced architecture fencing, and the
pre-commit harness describes itself as enforcing it. A boundary violation would currently merge
silently. The dependency hierarchy is a real design property here — [ADR
0013](../decisions/0013-per-sink-consumer-fan-out.md) depends on sink and source staying
independent.

## Action items

- [ ] Reproduce minimally outside this repo to confirm the one-rule-per-file hypothesis.
- [ ] Restructure the rules so each file matches exactly one — most likely by folding all three
      deny sets into per-layer rules with mutually exclusive `files` globs, each carrying its own
      copy of the shared `main` allow-list.
- [ ] Add a **negative test** so this cannot regress silently: a fixture with a deliberate boundary
      violation that CI asserts is reported. A rule that stops matching is invisible without one —
      which is exactly how this survived.
- [ ] Correct `AGENTS.md` once fencing is real, or soften the claim until it is.

## Related

- `docs/todos/golangci_lint_v2_migration.md` — the config migration this was found during.

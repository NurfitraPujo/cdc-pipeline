# TODO: web/ lint and typecheck backlog

**Found:** 2026-08-01, while unbreaking the pre-commit harness.
**Status:** deferred. Biome is scoped to staged files in the hook; **`tsc --noEmit` is currently
not enforced at all**.

## What is failing

`pnpm check` in `web/` runs `biome check && tsc --noEmit`. Both fail on `main`:

| Check | Findings | Where |
|---|---|---|
| biome | 15 (`noExplicitAny`, formatting, import order) | `web/src/routes/sinks/**`, `web/src/components/PipelineTable.tsx`, `web/src/test/**` |
| tsc | 25 errors | **all in `web/src/test/stubs/radix-stub.ts`** |

These predate the harness working. They are not from the multi-schema change — the affected route
files are `sinks/`, which that work never touched.

## Current mitigation, and what it gives up

`.git-hooks/pre-commit` now runs `npx biome check --staged`, so only files a commit touches are
linted. That keeps the sensor useful without blocking every commit.

**`tsc --noEmit` was dropped from the hook.** It cannot be scoped to staged files — a type error is
a property of the whole program, not of one file. So the frontend currently has **no typecheck gate
anywhere**: not in the hook, and not in CI (which has no test or lint step at all).

That is a real regression in coverage versus what `pnpm check` intended, and it is the most
important item below.

## Action items

- [ ] **Fix `web/src/test/stubs/radix-stub.ts` first (25 errors, one file).** It is the only thing
      standing between the repo and a working typecheck gate. The errors are `AnyProps` not
      satisfying `Record<string, unknown>` and a `type` property missing from
      `HTMLAttributes<HTMLElement>` — an index-signature problem in the stub's prop typing, not a
      symptom of anything wrong in application code.
- [ ] Once it is clean, restore `tsc --noEmit` to the pre-commit hook and add it to CI.
- [ ] Clear the 15 biome findings, then drop `--staged` from the hook.
- [ ] `noExplicitAny` in `web/src/routes/sinks/**` is the largest cluster — sink option payloads are
      genuinely dynamic, so this may want a real discriminated-union type for sink options rather
      than a blanket suppression.

## Related

- `docs/todos/golangci_lint_backlog.md` — the same pattern on the Go side.
- `docs/todos/golangci_lint_v2_migration.md` — why the harness was silent long enough for all of
  this to accumulate.

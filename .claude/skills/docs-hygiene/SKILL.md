---
name: docs-hygiene
description: Review a change for documentation, ADR and memory drift. Use before opening a PR, after landing a significant change, or when docs are suspected stale. Covers the judgement calls that scripts/check-docs-hygiene.sh cannot make — whether a documented claim is still true, whether a decision deserves an ADR, and whether something belongs in memory.
---

# Docs hygiene review

`scripts/check-docs-hygiene.sh` catches mechanical staleness: dead file references,
broken ADR links, swagger drift. It runs in pre-commit and CI and it blocks.

This skill covers what a script cannot judge. Run it against a diff.

**Run the sensor first.** If it fails, fix that before starting — there is no point reasoning about
claims in a doc that cites files which no longer exist.

```bash
scripts/check-docs-hygiene.sh
```

## 1. Scope the change

```bash
rtk git diff --stat main...HEAD
rtk git diff main...HEAD -- '*.go'
```

Identify what changed *behaviourally*, not just textually. A renamed variable is not drift; a
changed default, a new config option, a changed identity format, or a changed failure mode is.

## 2. Hunt for invalidated claims

For each behavioural change, ask **which document asserts something that is now false**. Grep for
the concepts, not the identifiers — the stale sentence rarely contains the symbol you changed.

Highest-yield places, in order:

| Where | What goes stale |
|---|---|
| `README.md` Core Features | capability claims, and *why* a mechanism exists |
| `internal/**/AGENT.md` | layer conventions and invariants |
| `AGENTS.md` | cross-cutting conventions, terminology |
| `LOCAL_DEVELOPMENT.md` | commands a human copy-pastes — these fail loudly and embarrassingly |
| `docs/openapi.yaml` | endpoint behaviour, response codes, field semantics |
| `config.example.yaml` | new/changed options, and defaults |
| `internal/vendor/go-pq-cdc/PATCHES.md` | any vendored edit — **mandatory**, see §6 |
| `rfc/` | design records for significant changes; kept current, not archived |
| `docs/todos/` | the live backlog — see §4 |

Two failure modes to look for specifically, because both have happened here:

- **A doc that describes a mechanism's *purpose* inaccurately.** Example: the README described
  correlation IDs as preventing "spoofed acknowledgements"; they are a generation token guarding
  against *stale* acks. Not wrong about the code, wrong about the reason — which is worse, because
  it misleads the next design decision.
- **A doc whose example no longer runs.** Example: `LOCAL_DEVELOPMENT.md` told the reader to run an
  unqualified `SELECT` against Databend, which fails once a Postgres schema maps to a Databend
  database. Copy-pasteable commands deserve a literal check.

## 3. Decide whether an ADR is warranted

Write one when the change is a **decision**, not merely an implementation. Signals:

- It is hard to reverse (a persisted format, an identity scheme, a wire field).
- It contradicts an obvious default, so someone will "fix" it back.
- A reasonable alternative was rejected for a non-obvious reason.
- It trades one failure mode for another.

Do **not** write one for a bug fix, a refactor, or a choice with no live alternative.

If warranted, follow `docs/decisions/README.md`. Non-negotiables:

- Record **rejected options and why** — that is the part with lasting value.
- Record the **bad consequences**, not only the good. An ADR listing only benefits is marketing.
- If a rationale is inferred rather than documented, **say so** explicitly.
- If the code has drifted from the decision, record the drift rather than hiding it.
- ADRs are immutable. Supersede with a new one; never edit a decision to match new reality.

## 4. Reconcile the backlog

`docs/todos/` is a **living** document set: deferred work, known defects, design flaws, missing
tests, tech debt. Two directions to check on every review.

**Did this change resolve or invalidate an entry?** A TODO describing code that now behaves
differently is worse than no TODO — someone will act on it. Mark it done with a date, or rewrite it
to match reality. If the premise is gone entirely, say so rather than deleting silently.

**Did this change *create* an entry?** Anything found and deliberately not fixed belongs here
before the PR lands: a bug you decided not to chase, a design flaw you worked around, a test you
knew was missing, debt you took on knowingly. Deferring is fine. Deferring silently is how it gets
rediscovered at the worst moment.

Give each entry enough context to act on later — the failure mode, real file paths, and *why* it
was deferred. If a path does not exist yet (a runbook to be written), mark the line
`<!-- hygiene:planned -->` so the sensor allows it.

## 5. Decide what belongs in memory

Memory lives outside the repo, so CI can never check it. It is for things a future session cannot
recover from the code.

Save: user preferences and instructions with their reasoning; hard-won operational technique
(how to run a flaky suite, which failure looks like a bug but isn't); externally-verified facts
about dependencies that are not in their docs.

Do not save: anything the repo already records — code structure, past fixes, git history, or
content already in an ADR or `AGENTS.md`. A memory duplicating a doc will outlive its accuracy.

Follow the memory format in the system prompt, and add a one-line pointer to `MEMORY.md`.

## 6. Vendored dependency edits

If the diff touches `internal/vendor/go-pq-cdc/`, both are **mandatory** (see
`docs/decisions/0007-go-pq-cdc-stays-an-in-tree-fork.md`):

1. A `// vendored-patch: <ID>` marker at each edit site.
2. A `PATCHES.md` section: problem, fix, backward compatibility, and any cross-patch dependency.

This is not bookkeeping. MS-2's entry was initially omitted, and without it an upstream re-sync
silently reverts to a hardcoded `'public'` and reintroduces a crash-on-every-restart bug.

## 7. Report

Produce a short report:

- **Stale claims found** — file:line, the false statement, the correction.
- **ADRs warranted** — the decision, and why it meets the bar.
- **Memory entries warranted** — the fact, and why it is not recoverable from the repo.
- **Backlog changes** — entries resolved, invalidated, or newly added.
- **Drift** — where code and an existing ADR disagree, and which one is wrong.
- **Nothing found** is a valid outcome. Say it plainly rather than inventing work.

Then apply the fixes, re-run the sensor, and run the normal gate
(`go test -short $(go list ./internal/... | grep -v '/test/e2e')`).

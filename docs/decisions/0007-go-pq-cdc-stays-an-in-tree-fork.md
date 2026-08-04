---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: VENDOR.md, internal/vendor/go-pq-cdc/PATCHES.md
---

# `go-pq-cdc` stays a hand-maintained fork in-tree

## Context and Problem Statement

`github.com/Trendyol/go-pq-cdc` lives at `internal/vendor/go-pq-cdc/` behind a `go.mod` `replace`.
It is not a vendor copy but a **fork**: no submodule, no upstream remote, syncing is a manual
`rsync` + `patch -p3` recipe in `VENDOR.md`.

Multi-schema support required two further patches, raising the count to eight. That prompted the
question of whether to promote it to a real fork repository now.

**Update (2026-08-02)**: WS-7 (the TOAST hazard, `plans/cdc_custom_object_transform_remediation.md`)
required a ninth patch, `WS7-1`, to `Data.DecodeWithColumn`/`format.Update` — see
`internal/vendor/go-pq-cdc/PATCHES.md`'s WS7-1 entry and
`docs/decisions/0018-toast-unchanged-signalled-via-columnkinds.md` for the pipeline-side contract
it feeds. This does not change the decision below (option 1, in-tree, was re-confirmed rather than
revisited for this addition), but the patch count referenced throughout this ADR and `VENDOR.md`
is now **nine**, not eight.

## Decision Drivers

* Divergence is already substantial and load-bearing: T0-3 guards a real data-loss bug, and T0-2 is
  **API-breaking across three exported interfaces**, so a partial re-apply fails to compile.
* Promoting to a fork repo means a new repository to own, with CI and release plumbing.
* Multi-schema work should not be blocked on a dependency-management migration.

## Considered Options

1. **Keep in-tree**, patch in place with the existing convention.
2. **Promote to a fork repo first**, then patch there.
3. **Skip the vendored fixes** and work around them in our own code.

## Decision Outcome

Chosen: **option 1** for this change, with promotion left as a standing recommendation.

Two patches were added:

* **MS-1** — pins `search_path` on the regular (non-replication) connection via a libpq startup
  parameter, so it survives reconnects rather than relying on a one-time `SET`.
* **MS-2** — makes the snapshot coordinator's `cdc_snapshot_*` bookkeeping schema-aware. It
  resolves `metadataSchema` once and uses that single field for both the `CREATE` and the existence
  check, so the two cannot drift.

**MS-1 and MS-2 must be replayed together.** MS-1 alone actively breaks snapshotting: pinning
`search_path` makes the coordinator create its tables in the whitelisted schema while checking for
them in `public`, so `initTables` re-runs `CREATE TABLE` and errors on every restart. This
cross-patch dependency is recorded in both `PATCHES.md` sections.

### Consequences

* Good: no migration blocking the feature; patches follow the established convention.
* Bad: divergence grows to eight patches (nine as of the 2026-08-02 WS7-1 addition above), making
  the next upstream sync harder.
* Bad: the vendored module cannot be tested standalone (`missing go.sum entry`), so its logic has no
  executable unit coverage in its own module — it is only exercised through the parent.

## More Information

**Conventions, mandatory:** every edit carries a `// vendored-patch: <ID>` marker **and** a
`PATCHES.md` entry. Skipping the entry is not cosmetic — MS-2's was initially omitted, and without
it an upstream re-sync silently reverts to the hardcoded `'public'` and reintroduces the
crash-on-every-restart bug.

`VENDOR.md` continues to recommend promotion to a real fork repo, which would turn re-syncs into
`git merge upstream/main`. Given T0-2's API-breaking surface, that migration is overdue.

> **STATUS (2026-08-01): OBSOLETE — will not fix as written.**
> This TODO assumes a single `panic`-to-`logger.Error` patch. The fork now carries **eight**
> patches (see `internal/vendor/go-pq-cdc/PATCHES.md`), several load-bearing for correctness:
> T0-3 guards a real data-loss bug, and T0-2 is API-breaking across three exported interfaces.
> "Remove the patched dependency" is no longer feasible. The live option is the one VENDOR.md
> already recommends: promote the in-tree fork to a real fork repo and point `replace` at it.

# TODO: Resolve Patched Upstream Dependency (`go-pq-cdc`)

## Context
Currently, the pipeline relies on a custom patch applied to the `go-pq-cdc` library (in `internal/vendor/go-pq-cdc/pq/replication/stream.go:211`). 
The patch changes a `panic("corrupted connection")` to `logger.Error("corrupted connection")` to prevent the entire worker process from crashing on connection EOF during shutdown.

## The Problem
Maintaining a patched vendor dependency is a long-term maintenance burden and increases the risk of regressions or missing critical security updates from the upstream project.

## Action Items
- [ ] Attempt to contribute the patch upstream to `go-pq-cdc` via a Pull Request.
- [ ] If the maintainer is unresponsive or rejects the PR, evaluate migrating to an actively maintained alternative like `pglogrepl` to shed this technical debt.
- [ ] Once resolved, remove the patched dependency and update `VENDOR.md`.

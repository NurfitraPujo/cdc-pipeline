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

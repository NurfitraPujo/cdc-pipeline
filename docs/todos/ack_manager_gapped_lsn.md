# TODO: AckManager Watermark Stalling on Gapped PostgreSQL LSNs

## Context
The `AckManager` tracks observed LSNs from PostgreSQL replication and advances the watermark when observed LSNs are confirmed. The watermark is then used to send standby status updates to PostgreSQL.

## The Problem (Threat)
`AckManager` is implemented using a simple contiguous integer advancement logic:
```go
	for {
		next := a.watermark + 1
		acked, ok := a.pending[next]
		if !ok || !acked {
			break
		}
		a.watermark = next
		delete(a.pending, next)
	}
```
This logic assumes that LSNs are sequential, contiguous integers (i.e. `n, n+1, n+2`).
However, PostgreSQL Write-Ahead Log (WAL) LSNs represent byte offsets and are not contiguous; they have arbitrary gaps (e.g. `238472938`, `238473016`, `238473104`).
As a result:
1. `next := a.watermark + 1` will search for a non-existent LSN.
2. The watermark will never advance past `0` (or the hydrated checkpoint LSN), even after all observed LSNs are successfully processed and confirmed.
3. Standby status updates will continuously report a stale or zero LSN to PostgreSQL.
4. Because the confirmed flush LSN never advances in the replication slot, PostgreSQL will retain WAL files indefinitely, eventually filling up the disk space and causing database outage.

## Action Items
- [ ] Refactor `AckManager` to handle non-contiguous monotonic LSN sequences (e.g. by using a sorted list/slice of observed LSNs or a tree-based structure, rather than assuming `+1` increment).
- [ ] Add unit tests in `ack_test.go` that simulate realistic gapped PostgreSQL LSN sequences (e.g. `100, 150, 200`) and verify the watermark advances correctly.

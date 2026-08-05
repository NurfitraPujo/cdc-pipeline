package protocol

import "time"

// WAL budget arithmetic from plan section 5 (2026-08-03-pipeline-lifecycle-
// control.md): with the production source's max_slot_wal_keep_size fixed at
// 30 GB (OQ-2) and the pause-duration ceiling fixed at 4 h (OQ-3), a pause
// only survives its full term if the source sustains WAL growth under
//
//	WALBudgetBytes / MaxPauseTTL = 7.5 GB/h =~ 2.1 MB/s
//
// Above that rate the WAL guard (WS-4) trips before the timer does,
// escalating Paused straight to Stopping (plan section 4.3,
// "Paused | wal_guard_breach | Stopping"). WS-3's job is to project that
// breach and expose it -- see ProjectedTimeToBreach -- so the pause request
// can warn the operator before confirming, rather than let them discover it
// afterwards.
const (
	// WALBudgetBytes mirrors the production source's max_slot_wal_keep_size
	// (OQ-2): the amount of WAL PostgreSQL retains for a slot before it
	// starts invalidating it.
	WALBudgetBytes int64 = 30 * 1024 * 1024 * 1024 // 30 GiB

	// MaxPauseTTL is the pause-duration ceiling (OQ-3). PausePipeline
	// (internal/api/handler.go) rejects any requested ttl longer than this,
	// and it doubles as the "requested TTL" half of the time-to-breach
	// comparison when no ttl is given (an unbounded pause is compared
	// against the ceiling, since that is the longest it is allowed to run).
	MaxPauseTTL = 4 * time.Hour
)

// ProjectedTimeToBreach divides the WAL budget still remaining by the
// observed WAL growth rate to answer "how long until this slot's WAL
// budget is exhausted at the current write rate" (plan section 5).
// growthBytesPerSec is expected to come from two samples of the existing
// cdc_source_slot_lag_bytes probe (querySlotLagBytes in
// internal/source/postgres/source.go) taken apart in time -- this function
// is deliberately just the division, so it needs no database access and is
// trivially unit-testable, matching WS-3's "expose it" requirement without
// duplicating WS-4's signal.
//
// ok is false when no meaningful projection exists: a non-positive budget
// or a non-positive (flat or shrinking) growth rate never breaches, so
// there is nothing useful to warn about.
func ProjectedTimeToBreach(remainingBudgetBytes int64, growthBytesPerSec float64) (projected time.Duration, ok bool) {
	if remainingBudgetBytes <= 0 || growthBytesPerSec <= 0 {
		return 0, false
	}
	seconds := float64(remainingBudgetBytes) / growthBytesPerSec
	return time.Duration(seconds * float64(time.Second)), true
}

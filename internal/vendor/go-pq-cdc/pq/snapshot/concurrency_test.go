package snapshot

import (
	"sync"
	"testing"
)

// TestChunkHeartbeatTracksMultiple verifies that every concurrently-added chunk
// is reported by snapshot, so the single heartbeat goroutine refreshes each
// in-flight chunk's claim rather than only the last one to be added. This is
// the fix for the SC-1 starvation hazard: the historical heartbeat kept one
// activeChunkID, so with N concurrent workers every chunk but the last to be
// notified lost its heartbeat and could be re-claimed at ClaimTimeout.
func TestChunkHeartbeatTracksMultiple(t *testing.T) {
	hb := newChunkHeartbeat()
	hb.add(1)
	hb.add(2)
	hb.add(3)

	got := map[int64]bool{}
	for _, id := range hb.snapshot() {
		got[id] = true
	}
	for _, want := range []int64{1, 2, 3} {
		if !got[want] {
			t.Fatalf("chunk %d missing from heartbeat registry (got %v)", want, got)
		}
	}

	hb.remove(2)
	if len(hb.snapshot()) != 2 {
		t.Fatalf("expected 2 active chunks after removal of 2, got %d", len(hb.snapshot()))
	}
	for _, id := range hb.snapshot() {
		if id == 2 {
			t.Fatalf("removed chunk 2 still present in heartbeat registry")
		}
	}
}

// TestChunkHeartbeatConcurrency drives add/remove/snapshot from many goroutines
// to prove the registry is race-free under the same pattern the SC-1 worker
// goroutines use (each worker registers its chunk on claim and unregisters on
// completion). Run with -race.
func TestChunkHeartbeatConcurrency(t *testing.T) {
	hb := newChunkHeartbeat()
	const workers = 8
	const perWorker = 200

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := int64(base*perWorker + i)
				hb.add(id)
				hb.snapshot() // concurrent read while others add/remove
				hb.remove(id)
			}
		}(w)
	}
	wg.Wait()

	if ids := hb.snapshot(); len(ids) != 0 {
		t.Fatalf("expected 0 active chunks after all workers finished, got %d (%v)", len(ids), ids)
	}
}

// safeRetryError mimics pgx's *pgconn.connLockError{"conn busy"}: it declares
// SafeToRetry() == true, meaning no bytes were sent to the server and the same
// statement can be re-issued. This is exactly the error a concurrent Exec on a
// single shared PgConn returns.
type safeRetryError struct{}

func (safeRetryError) Error() string     { return "conn busy" }
func (safeRetryError) SafeToRetry() bool { return true }

// plainError does not implement SafeToRetry and must not be classified
// transient on the strength of the new check alone.
type plainError struct{}

func (plainError) Error() string { return "boom" }

// TestIsTransientError_SafeToRetry verifies the isTransientError classifier now
// consults pgconn.SafeToRetry, so a conn-busy error that ever slips past connMu
// is retried by retryDBOperation instead of failing fast and aborting the whole
// snapshot (vendored-patch: SC-1).
func TestIsTransientError_SafeToRetry(t *testing.T) {
	if !isTransientError(safeRetryError{}) {
		t.Fatal("expected SafeToRetry error to be classified transient")
	}
	if isTransientError(plainError{}) {
		t.Fatal("expected non-SafeToRetry error to be classified non-transient")
	}
}

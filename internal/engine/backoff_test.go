package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// WS-5 item 3 / docs/todos/custom_object_cdc_followups.md item 4: a
// RetryConfig whose InitialInterval/MaxInterval are left at their zero
// value used to make handleSinkError's exponential-backoff loop stay at
// backoff=0 forever, falling through to a flat, un-jittered 5s retry --
// a tight loop against a dependency that is, by construction, already
// degraded. applyJitter is the one piece of that fix cheap to unit test in
// isolation (a pure function); the floor behaviour (defaultRetryInitialInterval
// / defaultRetryMaxInterval) is exercised through defaultRetryConfig and
// deriveAckWait in factory_ws5_test.go, and end-to-end through the existing
// dlq_test.go / drain_test.go suites that already drive handleSinkError
// through a real Consumer.
// ----------------------------------------------------------------------------

func TestApplyJitter_StaysWithinBoundAndNeverZero(t *testing.T) {
	base := 2 * time.Second
	for i := 0; i < 200; i++ {
		got := applyJitter(base)
		require.Greater(t, got, time.Duration(0), "jittered backoff must never be zero or negative -- that would degrade back to a busy loop")
		lower := time.Duration(float64(base) * (1 - backoffJitterFraction))
		upper := time.Duration(float64(base) * (1 + backoffJitterFraction))
		assert.GreaterOrEqual(t, got, lower, "jittered backoff must not fall further than -%.0f%% below base", backoffJitterFraction*100)
		assert.LessOrEqual(t, got, upper, "jittered backoff must not exceed +%.0f%% above base", backoffJitterFraction*100)
	}
}

// TestApplyJitter_Desynchronizes is the property that actually matters for
// WS-5: two consumer instances computing "the same" exponential backoff for
// the same failure must not retry at the exact same instant (a
// thundering-herd redelivery against an already-degraded dependency).
// Statistically near-certain to differ across enough samples; this is not
// about dodging a race, it's about proving jitter is actually applied
// (deterministic output here would mean the RNG path was bypassed).
func TestApplyJitter_Desynchronizes(t *testing.T) {
	base := 5 * time.Second
	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		seen[applyJitter(base)] = true
	}
	assert.Greater(t, len(seen), 1, "applyJitter must produce varying output across calls, not a fixed value")
}

func TestApplyJitter_ZeroOrNegativeInput_FloorsToInitialInterval(t *testing.T) {
	assert.Equal(t, defaultRetryInitialInterval, applyJitter(0))
	assert.Equal(t, defaultRetryInitialInterval, applyJitter(-1*time.Second))
}

// TestApplyJitter_TinyBackoff_NeverCollapsesToZero covers a small base
// duration where a downward jitter swing could otherwise round to <= 0.
func TestApplyJitter_TinyBackoff_NeverCollapsesToZero(t *testing.T) {
	for i := 0; i < 200; i++ {
		got := applyJitter(2 * time.Millisecond)
		assert.GreaterOrEqual(t, got, time.Millisecond, "even a small backoff must floor at 1ms, never 0 or negative")
	}
}

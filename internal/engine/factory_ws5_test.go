package engine

import (
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	transformernats "github.com/NurfitraPujo/cdc-pipeline/internal/transformer/nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// WS-5 items 3, 4, 6 / docs/todos/custom_object_cdc_followups.md items 3, 4.
// ----------------------------------------------------------------------------

func TestDefaultRetryConfig_HasNonZeroIntervals(t *testing.T) {
	rc := defaultRetryConfig()
	assert.Equal(t, 3, rc.MaxRetries)
	assert.Greater(t, rc.InitialInterval, time.Duration(0), "InitialInterval must not be zero -- a zero value is exactly the WS-5 item 3 bug (backoff *= 2 stays 0 forever)")
	assert.Greater(t, rc.MaxInterval, time.Duration(0), "MaxInterval must not be zero")
	assert.LessOrEqual(t, rc.InitialInterval, rc.MaxInterval, "InitialInterval must not exceed MaxInterval")
}

// TestDeriveAckWait_NoTransformerProcessor_KeepsDefault is the
// configuration that does NOT exercise the derivation at all: a pipeline
// with no nats/protobuf processor (e.g. a plain postgres_debug/databend-only
// pipeline) has nothing to derive a worst case from and must get exactly
// the pre-fix flat 30s, unchanged.
func TestDeriveAckWait_NoTransformerProcessor_KeepsDefault(t *testing.T) {
	cfg := protocol.PipelineConfig{BatchSize: 500}
	assert.Equal(t, defaultAckWait, deriveAckWait(cfg, 0))
}

// TestDeriveAckWait_ScalesWithBatchSizeAndTransformerTimeout is the WS-5
// item 6 / followups.md item 3 regression test: a pipeline with a
// nats/protobuf processor and a real batch size must get an AckWait sized
// to survive the worst-case chunked-batch wall clock (BatchSize *
// per-chunk timeout), not the flat 30s that let JetStream redeliver a
// batch still being worked. Uses a batch/timeout combination that stays
// under defaultMaxAckWait so this test is purely about the scaling
// behaviour -- TestDeriveAckWait_ClampsAtDefaultCeiling below covers the
// ceiling itself.
func TestDeriveAckWait_ScalesWithBatchSizeAndTransformerTimeout(t *testing.T) {
	cfg := protocol.PipelineConfig{
		BatchSize: 50,
		Processors: []protocol.ProcessorConfig{
			{Type: "nats/protobuf", Options: map[string]interface{}{"timeout_ms": float64(2000)}},
		},
	}
	got := deriveAckWait(cfg, 0)
	want := time.Duration(50)*2000*time.Millisecond + sinkSubscriberLatencyMargin
	require.Less(t, want, defaultMaxAckWait, "test setup error: this case must land below the ceiling to test scaling, not clamping")
	assert.Equal(t, want, got)
	assert.Greater(t, got, defaultAckWait, "a real batch/timeout combination must produce an AckWait well above the old flat 30s default")
}

// TestDeriveAckWait_ClampsAtDefaultCeiling is the ratified-decision
// regression test (docs/decisions/0022-ackwait-ceiling-ten-minutes.md):
// BatchSize:1000, timeout_ms:20000 computes to ~5.6h unclamped -- a worse
// availability posture than the flat 30s this feature replaced, since
// MaxAckPending = BatchSize*2 stalls the whole subscriber behind one
// in-flight batch for however long AckWait is. deriveAckWait must clamp to
// defaultMaxAckWait (10 minutes) when no explicit ceiling is given, not the
// derived multi-hour value.
func TestDeriveAckWait_ClampsAtDefaultCeiling(t *testing.T) {
	cfg := protocol.PipelineConfig{
		BatchSize: 1000,
		Processors: []protocol.ProcessorConfig{
			{Type: "nats/protobuf", Options: map[string]interface{}{"timeout_ms": float64(20000)}},
		},
	}
	unclamped := time.Duration(1000) * 20000 * time.Millisecond
	require.Greater(t, unclamped, defaultMaxAckWait, "test setup error: this case must exceed the ceiling to actually test clamping")

	got := deriveAckWait(cfg, 0)
	assert.Equal(t, defaultMaxAckWait, got, "must clamp to defaultMaxAckWait (10 minutes) with no explicit ceiling override")
	assert.Equal(t, 10*time.Minute, got, "the ratified ceiling is specifically 10 minutes -- see docs/decisions/0022-ackwait-ceiling-ten-minutes.md")
}

// TestDeriveAckWait_CustomCeilingOverridesDefault covers the "configurable,
// not a bare constant" requirement: a caller-supplied ceiling (as
// PipelineFactory.AckWaitCeiling threads through) must be honoured instead
// of defaultMaxAckWait, in both directions -- tighter than the default and
// looser than the default -- proving the parameter actually changes the
// clamp rather than being ignored in favour of the constant.
func TestDeriveAckWait_CustomCeilingOverridesDefault(t *testing.T) {
	cfg := protocol.PipelineConfig{
		BatchSize: 1000,
		Processors: []protocol.ProcessorConfig{
			{Type: "nats/protobuf", Options: map[string]interface{}{"timeout_ms": float64(20000)}},
		},
	}
	unclamped := time.Duration(1000) * 20000 * time.Millisecond

	tighter := 2 * time.Minute
	require.Less(t, tighter, defaultMaxAckWait)
	got := deriveAckWait(cfg, tighter)
	assert.Equal(t, tighter, got, "a tighter explicit ceiling must be honoured, not silently widened back to the 10-minute default")

	looser := 30 * time.Minute
	require.Greater(t, looser, defaultMaxAckWait)
	require.Less(t, looser, unclamped, "test setup error: looser ceiling must still be below the unclamped derivation to prove it's actually the active bound")
	got = deriveAckWait(cfg, looser)
	assert.Equal(t, looser, got, "a looser explicit ceiling must be honoured, not silently narrowed back to the 10-minute default")
}

// TestDeriveAckWait_UsesTransformerDefaultTimeoutWhenUnset covers a
// nats/protobuf processor with no explicit timeout_ms -- deriveAckWait must
// use the same WS-5 item 4 default (transformernats.DefaultTimeoutMs) the
// transformer itself would compute, not silently treat it as "nothing to
// derive from". Uses a small batch size so the result stays under
// defaultMaxAckWait -- this test is about which timeout value gets used,
// not the ceiling.
func TestDeriveAckWait_UsesTransformerDefaultTimeoutWhenUnset(t *testing.T) {
	cfg := protocol.PipelineConfig{
		BatchSize: 10,
		Processors: []protocol.ProcessorConfig{
			{Type: "nats/protobuf", Options: map[string]interface{}{}},
		},
	}
	got := deriveAckWait(cfg, 0)
	wantTimeoutMs := transformernats.DefaultTimeoutMs(10)
	want := time.Duration(10)*time.Duration(wantTimeoutMs)*time.Millisecond + sinkSubscriberLatencyMargin
	require.Less(t, want, defaultMaxAckWait, "test setup error: this case must land below the ceiling")
	assert.Equal(t, want, got)
}

// TestDeriveAckWait_NeverBelowDefaultFloor covers a tiny batch size /
// timeout combination whose raw product would fall below the historical
// 30s default -- deriveAckWait must still floor at defaultAckWait rather
// than shrinking AckWait below what every pipeline already relied on.
func TestDeriveAckWait_NeverBelowDefaultFloor(t *testing.T) {
	cfg := protocol.PipelineConfig{
		BatchSize: 1,
		Processors: []protocol.ProcessorConfig{
			{Type: "nats/protobuf", Options: map[string]interface{}{"timeout_ms": float64(100)}},
		},
	}
	got := deriveAckWait(cfg, 0)
	assert.Equal(t, defaultAckWait, got, "must floor at defaultAckWait, never derive something smaller")
}

func TestDefaultTimeoutMs_FloorsAndScales(t *testing.T) {
	assert.Equal(t, 15000, transformernats.DefaultTimeoutMs(0), "unknown/zero batch size must floor at 15000ms")
	assert.Equal(t, 15000, transformernats.DefaultTimeoutMs(100), "5ms*100=500 is below the floor, must still floor at 15000ms")
	assert.Equal(t, 5000*5, transformernats.DefaultTimeoutMs(5000), "a large batch size must scale the default (5ms*5000=25000ms)")
}

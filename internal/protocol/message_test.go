package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordAck_RoundTrip_NonZero covers the msgp codec paths the
// generated zero-value tests (message_gen_test.go) never exercise: a
// multi-element []uint64 LSNs slice and a non-zero time.Time Timestamp.
// msgp drops the monotonic clock reading on encode, so equality must be
// checked with Timestamp.Equal, not ==/reflect.DeepEqual.
func TestRecordAck_RoundTrip_NonZero(t *testing.T) {
	ts := time.Now()
	original := RecordAck{
		PipelineID: "pipeline-1",
		SourceID:   "source-1",
		SinkID:     "sink-1",
		LSNs:       []uint64{100, 200, 300, 12345678901234},
		Timestamp:  ts,
	}

	bts, err := original.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded RecordAck
	left, err := decoded.UnmarshalMsg(bts)
	require.NoError(t, err)
	assert.Empty(t, left)

	assert.Equal(t, original.PipelineID, decoded.PipelineID)
	assert.Equal(t, original.SourceID, decoded.SourceID)
	assert.Equal(t, original.SinkID, decoded.SinkID)
	assert.Equal(t, original.LSNs, decoded.LSNs)
	assert.True(t, original.Timestamp.Equal(decoded.Timestamp), "Timestamp round-trip mismatch: got %v, want %v", decoded.Timestamp, original.Timestamp)
}

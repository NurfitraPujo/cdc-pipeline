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

// TestMessage_TableStaysBare_TableSchemaIsSibling locks §2.2: Table never
// carries schema qualification; TableSchema is a separate wire field that
// round-trips independently through the generated msgp codec.
func TestMessage_TableStaysBare_TableSchemaIsSibling(t *testing.T) {
	original := Message{
		SourceID:    "source-1",
		Table:       "orders",
		TableSchema: "sales",
		Op:          OpInsert,
	}

	bts, err := original.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded Message
	left, err := decoded.UnmarshalMsg(bts)
	require.NoError(t, err)
	assert.Empty(t, left)

	assert.Equal(t, "orders", decoded.Table, "Table must stay bare")
	assert.Equal(t, "sales", decoded.TableSchema)
}

// TestMessage_LegacyDecode_MissingTableSchema simulates a message encoded
// before TableSchema existed (or one that never set it, e.g. from an
// in-flight buffer stream written pre-upgrade): the field is omitted on the
// wire (msgp "omitempty"), decodes to the zero value, and NormalizeSchema
// must map that to "public" -- the same rule bare-configured tables use, so
// legacy and bare-config are one rule, not two (plan §2.2 "Costs").
func TestMessage_LegacyDecode_MissingTableSchema(t *testing.T) {
	original := Message{
		SourceID: "source-1",
		Table:    "orders",
		// TableSchema deliberately left unset, as a pre-upgrade producer
		// would never have written it.
		Op: OpInsert,
	}

	bts, err := original.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded Message
	left, err := decoded.UnmarshalMsg(bts)
	require.NoError(t, err)
	assert.Empty(t, left)

	assert.Equal(t, "", decoded.TableSchema, "legacy wire form omits tsch entirely")
	assert.Equal(t, "public", NormalizeSchema(decoded.TableSchema))
}

// TestSchemaDiff_TableSchema_RoundTrip covers the sibling field added to
// SchemaDiff, which previously carried no schema at all (plan §2.2 "Also
// required").
func TestSchemaDiff_TableSchema_RoundTrip(t *testing.T) {
	original := SchemaDiff{
		Table:       "orders",
		TableSchema: "sales",
		Timestamp:   time.Now(),
		Source:      "source-1",
		Added:       map[string]string{"col": "text"},
	}

	bts, err := original.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded SchemaDiff
	left, err := decoded.UnmarshalMsg(bts)
	require.NoError(t, err)
	assert.Empty(t, left)

	assert.Equal(t, "orders", decoded.Table)
	assert.Equal(t, "sales", decoded.TableSchema)
}

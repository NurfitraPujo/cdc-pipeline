package postgres

import (
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSanitizeValue_SurvivesMsgpackTransport is the regression guard for the
// review findings on the nats/protobuf transform-request encoder work:
//
//  1. sanitizePayload's first branch used to be `driver.Valuer`, which
//     pgtype.Numeric/Interval/Bits all satisfy -- so a NUMERIC column's
//     pgtype.Numeric was silently collapsed to a plain Go string before the
//     transformer's encodeTypedValue ever ran, permanently losing the
//     decimal_value routing (every numeric column shipped as string_value).
//     The codec-level test in internal/transformer/nats calls
//     encodeTypedValue directly and could not catch this, because it never
//     goes through sanitizePayload -- the exact "hand-constructed value real
//     WAL data never hits" failure mode one layer removed.
//  2. protocol.Message.Data crosses an internal NATS JetStream hop as
//     msgpack (generated WriteIntf/ReadIntf in message_gen.go), whose
//     reflection fallback only supports Ptr/Slice/Map. A struct
//     (pgtype.Numeric) or a fixed-size array ([16]byte, the real decode
//     type for a WAL uuid column) both hit msgp.ErrUnsupportedType and fail
//     MarshalMsg outright -- not a silent corruption, a hard batch stall.
//  3. An in-band string marker on the Data value itself (an earlier version
//     of this fix) was rejected: Data is read unconditionally by every
//     sink and by transformer/builtin.go, none of which know about a
//     transformer-private encoding, so the marker leaked verbatim into
//     every consumer that isn't the nats/protobuf transformer -- a hard
//     encoding error for sink/postgresdebug (NUL byte into Postgres text)
//     and silent wrong values for sink/databend and PK WHERE clauses
//     everywhere else. The fix is instead a side-channel,
//     protocol.Message.ColumnKinds, so Data always stays exactly what every
//     existing consumer already reads today, and only a kind-aware consumer
//     needs to look at ColumnKinds at all.
//
// This test decodes with real pgtype codecs (not hand-written literals),
// runs the result through the real sanitizeValue/sanitizePayload, and then
// round-trips the *whole* protocol.Message (Data AND ColumnKinds) through
// its generated MarshalMsg/UnmarshalMsg -- the literal call the internal
// transport makes -- to prove all three properties at once: Data is
// unchanged from what a kind-unaware consumer already expects, ColumnKinds
// carries the routing hint, and both survive the real msgpack round-trip.
func TestSanitizeValue_SurvivesMsgpackTransport(t *testing.T) {
	typeMap := pgtype.NewMap()

	decode := func(t *testing.T, typeName string, text string) interface{} {
		t.Helper()
		dt, ok := typeMap.TypeForName(typeName)
		require.True(t, ok, "pgtype has no registered type %q", typeName)
		v, err := dt.Codec.DecodeValue(typeMap, dt.OID, pgtype.TextFormatCode, []byte(text))
		require.NoError(t, err, "decoding %q as %s", text, typeName)
		return v
	}

	// roundTrip marshals and unmarshals a full Message (Data and
	// ColumnKinds) through its real msgp generated methods -- the exact
	// call the internal NATS JetStream producer/consumer boundary makes --
	// and fails the test with the real msgp error if the payload isn't
	// transport-safe, rather than silently swallowing it.
	roundTrip := func(t *testing.T, data map[string]any, kinds map[string]string) protocol.Message {
		t.Helper()
		msg := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Data: data, ColumnKinds: kinds}
		b, err := msg.MarshalMsg(nil)
		require.NoError(t, err, "Message.MarshalMsg must not fail -- this is exactly the hard failure mode pre-fix uuid/numeric columns hit")
		var out protocol.Message
		_, err = out.UnmarshalMsg(b)
		require.NoError(t, err)
		return out
	}

	t.Run("numeric column: Data stays the plain exact decimal text every sink already reads, ColumnKinds carries the routing hint", func(t *testing.T) {
		v := decode(t, "numeric", "1500.50")
		_, isNumeric := v.(pgtype.Numeric)
		require.True(t, isNumeric)

		sanitized, kind := sanitizeValue(v)

		// Must not still be the raw struct -- that would fail MarshalMsg.
		_, stillNumeric := sanitized.(pgtype.Numeric)
		assert.False(t, stillNumeric, "sanitizeValue must not pass pgtype.Numeric through raw -- it cannot cross the msgpack transport")

		// Must be the plain exact text, with NO marker/prefix -- this is
		// the value a pipeline running WITHOUT nats/protobuf, or any other
		// consumer of Data, sees. It must be identical to what shipped
		// before TypedValue existed.
		gotStr, ok := sanitized.(string)
		require.True(t, ok, "expected a plain string, got %T: %v", sanitized, sanitized)
		assert.Equal(t, "1500.50", gotStr, "Data value must be the bare exact decimal text -- no in-band marker")
		assert.Equal(t, protocol.ColumnKindDecimal, kind)

		out := roundTrip(t, map[string]any{"price": sanitized}, map[string]string{"price": kind})

		assert.Equal(t, "1500.50", out.Data["price"], "Data must survive the transport completely unchanged")
		require.NotNil(t, out.ColumnKinds)
		assert.Equal(t, protocol.ColumnKindDecimal, out.ColumnKinds["price"], "ColumnKinds must survive the transport and still name the decimal column")
	})

	t.Run("numeric NULL survives transport as a real nil, with no kind hint", func(t *testing.T) {
		dt, _ := typeMap.TypeForName("numeric")
		nullVal, err := dt.Codec.DecodeValue(typeMap, dt.OID, pgtype.TextFormatCode, nil)
		require.NoError(t, err)
		require.Nil(t, nullVal)

		sanitized, kind := sanitizeValue(nullVal)
		assert.Nil(t, sanitized)
		assert.Empty(t, kind)

		out := roundTrip(t, map[string]any{"price": sanitized}, nil)
		assert.Nil(t, out.Data["price"])
		assert.Nil(t, out.ColumnKinds)
	})

	t.Run("uuid column survives transport as a plain UUID string, no kind hint needed", func(t *testing.T) {
		v := decode(t, "uuid", "550e8400-e29b-41d4-a716-446655440000")
		_, is16 := v.([16]byte)
		require.True(t, is16, "expected [16]byte, got %T", v)

		sanitized, kind := sanitizeValue(v)
		assert.Empty(t, kind, "a uuid string needs no routing hint -- it's already what encodeTypedValue's string_value case wants")

		out := roundTrip(t, map[string]any{"ext_id": sanitized}, nil)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", out.Data["ext_id"])
	})

	t.Run("uuid array column survives transport (recursive sanitize, HIGH finding)", func(t *testing.T) {
		dt, ok := typeMap.TypeForName("_uuid")
		require.True(t, ok)
		v, err := dt.Codec.DecodeValue(typeMap, dt.OID, pgtype.TextFormatCode, []byte(`{550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}`))
		require.NoError(t, err)

		sanitized, _ := sanitizeValue(v)
		out := roundTrip(t, map[string]any{"tags": sanitized}, nil)

		got, ok := out.Data["tags"].([]any)
		require.True(t, ok, "expected []any after transport, got %T", out.Data["tags"])
		require.Len(t, got, 2)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", got[0])
		assert.Equal(t, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", got[1])
	})

	t.Run("numeric array column survives transport without a hard MarshalMsg failure (HIGH finding)", func(t *testing.T) {
		dt, ok := typeMap.TypeForName("_numeric")
		require.True(t, ok)
		v, err := dt.Codec.DecodeValue(typeMap, dt.OID, pgtype.TextFormatCode, []byte(`{1500.50,2.75}`))
		require.NoError(t, err)

		sanitized, _ := sanitizeValue(v)
		// Must not still contain raw pgtype.Numeric elements -- would fail
		// MarshalMsg exactly like the scalar case.
		out := roundTrip(t, map[string]any{"amounts": sanitized}, nil)

		got, ok := out.Data["amounts"].([]any)
		require.True(t, ok, "expected []any after transport, got %T", out.Data["amounts"])
		require.Len(t, got, 2)
		for _, elem := range got {
			s, ok := elem.(string)
			require.True(t, ok, "expected plain string element, got %T: %v", elem, elem)
			assert.NotContains(t, s, "\x00", "array elements must never carry an in-band marker")
		}
		assert.Equal(t, "1500.50", got[0])
		assert.Equal(t, "2.75", got[1])
	})

	t.Run("sanitizePayload wires ColumnKinds only for columns that need it, leaving Data untouched", func(t *testing.T) {
		numeric := decode(t, "numeric", "1500.50")
		row := map[string]any{
			"price": numeric,
			"name":  "alice",
			"age":   int32(25),
		}

		data, kinds := sanitizePayload(row)

		assert.Equal(t, "1500.50", data["price"])
		assert.Equal(t, "alice", data["name"])
		assert.Equal(t, int32(25), data["age"])

		require.NotNil(t, kinds)
		assert.Equal(t, protocol.ColumnKindDecimal, kinds["price"])
		_, hasName := kinds["name"]
		assert.False(t, hasName, "a plain text column must not get a kind entry")
		_, hasAge := kinds["age"]
		assert.False(t, hasAge, "a plain int column must not get a kind entry")
	})

	t.Run("sanitizePayload returns a nil ColumnKinds when nothing needs one -- matches pre-fix shape exactly", func(t *testing.T) {
		data, kinds := sanitizePayload(map[string]any{"name": "alice", "age": int32(25)})
		assert.Equal(t, "alice", data["name"])
		assert.Nil(t, kinds, "no column needed a kind hint, so ColumnKinds must be nil -- a pipeline with no NUMERIC columns sees byte-identical messages to before this fix")
	})
}

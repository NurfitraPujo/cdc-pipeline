package nats

import (
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeTypedValue_RealPgtypeCodecDecode drives encodeTypedValue with the
// exact values pgx's own TextFormatCode codecs decode WAL tuples into (see
// internal/vendor/go-pq-cdc/pq/message/tuple/data.go:99 --
// dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)), not
// hand-constructed Go literals. Hand-written literals (e.g. a bare
// uuid.UUID{}) can accidentally match a case that real WAL data never hits --
// that is exactly how the pre-fix uuid ([16]byte, not uuid.UUID) and numeric
// (not a fmt.Stringer) bugs survived
// TestNatsProtoTransformer_AllColumnTypesSanitization. This test is the
// regression guard for that class of bug.
func TestEncodeTypedValue_RealPgtypeCodecDecode(t *testing.T) {
	typeMap := pgtype.NewMap()

	decode := func(t *testing.T, typeName string, text string) interface{} {
		t.Helper()
		dt, ok := typeMap.TypeForName(typeName)
		require.True(t, ok, "pgtype has no registered type %q", typeName)
		v, err := dt.Codec.DecodeValue(typeMap, dt.OID, pgtype.TextFormatCode, []byte(text))
		require.NoError(t, err, "decoding %q as %s", text, typeName)
		return v
	}

	t.Run("numeric decodes to decimal_value with exact text, never a float or a struct dump", func(t *testing.T) {
		v := decode(t, "numeric", "1500.50")
		_, isNumeric := v.(pgtype.Numeric)
		require.True(t, isNumeric, "expected pgtype.Numeric, got %T (if this changed, the encoder's case must change with it)", v)

		tv := encodeTypedValue(v, "")
		require.NotNil(t, tv.GetKind())
		assert.Equal(t, "1500.50", tv.GetDecimalValue(), "must be the exact decimal text, not a float round-trip")
		assert.Empty(t, tv.GetStringValue(), "must not fall through to string_value")
	})

	t.Run("numeric large precision value never rounds through float64", func(t *testing.T) {
		// 2^53+5 has no exact float64 representation as an integer amount;
		// if this ever silently round-trips through float64 the digits will
		// drift.
		v := decode(t, "numeric", "9007199254740997.123456789")
		tv := encodeTypedValue(v, "")
		assert.Equal(t, "9007199254740997.123456789", tv.GetDecimalValue())
	})

	t.Run("numeric NULL", func(t *testing.T) {
		dt, ok := typeMap.TypeForName("numeric")
		require.True(t, ok)
		nullVal, err := dt.Codec.DecodeValue(typeMap, dt.OID, pgtype.TextFormatCode, nil)
		require.NoError(t, err)
		assert.Nil(t, nullVal)
		tv := encodeTypedValue(nullVal, "")
		assert.NotNil(t, tv.GetNullValue())
	})

	t.Run("uuid decodes to [16]byte, not uuid.UUID or []byte, and must become string_value", func(t *testing.T) {
		v := decode(t, "uuid", "550e8400-e29b-41d4-a716-446655440000")
		_, is16 := v.([16]byte)
		require.True(t, is16, "expected [16]byte, got %T -- the [16]byte case in encodeTypedValue must track this", v)

		tv := encodeTypedValue(v, "")
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", tv.GetStringValue())
		assert.Empty(t, tv.GetJsonValue(), "must not fall through to a json_value array of bytes")
	})

	t.Run("date decodes to time.Time -- documented limitation, becomes timestamp_value not date_value", func(t *testing.T) {
		v := decode(t, "date", "2024-03-15")
		_, isTime := v.(time.Time)
		require.True(t, isTime, "expected time.Time, got %T", v)

		tv := encodeTypedValue(v, "")
		require.NotNil(t, tv.GetTimestampValue())
		assert.Equal(t, 2024, tv.GetTimestampValue().AsTime().Year())
		assert.Equal(t, time.March, tv.GetTimestampValue().AsTime().Month())
		assert.Equal(t, 15, tv.GetTimestampValue().AsTime().Day())
	})

	t.Run("inet decodes to netip.Prefix -- Stringer, must be string_value not decimal_value", func(t *testing.T) {
		v := decode(t, "inet", "192.168.1.5/32")
		tv := encodeTypedValue(v, "")
		assert.Equal(t, "192.168.1.5/32", tv.GetStringValue())
		assert.Empty(t, tv.GetDecimalValue(), "a Stringer must never be misrouted into decimal_value")
	})

	t.Run("macaddr decodes to net.HardwareAddr -- Stringer, must be string_value not decimal_value", func(t *testing.T) {
		v := decode(t, "macaddr", "08:00:2b:01:02:03")
		tv := encodeTypedValue(v, "")
		assert.Equal(t, "08:00:2b:01:02:03", tv.GetStringValue())
		assert.Empty(t, tv.GetDecimalValue())
	})

	t.Run("interval decodes to pgtype.Interval -- not a Stringer, must be string_value not a struct dump", func(t *testing.T) {
		v := decode(t, "interval", "1 mon 2 days 00:00:00")
		_, isInterval := v.(pgtype.Interval)
		require.True(t, isInterval, "expected pgtype.Interval, got %T", v)

		tv := encodeTypedValue(v, "")
		assert.NotEmpty(t, tv.GetStringValue())
		assert.NotContains(t, tv.GetStringValue(), "{", "must not be a raw Go struct dump")
		// MEDIUM finding: the emitted text must be real Postgres interval
		// syntax -- exactly what pgtype.Interval's own Value() (the
		// codec's TextFormatCode encoder) produces, not a hand-rolled,
		// non-round-trippable format like "1mon 2d 0us".
		wantVal, err := v.(pgtype.Interval).Value()
		require.NoError(t, err)
		assert.Equal(t, wantVal, tv.GetStringValue())
	})

	t.Run("bits decodes to pgtype.Bits -- not a Stringer, must be string_value not a struct dump", func(t *testing.T) {
		v := decode(t, "bit", "1010")
		_, isBits := v.(pgtype.Bits)
		require.True(t, isBits, "expected pgtype.Bits, got %T", v)

		tv := encodeTypedValue(v, "")
		assert.Equal(t, "1010", tv.GetStringValue())
	})
}

// TestEncodeTypedValue_ColumnKindDecimalAcrossPackageBoundary closes the
// loop between internal/source/postgres.sanitizeValue (which cannot be
// called from this package -- it's unexported in a different package, and
// covered directly by TestSanitizeValue_SurvivesMsgpackTransport there) and
// this package's encodeTypedValue: it proves that the plain, unmarked
// decimal-text string sanitizeValue produces for a NUMERIC column --
// together with the protocol.ColumnKindDecimal hint travelling via
// protocol.Message.ColumnKinds, not any encoding baked into the string
// itself -- resolves to decimal_value here, not string_value. An earlier
// revision used an in-band NUL-prefixed marker on the string; that was
// rejected on review because Data is read unconditionally by every sink and
// by transformer/builtin.go, none of which know about a transformer-private
// string convention, so the marker leaked into consumers that never asked
// for it (a hard Postgres encoding error for sink/postgresdebug, silently
// wrong values everywhere else). The kind now travels out-of-band, so Data
// itself is byte-identical to what a kind-unaware consumer already expects.
func TestEncodeTypedValue_ColumnKindDecimalAcrossPackageBoundary(t *testing.T) {
	tv := encodeTypedValue("1500.50", protocol.ColumnKindDecimal)
	assert.Equal(t, "1500.50", tv.GetDecimalValue())
	assert.Empty(t, tv.GetStringValue())

	// A genuine, unmarked text/CITEXT value with no kind hint must still
	// route to string_value.
	plain := encodeTypedValue("just a normal string", "")
	assert.Equal(t, "just a normal string", plain.GetStringValue())
	assert.Empty(t, plain.GetDecimalValue())

	// The same plain string is NOT routed to decimal_value just because it
	// looks numeric -- only the explicit kind hint decides, never a
	// heuristic on the value's shape.
	notDecimal := encodeTypedValue("1500.50", "")
	assert.Equal(t, "1500.50", notDecimal.GetStringValue())
	assert.Empty(t, notDecimal.GetDecimalValue())
}

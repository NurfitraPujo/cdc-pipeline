package tuple

import (
	"encoding/binary"
	"testing"
)

// buildTupleWire builds the raw wire bytes for a single tuple ('N', 'O', or
// 'K') the same way the real Postgres logical replication stream would --
// this drives NewData/Decode through their real byte-parsing path (per the
// project's verification standard) instead of constructing a Data{} struct
// literal that could never actually come off the wire.
func buildTupleWire(tupleType byte, cols []struct {
	dataType byte
	text     string
}) []byte {
	buf := []byte{tupleType}

	colNumBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(colNumBytes, uint16(len(cols)))
	buf = append(buf, colNumBytes...)

	for _, c := range cols {
		buf = append(buf, c.dataType)
		if c.dataType == DataTypeText || c.dataType == DataTypeBinary {
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, uint32(len(c.text)))
			buf = append(buf, lenBytes...)
			buf = append(buf, []byte(c.text)...)
		}
	}
	return buf
}

func textCol(s string) struct {
	dataType byte
	text     string
} {
	return struct {
		dataType byte
		text     string
	}{dataType: DataTypeText, text: s}
}

func nullCol() struct {
	dataType byte
	text     string
} {
	return struct {
		dataType byte
		text     string
	}{dataType: DataTypeNull}
}

func toastCol() struct {
	dataType byte
	text     string
} {
	return struct {
		dataType byte
		text     string
	}{dataType: DataTypeToast}
}

func textRelationColumns(names ...string) []RelationColumn {
	cols := make([]RelationColumn, len(names))
	for i, n := range names {
		// OID 25 = text; decodeTextColumnData falls back to plain string
		// for any OID the pgtype map doesn't resolve specially, which text
		// (25) does resolve to a plain string codec -- exercising the real
		// decode path, not a stub.
		cols[i] = RelationColumn{Name: n, DataType: 25}
	}
	return cols
}

// TestDecodeWithColumn_TextAndNull drives NewData -> DecodeWithColumn
// through the real wire-decode path for the ordinary (non-TOAST) case,
// establishing the baseline the TOAST tests below diff against.
func TestDecodeWithColumn_TextAndNull(t *testing.T) {
	wire := buildTupleWire('N', []struct {
		dataType byte
		text     string
	}{textCol("alice"), nullCol()})

	d, err := NewData(wire, 'N', 0)
	if err != nil {
		t.Fatalf("NewData: %v", err)
	}

	decoded, toasted, err := d.DecodeWithColumn(textRelationColumns("name", "bio"))
	if err != nil {
		t.Fatalf("DecodeWithColumn: %v", err)
	}

	if got, ok := decoded["name"]; !ok || got != "alice" {
		t.Fatalf("decoded[name] = %v, %v; want \"alice\", true", got, ok)
	}
	bioVal, bioOK := decoded["bio"]
	if !bioOK {
		t.Fatalf("decoded[bio] missing entirely; a real NULL must have an explicit nil entry, not an absent key")
	}
	if bioVal != nil {
		t.Fatalf("decoded[bio] = %v; want nil (genuine NULL)", bioVal)
	}
	if toasted != nil {
		t.Fatalf("toasted = %v; want nil, nothing in this tuple is TOASTed", toasted)
	}
}

// TestDecodeWithColumn_ToastedColumnOmittedNotNulled is the WS-7 regression
// test. Under REPLICA IDENTITY DEFAULT with an unchanged key, Postgres
// sends a TOAST-marker byte ('u') for an unchanged out-of-line column
// instead of its value. Before the WS-7 fix, DecodeWithColumn's switch had
// no case for DataTypeToast at all, so the column silently got no map
// entry -- byte-for-byte indistinguishable from the DataTypeNull case
// (which explicitly writes decoded[colName] = nil). That conflation is
// exactly the hazard: a consumer cannot tell "this is really NULL" from
// "Postgres just didn't send the value" by looking at decoded alone.
//
// This test would have passed before the fix too (decoded["bio"] was
// always absent) -- the assertion that actually distinguishes the fix is
// the toasted return value, verified explicitly below.
func TestDecodeWithColumn_ToastedColumnOmittedNotNulled(t *testing.T) {
	wire := buildTupleWire('N', []struct {
		dataType byte
		text     string
	}{textCol("alice"), toastCol()})

	d, err := NewData(wire, 'N', 0)
	if err != nil {
		t.Fatalf("NewData: %v", err)
	}

	decoded, toasted, err := d.DecodeWithColumn(textRelationColumns("name", "long_bio"))
	if err != nil {
		t.Fatalf("DecodeWithColumn: %v", err)
	}

	if _, present := decoded["long_bio"]; present {
		t.Fatalf("decoded[long_bio] must be absent for an unchanged TOASTed column, got a map entry: %v", decoded["long_bio"])
	}

	if len(toasted) != 1 || toasted[0] != "long_bio" {
		t.Fatalf("toasted = %v; want exactly [\"long_bio\"] -- this is the WS-7 signal a caller needs to disambiguate \"omitted because unchanged TOAST\" from \"omitted because this producer never sends the column\"", toasted)
	}

	if got := decoded["name"]; got != "alice" {
		t.Fatalf("decoded[name] = %v; want \"alice\" (an ordinary column in the same tuple must be unaffected)", got)
	}
}

// TestDecodeWithColumn_MultipleToastedColumns confirms the toasted slice
// reports every toasted column in the tuple, not just the first, and that
// column order in the relation is preserved correctly against a mixed
// text/null/toast tuple -- the configuration that does NOT exercise TOAST
// at all (TestDecodeWithColumn_TextAndNull) already covers the "no TOAST
// present" baseline; this covers "more than one".
func TestDecodeWithColumn_MultipleToastedColumns(t *testing.T) {
	wire := buildTupleWire('N', []struct {
		dataType byte
		text     string
	}{textCol("42"), toastCol(), nullCol(), toastCol()})

	d, err := NewData(wire, 'N', 0)
	if err != nil {
		t.Fatalf("NewData: %v", err)
	}

	decoded, toasted, err := d.DecodeWithColumn(textRelationColumns("id", "notes", "deleted_reason", "attachment"))
	if err != nil {
		t.Fatalf("DecodeWithColumn: %v", err)
	}

	if _, present := decoded["notes"]; present {
		t.Fatalf("decoded[notes] should be absent (TOASTed)")
	}
	if _, present := decoded["attachment"]; present {
		t.Fatalf("decoded[attachment] should be absent (TOASTed)")
	}
	if v, ok := decoded["deleted_reason"]; !ok || v != nil {
		t.Fatalf("decoded[deleted_reason] = %v, %v; want nil, true (genuine NULL, distinct from TOAST)", v, ok)
	}

	want := map[string]bool{"notes": true, "attachment": true}
	if len(toasted) != 2 {
		t.Fatalf("toasted = %v; want exactly 2 entries", toasted)
	}
	for _, name := range toasted {
		if !want[name] {
			t.Fatalf("unexpected toasted column %q; want one of %v", name, want)
		}
		delete(want, name)
	}
}

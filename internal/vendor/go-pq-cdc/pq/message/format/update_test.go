package format

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
)

// buildUpdateWire builds the raw bytes for a non-streamed UPDATE message
// carrying only a new tuple (no 'K'/'O' old tuple byte at all) -- exactly
// what Postgres sends under REPLICA IDENTITY DEFAULT when the replica
// identity (key) columns are unchanged. This is the WS-7 case: it is the
// only shape in which a genuinely-elided TOASTed column can reach
// DecodeWithColumn with no old-tuple fallback available to recover it
// from. Drives Update.decode's real byte-parsing path (per the project's
// verification standard) instead of constructing an Update{} literal.
func buildUpdateWire(oid uint32, cols []struct {
	dataType byte
	text     string
}) []byte {
	buf := []byte{'U'} // message type byte; decode() starts past it

	oidBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(oidBytes, oid)
	buf = append(buf, oidBytes...)

	buf = append(buf, UpdateTupleTypeNew) // 'N' -- no old tuple present at all

	colNumBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(colNumBytes, uint16(len(cols)))
	buf = append(buf, colNumBytes...)

	for _, c := range cols {
		buf = append(buf, c.dataType)
		if c.dataType == tuple.DataTypeText || c.dataType == tuple.DataTypeBinary {
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, uint32(len(c.text)))
			buf = append(buf, lenBytes...)
			buf = append(buf, []byte(c.text)...)
		}
	}
	return buf
}

func updTextCol(s string) struct {
	dataType byte
	text     string
} {
	return struct {
		dataType byte
		text     string
	}{dataType: tuple.DataTypeText, text: s}
}

func updToastCol() struct {
	dataType byte
	text     string
} {
	return struct {
		dataType byte
		text     string
	}{dataType: tuple.DataTypeToast}
}

func updNullCol() struct {
	dataType byte
	text     string
} {
	return struct {
		dataType byte
		text     string
	}{dataType: tuple.DataTypeNull}
}

// buildUpdateWireWithOldKey builds a non-streamed UPDATE message carrying a
// 'K' (key-only) old tuple followed by an 'N' new tuple -- exactly what
// Postgres sends under REPLICA IDENTITY DEFAULT when an UPDATE changes the
// replica-identity key column(s). Verified live against Postgres 15's real
// pgoutput wire output (docker: `postgres:15-alpine`, wal_level=logical,
// pgoutput publication, `pg_logical_slot_peek_binary_changes`) for an
// UPDATE that changes a table's PK while leaving a TOASTed column
// untouched: the 'K' tuple's non-key columns come through tagged
// DataTypeNull ('n'), never their real value, and the 'N' tuple's untouched
// TOASTed column comes through tagged DataTypeToast ('u') -- this helper
// reproduces that exact shape byte-for-byte, not an approximation.
func buildUpdateWireWithOldKey(oid uint32, oldTupleType byte, oldCols, newCols []struct {
	dataType byte
	text     string
}) []byte {
	buf := []byte{'U'}

	oidBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(oidBytes, oid)
	buf = append(buf, oidBytes...)

	buf = append(buf, oldTupleType) // 'K' or 'O'
	oldColNumBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(oldColNumBytes, uint16(len(oldCols)))
	buf = append(buf, oldColNumBytes...)
	for _, c := range oldCols {
		buf = append(buf, c.dataType)
		if c.dataType == tuple.DataTypeText || c.dataType == tuple.DataTypeBinary {
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, uint32(len(c.text)))
			buf = append(buf, lenBytes...)
			buf = append(buf, []byte(c.text)...)
		}
	}

	buf = append(buf, UpdateTupleTypeNew) // 'N'
	newColNumBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(newColNumBytes, uint16(len(newCols)))
	buf = append(buf, newColNumBytes...)
	for _, c := range newCols {
		buf = append(buf, c.dataType)
		if c.dataType == tuple.DataTypeText || c.dataType == tuple.DataTypeBinary {
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, uint32(len(c.text)))
			buf = append(buf, lenBytes...)
			buf = append(buf, []byte(c.text)...)
		}
	}

	return buf
}

func testRelation(oid uint32, colNames ...string) map[uint32]*Relation {
	cols := make([]tuple.RelationColumn, len(colNames))
	for i, n := range colNames {
		cols[i] = tuple.RelationColumn{Name: n, DataType: 25} // OID 25 = text
	}
	return map[uint32]*Relation{
		oid: {OID: oid, Namespace: "public", Name: "widgets", Columns: cols},
	}
}

// TestNewUpdate_NoOldTuple_ToastedColumnSurfacedNotDropped is the WS-7
// regression test at the format.Update layer: under REPLICA IDENTITY
// DEFAULT with an unchanged key, the wire never carries an old tuple at
// all (OldTupleData stays nil), so update.go's own toast-backfill loop
// ("because toasted columns not sent until the toasted column updated")
// never runs -- the only way the elided column can be recovered is via
// DecodeWithColumn's toasted return, now threaded through as
// NewToastedColumns.
func TestNewUpdate_NoOldTuple_ToastedColumnSurfacedNotDropped(t *testing.T) {
	wire := buildUpdateWire(100, []struct {
		dataType byte
		text     string
	}{updTextCol("alice"), updToastCol()})

	upd, err := NewUpdate(wire, false, testRelation(100, "name", "long_bio"), time.Now())
	if err != nil {
		t.Fatalf("NewUpdate: %v", err)
	}

	if upd.OldTupleData != nil {
		t.Fatalf("OldTupleData = %+v; want nil for a bare 'N'-only wire message", upd.OldTupleData)
	}

	if _, present := upd.NewDecoded["long_bio"]; present {
		t.Fatalf("NewDecoded[long_bio] must be absent (TOASTed, unchanged), got %v", upd.NewDecoded["long_bio"])
	}
	if got := upd.NewDecoded["name"]; got != "alice" {
		t.Fatalf("NewDecoded[name] = %v; want \"alice\"", got)
	}

	if len(upd.NewToastedColumns) != 1 || upd.NewToastedColumns[0] != "long_bio" {
		t.Fatalf("NewToastedColumns = %v; want exactly [\"long_bio\"] -- without this, a downstream consumer cannot tell an elided TOAST column apart from a real NULL", upd.NewToastedColumns)
	}
}

// TestNewUpdate_KeyTupleChangedPK_ToastedColumnStillSurfaced is the
// regression test for the hole found in Opus validation review: under
// REPLICA IDENTITY DEFAULT, an UPDATE that changes the replica-identity key
// (id, here) sends a 'K' old tuple, so OldTupleData != nil and
// decode()'s pre-existing backfill loop runs. Before the WS7-1 K-tuple
// fix, that loop unconditionally copied the old tuple's column (tagged
// DataTypeNull for every non-key column in a 'K' tuple -- verified live
// against real pgoutput output, see buildUpdateWireWithOldKey) over the new
// tuple's genuine DataTypeToast marker, silently turning an
// unchanged-TOAST column into a fabricated NULL: the exact WS-7 bug,
// reintroduced one layer above DecodeWithColumn. This wire matches that
// live-verified shape exactly: old-key carries only "id" as real data,
// "wide"/"name" as DataTypeNull; new tuple carries "id" and "name" as real
// text and "wide" as DataTypeToast (untouched).
func TestNewUpdate_KeyTupleChangedPK_ToastedColumnStillSurfaced(t *testing.T) {
	oldCols := []struct {
		dataType byte
		text     string
	}{updTextCol("2"), updNullCol(), updNullCol()} // id=2 (old key), wide unknown, name unknown
	newCols := []struct {
		dataType byte
		text     string
	}{updTextCol("3"), updToastCol(), updTextCol("changed2")} // id=3, wide unchanged-TOAST, name changed

	wire := buildUpdateWireWithOldKey(100, UpdateTupleTypeKey, oldCols, newCols)

	upd, err := NewUpdate(wire, false, testRelation(100, "id", "wide", "name"), time.Now())
	if err != nil {
		t.Fatalf("NewUpdate: %v", err)
	}

	if upd.OldTupleData == nil {
		t.Fatalf("OldTupleData must be non-nil for a 'K' old tuple")
	}
	if upd.OldTupleType != UpdateTupleTypeKey {
		t.Fatalf("OldTupleType = %q; want 'K'", upd.OldTupleType)
	}

	if _, present := upd.NewDecoded["wide"]; present {
		t.Fatalf("NewDecoded[wide] must be absent (TOASTed, unchanged), got %v -- if this is present with a nil value, the K-tuple backfill fabricated a NULL over the TOAST marker", upd.NewDecoded["wide"])
	}
	if len(upd.NewToastedColumns) != 1 || upd.NewToastedColumns[0] != "wide" {
		t.Fatalf("NewToastedColumns = %v; want exactly [\"wide\"] -- a changed replica-identity key must not suppress the TOAST signal for an untouched column", upd.NewToastedColumns)
	}
	if got := upd.NewDecoded["id"]; got != "3" {
		t.Fatalf("NewDecoded[id] = %v; want \"3\"", got)
	}
	if got := upd.NewDecoded["name"]; got != "changed2" {
		t.Fatalf("NewDecoded[name] = %v; want \"changed2\"", got)
	}
}

// TestNewUpdate_FullOldTuple_ToastedColumnBackfillIsANoOp is the
// configuration that does NOT exercise the K-tuple hole: under REPLICA
// IDENTITY FULL ('O' old tuple, a genuinely full old row), Postgres itself
// sends DataTypeToast for an unchanged TOASTed column in the OLD tuple too
// (not DataTypeNull), so the pre-existing backfill copying old-into-new is
// a same-marker no-op and the column correctly still ends up flagged
// toasted -- proving the WS7-1 K-tuple guard (`oldCol.DataType !=
// tuple.DataTypeNull`) does not accidentally suppress the FULL-identity
// path, which the plan documents as already safe.
func TestNewUpdate_FullOldTuple_ToastedColumnBackfillIsANoOp(t *testing.T) {
	oldCols := []struct {
		dataType byte
		text     string
	}{updTextCol("2"), updToastCol(), updTextCol("orig")} // full old row; wide already unchanged-TOAST
	newCols := []struct {
		dataType byte
		text     string
	}{updTextCol("2"), updToastCol(), updTextCol("changed2")} // wide still unchanged-TOAST

	wire := buildUpdateWireWithOldKey(100, UpdateTupleTypeOld, oldCols, newCols)

	upd, err := NewUpdate(wire, false, testRelation(100, "id", "wide", "name"), time.Now())
	if err != nil {
		t.Fatalf("NewUpdate: %v", err)
	}
	if upd.OldTupleType != UpdateTupleTypeOld {
		t.Fatalf("OldTupleType = %q; want 'O'", upd.OldTupleType)
	}

	if _, present := upd.NewDecoded["wide"]; present {
		t.Fatalf("NewDecoded[wide] must be absent (TOASTed, unchanged)")
	}
	if len(upd.NewToastedColumns) != 1 || upd.NewToastedColumns[0] != "wide" {
		t.Fatalf("NewToastedColumns = %v; want exactly [\"wide\"] under REPLICA IDENTITY FULL too", upd.NewToastedColumns)
	}
}

// TestNewUpdate_NoToastedColumns is the configuration that does NOT
// exercise TOAST at all: an ordinary update where every column changed.
// NewToastedColumns must stay empty/nil so a normal update's Data map is
// completely unaffected by this feature.
func TestNewUpdate_NoToastedColumns(t *testing.T) {
	wire := buildUpdateWire(100, []struct {
		dataType byte
		text     string
	}{updTextCol("alice"), updTextCol("a short bio")})

	upd, err := NewUpdate(wire, false, testRelation(100, "name", "bio"), time.Now())
	if err != nil {
		t.Fatalf("NewUpdate: %v", err)
	}

	if len(upd.NewToastedColumns) != 0 {
		t.Fatalf("NewToastedColumns = %v; want empty, nothing in this update is TOASTed", upd.NewToastedColumns)
	}
	if got := upd.NewDecoded["bio"]; got != "a short bio" {
		t.Fatalf("NewDecoded[bio] = %v; want \"a short bio\"", got)
	}
}

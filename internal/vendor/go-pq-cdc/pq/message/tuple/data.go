package tuple

import (
	"encoding/binary"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	DataTypeNull   = uint8('n')
	DataTypeToast  = uint8('u')
	DataTypeText   = uint8('t')
	DataTypeBinary = uint8('b')
)

var typeMap = pgtype.NewMap()

type Data struct {
	Columns      DataColumns
	SkipByte     int
	ColumnNumber uint16
}

type DataColumns []*DataColumn

type DataColumn struct {
	Data     []byte
	Length   uint32
	DataType uint8
}

type RelationColumn struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Flags        uint8
}

func NewData(data []byte, tupleDataType uint8, skipByteLength int) (*Data, error) {
	if data[skipByteLength] != tupleDataType {
		return nil, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	d := &Data{}
	d.Decode(data, skipByteLength)

	return d, nil
}

func (d *Data) Decode(data []byte, skipByteLength int) {
	d.ColumnNumber = binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	for range d.ColumnNumber {
		col := new(DataColumn)
		col.DataType = data[skipByteLength]
		skipByteLength++

		switch col.DataType {
		case DataTypeNull, DataTypeToast:
		case DataTypeText, DataTypeBinary:
			col.Length = binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			col.Data = make([]byte, int(col.Length))
			copy(col.Data, data[skipByteLength:])

			skipByteLength += int(col.Length)
		}

		d.Columns = append(d.Columns, col)
	}
	d.SkipByte = skipByteLength
}

// vendored-patch: WS7-1 - DecodeWithColumn's signature grew a third return
// value (toasted []string) so a caller can learn which columns were
// DataTypeToast, not just decode the rest. Upstream's DecodeWithColumn
// returns only (map[string]any, error). If upstream has since added its own
// handling for DataTypeToast (e.g. surfacing it a different way, or the
// column-list shape has otherwise changed), reconcile with that instead of
// blindly re-adding this parameter -- but do not drop the underlying
// capability: something upstream, or this patch, must still tell a caller
// "this column's absence means unchanged-TOAST, not NULL." See the
// case DataTypeToast marker below and PATCHES.md's WS7-1 entry for why.
//
// DecodeWithColumn decodes d's columns into a name-keyed map. A column
// carrying DataTypeToast (WS-7: an unchanged TOASTed value Postgres elided
// from the WAL tuple under REPLICA IDENTITY DEFAULT) intentionally gets NO
// entry in decoded -- writing a decoded[colName] = nil there would be
// indistinguishable from a genuine NULL, which is exactly the hazard this
// method must not reproduce. Instead its name is appended to the returned
// toasted slice, so callers can surface it as a distinct signal
// (protocol.ColumnKindToastedUnchanged) rather than silently letting it
// vanish. toasted is nil when no column in this call was toasted-and-
// unchanged, matching the pre-existing zero-value shape callers already
// expect when nothing needs the extra signal.
func (d *Data) DecodeWithColumn(columns []RelationColumn) (decoded map[string]any, toasted []string, err error) {
	decoded = make(map[string]any, d.ColumnNumber)
	for idx, col := range d.Columns {
		colName := columns[idx].Name
		switch col.DataType {
		case DataTypeNull:
			decoded[colName] = nil
		case DataTypeText:
			val, decErr := decodeTextColumnData(col.Data, columns[idx].DataType)
			if decErr != nil {
				return nil, nil, errors.Wrap(decErr, "decode column")
			}
			decoded[colName] = val
		// vendored-patch: WS7-1 - upstream's switch has no case here at all,
		// so a TOASTed-and-unchanged column silently gets no entry in
		// decoded, indistinguishable from DataTypeNull's explicit nil above.
		// This one line is the entire fix: record the column name instead of
		// dropping it. It will compile away silently if ever deleted (the
		// switch is still valid with the case missing) -- if a re-sync merge
		// drops just this arm while keeping the three-value return signature,
		// nothing fails to compile and no test that doesn't specifically
		// construct a TOAST-marker wire message will catch it. See
		// PATCHES.md's WS7-1 Re-sync Risk Callout before removing or
		// "simplifying" this case.
		case DataTypeToast:
			toasted = append(toasted, colName)
		}
	}

	return decoded, toasted, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

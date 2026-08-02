package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/go-playground/errors"
)

const (
	UpdateTupleTypeKey = 'K'
	UpdateTupleTypeOld = 'O'
	UpdateTupleTypeNew = 'N'
)

type Update struct {
	MessageTime time.Time
	NewTupleData *tuple.Data
	NewDecoded   map[string]any
	// NewToastedColumns lists the columns from NewDecoded's relation that
	// Postgres omitted from the WAL's new tuple because their value is an
	// unchanged TOASTed value (WS-7). Non-empty in two cases: (1)
	// OldTupleData is nil -- REPLICA IDENTITY DEFAULT, replica-identity key
	// unchanged -- nothing backfills the marker at all; and (2)
	// OldTupleData is present as a 'K' (key-only) tuple -- REPLICA IDENTITY
	// DEFAULT, key CHANGED -- decode()'s backfill loop below deliberately
	// does NOT overwrite the marker for a non-key column in this case
	// (verified live against Postgres 15's pgoutput wire format: a 'K'
	// tuple's non-key columns are tagged DataTypeNull, not their real
	// value, so blindly copying them would fabricate a NULL over a
	// genuine unchanged-TOAST column -- see the vendored-patch: WS7-1
	// comment in decode() for the full reasoning). Only a full REPLICA
	// IDENTITY FULL 'O' old tuple safely backfills every marker (PG sends
	// 'u' there too for an unchanged TOASTed column, so the copy is a
	// same-marker no-op), which is why NewToastedColumns stays empty in
	// that case specifically. These names are guaranteed absent from
	// NewDecoded; callers must not treat that absence as NULL.
	NewToastedColumns []string
	OldTupleData      *tuple.Data
	OldDecoded        map[string]any
	TableNamespace    string
	TableName         string
	OID               uint32
	XID               uint32
	OldTupleType      uint8
}

func NewUpdate(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Update, error) {
	msg := &Update{
		MessageTime: serverTime,
	}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, errors.New("relation not found")
	}

	msg.TableNamespace = rel.Namespace
	msg.TableName = rel.Name

	var err error

	if msg.OldTupleData != nil {
		// The old tuple is either a full REPLICA IDENTITY FULL row or a
		// key-only tuple; neither carries a meaningful unchanged-TOAST
		// marker for our purposes, so its toasted return is discarded.
		msg.OldDecoded, _, err = msg.OldTupleData.DecodeWithColumn(rel.Columns)
		if err != nil {
			return nil, err
		}
	}

	msg.NewDecoded, msg.NewToastedColumns, err = msg.NewTupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Update) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return errors.Newf("streamed transaction update message length must be at least 11 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return errors.Newf("update message length must be at least 7 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	var err error

	switch m.OldTupleType {
	case UpdateTupleTypeKey, UpdateTupleTypeOld:
		m.OldTupleData, err = tuple.NewData(data, m.OldTupleType, skipByte)
		if err != nil {
			return errors.Wrap(err, "update message old tuple data")
		}
		skipByte = m.OldTupleData.SkipByte
		fallthrough
	case UpdateTupleTypeNew:
		m.NewTupleData, err = tuple.NewData(data, UpdateTupleTypeNew, skipByte)
		if err != nil {
			return errors.Wrap(err, "update message new tuple data")
		}

		if m.OldTupleData != nil {
			for i, col := range m.NewTupleData.Columns {
				// because toasted columns not sent until the toasted column updated
				if col.DataType == tuple.DataTypeToast {
					// vendored-patch: WS7-1 - a 'K' (key-only) old tuple
					// carries real values ONLY for the replica-identity key
					// columns; every other column comes through tagged
					// DataTypeNull, meaning "not part of the key, no data
					// here" -- NOT "this column is really NULL". Verified
					// live against Postgres 15's pgoutput wire format
					// (UPDATE that changes the PK under REPLICA IDENTITY
					// DEFAULT with an untouched TOASTed column): the 'K'
					// tuple's non-key columns are tagged 'n', and the 'N'
					// tuple's untouched wide column is correctly tagged 'u'.
					// Unconditionally copying m.OldTupleData.Columns[i] here
					// (upstream's behaviour) overwrites that 'u' marker with
					// the old tuple's 'n', silently turning an
					// unchanged-TOAST column into a fabricated NULL -- the
					// exact WS-7 bug, reintroduced one layer above
					// DecodeWithColumn's own fix, for every UPDATE that
					// changes the replica-identity key. A REPLICA IDENTITY
					// FULL 'O' old tuple does not have this problem: PG
					// sends 'u' there too for an unchanged TOASTed column
					// (verified: the old tuple mirrors the same
					// toast-elision, so backfilling 'u' with 'u' is a no-op
					// that keeps the marker), so only the 'K' case needs
					// this guard.
					oldCol := m.OldTupleData.Columns[i]
					if m.OldTupleType == UpdateTupleTypeOld || oldCol.DataType != tuple.DataTypeNull {
						m.NewTupleData.Columns[i] = oldCol
					}
					// else: leave the new tuple's DataTypeToast marker
					// alone so DecodeWithColumn's WS7-1 toasted-column
					// reporting still fires for it.
				}
			}
		}
	default:
		return errors.New("update message undefined tuple type")
	}

	return nil
}

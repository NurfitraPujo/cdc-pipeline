package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/go-playground/errors"
)

type Delete struct {
	MessageTime    time.Time
	OldTupleData   *tuple.Data
	OldDecoded     map[string]any
	TableNamespace string
	TableName      string
	OID            uint32
	XID            uint32
	OldTupleType   uint8
}

func NewDelete(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Delete, error) {
	msg := &Delete{
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

	// A DELETE's old tuple never carries an unchanged-TOAST marker in the
	// sense WS-7 cares about (it's either the full old row under REPLICA
	// IDENTITY FULL, or just the key columns -- non-key columns are absent
	// from d.Columns entirely under DEFAULT, not present-as-toast), so the
	// toasted return is discarded here.
	msg.OldDecoded, _, err = msg.OldTupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Delete) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return errors.Newf("streamed transaction delete message length must be at least 11 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return errors.Newf("delete message length must be at least 7 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	var err error

	m.OldTupleData, err = tuple.NewData(data, m.OldTupleType, skipByte)
	if err != nil {
		return errors.Wrap(err, "delete message old tuple data")
	}

	return nil
}

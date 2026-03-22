package protocol

import "time"

//go:generate msgp

type SchemaMetadata struct {
	Table     string            `msg:"tbl" json:"table"`
	Schema    string            `msg:"sch" json:"schema"`
	Columns   map[string]string `msg:"cols" json:"columns"` // Name -> Type
	PKColumns []string          `msg:"pks" json:"pk_columns"`
}

type Message struct {
	SourceID  string          `msg:"sid"`
	Table     string          `msg:"tbl"`
	Op        string          `msg:"op"` // "insert", "update", "delete", "snapshot", "schema_change"
	LSN       uint64          `msg:"lsn"`
	PK        string          `msg:"pk"`
	Payload   []byte          `msg:"pay"`
	Timestamp time.Time       `msg:"ts"`
	Schema    *SchemaMetadata `msg:"meta,omitempty" json:"schema,omitempty"`
}

type MessageBatch []Message

func UnmarshalMessageBatch(b []byte, batch *[]Message) ([]byte, error) {
	var m MessageBatch
	rest, err := m.UnmarshalMsg(b)
	if err != nil {
		return nil, err
	}
	*batch = []Message(m)
	return rest, nil
}

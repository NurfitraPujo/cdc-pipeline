package protocol

import "time"

//go:generate msgp

type SchemaMetadata struct {
	Table     string            `msg:"tbl" json:"table"`
	Schema    string            `msg:"sch" json:"schema"`
	Columns   map[string]string `msg:"cols" json:"columns"` // Name -> Type
	PKColumns []string          `msg:"pks" json:"pk_columns"`
}

type TypeChange struct {
	OldType    string `msg:"old" json:"old_type"`
	NewType    string `msg:"new" json:"new_type"`
	ChangeType string `msg:"type" json:"change_type"`
}

type SchemaDiff struct {
	Table         string                `msg:"tbl" json:"table"`
	Timestamp     time.Time             `msg:"ts" json:"timestamp"`
	Source        string                `msg:"src" json:"source"`
	Added         map[string]string     `msg:"add" json:"added"`
	Removed       []string              `msg:"rem" json:"removed"`
	TypeChanges   map[string]TypeChange `msg:"type" json:"type_changes"`
	CorrelationID string                `msg:"c_id" json:"correlation_id"`
}

type Message struct {
	SourceID      string                 `msg:"sid"`
	Table         string                 `msg:"tbl"`
	Op            string                 `msg:"op"` // "insert", "update", "delete", "snapshot", "schema_change"
	LSN           uint64                 `msg:"lsn"`
	PK            string                 `msg:"pk"`
	UUID          string                 `msg:"uuid"`
	Data          map[string]interface{} `msg:"data,omitempty"`
	Payload       []byte                 `msg:"pay"`
	Timestamp     time.Time              `msg:"ts"`
	Schema        *SchemaMetadata        `msg:"meta,omitempty" json:"schema,omitempty"`
	CorrelationID string                 `msg:"c_id,omitempty" json:"correlation_id,omitempty"`
	Diff          *SchemaDiff            `msg:"diff,omitempty" json:"diff,omitempty"`
}

const (
	OpSchemaChange    = "schema_change"
	OpSchemaChangeAck = "schema_change_ack"
)

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

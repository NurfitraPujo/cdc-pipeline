package protocol

import "time"

//go:generate msgp

type OperationType string

const (
	OpInsert          OperationType = "insert"
	OpUpdate          OperationType = "update"
	OpDelete          OperationType = "delete"
	OpSnapshot        OperationType = "snapshot"
	OpSchemaChange    OperationType = "schema_change"
	OpSchemaChangeAck OperationType = "schema_change_ack"
	OpRecordAck       OperationType = "record_ack" // replaces the bare "ack" string literal
	OpDrainMarker     OperationType = "drain_marker"
)

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
	TableSchema   string                `msg:"tsch,omitempty" json:"table_schema,omitempty"`
	Timestamp     time.Time             `msg:"ts" json:"timestamp"`
	Source        string                `msg:"src" json:"source"`
	Added         map[string]string     `msg:"add" json:"added"`
	Removed       []string              `msg:"rem" json:"removed"`
	TypeChanges   map[string]TypeChange `msg:"type" json:"type_changes"`
	CorrelationID string                `msg:"c_id" json:"correlation_id"`
}

// RecordAck is published by a consumer on AcksTopic after a durable sink write.
// One message per successful flush; LSNs lists every LSN in the flushed batch.
type RecordAck struct {
	PipelineID string    `msg:"pid"`
	SourceID   string    `msg:"sid"`
	SinkID     string    `msg:"snk"`
	LSNs       []uint64  `msg:"lsns"`
	Timestamp  time.Time `msg:"ts"`
}

type Message struct {
	SourceID      string                 `msg:"sid"`
	SinkID        string                 `msg:"snk,omitempty" json:"sink_id,omitempty"`
	Table         string                 `msg:"tbl"`            // bare table name -- never qualified, see MULTI_SCHEMA_PLAN.md §2.2
	TableSchema   string                 `msg:"tsch,omitempty" json:"table_schema,omitempty"` // sibling schema; "" decodes to "public" via NormalizeSchema
	Op            OperationType         `msg:"op"` // "insert", "update", "delete", "snapshot", "schema_change"
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

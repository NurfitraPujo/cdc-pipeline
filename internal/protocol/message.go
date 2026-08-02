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
	SourceID    string        `msg:"sid"`
	SinkID      string        `msg:"snk,omitempty" json:"sink_id,omitempty"`
	Table       string        `msg:"tbl"`                                           // bare table name -- never qualified, see MULTI_SCHEMA_PLAN.md §2.2
	TableSchema string        `msg:"tsch,omitempty" json:"table_schema,omitempty"` // sibling schema; "" decodes to "public" via NormalizeSchema
	Op          OperationType `msg:"op"`                                            // "insert", "update", "delete", "snapshot", "schema_change"
	LSN         uint64        `msg:"lsn"`
	PK          string        `msg:"pk"`
	UUID        string        `msg:"uuid"`
	Data        map[string]interface{} `msg:"data,omitempty"`
	// ColumnKinds carries a source-value-kind hint for entries in Data whose
	// Go type cannot itself cross the msgpack transport (see MarshalMsg's
	// generated WriteIntf, which only supports Ptr/Slice/Map -- a struct
	// like pgtype.Numeric is msgp.ErrUnsupportedType). Data therefore always
	// carries the plain, sink-safe Go value (e.g. the exact decimal text as
	// a string, unchanged from what every existing sink already reads
	// today); ColumnKinds is a side-channel a kind-aware *consumer* (the
	// nats/protobuf transformer's encodeTypedValue) may consult to recover
	// routing information Data's plain value alone cannot carry (e.g.
	// "route this string to TypedValue.decimal_value, not string_value").
	// Consumers that don't know about ColumnKinds -- both sinks, any future
	// processor -- see exactly the same Data they see today: zero format
	// change, zero regression risk. Values are currently only "decimal";
	// treat unrecognised values as informational and ignorable, not an
	// error, so a newer producer talking to an older consumer degrades to
	// "no kind hint" rather than breaking it.
	ColumnKinds   map[string]string `msg:"ckinds,omitempty" json:"column_kinds,omitempty"`
	Payload       []byte            `msg:"pay"`
	Timestamp     time.Time         `msg:"ts"`
	Schema        *SchemaMetadata   `msg:"meta,omitempty" json:"schema,omitempty"`
	CorrelationID string            `msg:"c_id,omitempty" json:"correlation_id,omitempty"`
	Diff          *SchemaDiff       `msg:"diff,omitempty" json:"diff,omitempty"`
}

// ColumnKindDecimal is the ColumnKinds value meaning: the corresponding
// Data[col] entry is exact decimal text (from a source pgtype.Numeric,
// never a float) and a kind-aware encoder should route it to
// TypedValue.decimal_value rather than TypedValue.string_value.
const ColumnKindDecimal = "decimal"

// ColumnKindToastedUnchanged is the ColumnKinds value meaning: the
// corresponding column has NO entry in Data -- not because its value is
// NULL (a real NULL always gets an explicit Data[col] = nil entry, see
// tuple.Data.DecodeWithColumn's DataTypeNull case), but because Postgres
// logical decoding omitted it from the WAL tuple entirely. This happens
// under REPLICA IDENTITY DEFAULT when a TOASTed (out-of-line, >~2KB)
// column's value did not change in this UPDATE: Postgres sends a TOAST
// pointer marker instead of the value, and there is no old-tuple copy to
// recover it from (WS-7 / the "TOAST hazard").
//
// A consumer that only looks at Data's keys cannot tell "omitted because
// unchanged" apart from "omitted because this producer never sends the
// column" -- both look identical. ColumnKindToastedUnchanged disambiguates
// this: any column named here is guaranteed to be a genuine, currently
// existing value that this message simply does not carry, and a consumer
// doing a wholesale row replace (e.g. this pipeline's Databend sink's
// REPLACE INTO, or daya-core's custom_object upsert) MUST NOT write NULL
// or any default over that column -- it must either omit the column from
// the write entirely (a true partial UPDATE) or fetch-and-carry-forward
// the column's current value before writing. Only ever set on OpUpdate
// messages; INSERT and SNAPSHOT rows always carry every column's actual
// value (there being no unchanged prior value for Postgres to elide).
const ColumnKindToastedUnchanged = "toasted_unchanged"

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

package protocol

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var reID = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

//go:generate msgp

// NATS KV Constants
const (
	KVBucketName = "cdc-dp-config"

	// Configuration Keys
	KeyGlobalConfig      = "cdc.config.global"
	KeyAuthConfig        = "cdc.config.auth"
	PrefixPipelineConfig = "cdc.config.pipelines."
	PrefixSourceConfig   = "cdc.config.sources."
	PrefixSinkConfig     = "cdc.config.sinks."

	// Operational/State Keys
	PrefixPipelineState = "cdc.pipeline."
	PrefixWorkerState   = "cdc.worker."

	// Summary and Cache Keys
	KeyGlobalSummary     = "cdc.stats.global_summary"
	PrefixDiscoveryCache = "cdc.discovery."

	// KeyManagerSweepLease is the single, bucket-wide leader-election key
	// StartLeaseLoop (internal/config/lease.go) uses to elect the one
	// ConfigManager replica (of the 3-20 production pods,
	// deploy/helm-chart/values.production.yml minReplicas/maxReplicas)
	// allowed to run the pause-expiry ticker's sweep body (WS-3/WS-4/WS-6/
	// WS-7). Deliberately outside PrefixPipelineState: it is not
	// per-pipeline state and must never collide with
	// pipelineIDFromLifecycleKey's ".lifecycle"-suffix scan over
	// PrefixPipelineState keys (internal/config/pause_expiry.go).
	KeyManagerSweepLease = "cdc.control.manager_sweep_lease"
)

// Helper functions for key construction
func DiscoveryCacheKey(sourceID string) string {
	return PrefixDiscoveryCache + sourceID
}

func TransitionStateKey(id string) string {
	return fmt.Sprintf("%s%s.transition", PrefixPipelineState, id)
}
func PipelineConfigKey(id string) string {
	return PrefixPipelineConfig + id
}

func SourceConfigKey(id string) string {
	return PrefixSourceConfig + id
}

func SinkConfigKey(id string) string {
	return PrefixSinkConfig + id
}

func WorkerHeartbeatKey(id string) string {
	return fmt.Sprintf("%s%s.heartbeat", PrefixWorkerState, id)
}

func TableStatsKey(pid, sid, sinkID string, table TableRef) string {
	return fmt.Sprintf("%s%s.sources.%s.sinks.%s.tables.%s.stats", PrefixPipelineState, pid, sid, sinkID, table.KeyToken())
}

type TableStatsKeyInfo struct {
	PipelineID string
	SourceID   string
	SinkID     string
	Table      string // the raw KeyToken(), e.g. "orders" or "sales=orders"
}

// ParseTableStatsKey parses a key built by TableStatsKey. It parses from
// BOTH ENDS -- fixed prefix tokens 0..7, terminal "stats" -- rather than
// asserting a fixed length and a positional "stats" token (the previous
// version silently returned nil the moment the table token contained an
// extra "."; see MULTI_SCHEMA_PLAN.md §2.3). KeyToken() never emits a "."
// today, but this stays correct even if that ever changes.
func ParseTableStatsKey(key string) *TableStatsKeyInfo {
	// Format: cdc.pipeline.{pid}.sources.{sid}.sinks.{sinkID}.tables.{token}.stats
	parts := strings.Split(key, ".")
	if len(parts) < 10 {
		return nil
	}
	if parts[0] != "cdc" || parts[1] != "pipeline" || parts[3] != "sources" || parts[5] != "sinks" || parts[7] != "tables" {
		return nil
	}
	if parts[len(parts)-1] != "stats" {
		return nil
	}
	return &TableStatsKeyInfo{
		PipelineID: parts[2],
		SourceID:   parts[4],
		SinkID:     parts[6],
		Table:      strings.Join(parts[8:len(parts)-1], "."),
	}
}

func ProducerTableStatsKey(pid, sid string, table TableRef) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.stats", PrefixPipelineState, pid, sid, table.KeyToken())
}

func IngressCheckpointKey(pid, sid string, table TableRef) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.ingress_checkpoint", PrefixPipelineState, pid, sid, table.KeyToken())
}

func EgressCheckpointKey(pid, sid, sinkID string, table TableRef) string {
	return fmt.Sprintf("%s%s.sources.%s.sinks.%s.tables.%s.egress_checkpoint", PrefixPipelineState, pid, sid, sinkID, table.KeyToken())
}

// TableMetadataKey addresses the per-table schema metadata blob written on
// dynamic-table discovery. Previously an inline fmt.Sprintf with no builder
// at all (MULTI_SCHEMA_PLAN.md §3 Stage 1).
func TableMetadataKey(pid, sid string, table TableRef) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.metadata", PrefixPipelineState, pid, sid, table.KeyToken())
}

// SourceWatermarkKey addresses a per-source, per-pipeline observability
// record of the AckManager watermark (see WI-7 §3 "persistWatermark").
// Unlike IngressCheckpointKey, this is not consulted on resume -- the
// replication slot's own confirmed_flush_lsn is the resume authority. It
// exists purely so dashboards/operators can see current watermark
// progress without querying PostgreSQL directly.
func SourceWatermarkKey(pid, sid string) string {
	return fmt.Sprintf("%s%s.sources.%s.watermark", PrefixPipelineState, pid, sid)
}

func DLQTopic(pid string) string {
	return fmt.Sprintf("cdc_pipeline_%s_dlq", pid)
}

func AcksTopic(pid string) string {
	return fmt.Sprintf("cdc_pipeline_%s_acks", pid)
}

func PipelineStatusPrefix(pid string) string {
	return fmt.Sprintf("%s%s.sources.", PrefixPipelineState, pid)
}

type UserConfig struct {
	Username string `msg:"username" yaml:"username" json:"username"`
	Password string `msg:"password" yaml:"password" json:"password"`
}

func (u UserConfig) Validate() error {
	return validation.ValidateStruct(&u,
		validation.Field(&u.Username, validation.Required, validation.Length(3, 50)),
		validation.Field(&u.Password, validation.Required, validation.Length(5, 100)),
	)
}

type GlobalConfig struct {
	BatchSize int           `msg:"batch_size" yaml:"batch_size" json:"batch_size"`
	BatchWait time.Duration `msg:"batch_wait" yaml:"batch_wait" json:"batch_wait" swaggertype:"string" example:"5s"`
	Retry     RetryConfig   `msg:"retry" yaml:"retry" json:"retry"`

	// Timeout configurations
	DrainTimeout       time.Duration `msg:"drain_timeout" yaml:"drain_timeout" json:"drain_timeout" swaggertype:"string" example:"30s"`
	ShutdownTimeout    time.Duration `msg:"shutdown_timeout" yaml:"shutdown_timeout" json:"shutdown_timeout" swaggertype:"string" example:"30s"`
	StabilizationDelay time.Duration `msg:"stabilization_delay" yaml:"stabilization_delay" json:"stabilization_delay" swaggertype:"string" example:"2s"`
	CrashRecoveryDelay time.Duration `msg:"crash_recovery_delay" yaml:"crash_recovery_delay" json:"crash_recovery_delay" swaggertype:"string" example:"5s"`
	GlobalReloadDelay  time.Duration `msg:"global_reload_delay" yaml:"global_reload_delay" json:"global_reload_delay" swaggertype:"string" example:"2s"`
}

func (g GlobalConfig) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.BatchSize, validation.Required, validation.Min(1)),
		validation.Field(&g.BatchWait, validation.Required, validation.Min(time.Millisecond*100)),
		validation.Field(&g.Retry),
		validation.Field(&g.DrainTimeout, validation.Min(time.Second)),
		validation.Field(&g.ShutdownTimeout, validation.Min(time.Second)),
		validation.Field(&g.StabilizationDelay, validation.Min(0)),
		validation.Field(&g.CrashRecoveryDelay, validation.Min(0)),
		validation.Field(&g.GlobalReloadDelay, validation.Min(0)),
	)
}

// SetDefaults sets default values for timeout fields if not set
func (g *GlobalConfig) SetDefaults() {
	if g.DrainTimeout == 0 {
		g.DrainTimeout = 30 * time.Second
	}
	if g.ShutdownTimeout == 0 {
		g.ShutdownTimeout = 30 * time.Second
	}
	if g.StabilizationDelay == 0 {
		g.StabilizationDelay = 2 * time.Second
	}
	if g.CrashRecoveryDelay == 0 {
		g.CrashRecoveryDelay = 5 * time.Second
	}
	if g.GlobalReloadDelay == 0 {
		g.GlobalReloadDelay = 2 * time.Second
	}
}

type RetryConfig struct {
	MaxRetries      int           `msg:"max_retries" yaml:"max_retries" json:"max_retries"`
	InitialInterval time.Duration `msg:"init_interval" yaml:"initial_interval" json:"initial_interval" swaggertype:"string" example:"1s"`
	MaxInterval     time.Duration `msg:"max_interval" yaml:"max_interval" json:"max_interval" swaggertype:"string" example:"30s"`
	EnableDLQ       bool          `msg:"enable_dlq" yaml:"enable_dlq" json:"enable_dlq"`
}

func (r RetryConfig) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.MaxRetries, validation.Min(0)),
		validation.Field(&r.InitialInterval, validation.Min(time.Millisecond*100)),
		validation.Field(&r.MaxInterval, validation.Min(time.Millisecond*100)),
	)
}

type ProcessorConfig struct {
	Name           string                 `msg:"name" yaml:"name" json:"name"`
	Type           string                 `msg:"type" yaml:"type" json:"type"` // e.g., "mask", "filter", "custom"
	Options        map[string]interface{} `msg:"options" yaml:"options" json:"options"`
	OperationTypes []OperationType        `msg:"operation_types" yaml:"operation_types" json:"operation_types"`
}

func (p ProcessorConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.Type, validation.Required),
		// A processor with no operation types matches nothing and is
		// silently skipped in its entirety by Consumer.processMessages
		// (engine/consumer.go), with no warning and no match-all default --
		// the pipeline then reports "Running" while transforming nothing.
		// Require it explicitly at config load instead (WS-8 item 1).
		//
		// Each entry must also be a known OperationType. Requiring only a
		// non-empty list left the exact hole the length check was meant to
		// close: a typo ("insrt") passed validation, matched no message, and
		// produced a pipeline that reported healthy while silently
		// transforming nothing.
		validation.Field(&p.OperationTypes,
			validation.Required,
			validation.Length(1, 0),
			validation.Each(validation.By(validateOperationType)),
		),
	)
}

// DesiredState is operator intent for a pipeline, distinct from lifecycle
// state (what the system is actually doing, internal/protocol/lifecycle.go
// State) and health (derived from heartbeat). See
// plans/2026-08-03-pipeline-lifecycle-control.md section 4.1.
//
// Deliberately small: it records intent, not mechanism. The full lifecycle
// machinery that turns "paused" into a drained worker with a retained slot,
// or "stopped" into a dropped slot, arrives in later workstreams (WS-2+);
// WS-1 only wires the field through config, validation and ConfigManager's
// decision to run a worker at all.
type DesiredState string

// DesiredState values: the operator intents desired_state may hold.
const (
	DesiredStateRunning DesiredState = "running"
	DesiredStatePaused  DesiredState = "paused"
	DesiredStateStopped DesiredState = "stopped"
)

// validDesiredStates is the closed set desired_state may hold. Empty is also
// accepted by Validate -- see EffectiveDesiredState -- so that every
// PipelineConfig written before this field existed keeps loading and keeps
// meaning "running".
var validDesiredStates = map[DesiredState]bool{
	DesiredStateRunning: true,
	DesiredStatePaused:  true,
	DesiredStateStopped: true,
}

func validateDesiredState(value interface{}) error {
	ds, _ := value.(DesiredState)
	if ds == "" {
		return nil
	}
	if !validDesiredStates[ds] {
		return fmt.Errorf("unknown desired_state %q", string(ds))
	}
	return nil
}

type PipelineConfig struct {
	ID         string            `msg:"id" yaml:"id" json:"id"`
	Name       string            `msg:"name" yaml:"name" json:"name"`
	Sources    []string          `msg:"sources" yaml:"sources" json:"sources"`
	Sinks      []string          `msg:"sinks" yaml:"sinks" json:"sinks"`
	Processors []ProcessorConfig `msg:"processors" yaml:"processors" json:"processors"`
	Tables     []string          `msg:"tables" yaml:"tables" json:"tables"`
	BatchSize  int               `msg:"batch_size" yaml:"batch_size" json:"batch_size"`                                    // Override
	BatchWait  time.Duration     `msg:"batch_wait" yaml:"batch_wait" json:"batch_wait" swaggertype:"string" example:"10s"` // Override
	Retry      *RetryConfig      `msg:"retry" yaml:"retry" json:"retry"`
	// DesiredState records operator intent -- running, paused or stopped.
	// Empty means "running" (see EffectiveDesiredState), so configs written
	// before this field existed round-trip unchanged.
	DesiredState DesiredState `msg:"desired_state" yaml:"desired_state" json:"desired_state"`
}

// EffectiveDesiredState returns the operator's intent for this pipeline,
// treating an empty (pre-WS-1) value as DesiredStateRunning so old configs
// keep their prior behaviour of "every configured pipeline runs".
func (p PipelineConfig) EffectiveDesiredState() DesiredState {
	if p.DesiredState == "" {
		return DesiredStateRunning
	}
	return p.DesiredState
}

func (p PipelineConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.ID, validation.Required, validation.Match(reID)),
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.Sources, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Sinks, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Tables, validation.Each(validation.By(validateTableIdentifier))),
		validation.Field(&p.Retry),
		// ozzo-validation already descends into each slice element that
		// implements Validatable (ProcessorConfig.Validate below), so no
		// explicit validation.Each(validation.By(...)) is needed here
		// (WS-9 round-3 fix: the old validation.Each call ran
		// ProcessorConfig.Validate() a second time, producing duplicate
		// error keys).
		validation.Field(&p.Processors),
		validation.Field(&p.DesiredState, validation.By(validateDesiredState)),
	)
}

type SourceConfig struct {
	ID                string        `msg:"id" yaml:"id" json:"id"`
	Type              string        `msg:"type" yaml:"type" json:"type"` // e.g., "postgres"
	Host              string        `msg:"host" yaml:"host" json:"host"`
	Port              int           `msg:"port" yaml:"port" json:"port"`
	User              string        `msg:"user" yaml:"user" json:"user"`
	PassEncrypted     string        `msg:"pass" yaml:"pass" json:"pass"`
	Database          string        `msg:"database" yaml:"database" json:"database"`
	SlotName          string        `msg:"slot_name" yaml:"slot_name" json:"slot_name"`
	PublicationName   string        `msg:"publication_name" yaml:"publication_name" json:"publication_name"`
	BatchSize         int           `msg:"batch_size" yaml:"batch_size" json:"batch_size"`
	BatchWait         time.Duration `msg:"batch_wait" yaml:"batch_wait" json:"batch_wait" swaggertype:"string" example:"5s"`
	DiscoveryInterval time.Duration `msg:"disc_int" yaml:"discovery_interval" json:"discovery_interval" swaggertype:"string" example:"30s"`
	SnapshotChunkSize int           `msg:"snap_size" yaml:"snapshot_chunk_size" json:"snapshot_chunk_size"`
	SnapshotInterval  time.Duration `msg:"snap_int" yaml:"snapshot_interval" json:"snapshot_interval" swaggertype:"string" example:"1s"`
	// Schemas restricts table discovery to these PostgreSQL schemas.
	// Empty or nil means "public" ONLY -- deliberately not "all schemas":
	// every config predating multi-schema support has this field empty, and
	// defaulting to all would silently begin replicating unrelated schemas on
	// upgrade (MULTI_SCHEMA_PLAN.md §3 Stage 2, §8 item 4).
	Schemas []string `msg:"schemas" yaml:"schemas" json:"schemas"`
	Tables  []string `msg:"tables" yaml:"tables" json:"tables"`
}

func (s SourceConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.ID, validation.Required, validation.Match(reID)),
		validation.Field(&s.Type, validation.Required, validation.In("postgres")),
		validation.Field(&s.Host, validation.Required),
		validation.Field(&s.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&s.Database, validation.Required),
		validation.Field(&s.Schemas, validation.Each(validation.By(validateTableIdentifier))),
		validation.Field(&s.Tables, validation.Each(validation.By(validateTableIdentifier))),
	)
}

// validOperationTypes is the closed set a processor may subscribe to. Kept
// beside the OperationType constants in message.go -- a new operation must be
// added here too, or configs naming it are rejected.
var validOperationTypes = map[OperationType]bool{
	OpInsert:          true,
	OpUpdate:          true,
	OpDelete:          true,
	OpSnapshot:        true,
	OpSchemaChange:    true,
	OpSchemaChangeAck: true,
}

func validateOperationType(value interface{}) error {
	op, ok := value.(OperationType)
	if !ok {
		return fmt.Errorf("expected an operation type, got %T", value)
	}
	if !validOperationTypes[op] {
		return fmt.Errorf("unknown operation type %q", string(op))
	}
	return nil
}

// validateTableIdentifier rejects "=" (reserved for KeyToken's schema
// separator, see TableRef.KeyToken) and more than one "." (the schema.table
// separator) in a single Schemas/Tables entry, keeping KeyToken() injective.
// A bare or schema-qualified identifier ("orders", "sales.orders") is valid;
// ParseTableRef performs the equivalent check at the point of use.
func validateTableIdentifier(value interface{}) error {
	s, _ := value.(string)
	if _, err := ParseTableRef(s); err != nil {
		return err
	}
	return nil
}

type SinkConfig struct {
	ID            string                 `msg:"id" yaml:"id" json:"id"`
	Type          string                 `msg:"type" yaml:"type" json:"type"` // e.g., "databend", "postgres_debug"
	DSN           string                 `msg:"dsn" yaml:"dsn" json:"dsn"`    // Data Source Name
	MaxAckPending int                    `msg:"max_ack" yaml:"max_ack_pending" json:"max_ack_pending"`
	Options       map[string]interface{} `msg:"options" yaml:"options" json:"options"`
}

func (s SinkConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.ID, validation.Required, validation.Match(reID)),
		validation.Field(&s.Type, validation.Required, validation.In("databend", "postgres_debug")),
		validation.Field(&s.DSN, validation.Required),
	)
}

func (s *SourceConfig) Decrypt() error {
	if s.PassEncrypted == "" {
		return nil
	}
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		return fmt.Errorf("source %s: %w", s.ID, err)
	}
	decrypted, err := crypto.Decrypt(s.PassEncrypted, key)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.ID, err)
	}
	s.PassEncrypted = decrypted
	return nil
}

func (s *SinkConfig) Decrypt() error {
	if s.DSN == "" {
		return nil
	}
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		return fmt.Errorf("sink %s: %w", s.ID, err)
	}
	decrypted, err := crypto.Decrypt(s.DSN, key)
	if err != nil {
		return fmt.Errorf("sink %s: %w", s.ID, err)
	}
	s.DSN = decrypted
	return nil
}

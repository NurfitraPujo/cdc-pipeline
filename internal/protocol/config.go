package protocol

import (
	"fmt"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
)

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

func TableStatsKey(pid, sid, sinkID, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.sinks.%s.tables.%s.stats", PrefixPipelineState, pid, sid, sinkID, table)
}

type TableStatsKeyInfo struct {
	PipelineID string
	SourceID   string
	SinkID     string
	Table      string
}

func ParseTableStatsKey(key string) *TableStatsKeyInfo {
	// Format: cdc.pipeline.{pid}.sources.{sid}.sinks.{sinkID}.tables.{table}.stats
	parts := strings.Split(key, ".")
	if len(parts) < 10 || parts[0] != "cdc" || parts[1] != "pipeline" || parts[3] != "sources" || parts[5] != "sinks" || parts[7] != "tables" || parts[9] != "stats" {
		return nil
	}
	return &TableStatsKeyInfo{
		PipelineID: parts[2],
		SourceID:   parts[4],
		SinkID:     parts[6],
		Table:      parts[8],
	}
}

func ProducerTableStatsKey(pid, sid, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.stats", PrefixPipelineState, pid, sid, table)
}

func IngressCheckpointKey(pid, sid, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.ingress_checkpoint", PrefixPipelineState, pid, sid, table)
}

func EgressCheckpointKey(pid, sid, sinkID, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.sinks.%s.tables.%s.egress_checkpoint", PrefixPipelineState, pid, sid, sinkID, table)
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
	Name    string                 `msg:"name" yaml:"name" json:"name"`
	Type    string                 `msg:"type" yaml:"type" json:"type"` // e.g., "mask", "filter", "custom"
	Options map[string]interface{} `msg:"options" yaml:"options" json:"options"`
}

func (p ProcessorConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.Type, validation.Required),
	)
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
}

func (p PipelineConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.ID, validation.Required, is.Alphanumeric),
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.Sources, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Sinks, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Tables, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Retry),
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
	Schemas           []string      `msg:"schemas" yaml:"schemas" json:"schemas"`
	Tables            []string      `msg:"tables" yaml:"tables" json:"tables"`
}

func (s SourceConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.ID, validation.Required, is.Alphanumeric),
		validation.Field(&s.Type, validation.Required, validation.In("postgres")),
		validation.Field(&s.Host, validation.Required),
		validation.Field(&s.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&s.Database, validation.Required),
	)
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
		validation.Field(&s.ID, validation.Required, is.Alphanumeric),
		validation.Field(&s.Type, validation.Required, validation.In("databend", "postgres_debug")),
		validation.Field(&s.DSN, validation.Required),
	)
}

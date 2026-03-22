package protocol

import (
	"fmt"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
)

//go:generate msgp

// NATS KV Constants
const (
	KVBucketName = "daya-dp-config"

	// Configuration Keys
	KeyGlobalConfig      = "daya.config.global"
	KeyAuthConfig        = "daya.config.auth"
	PrefixPipelineConfig = "daya.config.pipelines."
	PrefixSourceConfig   = "daya.config.sources."
	PrefixSinkConfig     = "daya.config.sinks."

	// Operational/State Keys
	PrefixPipelineState = "daya.pipeline."
	PrefixWorkerState   = "daya.worker."
)

// Helper functions for key construction
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

func TableStatsKey(pid, sid, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.stats", PrefixPipelineState, pid, sid, table)
}

func IngressCheckpointKey(pid, sid, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.ingress_checkpoint", PrefixPipelineState, pid, sid, table)
}

func EgressCheckpointKey(pid, sid, table string) string {
	return fmt.Sprintf("%s%s.sources.%s.tables.%s.egress_checkpoint", PrefixPipelineState, pid, sid, table)
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
}

func (g GlobalConfig) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.BatchSize, validation.Required, validation.Min(1)),
		validation.Field(&g.BatchWait, validation.Required, validation.Min(time.Millisecond*100)),
	)
}

type PipelineConfig struct {
	ID        string        `msg:"id" yaml:"id" json:"id"`
	Name      string        `msg:"name" yaml:"name" json:"name"`
	Sources   []string      `msg:"sources" yaml:"sources" json:"sources"`
	Sinks     []string      `msg:"sinks" yaml:"sinks" json:"sinks"`
	Tables    []string      `msg:"tables" yaml:"tables" json:"tables"`
	BatchSize int           `msg:"batch_size" yaml:"batch_size" json:"batch_size"` // Override
	BatchWait time.Duration `msg:"batch_wait" yaml:"batch_wait" json:"batch_wait" swaggertype:"string" example:"10s"` // Override
}

func (p PipelineConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.ID, validation.Required, is.Alphanumeric),
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.Sources, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Sinks, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Tables, validation.Required, validation.Length(1, 0)),
	)
}

type SourceConfig struct {
	ID              string `msg:"id" yaml:"id" json:"id"`
	Type            string `msg:"type" yaml:"type" json:"type"` // e.g., "postgres"
	Host            string `msg:"host" yaml:"host" json:"host"`
	Port            int    `msg:"port" yaml:"port" json:"port"`
	User            string `msg:"user" yaml:"user" json:"user"`
	PassEncrypted   string `msg:"pass" yaml:"pass" json:"pass"`
	Database        string `msg:"database" yaml:"database" json:"database"`
	SlotName        string `msg:"slot_name" yaml:"slot_name" json:"slot_name"`
	PublicationName string `msg:"publication_name" yaml:"publication_name" json:"publication_name"`
	BatchSize       int           `msg:"batch_size" yaml:"batch_size" json:"batch_size"`
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
	ID   string `msg:"id" yaml:"id" json:"id"`
	Type string `msg:"type" yaml:"type" json:"type"` // e.g., "databend"
	DSN  string `msg:"dsn" yaml:"dsn" json:"dsn"`   // Data Source Name
}

func (s SinkConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.ID, validation.Required, is.Alphanumeric),
		validation.Field(&s.Type, validation.Required, validation.In("databend")),
		validation.Field(&s.DSN, validation.Required),
	)
}

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

	// Operational/State Keys
	PrefixPipelineState = "daya.pipeline."
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
	Username string `msg:"username" yaml:"username"`
	Password string `msg:"password" yaml:"password"`
}

func (u UserConfig) Validate() error {
	return validation.ValidateStruct(&u,
		validation.Field(&u.Username, validation.Required, validation.Length(3, 50)),
		validation.Field(&u.Password, validation.Required, validation.Length(5, 100)),
	)
}

type GlobalConfig struct {
	BatchSize int           `msg:"batch_size" yaml:"batch_size"`
	BatchWait time.Duration `msg:"batch_wait" yaml:"batch_wait"`
}

func (g GlobalConfig) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.BatchSize, validation.Required, validation.Min(1)),
		validation.Field(&g.BatchWait, validation.Required, validation.Min(time.Millisecond*100)),
	)
}

type PipelineConfig struct {
	ID        string        `msg:"id" yaml:"id"`
	Name      string        `msg:"name" yaml:"name"`
	Sources   []string      `msg:"sources" yaml:"sources"`
	Sinks     []string      `msg:"sinks" yaml:"sinks"`
	Tables    []string      `msg:"tables" yaml:"tables"`
	BatchSize int           `msg:"batch_size" yaml:"batch_size"` // Override
	BatchWait time.Duration `msg:"batch_wait" yaml:"batch_wait"` // Override
}

func (p PipelineConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.ID, validation.Required, is.Alphanumeric),
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.Sources, validation.Required, validation.Length(1, 0)),
		validation.Field(&p.Tables, validation.Required, validation.Length(1, 0)),
	)
}

type SourceConfig struct {
	ID              string `msg:"id" yaml:"id"`
	Type            string `msg:"type" yaml:"type"` // e.g., "postgres"
	Host            string `msg:"host" yaml:"host"`
	Port            int    `msg:"port" yaml:"port"`
	User            string `msg:"user" yaml:"user"`
	PassEncrypted   string `msg:"pass" yaml:"pass"`
	Database        string `msg:"database" yaml:"database"`
	SlotName        string `msg:"slot_name" yaml:"slot_name"`
	PublicationName string `msg:"publication_name" yaml:"publication_name"`
	BatchSize       int    `msg:"batch_size" yaml:"batch_size"`
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

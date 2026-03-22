package protocol

import (
	"fmt"
	"time"
)

//go:generate msgp

// NATS KV Constants
const (
	KVBucketName = "daya-dp-config"

	// Configuration Keys
	KeyGlobalConfig      = "daya.config.global"
	PrefixPipelineConfig = "daya.config.pipelines."
	PrefixSourceConfig   = "daya.config.sources."

	// Operational/State Keys
	PrefixPipelineState = "daya.pipeline."
)

// Helper functions for key construction
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

type GlobalConfig struct {
	BatchSize int           `msg:"batch_size"`
	BatchWait time.Duration `msg:"batch_wait"`
}

type PipelineConfig struct {
	ID        string        `msg:"id"`
	Name      string        `msg:"name"`
	Sources   []string      `msg:"sources"`
	Sinks     []string      `msg:"sinks"`
	Tables    []string      `msg:"tables"`
	BatchSize int           `msg:"batch_size"` // Override
	BatchWait time.Duration `msg:"batch_wait"` // Override
}

type SourceConfig struct {
	ID              string `msg:"id"`
	Type            string `msg:"type"` // e.g., "postgres"
	Host            string `msg:"host"`
	Port            int    `msg:"port"`
	User            string `msg:"user"`
	PassEncrypted   string `msg:"pass"`
	Database        string `msg:"database"`
	SlotName        string `msg:"slot_name"`
	PublicationName string `msg:"publication_name"`
	BatchSize       int    `msg:"batch_size"`
}

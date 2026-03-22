package protocol

import "time"

//go:generate msgp

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

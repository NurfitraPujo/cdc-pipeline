package protocol

//go:generate msgp

type PipelineConfig struct {
	ID      string   `msg:"id"`
	Name    string   `msg:"name"`
	Sources []string `msg:"sources"`
	Sinks   []string `msg:"sinks"`
	Tables  []string `msg:"tables"`
}

type SourceConfig struct {
	ID            string `msg:"id"`
	Type          string `msg:"type"` // e.g., "postgres"
	Host          string `msg:"host"`
	Port          int    `msg:"port"`
	User          string `msg:"user"`
	PassEncrypted string `msg:"pass"`
}

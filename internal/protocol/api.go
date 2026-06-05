package protocol

import "time"

// API-only response types. These are part of the HTTP API contract (mirrored
// in docs/openapi.yaml) but not persisted to NATS KV or used by the worker.

type ErrorResponse struct {
	Error     string    `json:"error"`
	StartedAt time.Time `json:"started_at,omitempty" swaggertype:"string" example:"2024-01-15T10:30:00Z"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

type AcceptedResponse struct {
	Status string `json:"status"`
}

type PipelineListResponse struct {
	Pipelines []PipelineConfig `json:"pipelines"`
	Total     int              `json:"total"`
	Page      int              `json:"page"`
	Limit     int              `json:"limit"`
}

type PipelineStatusResponse struct {
	PipelineID string                          `json:"pipeline_id"`
	Status     map[string]any                  `json:"status"`
	Tables     map[string]TableStats           `json:"tables"`
	Sinks      map[string]map[string]TableStats `json:"sinks"`
}

type SourceListResponse struct {
	Sources []SourceConfig `json:"sources"`
}

type SinkListResponse struct {
	Sinks []SinkConfig `json:"sinks"`
}

type SourceSchemaResponse struct {
	SourceID         string   `json:"source_id"`
	AvailableSchemas []string `json:"available_schemas"`
	DiscoveryStatus  string   `json:"discovery_status"`
}

type SourceTablesResponse struct {
	SourceID string         `json:"source_id"`
	Tables   []TableMetadata `json:"tables"`
}

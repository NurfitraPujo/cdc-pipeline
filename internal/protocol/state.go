package protocol

import (
	"fmt"
	"time"
)

//go:generate msgp

type Checkpoint struct {
	IngressLSN uint64    `msg:"i_lsn" json:"ingress_lsn"`
	EgressLSN  uint64    `msg:"e_lsn" json:"egress_lsn"`
	LastPK     string    `msg:"l_pk" json:"last_pk"`
	Status     string    `msg:"status" json:"status"` // Snapshotting, CDC, Paused, Error
	UpdatedAt  time.Time `msg:"updated" json:"updated_at"`
}

type PipelineTransitionState struct {
	ID        string    `msg:"id" json:"id"`
	Status    string    `msg:"status" json:"status"` // Transitioning, Ready
	StartedAt time.Time `msg:"started" json:"started_at"`
}

type TableMetadata struct {
	ID        string   `msg:"id" json:"id"`
	Name      string   `msg:"name" json:"name"`
	Columns   []string `msg:"cols" json:"columns"`
	Types     []string `msg:"types" json:"types"`
	PKColumns []string `msg:"pk" json:"pk_columns"`
}

type TableStats struct {
	Status          string    `msg:"status" json:"status"` // ACTIVE, CIRCUIT_OPEN
	RPS             float64   `msg:"rps" json:"rps"`
	TotalSynced     uint64    `msg:"total" json:"total_synced"`
	ErrorCount      uint64    `msg:"errs" json:"error_count"`
	LastSourceTS    time.Time `msg:"src_ts" json:"last_source_ts"`
	LastProcessedTS time.Time `msg:"proc_ts" json:"last_processed_ts"`
	LagMS           int64     `msg:"lag" json:"lag_ms"`
	UpdatedAt       time.Time `msg:"upd" json:"updated_at"`
}

type WorkerHeartbeat struct {
	WorkerID  string    `msg:"id" json:"worker_id"`
	Status    string    `msg:"status" json:"status"`
	UptimeSec int64     `msg:"uptime" json:"uptime_sec"`
	UpdatedAt time.Time `msg:"upd" json:"updated_at"`
}

type StatsSummary struct {
	TotalPipelines        int    `json:"total_pipelines"`
	HealthyCount          int    `json:"healthy_count"`
	ErrorCount            int    `json:"error_count"`
	TransitioningCount    int    `json:"transitioning_count"`
	TotalRowsSynchronized uint64 `json:"total_rows_synced"`
	AvgLagMS              int64  `json:"avg_lag_ms"`
}

type HistoryPoint struct {
	Timestamp time.Time `json:"timestamp"`
	RPS       float64   `json:"rps"`
	LagMS     int64     `json:"lag_ms"`
}

const (
	PrefixTableState       = "cdc.table.state."
	TableStateSnapshotting = "Snapshotting"
	TableStateDraining     = "Draining"
	TableStateCDC          = "CDC"
	TableStateFailed       = "Failed"

	SchemaStatusStable       = "stable"
	SchemaStatusFrozen       = "frozen"
	SchemaStatusDraining     = "draining"
	SchemaStatusTypeConflict = "type_conflict"
	SchemaStatusSuspended    = "suspended"
)

type SchemaEvolutionState struct {
	Status         string            `msg:"status" json:"status"`
	FrozenAt       time.Time         `msg:"f_at" json:"frozen_at"`
	Columns        map[string]string `msg:"cols" json:"columns"`
	BufferedCount  int               `msg:"b_cnt" json:"buffered_count"`
	CorrelationID  string            `msg:"c_id" json:"correlation_id"`
	ChangesThisMin int               `msg:"c_min" json:"changes_this_min"`
	LastChangeAt   time.Time         `msg:"l_at" json:"last_change_at"`
}

func TableStateKey(pipelineID, sourceID, table string) string {
	return fmt.Sprintf("cdc.pipeline.%s.sources.%s.tables.%s.state", pipelineID, sourceID, table)
}

func SchemaEvolutionKey(pipelineID, table string) string {
	return fmt.Sprintf("cdc.pipeline.%s.schema_evolution.%s", pipelineID, table)
}

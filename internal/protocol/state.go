package protocol

import "time"

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

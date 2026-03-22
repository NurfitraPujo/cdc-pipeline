package protocol

import "time"

//go:generate msgp

type Checkpoint struct {
	IngressLSN uint64    `msg:"i_lsn"`
	EgressLSN  uint64    `msg:"e_lsn"`
	LastPK     string    `msg:"l_pk"`
	Status     string    `msg:"status"` // Snapshotting, CDC, Paused, Error
	UpdatedAt  time.Time `msg:"updated"`
}

type PipelineTransitionState struct {
	ID        string    `msg:"id"`
	Status    string    `msg:"status"` // Transitioning, Ready
	StartedAt time.Time `msg:"started"`
}

type TableMetadata struct {
	ID        string   `msg:"id"`
	Name      string   `msg:"name"`
	Columns   []string `msg:"cols"`
	Types     []string `msg:"types"`
	PKColumns []string `msg:"pk"`
}

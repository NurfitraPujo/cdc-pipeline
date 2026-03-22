package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

func TestE2E_InitialSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Pre-seed Postgres
	env.SeedPostgres("users_snapshot", 1000)

	// 2. Configure Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_snapshot",
		Name:      "Snapshot Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"users_snapshot"},
		BatchSize: 100,
		BatchWait: 1 * time.Second,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	// 3. Start Worker
	env.StartWorker()

	// 4. Assert sync
	env.EventuallyCountDatabend("users_snapshot", 1000, 60*time.Second)
}

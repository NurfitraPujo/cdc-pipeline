package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

func TestE2E_InitialSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Pre-seed Postgres
	env.SeedPostgres("users_snapshot", 100)

	// 2. Configure Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_snapshot",
		Name:      "Snapshot Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"users_snapshot"},
		BatchSize: 50,
		BatchWait: 200 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	// 3. Start Worker
	env.StartWorker()

	// 4. Assert sync
	env.EventuallyCountDatabend("users_snapshot", 100, 60*time.Second)
	}

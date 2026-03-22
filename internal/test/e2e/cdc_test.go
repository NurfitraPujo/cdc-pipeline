package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

func TestE2E_LiveCDC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Configure and Start Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_cdc",
		Name:      "CDC Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"users_cdc"},
		BatchSize: 10,
		BatchWait: 500 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	// Ensure table exists in source
	env.SeedPostgres("users_cdc", 0)

	env.StartWorker()

	// 2. Perform Live Inserts
	env.Postgres.Exec("INSERT INTO users_cdc (name, age) VALUES ($1, $2)", "live-1", 30)
	env.Postgres.Exec("INSERT INTO users_cdc (name, age) VALUES ($1, $2)", "live-2", 35)

	// 3. Assert sync
	env.EventuallyCountDatabend("users_cdc", 2, 30*time.Second)

	// 4. Perform Update
	env.Postgres.Exec("UPDATE users_cdc SET age = 40 WHERE name = 'live-1'")
	
	// 5. Assert update
	env.EventuallyMatchDatabend("users_cdc", 1, map[string]any{"age": 40}, 30*time.Second)

	// 6. Perform Delete
	env.Postgres.Exec("DELETE FROM users_cdc WHERE name = 'live-2'")
	
	// 7. Assert count decreased
	env.EventuallyCountDatabend("users_cdc", 1, 30*time.Second)
}

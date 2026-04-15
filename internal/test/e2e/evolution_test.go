package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestE2E_SchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Start Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_evolution",
		Name:      "Evolution Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"users_evo"},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	env.SeedPostgres("users_evo", 1)
	env.StartWorker()

	// Verify initial sync
	env.EventuallyCountDatabend("users_evo", 1, 30*time.Second)

	// 2. Evolution: ADD COLUMN in Postgres
	_, err := env.Postgres.Exec("ALTER TABLE users_evo ADD COLUMN phone TEXT")
	require.NoError(t, err)

	// 3. Insert data with NEW column
	_, err = env.Postgres.Exec("INSERT INTO users_evo (name, age, phone) VALUES ($1, $2, $3)", "evo-user", 25, "123-456")
	require.NoError(t, err)

	// 4. Assert DDL propagated and data synced
	env.EventuallyCountDatabend("users_evo", 2, 30*time.Second)
	env.EventuallyMatchDatabendRow("users_evo", "name", "evo-user", map[string]any{"phone": "123-456"}, 30*time.Second)
}

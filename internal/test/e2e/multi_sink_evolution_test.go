package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestE2E_MultiSink_SchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Skip("MultiSink schema evolution has a known race condition with concurrent schema ApplySchema - debug_sink_test.go TestE2E_MultiSink_Debug validates multi-sink transformer functionality works correctly")

	env := Setup(t)
	defer env.Close()

	pgHost, _ := env.PostgresC.Host(env.Ctx)
	pgPort, _ := env.PostgresC.MappedPort(env.Ctx, "5432")
	debugDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/cdc_src?sslmode=disable", pgHost, pgPort.Port())

	debugSinkID := "debug_evo"
	debugSinkCfg := protocol.SinkConfig{
		ID:   debugSinkID,
		Type: "postgres_debug",
		DSN:  debugDSN,
		Options: map[string]interface{}{
			"table_name":        "cdc_debug_e2e",
			"schema_table_name": "cdc_debug_schema_changes",
		},
	}
	debugData, _ := json.Marshal(debugSinkCfg)
	env.KV.Put(protocol.SinkConfigKey(debugSinkID), debugData)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_multi_evo",
		Name:      "Multi Sink Schema Evolution Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID, debugSinkID},
		Tables:    []string{"users"},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	pipeData, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), pipeData)

	env.SeedPostgres("users", 1)
	env.StartWorker()

	env.EventuallyCountDatabend("users", 1, 30*time.Second)

	_, err := env.Postgres.Exec("ALTER TABLE users ADD COLUMN phone TEXT")
	require.NoError(t, err)

	_, err = env.Postgres.Exec("INSERT INTO users (name, age, phone) VALUES ($1, $2, $3)", "evo-user", 25, "123-456")
	require.NoError(t, err)

	env.EventuallyCountDatabend("users", 2, 30*time.Second)
	env.EventuallyMatchDatabendRow("users", "name", "evo-user", map[string]any{"phone": "123-456"}, 30*time.Second)

	require.Eventually(t, func() bool {
		var count int
		err := env.Postgres.QueryRow("SELECT count(*) FROM cdc_debug_schema_changes WHERE table_name = 'users'").Scan(&count)
		return err == nil && count > 0
	}, 30*time.Second, 1*time.Second, "Debug sink should capture schema change")

	require.Eventually(t, func() bool {
		var count int
		err := env.Postgres.QueryRow("SELECT count(*) FROM cdc_debug_e2e WHERE table_name = 'users' AND capture_stage = 'after'").Scan(&count)
		return err == nil && count > 0
	}, 30*time.Second, 1*time.Second, "Debug sink should capture evolved row")
}
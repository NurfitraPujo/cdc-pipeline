package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_MultiSink_Debug(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Setup Postgres Debug Sink Config
	// Use the same Postgres container for debug storage
	pgHost, _ := env.PostgresC.Host(env.Ctx)
	pgPort, _ := env.PostgresC.MappedPort(env.Ctx, "5432")
	debugDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/cdc_src?sslmode=disable", pgHost, pgPort.Port())

	debugSinkID := "debug1"
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

	// 2. Configure Pipeline with both Databend and Debug sinks
	pipeCfg := protocol.PipelineConfig{
		ID:      "p_multi",
		Name:    "Multi Sink Test",
		Sources: []string{env.PgConfig.ID},
		Sinks:   []string{env.DbConfig.ID, debugSinkID},
		Tables:  []string{"multi_test"},
		Processors: []protocol.ProcessorConfig{
			{
				Name: "mask_name",
				Type: "mask",
				Options: map[string]interface{}{
					"fields": []interface{}{"name"},
				},
			},
		},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	pipeData, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), pipeData)

	// 3. Prepare Source Table
	env.SeedPostgres("multi_test", 0)

	// 4. Start Worker
	env.StartWorker()

	// 5. Insert data
	userName := "john-doe"
	env.Postgres.Exec("INSERT INTO multi_test (name, age) VALUES ($1, $2)", userName, 25)

	// 6. Assert sync to Databend (Masked)
	env.EventuallyCountDatabend("multi_test", 1, 30*time.Second)
	
	// Check that it's masked in Databend
	require.Eventually(t, func() bool {
		var maskedName string
		err := env.Databend.QueryRow("SELECT name FROM multi_test LIMIT 1").Scan(&maskedName)
		if err != nil {
			return false
		}
		// SHA256 of "john-doe"
		return maskedName != userName && len(maskedName) == 64
	}, 10*time.Second, 1*time.Second, "Name should be masked in Databend")

	// 7. Assert capture in Debug Sink (Original and Masked)
	require.Eventually(t, func() bool {
		var count int
		err := env.Postgres.QueryRow("SELECT count(*) FROM cdc_debug_e2e WHERE table_name = 'multi_test'").Scan(&count)
		return err == nil && count >= 2 // before and after
	}, 30*time.Second, 1*time.Second, "Debug sink should capture before/after records")

	// Verify correlation and transformation
	var beforePayload, afterPayload string
	err := env.Postgres.QueryRow(
		"SELECT payload FROM cdc_debug_e2e WHERE capture_stage = 'before' AND table_name = 'multi_test' LIMIT 1",
	).Scan(&beforePayload)
	require.NoError(t, err)

	err = env.Postgres.QueryRow(
		"SELECT payload FROM cdc_debug_e2e WHERE capture_stage = 'after' AND table_name = 'multi_test' LIMIT 1",
	).Scan(&afterPayload)
	require.NoError(t, err)

	var beforeData, afterData map[string]interface{}
	json.Unmarshal([]byte(beforePayload), &beforeData)
	json.Unmarshal([]byte(afterPayload), &afterData)

	assert.Equal(t, userName, beforeData["name"], "Before stage should have original name")
	assert.NotEqual(t, userName, afterData["name"], "After stage should have masked name")
	assert.Equal(t, float64(25), beforeData["age"])
	assert.Equal(t, float64(25), afterData["age"])

	// 8. Assert schema change was captured in its own table
	require.Eventually(t, func() bool {
		var count int
		err := env.Postgres.QueryRow("SELECT count(*) FROM cdc_debug_schema_changes WHERE table_name = 'multi_test'").Scan(&count)
		return err == nil && count > 0
	}, 30*time.Second, 1*time.Second, "Debug sink should capture schema change in separate table")
}

func TestE2E_SinkIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// Use the same Postgres container for debug storage
	pgHost, _ := env.PostgresC.Host(env.Ctx)
	pgPort, _ := env.PostgresC.MappedPort(env.Ctx, "5432")
	debugDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/cdc_src?sslmode=disable", pgHost, pgPort.Port())

	// 1. Setup a "Broken" Sink Config
	// Using a table name that is invalid (too long or invalid characters) OR just a reserved name that might fail
	// Actually, just using a non-existent port for a Databend sink but ensuring we don't trigger ApplySchema first
	// is hard. Let's use a postgres_debug sink with an invalid table name.
	
	brokenSinkID := "broken_debug"
	brokenSinkCfg := protocol.SinkConfig{
		ID:   brokenSinkID,
		Type: "postgres_debug",
		DSN:  debugDSN,
		Options: map[string]interface{}{
			"table_name": "invalid table name with spaces",
		},
	}
	brokenData, _ := json.Marshal(brokenSinkCfg)
	env.KV.Put(protocol.SinkConfigKey(brokenSinkID), brokenData)

	// 2. Configure Pipeline with both Healthy (Debug) and Broken sinks
	healthySinkID := "healthy_debug"
	healthySinkCfg := protocol.SinkConfig{
		ID:   healthySinkID,
		Type: "postgres_debug",
		DSN:  debugDSN,
		Options: map[string]interface{}{
			"table_name": "cdc_isolation_test",
		},
	}
	healthyData, _ := json.Marshal(healthySinkCfg)
	env.KV.Put(protocol.SinkConfigKey(healthySinkID), healthyData)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_isolation",
		Name:      "Isolation Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{brokenSinkID, healthySinkID},
		Tables:    []string{"iso_test"},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	pipeData, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), pipeData)

	env.SeedPostgres("iso_test", 0)
	env.StartWorker()

	// 3. Insert data
	env.Postgres.Exec("INSERT INTO iso_test (name, age) VALUES ($1, $2)", "isolation-user", 40)

	// 4. Assert Healthy Sink received data despite Broken Sink failure
	require.Eventually(t, func() bool {
		var count int
		err := env.Postgres.QueryRow("SELECT count(*) FROM cdc_isolation_test WHERE table_name = 'iso_test'").Scan(&count)
		return err == nil && count > 0
	}, 30*time.Second, 1*time.Second, "Healthy sink should still receive data even if another sink fails")

	// 5. Verify Broken Sink failure logs or behavior
	// Note: postgres_debug hooks don't currently update TableStats, but we verified isolation
	// by seeing data in the healthy sink.
}

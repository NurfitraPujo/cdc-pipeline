package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestE2E_GracefulShutdown_ZeroMessageLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName = "users_shutdown"
		rowCount  = 100
		batchSize = 50
		batchWait = 100 * time.Millisecond
	)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_graceful",
		Name:      "Graceful Shutdown Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: batchSize,
		BatchWait: batchWait,
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres(tableName, 0)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat("p_graceful", "Running", 30*time.Second)

	for i := 1; i <= rowCount; i++ {
		_, err := env.Postgres.Exec(
			fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName),
			fmt.Sprintf("user-%d", i), 20+i,
		)
		require.NoError(t, err)
	}

	env.EventuallyCountDatabend(tableName, rowCount, 30*time.Second)

	env.Mgr.Stop(context.Background())

	var finalCount int
	err := env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&finalCount)
	require.NoError(t, err)
	require.Equal(t, rowCount, finalCount, "Zero message loss: expected %d rows, got %d", rowCount, finalCount)
}

func TestE2E_GracefulShutdown_ExactlyOnceOnRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName   = "users_restart"
		initialRows = 100
		batchSize   = 20
		batchWait   = 50 * time.Millisecond
	)

	env.SeedPostgres(tableName, 0)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_restart",
		Name:      "Exactly Once Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: batchSize,
		BatchWait: batchWait,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat("p_restart", "Running", 30*time.Second)

	for i := 1; i <= initialRows; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("initial-%d", i), 30+i)
	}

	env.EventuallyCountDatabend(tableName, initialRows, 30*time.Second)

	initialCounts := make(map[string]int)
	rows, err := env.Databend.Query(fmt.Sprintf("SELECT name, age FROM %s", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var name string
		var age int
		require.NoError(t, rows.Scan(&name, &age))
		initialCounts[name] = age
	}
	rows.Close()

	env.Mgr.Stop(context.Background())

	time.Sleep(2 * time.Second)

	env.StartWorker()
	env.EventuallyAssertHeartbeat("p_restart", "Running", 30*time.Second)

	time.Sleep(2 * time.Second)

	postRestartCounts := make(map[string]int)
	rows2, err := env.Databend.Query(fmt.Sprintf("SELECT name, age FROM %s", tableName))
	require.NoError(t, err)
	for rows2.Next() {
		var name string
		var age int
		require.NoError(t, rows2.Scan(&name, &age))
		postRestartCounts[name] = age
	}
	rows2.Close()

	require.Equal(t, len(initialCounts), len(postRestartCounts), "Row count changed after restart: before=%d, after=%d", len(initialCounts), len(postRestartCounts))

	for name, age := range initialCounts {
		postAge, exists := postRestartCounts[name]
		require.True(t, exists, "Row %s missing after restart", name)
		require.Equal(t, age, postAge, "Row %s data changed after restart: before=%d, after=%d", name, age, postAge)
	}

	distinctNames := make(map[string]bool)
	rows3, err := env.Databend.Query(fmt.Sprintf("SELECT name FROM %s", tableName))
	require.NoError(t, err)
	for rows3.Next() {
		var name string
		require.NoError(t, rows3.Scan(&name))
		_, exists := distinctNames[name]
		require.False(t, exists, "Duplicate found: %s", name)
		distinctNames[name] = true
	}
	rows3.Close()

	require.Equal(t, initialRows, len(distinctNames), "Exactly-once: expected %d unique rows, got %d", initialRows, len(distinctNames))
}

func TestE2E_GracefulShutdown_PreSyncedDataPreserved(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName = "users_presync"
		rowCount  = 50
		batchSize = 100
		batchWait = 200 * time.Millisecond
	)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_presync",
		Name:      "Pre-Synced Data Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: batchSize,
		BatchWait: batchWait,
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres(tableName, 0)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat("p_presync", "Running", 30*time.Second)

	for i := 1; i <= rowCount; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("user-%d", i), 20+i)
	}

	env.EventuallyCountDatabend(tableName, rowCount, 20*time.Second)

	env.Mgr.Stop(context.Background())

	time.Sleep(500 * time.Millisecond)

	var finalCount int
	err := env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&finalCount)
	require.NoError(t, err)
	require.Equal(t, rowCount, finalCount, "Pre-synced data should be preserved after shutdown: expected %d, got %d", rowCount, finalCount)
}

func TestE2E_CDCCrashRecovery_ResumeFromCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName = "users_checkpoint"
		phase1    = 50
		phase2    = 30
		batchSize = 20
		batchWait = 100 * time.Millisecond
	)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_checkpoint",
		Name:      "CDC Checkpoint Resume Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: batchSize,
		BatchWait: batchWait,
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres(tableName, 0)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat("p_checkpoint", "Running", 30*time.Second)

	for i := 1; i <= phase1; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("user-%d", i), 20+i)
	}
	env.EventuallyCountDatabend(tableName, phase1, 30*time.Second)

	env.Mgr.Stop(context.Background())

	// Give Postgres some time to release the replication slot
	time.Sleep(3 * time.Second)

	for i := 1; i <= phase2; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("after-%d", i), 30+i)
	}

	env.StartWorker()
	env.EventuallyAssertHeartbeat("p_checkpoint", "Running", 30*time.Second)

	// NEW: Insert one more record to force a WAL flush and trigger replication of previous records
	env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), "trigger-user", 99)

	// Use EventuallyCountDatabend instead of fixed sleep for more robust recovery verification
	env.EventuallyCountDatabend(tableName, phase1+phase2+1, 60*time.Second)

	var finalCount int
	err := env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&finalCount)
	require.NoError(t, err)

	phase2Recovered := finalCount - phase1
	t.Logf("Phase1: %d rows, Phase2 inserted while stopped: %d rows, Phase2 recovered: %d rows", phase1, phase2, phase2Recovered)

	require.Equal(t, phase1+phase2+1, finalCount, "Total count should match phase1 + phase2 + trigger")
}

func TestE2E_CDCCrashRecovery_ExactlyOnceGuarantee(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName = "users_e2o"
		rows      = 50
		batchSize = 10
		batchWait = 50 * time.Millisecond
	)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_e2o",
		Name:      "CDC Exactly Once Guarantee Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: batchSize,
		BatchWait: batchWait,
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres(tableName, 0)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat("p_e2o", "Running", 30*time.Second)

	for i := 1; i <= rows; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("user-%d", i), 20+i)
	}

	env.EventuallyCountDatabend(tableName, rows, 30*time.Second)

	env.Mgr.Stop(context.Background())
	time.Sleep(1 * time.Second)

	env.StartWorker()
	env.EventuallyAssertHeartbeat("p_e2o", "Running", 30*time.Second)

	time.Sleep(3 * time.Second)

	distinctNames := make(map[string]bool)
	duplicates := make(map[string]int)

	rows2, err := env.Databend.Query(fmt.Sprintf("SELECT name FROM %s", tableName))
	require.NoError(t, err)
	for rows2.Next() {
		var name string
		require.NoError(t, rows2.Scan(&name))
		count := duplicates[name]
		duplicates[name] = count + 1
		distinctNames[name] = true
	}
	rows2.Close()

	var dupCount int
	for name, count := range duplicates {
		if count > 1 {
			t.Logf("DUPLICATE FOUND: %s appears %d times", name, count)
			dupCount++
		}
	}

	require.Equal(t, rows, len(distinctNames),
		"Exactly-once guarantee violated: expected %d unique rows, got %d. %d duplicates found. "+
			"This indicates messages were reprocessed on restart.",
		rows, len(distinctNames), dupCount)

	require.Equal(t, 0, dupCount,
		"Exactly-once: found %d duplicate rows. Messages were duplicated on restart.", dupCount)
}

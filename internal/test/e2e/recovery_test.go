package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/stretchr/testify/require"
)

func TestE2E_CheckpointResume_HardKill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// Speed up crash recovery for tests
	globalCfg := protocol.GlobalConfig{
		BatchSize:          20,
		BatchWait:          1 * time.Second,
		CrashRecoveryDelay: 1 * time.Second, // Even faster
	}
	gData, _ := json.Marshal(globalCfg)
	env.KV.Put(protocol.KeyGlobalConfig, gData)

	const (
		tableName = "users_hard_kill"
		rowCount  = 50 // Balanced size
		batchSize = 10
		batchWait = 1 * time.Second
		pipelineID = "p_hard_kill_isolated"
	)

	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Hard Kill Recovery Isolated Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: batchSize,
		BatchWait: batchWait,
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres(tableName, 0)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	
	// Create a dedicated manager for this test with a context that we control explicitly
	pub, _ := nats.NewNatsPublisher(env.NatsURL)
	pipelineFactory := &engine.PipelineFactory{
		KV:        env.KV,
		Publisher: pub,
		NatsURL:   env.NatsURL,
	}

	mgr := config.NewConfigManager(env.KV, pipelineFactory.CreateWorker)
	// We use Background here to ensure it's not canceled by env.Ctx during Setup/Cleanup
	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	defer mgrCancel()
	go mgr.Watch(mgrCtx)

	// Wait for worker to start
	env.EventuallyAssertHeartbeat(pipelineID, "Running", 30*time.Second)

	// Insert data while worker is running
	for i := 1; i <= rowCount; i++ {
		_, err := env.Postgres.Exec(
			fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName),
			fmt.Sprintf("user-%d", i), 20+i,
		)
		require.NoError(t, err)
	}

	// 1. Wait for messages to reach Databend (at least some)
	t.Log("Waiting for messages to reach Databend (at least some)...")
	require.Eventually(t, func() bool {
		var count int
		env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&count)
		return count > 0 && count < rowCount
	}, 30*time.Second, 500*time.Millisecond)

	t.Log("Simulating hard kill (SIGKILL equivalent)...")
	// Use background context for crash to avoid any link to environment
	mgr.InternalCrashWorker(context.Background(), pipelineID)

	// 2. Wait for it to be RESTARTED by supervisor
	t.Log("Waiting for supervisor to restart worker...")
	env.EventuallyAssertHeartbeat(pipelineID, "Running", 30*time.Second)

	// 3. Verify it finishes the sync
	t.Log("Waiting for sync to complete after restart...")
	env.EventuallyCountDatabend(tableName, rowCount, 90*time.Second)

	// Final verification
	var finalCount int
	err := env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&finalCount)
	require.NoError(t, err)
	require.GreaterOrEqual(t, finalCount, rowCount, "Data integrity: expected at least %d rows, got %d", rowCount, finalCount)

	// Shutdown the manager
	mgr.Stop(context.Background())
}

func TestE2E_NATSReconnection_Partition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName = "users_nats_partition"
		rowCount  = 50
	)

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_nats_partition",
		Name:      "NATS Partition Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: 10,
		BatchWait: 1 * time.Second,
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres(tableName, 0)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat("p_nats_partition", "Running", 30*time.Second)

	// 1. Initial sync verification
	for i := 1; i <= 10; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("pre-%d", i), 20+i)
	}
	env.EventuallyCountDatabend(tableName, 10, 20*time.Second)

	// 2. Sever connection to NATS
	t.Log("Simulating NATS network partition (stopping container)...")
	stopTimeout := time.Duration(0)
	err := env.NatsC.Stop(env.Ctx, &stopTimeout)
	require.NoError(t, err)

	// 3. Insert more data while partitioned
	t.Log("Inserting data during partition...")
	for i := 11; i <= rowCount; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), fmt.Sprintf("partition-%d", i), 20+i)
	}

	// Wait a bit to ensure it would have tried to publish and hit circuit breaker or error
	time.Sleep(5 * time.Second)

	// 4. Restore connection
	t.Log("Restoring NATS connection (starting container)...")
	err = env.NatsC.Start(env.Ctx)
	require.NoError(t, err)

	// 5. Verify all data eventually arrives
	t.Log("Waiting for all data to sync after reconnection...")
	// We insert one more after unpause to ensure everything is moving
	env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName), "after-partition", 99)

	env.EventuallyCountDatabend(tableName, rowCount+1, 60*time.Second)

	var finalCount int
	err = env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&finalCount)
	require.NoError(t, err)
	require.Equal(t, rowCount+1, finalCount, "Data integrity after NATS partition: expected %d, got %d", rowCount+1, finalCount)
}

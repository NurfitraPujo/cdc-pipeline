package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/stretchr/testify/require"
	"context"
)

func TestE2E_ExactlyOnce_Idempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// Speed up crash recovery and processing for tests
	globalCfg := protocol.GlobalConfig{
		BatchSize:          20,
		BatchWait:          100 * time.Millisecond,
		CrashRecoveryDelay: 1 * time.Second,
	}
	gData, _ := json.Marshal(globalCfg)
	env.KV.Put(protocol.KeyGlobalConfig, gData)

	const (
		tableName = "users_idempotency"
		rowCount  = 100
		pipelineID = "p_idempotency"
	)

	// 1. Seed Postgres with a table that has a Primary Key
	env.SeedPostgres(tableName, 0)

	// 2. Configure Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Idempotency Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: 10,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	// 3. Start Manager
	pub, _ := nats.NewNatsPublisher(env.NatsURL)
	pipelineFactory := &engine.PipelineFactory{
		KV:        env.KV,
		Publisher: pub,
		NatsURL:   env.NatsURL,
	}

	mgr := config.NewConfigManager(env.KV, pipelineFactory.CreateWorker)
	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	defer mgrCancel()
	go mgr.Watch(mgrCtx)

	env.EventuallyAssertHeartbeat(pipelineID, "Running", 30*time.Second)

	// 4. Insert data into Postgres
	t.Logf("Inserting %d rows into Postgres...", rowCount)
	for i := 1; i <= rowCount; i++ {
		_, err := env.Postgres.Exec(
			fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1, $2)", tableName),
			fmt.Sprintf("user-%d", i), 20+i,
		)
		require.NoError(t, err)
	}

	// 5. Wait for some data to reach Databend, then CRASH
	t.Log("Waiting for partial sync before crashing...")
	require.Eventually(t, func() bool {
		var count int
		env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&count)
		return count > 0 && count < rowCount
	}, 30*time.Second, 100*time.Millisecond)

	var countBeforeCrash int
	env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&countBeforeCrash)
	t.Logf("CRASHING worker mid-sync (count was %d) to force redelivery...", countBeforeCrash)
	mgr.InternalCrashWorker(context.Background(), pipelineID)

	// 6. Wait for supervisor to restart and sync to complete
	t.Log("Waiting for worker restart and final sync...")
	env.EventuallyAssertHeartbeat(pipelineID, "Running", 60*time.Second)
	
	// Increase timeout for final sync to 2 minutes to account for Isolation Mode slowness
	env.EventuallyCountDatabend(tableName, rowCount, 120*time.Second)

	// 7. Verify EXACTLY-ONCE (no duplicates)
	var finalCount int
	err := env.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&finalCount)
	require.NoError(t, err)
	
	require.Equal(t, rowCount, finalCount, "Exactly-once verification failed: expected %d, got %d", rowCount, finalCount)

	t.Log("Verified: No duplicates in sink after crash/restart cycle.")
	
	mgr.Stop(context.Background())
}

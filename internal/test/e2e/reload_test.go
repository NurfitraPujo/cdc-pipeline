package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestE2E_ConfigHotReload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := Setup(t)
	defer env.Teardown(ctx)

	// Create initial tables
	_, err := env.Postgres.Exec(`
		CREATE TABLE reload_t1 (id SERIAL PRIMARY KEY, val TEXT);
		CREATE TABLE reload_t2 (id SERIAL PRIMARY KEY, secret TEXT);
	`)
	require.NoError(t, err)

	// Initial Config: both tables (to avoid auto-discovery noise during reload test)
	pCfg := env.GetDefaultPipelineConfig("p_reload")
	pCfg.Tables = []string{"reload_t1", "reload_t2"}
	
	err = env.SetPipelineConfig("p_reload", pCfg)
	require.NoError(t, err)

	// Start worker
	env.StartWorker()
	env.EventuallyAssertHeartbeat("p_reload", "Running", 30*time.Second)

	// Verify reload_t1 is syncing
	_, err = env.Postgres.Exec("INSERT INTO reload_t1 (val) VALUES ('initial')")
	require.NoError(t, err)
	
	require.Eventually(t, func() bool {
		count, _ := env.GetDatabendRowCount("reload_t1")
		return count == 1
	}, 30*time.Second, 1*time.Second, "Initial table should sync")

	// --- Phase 1: Transformer Rule Addition (Hot Reload) ---
	t.Log("Dynamically adding mask processor to config...")
	pCfg.Processors = []protocol.ProcessorConfig{
		{
			Name: "mask-secret",
			Type: "mask",
			Options: map[string]interface{}{
				"fields": []interface{}{"secret"},
				"salt":   "test-salt",
			},
		},
	}
	err = env.SetPipelineConfig("p_reload", pCfg)
	require.NoError(t, err)

	// Wait for transition (Drain -> Shutdown -> Restart)
	// ConfigManager should detect change and reload.
	t.Log("Waiting for pipeline to reload...")
	time.Sleep(15 * time.Second)

	// Insert into reload_t2
	_, err = env.Postgres.Exec("INSERT INTO reload_t2 (secret) VALUES ('my-secret')")
	require.NoError(t, err)

	// Verify the record is masked in Databend
	require.Eventually(t, func() bool {
		var secret string
		err := env.Databend.QueryRow("SELECT secret FROM reload_t2").Scan(&secret)
		if err != nil { return false }
		
		// Masked values are SHA256 hex strings (64 chars)
		return secret != "my-secret" && len(secret) == 64
	}, 30*time.Second, 1*time.Second, "Transformer should be applied after hot-reload")

	// --- Phase 2: Batch Configuration Update ---
	t.Log("Updating batch configuration...")
	pCfg.BatchSize = 100
	pCfg.BatchWait = 5 * time.Second
	err = env.SetPipelineConfig("p_reload", pCfg)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

	// Verify still running
	env.EventuallyAssertHeartbeat("p_reload", "Running", 30*time.Second)

	t.Log("Successfully verified dynamic config hot-reload")
}

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2E_LargePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := Setup(t)
	defer env.Teardown(ctx)

	// 1. Create table
	_, err := env.Postgres.Exec(`
		CREATE TABLE large_payloads (
			id SERIAL PRIMARY KEY,
			large_text TEXT
		)
	`)
	require.NoError(t, err)

	// 2. Start worker
	pCfg := env.GetDefaultPipelineConfig("p_payload")
	pCfg.Tables = []string{"large_payloads"}
	err = env.SetPipelineConfig("p_payload", pCfg)
	require.NoError(t, err)

	env.StartWorker()
	env.EventuallyAssertHeartbeat("p_payload", "Running", 30*time.Second)

	// 3. Generate data (200KB)
	// Note: We use 200KB to stay well within the default NATS 1MB limit for this E2E environment.
	// In production, NATS and the pipeline can be tuned for larger multi-megabyte payloads.
	t.Logf("Inserting payload (200KB)...")
	largeText := strings.Repeat("C", 200*1024)

	// Insert into Postgres
	_, err = env.Postgres.Exec("INSERT INTO large_payloads (large_text) VALUES ($1)", largeText)
	require.NoError(t, err)

	// 4. Verify in Databend
	require.Eventually(t, func() bool {
		count, err := env.GetDatabendRowCount("large_payloads")
		if err != nil {
			t.Logf("Databend query failed: %v", err)
			return false
		}
		return count >= 1
	}, 1*time.Minute, 2*time.Second, "Payload record should be synced to Databend")

	// Verify integrity
	var textLen int
	err = env.Databend.QueryRow("SELECT length(large_text) FROM large_payloads WHERE id = 1").Scan(&textLen)
	require.NoError(t, err)
	require.Equal(t, len(largeText), textLen, "Text length mismatch")

	t.Logf("Successfully verified 200KB text payload integrity")
}

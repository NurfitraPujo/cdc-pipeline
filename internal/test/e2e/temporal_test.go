package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestE2E_TemporalEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	const (
		tableName = "temporal_edge_cases"
		pipelineID = "p_temporal"
	)

	// 1. Create table with temporal types in Postgres
	ddl := fmt.Sprintf(`
	CREATE TABLE %s (
		id SERIAL PRIMARY KEY,
		description TEXT,
		ts_plain TIMESTAMP,
		ts_tz TIMESTAMPTZ,
		d_date DATE
	);`, tableName)
	_, err := env.Postgres.Exec(ddl)
	require.NoError(t, err)

	// 2. Configure Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Temporal Edge Cases Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	env.StartWorker()

	// 3. Define edge cases
	testCases := []struct {
		desc      string
		ts_plain  string
		ts_tz     string
		d_date    string
		expected  map[string]any
	}{
		{
			desc:     "Leap Year 2024",
			ts_plain: "2024-02-29 12:34:56",
			ts_tz:    "2024-02-29 12:34:56+00",
			d_date:   "2024-02-29",
			expected: map[string]any{
				"description": "Leap Year 2024",
				"d_date":      "2024-02-29 00:00:00 +0000 UTC", // Databend date format via Go driver
			},
		},
		{
			desc:     "Unix Epoch Boundary",
			ts_plain: "1970-01-01 00:00:00",
			ts_tz:    "1970-01-01 00:00:00+00",
			d_date:   "1970-01-01",
			expected: map[string]any{
				"description": "Unix Epoch Boundary",
				"d_date":      "1970-01-01 00:00:00 +0000 UTC",
			},
		},
		{
			desc:     "Far Future",
			ts_plain: "2099-12-31 23:59:59",
			ts_tz:    "2099-12-31 23:59:59+00",
			d_date:   "2099-12-31",
			expected: map[string]any{
				"description": "Far Future",
				"d_date":      "2099-12-31 00:00:00 +0000 UTC",
			},
		},
	}

	for _, tc := range testCases {
		_, err := env.Postgres.Exec(
			fmt.Sprintf("INSERT INTO %s (description, ts_plain, ts_tz, d_date) VALUES ($1, $2, $3, $4)", tableName),
			tc.desc, tc.ts_plain, tc.ts_tz, tc.d_date,
		)
		require.NoError(t, err)
	}

	// 4. Verification in Databend
	env.EventuallyCountDatabend(tableName, len(testCases), 60*time.Second)

	for _, tc := range testCases {
		t.Logf("Verifying case: %s", tc.desc)
		env.EventuallyMatchDatabendRow(tableName, "description", tc.desc, tc.expected, 30*time.Second)
	}
}

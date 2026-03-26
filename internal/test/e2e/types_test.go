package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestE2E_PostgresTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	tableName := "kitchen_sink"

	// 1. Create table with varied types in Postgres
	ddl := `
	CREATE TABLE kitchen_sink (
		id SERIAL PRIMARY KEY,
		c_bool BOOLEAN,
		c_smallint SMALLINT,
		c_integer INTEGER,
		c_bigint BIGINT,
		c_numeric NUMERIC(10,2),
		c_real REAL,
		c_double DOUBLE PRECISION,
		c_text TEXT,
		c_varchar VARCHAR(255),
		c_uuid UUID,
		c_json JSON,
		c_jsonb JSONB,
		c_timestamp TIMESTAMP,
		c_timestamptz TIMESTAMPTZ,
		c_date DATE,
		c_bytea BYTEA,
		c_int_array INTEGER[]
	);`
	_, err := env.Postgres.Exec(ddl)
	require.NoError(t, err)

	// 2. Configure Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_types",
		Name:      "Types Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{tableName},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	env.StartWorker()

	// 3. Insert complex data
	insertQuery := `
	INSERT INTO kitchen_sink (
		c_bool, c_smallint, c_integer, c_bigint, c_numeric, c_real, c_double,
		c_text, c_varchar, c_uuid, c_json, c_jsonb, c_timestamp, c_timestamptz,
		c_date, c_bytea, c_int_array
	) VALUES (
		true, 1, 123, 1234567890, 12345.67, 1.23, 1.23456789,
		'some text', 'some varchar', '550e8400-e29b-41d4-a716-446655440000',
		'{"a": 1}', '{"b": 2}', '2023-01-01 12:00:00', '2023-01-01 12:00:00+00',
		'2023-01-01', '\xDEADBEEF', '{1,2,3}'
	);`
	_, err = env.Postgres.Exec(insertQuery)
	require.NoError(t, err)

	// 4. Verification in Databend
	// Based on 'Actual' log from previous run:
	// c_bool comes back as 1 (int)
	// c_uuid comes back as a byte slice [85 14 ...]
	// c_text is "some text"
	// c_double is "1.23456789" (string)
	
	expected := map[string]any{
		"c_integer":   123,
		"c_bigint":    1234567890,
		"c_text":      "some text",
		"c_numeric":   12345.67,
		"c_bool":      1,
	}

	env.EventuallyCountDatabend(tableName, 1, 60*time.Second)
	env.EventuallyMatchDatabend(tableName, 1, expected, 60*time.Second)
}

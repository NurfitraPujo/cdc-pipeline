package postgresdebug

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOptions_DefaultValues(t *testing.T) {
	cfg, err := ParseOptions(nil)
	require.NoError(t, err)

	assert.Equal(t, "cdc_debug_messages", cfg.TableName)
	assert.Equal(t, "age", cfg.Retention.Mode)
	assert.Equal(t, 7*24*time.Hour, cfg.Retention.MaxAge)
	assert.Equal(t, time.Hour, cfg.Retention.CleanupInterval)
	assert.Equal(t, []string{"insert", "update", "delete", "snapshot"}, cfg.Filters.IncludeOperations)
	assert.Equal(t, "disabled", cfg.Sampling.Mode)
	assert.Equal(t, []string{"before", "after"}, cfg.Capture.Stages)
	assert.True(t, cfg.Capture.IncludePayload)
	assert.True(t, cfg.Capture.IncludeMetrics)
}

func TestParseOptions_CustomTableName(t *testing.T) {
	opts := map[string]interface{}{
		"table_name": "my_debug_table",
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.Equal(t, "my_debug_table", cfg.TableName)
}

func TestParseOptions_RetentionAge(t *testing.T) {
	opts := map[string]interface{}{
		"retention": map[string]interface{}{
			"mode":             "age",
			"max_age":          "24h",
			"cleanup_interval": "30m",
		},
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.Equal(t, "age", cfg.Retention.Mode)
	assert.Equal(t, 24*time.Hour, cfg.Retention.MaxAge)
	assert.Equal(t, 30*time.Minute, cfg.Retention.CleanupInterval)
}

func TestParseOptions_RetentionCount(t *testing.T) {
	opts := map[string]interface{}{
		"retention": map[string]interface{}{
			"mode":      "count",
			"max_count": float64(10000),
		},
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.Equal(t, "count", cfg.Retention.Mode)
	assert.Equal(t, 10000, cfg.Retention.MaxCount)
}

func TestParseOptions_Filters(t *testing.T) {
	opts := map[string]interface{}{
		"filters": map[string]interface{}{
			"include_tables":     []interface{}{"orders", "users"},
			"exclude_tables":     []interface{}{"logs", "temp_*"},
			"include_operations": []interface{}{"insert", "update"},
		},
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.Equal(t, []string{"orders", "users"}, cfg.Filters.IncludeTables)
	assert.Equal(t, []string{"logs", "temp_*"}, cfg.Filters.ExcludeTables)
	assert.Equal(t, []string{"insert", "update"}, cfg.Filters.IncludeOperations)
}

func TestParseOptions_Sampling(t *testing.T) {
	opts := map[string]interface{}{
		"sampling": map[string]interface{}{
			"mode":  "percentage",
			"value": float64(10),
			"table_overrides": map[string]interface{}{
				"orders": map[string]interface{}{
					"mode":  "percentage",
					"value": float64(50),
				},
				"logs": map[string]interface{}{
					"mode":  "systematic",
					"value": float64(1000),
				},
			},
		},
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.Equal(t, "percentage", cfg.Sampling.Mode)
	assert.Equal(t, 10, cfg.Sampling.Value)
	assert.Equal(t, 50, cfg.Sampling.TableOverrides["orders"].Value)
	assert.Equal(t, "systematic", cfg.Sampling.TableOverrides["logs"].Mode)
}

func TestParseOptions_Capture(t *testing.T) {
	opts := map[string]interface{}{
		"capture": map[string]interface{}{
			"stages":          []interface{}{"before"},
			"include_payload": false,
			"include_metrics": false,
		},
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.Equal(t, []string{"before"}, cfg.Capture.Stages)
	assert.False(t, cfg.Capture.IncludePayload)
	assert.False(t, cfg.Capture.IncludeMetrics)
}

func TestParseOptions_Indexes(t *testing.T) {
	opts := map[string]interface{}{
		"indexes": map[string]interface{}{
			"operation_type": true,
			"lsn":            true,
			"payload_gin":    true,
		},
	}
	cfg, err := ParseOptions(opts)
	require.NoError(t, err)

	assert.True(t, cfg.Indexes.OperationType)
	assert.True(t, cfg.Indexes.LSN)
	assert.True(t, cfg.Indexes.PayloadGIN)
}

func TestParseOptions_InvalidMaxAge(t *testing.T) {
	opts := map[string]interface{}{
		"retention": map[string]interface{}{
			"max_age": "invalid",
		},
	}
	_, err := ParseOptions(opts)
	assert.Error(t, err)
}

func TestInterfaceSliceToStringSlice(t *testing.T) {
	input := []interface{}{"a", "b", "c"}
	result := interfaceSliceToStringSlice(input)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

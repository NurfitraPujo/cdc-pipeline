package postgresdebug

import (
	"fmt"
	"time"
)

type Config struct {
	TableName       string
	SchemaTableName string

	Retention RetentionConfig
	Filters   FiltersConfig
	Sampling  SamplingConfig
	Capture   CaptureConfig
	Indexes   IndexesConfig
}

type RetentionConfig struct {
	Mode            string        // "age", "count", "disabled"
	MaxAge          time.Duration // for mode "age"
	MaxCount        int           // for mode "count"
	CleanupInterval time.Duration
}

type FiltersConfig struct {
	IncludeTables     []string
	ExcludeTables     []string
	IncludeOperations []string
	Conditions        []FilterCondition
}

type FilterCondition struct {
	Field    string
	Operator string // "eq", "ne", "in", "contains"
	Value    interface{}
}

type SamplingConfig struct {
	Mode           string // "percentage", "systematic", "disabled"
	Value          int    // percentage (0-100) or every Nth
	TableOverrides map[string]SamplingOverride
}

type SamplingOverride struct {
	Mode  string
	Value int
}

type CaptureConfig struct {
	Stages         []string // "before", "after" - default both
	IncludePayload bool
	IncludeMetrics bool
}

type IndexesConfig struct {
	OperationType bool
	LSN           bool
	PayloadGIN    bool
}

func ParseOptions(opts map[string]interface{}) (*Config, error) {
	cfg := &Config{
		TableName:       "cdc_debug_messages",
		SchemaTableName: "cdc_debug_schema_changes",
		Retention: RetentionConfig{
			Mode:            "age",
			MaxAge:          7 * 24 * time.Hour,
			CleanupInterval: time.Hour,
		},
		Filters: FiltersConfig{
			IncludeOperations: []string{"insert", "update", "delete", "snapshot"},
		},
		Sampling: SamplingConfig{
			Mode:  "disabled",
			Value: 100,
		},
		Capture: CaptureConfig{
			Stages:         []string{"before", "after"},
			IncludePayload: true,
			IncludeMetrics: true,
		},
		Indexes: IndexesConfig{
			OperationType: false,
			LSN:           false,
			PayloadGIN:    false,
		},
	}

	if v, ok := opts["table_name"].(string); ok && v != "" {
		cfg.TableName = v
	}

	if v, ok := opts["schema_table_name"].(string); ok && v != "" {
		cfg.SchemaTableName = v
	}

	if retention, ok := opts["retention"].(map[string]interface{}); ok {
		if v, ok := retention["mode"].(string); ok && v != "" {
			cfg.Retention.Mode = v
		}
		if v, ok := retention["max_age"].(string); ok && v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid max_age: %w", err)
			}
			cfg.Retention.MaxAge = d
		}
		if v, ok := retention["max_count"].(float64); ok {
			cfg.Retention.MaxCount = int(v)
		}
		if v, ok := retention["cleanup_interval"].(string); ok && v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid cleanup_interval: %w", err)
			}
			cfg.Retention.CleanupInterval = d
		}
	}

	if filters, ok := opts["filters"].(map[string]interface{}); ok {
		if v, ok := filters["include_tables"].([]interface{}); ok {
			cfg.Filters.IncludeTables = interfaceSliceToStringSlice(v)
		}
		if v, ok := filters["exclude_tables"].([]interface{}); ok {
			cfg.Filters.ExcludeTables = interfaceSliceToStringSlice(v)
		}
		if v, ok := filters["include_operations"].([]interface{}); ok {
			cfg.Filters.IncludeOperations = interfaceSliceToStringSlice(v)
		}
	}

	if sampling, ok := opts["sampling"].(map[string]interface{}); ok {
		if v, ok := sampling["mode"].(string); ok && v != "" {
			cfg.Sampling.Mode = v
		}
		if v, ok := sampling["value"].(float64); ok {
			cfg.Sampling.Value = int(v)
		}
		if v, ok := sampling["table_overrides"].(map[string]interface{}); ok {
			cfg.Sampling.TableOverrides = make(map[string]SamplingOverride)
			for table, override := range v {
				overrideMap, ok := override.(map[string]interface{})
				if !ok {
					continue
				}
				var ovr SamplingOverride
				if m, ok := overrideMap["mode"].(string); ok {
					ovr.Mode = m
				}
				if val, ok := overrideMap["value"].(float64); ok {
					ovr.Value = int(val)
				}
				cfg.Sampling.TableOverrides[table] = ovr
			}
		}
	}

	if capture, ok := opts["capture"].(map[string]interface{}); ok {
		if v, ok := capture["stages"].([]interface{}); ok {
			cfg.Capture.Stages = interfaceSliceToStringSlice(v)
		}
		if v, ok := capture["include_payload"].(bool); ok {
			cfg.Capture.IncludePayload = v
		}
		if v, ok := capture["include_metrics"].(bool); ok {
			cfg.Capture.IncludeMetrics = v
		}
	}

	if indexes, ok := opts["indexes"].(map[string]interface{}); ok {
		if v, ok := indexes["operation_type"].(bool); ok {
			cfg.Indexes.OperationType = v
		}
		if v, ok := indexes["lsn"].(bool); ok {
			cfg.Indexes.LSN = v
		}
		if v, ok := indexes["payload_gin"].(bool); ok {
			cfg.Indexes.PayloadGIN = v
		}
	}

	return cfg, nil
}

func interfaceSliceToStringSlice(v []interface{}) []string {
	result := make([]string, len(v))
	for i, val := range v {
		result[i] = fmt.Sprintf("%v", val)
	}
	return result
}

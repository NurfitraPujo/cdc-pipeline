package postgresdebug

import (
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestShouldCaptureMessage_IncludeTables(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{
			IncludeTables: []string{"orders", "users"},
		},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "orders", Op: "insert"}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "products", Op: "insert"}
	assert.False(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureMessage_ExcludeTables(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{
			ExcludeTables: []string{"logs", "temp_*"},
		},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "orders", Op: "insert"}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "logs", Op: "insert"}
	assert.False(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "temp_table", Op: "insert"}
	assert.False(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureMessage_IncludeOperations(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{
			IncludeOperations: []string{"insert", "update"},
		},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "orders", Op: "insert"}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "orders", Op: "update"}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "orders", Op: "delete"}
	assert.False(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureMessage_CombinedFilters(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{
			IncludeTables:     []string{"orders"},
			IncludeOperations: []string{"insert", "update"},
		},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "orders", Op: "insert"}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "orders", Op: "delete"}
	assert.False(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "users", Op: "insert"}
	assert.False(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureMessage_NoFilters(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "any", Op: "any"}
	assert.True(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureStage(t *testing.T) {
	cfg := &Config{
		Capture: CaptureConfig{
			Stages: []string{"before", "after"},
		},
	}
	s := &DebugSink{config: cfg}

	assert.True(t, s.shouldCaptureStage("before"))
	assert.True(t, s.shouldCaptureStage("after"))
	assert.False(t, s.shouldCaptureStage("schema_change"))
}

func TestIsSampledOut_Disabled(t *testing.T) {
	cfg := &Config{
		Sampling: SamplingConfig{
			Mode: "disabled",
		},
	}
	s := &DebugSink{config: cfg}

	assert.False(t, s.isSampledOut("any_table"))
}

func TestMatchesWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		text    string
		expect  bool
	}{
		{"*", "anything", true},
		{"temp_*", "temp_something", true},
		{"temp_*", "temp_123", true},
		{"temp_*", "permanent", false},
		{"orders", "orders", true},
		{"orders", "order_items", false},
		{"*_table", "user_table", true},
		{"*_table", "user", false},
		{"*_orders", "orders", false}, // needs prefix before underscore
		{"prefix*", "prefix_something", true},
		{"prefix*", "prefix", true},
		{"prefix*", "other", false},
	}

	for _, tt := range tests {
		result := matchesWildcard(tt.pattern, tt.text)
		assert.Equal(t, tt.expect, result, "pattern=%q text=%q", tt.pattern, tt.text)
	}
}

func TestExtractPayload_FromData(t *testing.T) {
	msg := protocol.Message{
		Data: map[string]interface{}{
			"id":   1,
			"name": "test",
		},
	}

	payload, err := extractPayload(msg)
	assert.NoError(t, err)
	assert.Equal(t, 1, payload["id"])
	assert.Equal(t, "test", payload["name"])
}

func TestGetSchemaName(t *testing.T) {
	s := &DebugSink{}

	msg := protocol.Message{
		Schema: &protocol.SchemaMetadata{
			Schema: "public",
			Table:  "orders",
		},
	}
	assert.Equal(t, "public", s.getSchemaName(msg))

	msg = protocol.Message{}
	assert.Equal(t, "", s.getSchemaName(msg))
}

func TestComputeHash(t *testing.T) {
	payload1 := map[string]interface{}{
		"id":   1,
		"name": "test",
	}
	payload2 := map[string]interface{}{
		"id":   1,
		"name": "test",
	}
	payload3 := map[string]interface{}{
		"id":   2,
		"name": "test",
	}

	hash1 := computeHash(payload1)
	hash2 := computeHash(payload2)
	hash3 := computeHash(payload3)

	assert.Equal(t, hash1, hash2, "identical payloads should have same hash")
	assert.NotEqual(t, hash1, hash3, "different payloads should have different hash")
	assert.Len(t, hash1, 64, "SHA-256 hash should be 64 hex characters")
}

func TestUUIDGeneration(t *testing.T) {
	id1 := uuid.New().String()
	id2 := uuid.New().String()

	assert.NotEqual(t, id1, id2, "UUIDs should be unique")
	assert.Len(t, id1, 36, "UUID should be 36 characters")
}

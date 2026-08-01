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

	msg := protocol.Message{Table: "orders", Op: protocol.OpInsert}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "products", Op: protocol.OpInsert}
	assert.False(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureMessage_ExcludeTables(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{
			ExcludeTables: []string{"logs", "temp_*"},
		},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "orders", Op: protocol.OpInsert}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "logs", Op: protocol.OpInsert}
	assert.False(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "temp_table", Op: protocol.OpInsert}
	assert.False(t, s.shouldCaptureMessage(msg))
}

func TestShouldCaptureMessage_IncludeOperations(t *testing.T) {
	cfg := &Config{
		Filters: FiltersConfig{
			IncludeOperations: []string{"insert", "update"},
		},
	}
	s := &DebugSink{config: cfg}

	msg := protocol.Message{Table: "orders", Op: protocol.OpInsert}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "orders", Op: protocol.OpUpdate}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "orders", Op: protocol.OpDelete}
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

	msg := protocol.Message{Table: "orders", Op: protocol.OpInsert}
	assert.True(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "orders", Op: protocol.OpDelete}
	assert.False(t, s.shouldCaptureMessage(msg))

	msg = protocol.Message{Table: "users", Op: protocol.OpInsert}
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

// TestMatchesWildcard_DotNotTreatedAsRegexAny is the regression guard for
// MULTI_SCHEMA_PLAN.md §7.4 item 10. The old implementation built its regex
// as "^" + strings.ReplaceAll(pattern, "*", ".*") + "$" without escaping any
// other character first, so a literal "." in an operator-supplied filter
// pattern was interpreted as "any character" by the underlying regexp
// engine. A filter for "order." (e.g. an operator trying to match a table
// literally named "order.staging", or simply typing a stray dot) would then
// also match "orderX", "orderZ", etc. -- calling the real matchesWildcard
// function, not a recomputed pattern, so reverting the QuoteMeta fix makes
// this fail.
func TestMatchesWildcard_DotNotTreatedAsRegexAny(t *testing.T) {
	assert.True(t, matchesWildcard("order.", "order."), "exact literal match must still work")
	assert.False(t, matchesWildcard("order.", "orderX"), "a literal \".\" must not match an arbitrary character")
	assert.False(t, matchesWildcard("order.", "orders"), "a literal \".\" must not match an arbitrary character")

	// Combined with a real wildcard: only the "*" should behave as regex-any;
	// the "." stays literal.
	assert.True(t, matchesWildcard("sales.*", "sales.orders"))
	assert.False(t, matchesWildcard("sales.*", "salesXorders"), "the literal \".\" between sales and * must not match \"X\"")
}

// TestMatchesWildcard_SpecialCharsStayLiteralInWildcardPath forces the
// regex-building path (a "*" is present) with other regex-special
// characters in the same pattern, and asserts they are treated as literal
// text rather than regex syntax -- e.g. "(unclosed" would make
// `regexp.MustCompile`/`regexp.MatchString` return a compile error under the
// old "^"+ReplaceAll(pattern,"*",".*")+"$" construction; the fixed version
// quotes every non-"*" segment via regexp.QuoteMeta first, so it can never
// produce an invalid pattern from arbitrary operator input.
func TestMatchesWildcard_SpecialCharsStayLiteralInWildcardPath(t *testing.T) {
	assert.True(t, matchesWildcard("a(b*", "a(bXYZ"), "\"(\" must be literal, not regex-grouping")
	assert.False(t, matchesWildcard("a(b*", "aQbXYZ"))

	assert.True(t, matchesWildcard("(unclosed*", "(unclosedXYZ"), "an unbalanced \"(\" must not error out or panic")
	assert.False(t, matchesWildcard("(unclosed*", "unclosedXYZ"))
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

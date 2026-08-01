package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidation(t *testing.T) {
	t.Run("GlobalConfig", func(t *testing.T) {
		c := GlobalConfig{BatchSize: 0}
		if err := c.Validate(); err == nil {
			t.Error("Expected error for 0 BatchSize")
		}
		c.BatchSize = 100
		c.BatchWait = 0
		if err := c.Validate(); err == nil {
			t.Error("Expected error for 0 BatchWait")
		}
		c.BatchWait = 5 * time.Second
		if err := c.Validate(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("SourceConfig", func(t *testing.T) {
		c := SourceConfig{ID: "invalid id"}
		if err := c.Validate(); err == nil {
			t.Error("Expected error for invalid ID")
		}
		c.ID = "s1"
		c.Type = "postgres"
		c.Host = "localhost"
		c.Port = 5432
		c.User = "u"
		c.PassEncrypted = "p"
		c.Database = "db"
		if err := c.Validate(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("SinkConfig", func(t *testing.T) {
		c := SinkConfig{ID: "s1", Type: "invalid"}
		if err := c.Validate(); err == nil {
			t.Error("Expected error for invalid type")
		}
		c.Type = "databend"
		c.DSN = "http://..."
		if err := c.Validate(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("PipelineConfig", func(t *testing.T) {
		c := PipelineConfig{ID: "p1", Name: "n"}
		if err := c.Validate(); err == nil {
			t.Error("Expected error for missing sources/sinks")
		}
		c.Sources = []string{"s1"}
		c.Sinks = []string{"snk1"}
		c.Tables = []string{"table1"}
		if err := c.Validate(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("UserConfig", func(t *testing.T) {
		c := UserConfig{Username: ""}
		if err := c.Validate(); err == nil {
			t.Error("Expected error for empty username")
		}
		c.Username = "admin"
		c.Password = "admin"
		if err := c.Validate(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	// SourceConfig.Schemas/Tables and PipelineConfig.Tables previously had no
	// per-entry validation at all (MULTI_SCHEMA_PLAN.md §3 Stage 1, "Add
	// Validate() rules for Schemas/Tables"); an entry containing "=" would
	// silently corrupt TableRef.KeyToken()'s injectivity (§2.3) instead of
	// being rejected at config-write time.
	t.Run("SourceConfig rejects invalid Schemas/Tables entries", func(t *testing.T) {
		base := SourceConfig{ID: "s1", Type: "postgres", Host: "localhost", Port: 5432, Database: "db"}

		valid := base
		valid.Schemas = []string{"public", "sales"}
		valid.Tables = []string{"orders", "sales.orders"}
		require.NoError(t, valid.Validate())

		badSchema := base
		badSchema.Schemas = []string{"sales=evil"}
		assert.Error(t, badSchema.Validate())

		badTable := base
		badTable.Tables = []string{"a.b.c"}
		assert.Error(t, badTable.Validate())
	})

	t.Run("PipelineConfig rejects invalid Tables entries", func(t *testing.T) {
		base := PipelineConfig{ID: "p1", Name: "n", Sources: []string{"s1"}, Sinks: []string{"snk1"}}

		valid := base
		valid.Tables = []string{"orders", "sales.orders"}
		require.NoError(t, valid.Validate())

		bad := base
		bad.Tables = []string{"sales=orders"}
		assert.Error(t, bad.Validate())
	})
}

// TestParseTableStatsKey exercises the real production parser (not a
// hand-rolled reimplementation) against qualified, unqualified, and
// malformed keys. Reverting the both-ends rewrite back to the old positional
// `len(parts) < 10 || parts[9] != "stats"` check makes the "table token
// containing dots" case below (which the old code could never produce
// itself, but which is exactly the shape ParseTableStatsKey must tolerate
// per §2.3) fail: TestParseTableStatsKey_ToleratesDottedToken pins that.
func TestParseTableStatsKey(t *testing.T) {
	t.Run("qualified", func(t *testing.T) {
		key := TableStatsKey("p1", "s1", "sink1", TableRef{Schema: "sales", Table: "orders"})
		info := ParseTableStatsKey(key)
		require.NotNil(t, info)
		assert.Equal(t, "p1", info.PipelineID)
		assert.Equal(t, "s1", info.SourceID)
		assert.Equal(t, "sink1", info.SinkID)
		assert.Equal(t, "sales=orders", info.Table)
	})

	t.Run("unqualified", func(t *testing.T) {
		key := TableStatsKey("p1", "s1", "sink1", TableRef{Schema: "public", Table: "orders"})
		info := ParseTableStatsKey(key)
		require.NotNil(t, info)
		assert.Equal(t, "orders", info.Table)
	})

	t.Run("malformed", func(t *testing.T) {
		cases := []string{
			"",
			"not.a.stats.key",
			"cdc.pipeline.p1.sources.s1.sinks.sink1.tables.orders.NOTstats",
			"cdc.worker.p1.sources.s1.sinks.sink1.tables.orders.stats", // wrong 2nd token
		}
		for _, key := range cases {
			t.Run(key, func(t *testing.T) {
				assert.Nil(t, ParseTableStatsKey(key))
			})
		}
	})
}

// TestParseTableStatsKey_ToleratesDottedToken is the regression test for the
// positional-parsing bug: the old implementation asserted parts[9] == "stats"
// and silently returned nil for any key whose table token contained an extra
// ".". This has teeth -- reverting ParseTableStatsKey to
// `strings.Split(key, ".")` + `parts[9] == "stats"` makes this fail, because
// a dotted token pushes "stats" to a different fixed index.
func TestParseTableStatsKey_ToleratesDottedToken(t *testing.T) {
	key := "cdc.pipeline.p1.sources.s1.sinks.sink1.tables.weird.dotted.token.stats"
	info := ParseTableStatsKey(key)
	require.NotNil(t, info)
	assert.Equal(t, "weird.dotted.token", info.Table)
}

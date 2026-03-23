package protocol

import (
	"testing"
	"time"
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
}

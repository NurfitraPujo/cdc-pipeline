package postgresdebug

import (
	"context"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestDebugSink_Integration(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)
	defer postgresContainer.Terminate(ctx)

	// Get connection details
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)

	dsn := "postgres://testuser:testpass@" + host + ":" + mappedPort.Port() + "/testdb?sslmode=disable"

	// Create debug sink config
	cfg := &Config{
		TableName:       "cdc_debug_test",
		SchemaTableName: "cdc_debug_schema_test",
		Retention: RetentionConfig{
			Mode:            "disabled",
			CleanupInterval: time.Hour,
		},
		Filters: FiltersConfig{
			IncludeOperations: []string{"insert", "update", "delete"},
		},
		Sampling: SamplingConfig{
			Mode: "disabled",
		},
		Capture: CaptureConfig{
			Stages:         []string{"before", "after"},
			IncludePayload: true,
		},
	}

	// Create the debug sink
	sink, err := NewDebugSink("debug-sink-1", dsn, cfg)
	require.NoError(t, err)
	defer sink.Stop()

	t.Run("Table Created", func(t *testing.T) {
		// Table should be auto-created
		// Query to verify table exists
		var count int
		err := sink.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cdc_debug_test'",
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Capture Schema Change", func(t *testing.T) {
		msg := protocol.Message{
			SourceID: "source1",
			Table:    "orders",
			Op:       "schema_change",
			Schema: &protocol.SchemaMetadata{
				Table:  "orders",
				Schema: "public",
				Columns: map[string]string{
					"id":   "int",
					"name": "varchar",
				},
			},
			Timestamp: time.Now(),
		}

		err := sink.BatchUpload(ctx, []protocol.Message{msg})
		assert.NoError(t, err)

		// Verify it was captured
		var rowCount int
		err = sink.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_schema_test",
		).Scan(&rowCount)
		require.NoError(t, err)
		assert.Equal(t, 1, rowCount)
	})

	t.Run("Capture Before Message", func(t *testing.T) {
		// Use CaptureBefore directly
		messages := []protocol.Message{
			{
				SourceID:  "source1",
				Table:     "orders",
				Op:        "insert",
				LSN:       100,
				PK:        "1",
				UUID:      "msg-uuid-1",
				Data:      map[string]interface{}{"id": 1, "name": "Order 1"},
				Timestamp: time.Now(),
			},
		}

		sink.CaptureBefore(ctx, "pipeline-1", []string{}, messages)

		// Verify before record was captured
		var rowCount int
		err = sink.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_test WHERE capture_stage = 'before' AND table_name = 'orders'",
		).Scan(&rowCount)
		require.NoError(t, err)
		assert.Equal(t, 1, rowCount)
	})

	t.Run("Capture After Message", func(t *testing.T) {
		originals := []protocol.Message{
			{
				SourceID:  "source1",
				Table:     "orders",
				Op:        "insert",
				LSN:       101,
				PK:        "2",
				UUID:      "msg-uuid-2",
				Data:      map[string]interface{}{"id": 2, "name": "Original"},
				Timestamp: time.Now(),
			},
		}

		transformed := []protocol.Message{
			{
				SourceID:  "source1",
				Table:     "orders",
				Op:        "insert",
				LSN:       101,
				PK:        "2",
				UUID:      "msg-uuid-2",
				Data:      map[string]interface{}{"id": 2, "name": "Transformed"},
				Timestamp: time.Now(),
			},
		}

		sink.CaptureAfter(ctx, "pipeline-1", []string{"550e8400-e29b-41d4-a716-446655440000"}, []string{}, originals, transformed, nil)

		// Verify after record was captured
		var rowCount int
		err = sink.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_test WHERE capture_stage = 'after' AND table_name = 'orders'",
		).Scan(&rowCount)
		require.NoError(t, err)
		assert.Equal(t, 1, rowCount)
	})

	t.Run("Filter by Operation Type", func(t *testing.T) {
		// Create sink with only insert operation
		cfg := &Config{
			TableName:       "cdc_debug_filtered",
			SchemaTableName: "cdc_debug_schema_filtered",
			Retention: RetentionConfig{
				Mode: "disabled",
			},
			Filters: FiltersConfig{
				IncludeOperations: []string{"insert"},
			},
			Sampling: SamplingConfig{
				Mode: "disabled",
			},
			Capture: CaptureConfig{
				Stages: []string{"before"},
			},
		}

		sink2, err := NewDebugSink("debug-sink-2", dsn, cfg)
		require.NoError(t, err)
		defer sink2.Stop()

		// Insert message - should be captured
		insertMsg := []protocol.Message{
			{SourceID: "s1", Table: "t1", Op: "insert", Data: map[string]interface{}{"id": 1}},
		}
		sink2.CaptureBefore(ctx, "p1", []string{}, insertMsg)

		// Delete message - should NOT be captured
		deleteMsg := []protocol.Message{
			{SourceID: "s1", Table: "t1", Op: "delete", Data: map[string]interface{}{"id": 2}},
		}
		sink2.CaptureBefore(ctx, "p1", []string{}, deleteMsg)

		// Verify only insert was captured
		var count int
		err = sink2.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_filtered WHERE operation_type = 'insert'",
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		err = sink2.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_filtered WHERE operation_type = 'delete'",
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("Table Filtering", func(t *testing.T) {
		cfg := &Config{
			TableName:       "cdc_debug_table_filter",
			SchemaTableName: "cdc_debug_schema_table_filter",
			Retention: RetentionConfig{
				Mode: "disabled",
			},
			Filters: FiltersConfig{
				IncludeTables: []string{"orders"},
			},
			Sampling: SamplingConfig{
				Mode: "disabled",
			},
			Capture: CaptureConfig{
				Stages: []string{"before"},
			},
		}

		sink3, err := NewDebugSink("debug-sink-3", dsn, cfg)
		require.NoError(t, err)
		defer sink3.Stop()

		// Orders should be captured
		ordersMsg := []protocol.Message{
			{SourceID: "s1", Table: "orders", Op: "insert", Data: map[string]interface{}{"id": 1}},
		}
		sink3.CaptureBefore(ctx, "p1", []string{}, ordersMsg)

		// Users should NOT be captured
		usersMsg := []protocol.Message{
			{SourceID: "s1", Table: "users", Op: "insert", Data: map[string]interface{}{"id": 1}},
		}
		sink3.CaptureBefore(ctx, "p1", []string{}, usersMsg)

		var count int
		err = sink3.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_table_filter WHERE table_name = 'orders'",
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		err = sink3.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_debug_table_filter WHERE table_name = 'users'",
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

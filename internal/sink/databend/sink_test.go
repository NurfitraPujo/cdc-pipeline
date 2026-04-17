package databend

import (
	"context"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestDatabendSink(t *testing.T) {
	ctx := context.Background()
	// Using stable Databend nightly image known to work better in constrained envs
	req := testcontainers.ContainerRequest{
		Image:        "datafuselabs/databend:v1.2.345-nightly",
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor:   wait.ForHTTP("/v1/health").WithPort("8000/tcp").WithStartupTimeout(5 * time.Minute),
	}
	dbC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Skipping Databend test due to startup failure: %v", err)
		return
	}
	defer dbC.Terminate(ctx)

	host, _ := dbC.Host(ctx)
	port, _ := dbC.MappedPort(ctx, "8000")
	dsn := "http://root:@ " + host + ":" + port.Port()

	sink, err := NewDatabendSink("snk1", dsn)
	assert.NoError(t, err)
	defer sink.Stop()

	t.Run("Apply Schema and Upload Batch", func(t *testing.T) {
		schema := protocol.SchemaMetadata{
			Table: "test_sink",
			Columns: map[string]string{
				"id":   "INT64",
				"name": "STRING",
			},
			PKColumns: []string{"id"},
		}
		err = sink.ApplySchema(ctx, protocol.Message{Op: "schema_change", Schema: &schema})
		assert.NoError(t, err)

		batch := []protocol.Message{
			{
				SourceID: "s1",
				Table:    "test_sink",
				Op:       "insert",
				Payload:  []byte(`{"id":1, "name":"a"}`),
			},
		}
		err = sink.BatchUpload(ctx, batch)
		assert.NoError(t, err)
	})
}

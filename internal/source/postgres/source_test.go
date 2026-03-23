package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresSource(t *testing.T) {
	ctx := context.Background()
	pgC, err := tc_postgres.Run(ctx,
		"postgres:16-alpine",
		tc_postgres.WithDatabase("testdb"),
		tc_postgres.WithUsername("testuser"),
		tc_postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start Postgres: %v", err)
	}
	defer pgC.Terminate(ctx)

	src := NewPostgresSource("s1")
	assert.Equal(t, "s1", src.Name())

	t.Run("Start and Stop", func(t *testing.T) {
		// Stop is safe even if not started
		err := src.Stop()
		assert.NoError(t, err)
	})
}

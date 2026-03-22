package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func StartPostgres(ctx context.Context) (*postgres.PostgresContainer, error) {
	return postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("daya_src"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second)),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"postgres", "-c", "wal_level=logical"},
			},
		}),
	)
}

func StartNats(ctx context.Context) (*nats.NATSContainer, error) {
	return nats.Run(ctx,
		"nats:2.10-alpine",
		nats.WithJetStream(),
	)
}

func StartDatabend(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "datafuselabs/databend:latest",
			ExposedPorts: []string{"8000/tcp", "3307/tcp"},
			Wait:         wait.ForHTTP("/v1/health").WithPort("8000"),
		},
		Started: true,
	}
	c, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return nil, "", err
	}

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "8000")
	// Databend DSN format for databend-go: http://root:root@localhost:8000
	dsn := fmt.Sprintf("http://root:@%s:%s", host, port.Port())
	return c, dsn, nil
}

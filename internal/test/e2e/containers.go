package e2e

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testContainerProvider = testcontainers.ProviderDocker
)

func SetTestContainerProvider() {
	if os.Getenv("TESTCONTAINER_PROVIDER") == "podman" {
		testContainerProvider = testcontainers.ProviderPodman
	}
}

func StartPostgres(ctx context.Context) (*postgres.PostgresContainer, error) {
	return postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("cdc_src"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-c", "wal_level=logical"},
			},
			ProviderType: testContainerProvider,
		}),
	)
}

func StartNats(ctx context.Context) (*nats.NATSContainer, error) {
	return nats.Run(ctx,
		"nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-js"},
			},
			ProviderType: testContainerProvider,
		}),
	)
}

func StartDatabend(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "datafuselabs/databend:latest",
			ExposedPorts: []string{"8000/tcp"},
			WaitingFor:   wait.ForListeningPort("8000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started:      true,
		ProviderType: testContainerProvider,
	}
	c, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return nil, "", err
	}

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "8000")
	// Databend DSN format for databend-go: http://root:@localhost:8000
	dsn := fmt.Sprintf("http://root:@%s:%s", host, port.Port())
	return c, dsn, nil
}

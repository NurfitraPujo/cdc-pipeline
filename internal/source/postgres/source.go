package postgres

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/pqcdc"
	"github.com/fitrapujo/daya-data-pipeline/internal/protocol"
)

type PostgresSource struct {
	name string
	// Inner fields for go-pq-cdc configuration
}

func NewPostgresSource(name string) *PostgresSource {
	return &PostgresSource{name: name}
}

func (s *PostgresSource) Name() string {
	return s.name
}

func (s *PostgresSource) Start(ctx context.Context, config protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, error) {
	out := make(chan []protocol.Message)

	go func() {
		defer close(out)

		// Implementation of go-pq-cdc logic
		// 1. If checkpoint.Status == "Snapshotting" (or initial start), run snapshot
		// 2. Capture LSN before snapshot
		// 3. Emit Snapshot batches via 'out'
		// 4. Transition to "CDC"
		// 5. Start go-pq-cdc CDC from captured LSN
		// 6. Emit CDC events via 'out'
	}()

	return out, nil
}

func (s *PostgresSource) Stop() error {
	// Stop internal go-pq-cdc workers
	return nil
}

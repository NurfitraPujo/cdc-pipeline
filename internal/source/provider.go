package source

import (
	"context"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type Source interface {
	Name() string
	// Start begins fetching data from the source.
	// It should intelligently handle the transition from Snapshotting to CDC.
	Start(ctx context.Context, config protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, error)
	Stop() error
}

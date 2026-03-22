package source

import (
	"context"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type Source interface {
	Name() string
	// Start begins fetching data from the source.
	// It should intelligently handle the transition from Snapshotting to CDC.
	// msgChan: Channel to send batches of messages to.
	// ackChan: Channel to receive acknowledgments from the producer (one per batch).
	Start(ctx context.Context, config protocol.SourceConfig, checkpoint protocol.Checkpoint) (msgChan <-chan []protocol.Message, ackChan chan<- struct{}, err error)
	Stop() error
}

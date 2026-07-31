package source

import (
	"context"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// SourceAck tells the source that one sink has durably written a set of
// LSNs. It is the typed payload the engine forwards on the ack channel
// returned by Start, replacing the old anonymous struct{}{} signal
// that carried no LSN or sink identity.
type SourceAck struct {
	SinkID string
	LSNs   []uint64
}

type Source interface {
	Name() string
	// Start begins replication and returns a channel of decoded message
	// batches plus a channel the engine uses to report durable sink
	// writes back to the source. ackers is the set of sink IDs whose
	// confirmation is required before the slot may advance past an LSN.
	Start(ctx context.Context, config protocol.SourceConfig, checkpoint protocol.Checkpoint, ackers []string) (msgChan <-chan []protocol.Message, ackChan chan<- SourceAck, err error)
	Stop() error
	AlterPublication(ctx context.Context, tableName string) error
	UpdateXLogPos(ctx context.Context, lsn uint64) error
}

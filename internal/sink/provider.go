package sink

import (
	"context"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type Sink interface {
	Name() string
	// BatchUpload sends a batch of messages to the analytical sink.
	// It should handle deduplication and ensures idempotency.
	BatchUpload(ctx context.Context, messages []protocol.Message) error
	Stop() error
}

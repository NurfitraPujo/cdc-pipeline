package engine

import (
	"context"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type PipelineWorker interface {
	ID() string
	Drain() error
	Finished() <-chan struct{}
	Shutdown(ctx context.Context) error
}

// Transformer defines a programmatic interface for data pre-processing.
type Transformer interface {
	// Name returns the identifier for this transformer.
	Name() string
	// Transform modifies a message before it is sent to the sink.
	// Returns (transformed_message, should_continue, error).
	// If should_continue is false, the message is dropped.
	Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error)
}

// TransformerFactory is a function that creates a Transformer from config.
type TransformerFactory func(options map[string]interface{}) (Transformer, error)

var transformerRegistry = make(map[string]TransformerFactory)

// RegisterTransformer adds a new transformer factory to the global registry.
func RegisterTransformer(transformerType string, factory TransformerFactory) {
	transformerRegistry[transformerType] = factory
}

// GetTransformer retrieves a transformer factory by type.
func GetTransformer(transformerType string) (TransformerFactory, bool) {
	f, ok := transformerRegistry[transformerType]
	return f, ok
}

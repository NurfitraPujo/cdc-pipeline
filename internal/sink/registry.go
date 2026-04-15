package sink

import (
	"fmt"
)

// Factory is a function that creates a Sink from configuration.
type Factory func(sinkID string, dsn string, options map[string]interface{}) (Sink, error)

var registry = make(map[string]Factory)

// Register adds a new sink factory to the global registry.
func Register(sinkType string, factory Factory) {
	registry[sinkType] = factory
}

// New creates a new sink of the given type.
func New(sinkType string, sinkID string, dsn string, options map[string]interface{}) (Sink, error) {
	factory, ok := registry[sinkType]
	if !ok {
		return nil, fmt.Errorf("sink type %q not registered", sinkType)
	}
	return factory(sinkID, dsn, options)
}

// IsDebug returns true if the sink type is a debug sink.
// This helps the factory decide whether to wire up hooks.
func IsDebug(sinkType string) bool {
	return sinkType == "postgres_debug"
}

package transformer

import (
	"context"
	"fmt"
	"plugin"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

// Transformer defines a programmatic interface for data pre-processing.
type Transformer interface {
	Name() string
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

// LoadPlugin loads a transformer from a Go plugin (.so file).
// The plugin must export a variable named 'Transformer' that implements Transformer,
// OR a function named 'NewTransformer' that returns (Transformer, error).
func LoadPlugin(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("could not open plugin %s: %w", path, err)
	}

	symbol, err := p.Lookup("Factory")
	if err != nil {
		return fmt.Errorf("plugin %s must export a 'Factory' variable of type TransformerFactory: %w", path, err)
	}

	factory, ok := symbol.(*TransformerFactory)
	if !ok {
		return fmt.Errorf("symbol 'Factory' in plugin %s is not a TransformerFactory pointer", path)
	}

	// We'll use the filename (without extension) as the default type name if registration isn't handled by the plugin's init
	// But usually, the plugin's init() will call RegisterTransformer if it's compiled with access to this package.
	// For external plugins, we might need a manual registration step here.
	_ = factory
	return nil
}

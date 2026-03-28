# Internal Transformer: Pre-processing Plugins

The `internal/transformer` package provides a dedicated, extensible framework for data pre-processing. It supports both built-in transformers and dynamically loaded Go plugins.

## Core Design

The system uses a **Registry-based PnP Architecture**. Transformers are registered with a factory function and instantiated based on pipeline configuration.

### The `Transformer` Interface

```go
type Transformer interface {
    Name() string
    Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error)
}
```

## Plugin Support (Aspirational)

**Note**: Runtime plugin loading via Go plugins (`.so` files) is aspirational and not yet implemented. The current implementation uses a compile-time registry pattern.

When implemented, the approach would be:

### 1. Creating a Plugin

Create a new Go project (or file) with `package main`:

```go
package main

import (
	"context"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
)

type MyTransformer struct{}
func (t *MyTransformer) Name() string { return "my-plugin" }
func (t *MyTransformer) Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error) {
    // Custom logic here
    return m, true, nil
}

// Exported symbol 'Factory'
var Factory transformer.TransformerFactory = func(options map[string]interface{}) (transformer.Transformer, error) {
	return &MyTransformer{}, nil
}

func main() {}
```

### 2. Compiling the Plugin

```bash
go build -buildmode=plugin -o my_plugin.so my_plugin.go
```

### 3. Loading the Plugin

Future implementation would call `transformer.LoadPlugin("/path/to/my_plugin.so")` at startup.

**Current Approach**: Use `RegisterTransformer()` in your code to add custom transformers at compile time.

## Built-in Transformers

- **`mask`**: Hashes PII fields.
- **`uppercase`**: Converts specific string columns to uppercase.

## Conventions

- **Registration**: All built-in transformers call `RegisterTransformer` in their `init()` block.
- **Configuration**: Mapped via `ProcessorConfig` in the pipeline settings.
- **Error Handling**: Transformers should log errors but return the original message if the transformation is non-critical, or return `false` for `should_continue` to drop the message.

package engine

import (
	"context"
)

type PipelineWorker interface {
	ID() string
	Drain() error
	Finished() <-chan struct{}
	Shutdown(ctx context.Context) error
}

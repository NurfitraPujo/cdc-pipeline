package sink

import (
	"context"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

type PreTransformHook func(ctx context.Context, pipelineID string, transformerNames []string, messages []protocol.Message) []string

type PostTransformHook func(ctx context.Context, pipelineID string, correlationIDs []string, transformerNames []string, originals []protocol.Message, transformed []protocol.Message, filteredIndices []int)

type DebugCapturer interface {
	CaptureBefore(ctx context.Context, pipelineID string, transformerNames []string, messages []protocol.Message) []string
	CaptureAfter(ctx context.Context, pipelineID string, correlationIDs []string, transformerNames []string, originals []protocol.Message, transformed []protocol.Message, filteredIndices []int)
}

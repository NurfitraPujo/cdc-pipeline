package stream

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher interface {
	Publish(topic string, messages ...*message.Message) error
	Close() error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error)
	Close() error
}

// PendingCounter is an optional capability implemented by subscribers that
// can report their JetStream consumer's backlog size (NumPending). Consumers
// of a stream.Subscriber should type-assert for this rather than requiring
// it on the base interface, since not every Subscriber (e.g. test doubles)
// is backed by a real JetStream consumer. Used to detect an empty backlog
// deterministically instead of via a client-side idle timeout (plan 01a
// WI-9).
type PendingCounter interface {
	PendingCount(ctx context.Context) (uint64, error)
}

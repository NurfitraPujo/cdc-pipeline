package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// mockKeyWatcher is a test stub that implements nats.KeyWatcher.
type mockKeyWatcher struct {
	updates chan nats.KeyValueEntry
	stopped atomic.Bool
}

func (m *mockKeyWatcher) Context() context.Context {
	return context.Background()
}

func (m *mockKeyWatcher) Updates() <-chan nats.KeyValueEntry {
	return m.updates
}

func (m *mockKeyWatcher) Stop() error {
	if m.stopped.CompareAndSwap(false, true) {
		close(m.updates)
	}
	return nil
}

func TestWatchKeyValueUpdates_ClosesOnChannelClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := &mockKeyWatcher{updates: make(chan nats.KeyValueEntry, 1)}
	w.Stop() // close the channel before passing to helper

	var called atomic.Bool
	watchKeyValueUpdates(ctx, w, func(entry nats.KeyValueEntry) {
		called.Store(true)
	})

	select {
	case <-ctx.Done():
		t.Fatal("watchKeyValueUpdates did not return within timeout after channel close")
	default:
		// expected path: returns immediately
	}
	if called.Load() {
		t.Error("onUpdate handler should not have been called")
	}
}

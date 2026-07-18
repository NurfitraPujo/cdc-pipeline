package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// KeyValueUpdateHandler is called for each update received from the watcher.
type KeyValueUpdateHandler func(entry nats.KeyValueEntry)

// watchKeyValueUpdates runs the watcher loop, calling onUpdate for each entry.
// It returns cleanly when the watcher's Updates channel is closed.
func watchKeyValueUpdates(ctx context.Context, watcher nats.KeyWatcher, onUpdate KeyValueUpdateHandler) {
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				log.Warn().Msg("watcher closed, exiting key-value update loop")
				return
			}
			onUpdate(entry)
		}
	}
}

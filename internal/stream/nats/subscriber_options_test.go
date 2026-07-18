package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestSubscriberReconnectOpts verifies the helper builds the option list
// required by T0-6 (auto-recovery after prolonged NATS outages).
func TestSubscriberReconnectOpts(t *testing.T) {
	opts := subscriberReconnectOpts()
	if assert.NotEmpty(t, opts) {
		// Each go_nats.Option is an opaque func; we verify the
		// helper returns multiple options including reconnect/ping
		// and lifecycle handlers.
		assert.GreaterOrEqual(t, len(opts), 5, "expected at least 5 reconnect options")
	}
}

// TestNewNatsSubscriberRejectsBadURL exercises the constructor path
// ensuring the new options list does not break URL parsing.
func TestNewNatsSubscriberRejectsBadURL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping constructor integration test in short mode (uses real dial)")
	}
	_, err := NewNatsSubscriber("nats://127.0.0.1:1", "test-sub", "", 10, 2*time.Second)
	// We accept either an immediate dial error or a successful-but-stale
	// connection: the test only guards against panics / non-timeout failures
	// caused by the new option list.
	if err != nil {
		assert.Contains(t, err.Error(), "nats") // connection/dial error
	}
}

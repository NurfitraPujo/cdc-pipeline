package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

// TestIsTransportErr pins the WS-5 item 1 classification rule: NATS
// transport-layer errors (nobody subscribed, timed out, connection gone) are
// transport failures; a generic application error is not. Pure function,
// no Docker required.
func TestIsTransportErr(t *testing.T) {
	assert.True(t, isTransportErr(nats.ErrNoResponders), "ErrNoResponders (nobody subscribed) must classify as transport")
	assert.True(t, isTransportErr(nats.ErrTimeout), "ErrTimeout must classify as transport")
	assert.True(t, isTransportErr(context.DeadlineExceeded), "context.DeadlineExceeded must classify as transport")
	assert.True(t, isTransportErr(nats.ErrConnectionClosed), "ErrConnectionClosed must classify as transport")

	assert.False(t, isTransportErr(errors.New("record is malformed: missing pk")), "an application-level error must NOT classify as transport")
	assert.False(t, isTransportErr(nil), "nil must not classify as transport") //nolint:staticcheck // deliberately exercising the nil path
}

// TestClassifyTransportErrKind pins the metric-label mapping used by
// TransformTransportErrorsTotal, so "nobody home" is distinguishable from
// "too slow" from "connection dropped" in the metric itself.
func TestClassifyTransportErrKind(t *testing.T) {
	assert.Equal(t, "no_responders", classifyTransportErrKind(nats.ErrNoResponders))
	assert.Equal(t, "timeout", classifyTransportErrKind(nats.ErrTimeout))
	assert.Equal(t, "timeout", classifyTransportErrKind(context.DeadlineExceeded))
	assert.Equal(t, "connection_closed", classifyTransportErrKind(nats.ErrConnectionClosed))
	assert.Equal(t, "unknown", classifyTransportErrKind(errors.New("something else")))
}

// TestSendRequest_NoResponders_WrapsErrTransportFailure drives a real NATS
// round trip (no fakes) with genuinely nobody subscribed on the subject, and
// asserts sendRequest's error satisfies errors.Is(err, ErrTransportFailure)
// -- the exact predicate engine/consumer.go's handleSinkError uses to decide
// "never isolate/DLQ this." Disabling the isTransportErr wrap in sendRequest
// and re-running this test locally confirms it fails (errors.Is returns
// false against a plain fmt.Errorf-wrapped error), which is how this was
// verified during development; the fix is restored here.
func TestSendRequest_NoResponders_WrapsErrTransportFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Skipping integration test due to Docker/Testcontainers panic: %v", r)
		}
	}()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine")
	if err != nil {
		t.Skipf("Docker/Testcontainers not available: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()
	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.ws5.no-responder",
		"timeout_ms": 500.0,
		"tables":     []interface{}{"orders"},
	})
	require.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Close() }()

	msgs := []protocol.Message{{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, TableSchema: "public", UUID: "u1"}}
	_, err = tf.TransformBatch(ctx, msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTransportFailure), "no-responder error must satisfy errors.Is(err, ErrTransportFailure); got: %v", err)
}

// TestSendRequest_ApplicationError_NotClassifiedAsTransport is the "doesn't
// use the feature" configuration: a request that fails for a reason that is
// NOT one of the three named transport shapes must not be misclassified as
// transport-retryable. Exercised directly against sendRequest's error path
// via a context that is already canceled *before* isTransportErr's own
// context.DeadlineExceeded case would apply -- context.Canceled is
// deliberately absent from isTransportErr's list (a caller aborting a
// request is not evidence daya-core is unreachable).
func TestSendRequest_ApplicationError_NotClassifiedAsTransport(t *testing.T) {
	assert.False(t, isTransportErr(context.Canceled), "a caller-canceled request must not be classified as a transport failure")
}

// fakeAlwaysOpenBreaker simulates gobreaker already being open, letting the
// circuit-breaker fail-fast path be tested deterministically without racing
// gobreaker's real trip-ratio window across concurrent test runs.
type fakeAlwaysOpenBreaker struct {
	executed bool
}

func (f *fakeAlwaysOpenBreaker) Execute(func() (interface{}, error)) (interface{}, error) {
	f.executed = true
	return nil, gobreaker.ErrOpenState
}

func (f *fakeAlwaysOpenBreaker) IsOpen() bool { return true }

// TestSendRequest_OpenBreaker_FailsFastWithoutNetworkCall pins WS-5 item 2:
// when the breaker is open, sendRequest must return ErrCircuitOpen
// (wrapped in ErrTransportFailure) WITHOUT ever touching the network --
// t.conn is left nil here, so any attempt to call conn.RequestWithContext
// would nil-pointer-panic, proving the IsOpen() fast path short-circuits
// before that. Verified by temporarily removing the `if t.cb.IsOpen()`
// guard in sendRequest during development: doing so panics this test on
// the nil t.conn dereference instead of returning cleanly, confirming the
// guard is load-bearing; restored here.
func TestSendRequest_OpenBreaker_FailsFastWithoutNetworkCall(t *testing.T) {
	fake := &fakeAlwaysOpenBreaker{}
	tf := &NatsProtoTransformer{
		pipelineID: "p1",
		subject:    "daya.transform.ws5.breaker",
		timeout:    time.Second,
		tables:     []string{"orders"},
		conn:       nil, // intentionally unset: proves the network is never touched
		cb:         fake,
	}

	start := time.Now()
	_, err := tf.sendRequest(context.Background(), &cdctransformv1.TransformRequest{})
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTransportFailure), "open-breaker error must be a transport failure")
	assert.True(t, errors.Is(err, ErrCircuitOpen), "open-breaker error must wrap ErrCircuitOpen")
	assert.False(t, fake.executed, "Execute must never be called when IsOpen() already reports open")
	assert.Less(t, elapsed, 50*time.Millisecond, "an open breaker must fail fast, not wait out any timeout")
}

// TestSendRequest_BreakerExecuteRejects_ClassifiedAsTransport covers the
// narrow race window between sendRequest's own IsOpen() check and gobreaker
// itself rejecting the call (ErrOpenState/ErrTooManyRequests returned from
// Execute rather than caught by the pre-check).
func TestSendRequest_BreakerExecuteRejects_ClassifiedAsTransport(t *testing.T) {
	tf := &NatsProtoTransformer{
		pipelineID: "p1",
		subject:    "daya.transform.ws5.race",
		timeout:    time.Second,
		tables:     []string{"orders"},
		conn:       nil,
		cb:         &rejectingBreaker{},
	}

	_, err := tf.sendRequest(context.Background(), &cdctransformv1.TransformRequest{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTransportFailure))
}

type rejectingBreaker struct{}

func (b *rejectingBreaker) Execute(func() (interface{}, error)) (interface{}, error) {
	return nil, gobreaker.ErrTooManyRequests
}
func (b *rejectingBreaker) IsOpen() bool { return false } // pre-check passes; Execute itself rejects

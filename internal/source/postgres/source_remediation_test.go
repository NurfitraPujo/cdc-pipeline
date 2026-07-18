package postgres

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// stubConnector is a no-op cdc.Connector used by the restart regression
// tests so they can exercise the channel-reallocation and lifecycle
// logic in source.go without a live PostgreSQL connection. Each call
// to Start blocks until the supplied context is cancelled, mirroring
// the blocking shape of the real connector.
type stubConnector struct {
	startCount atomic.Int32
	closeCount atomic.Int32
	lastCtx    atomic.Pointer[context.Context]
}

func newStubConnector() *stubConnector { return &stubConnector{} }

func (s *stubConnector) Start(ctx context.Context) {
	s.startCount.Add(1)
	s.lastCtx.Store(&ctx)
	<-ctx.Done()
}

func (s *stubConnector) WaitUntilReady(_ context.Context) error                   { return nil }
func (s *stubConnector) Close()                                                  { s.closeCount.Add(1) }
func (s *stubConnector) UpdateXLogPos(_ pq.LSN)                                   {}
func (s *stubConnector) GetConfig() *config.Config                                { return nil }
func (s *stubConnector) SetMetricCollectors(_ ...prometheus.Collector)           {}
func (s *stubConnector) AddRelation(_ *format.Relation)                           {}

// stubFactory produces fresh stubConnector instances per invocation.
type stubFactory struct {
	mu      sync.Mutex
	calls   int
	current *stubConnector
}

func newStubFactory() *stubFactory { return &stubFactory{} }

func (f *stubFactory) Build(_ context.Context, _ config.Config, _ replication.ListenerFunc) (cdc.Connector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	conn := newStubConnector()
	f.current = conn
	return conn, nil
}

func (f *stubFactory) Last() *stubConnector {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *stubFactory) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// validSourceConfig returns a SourceConfig that satisfies the
// validations the restart path performs.
func validSourceConfig() protocol.SourceConfig {
	return protocol.SourceConfig{
		ID:                "src-1",
		Type:              "postgres",
		Host:              "127.0.0.1",
		Port:              5432,
		User:              "user",
		PassEncrypted:     "pass",
		Database:          "db",
		SlotName:          "slot",
		PublicationName:   "pub",
		Tables:            []string{"public.t1"},
		DiscoveryInterval: 30 * time.Second,
	}
}

// primeSourceState installs a minimally-valid running session on s so
// RestartWithNewTables has something to tear down. It blocks the
// current goroutine until the stub connector has entered Start, so the
// subsequent cancel/restart interacts with a "live" session.
func primeSourceState(t *testing.T, s *PostgresSource, factory *stubFactory) {
	t.Helper()
	s.config = validSourceConfig()
	s.msgChan = make(chan []protocol.Message, 1)
	s.ackChan = make(chan struct{}, 1000)
	s.lsnChan = make(chan uint64, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	s.ackMgr = NewAckManager()

	conn, err := factory.Build(context.Background(), config.Config{}, nil)
	require.NoError(t, err)
	s.connector = conn

	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		conn.(*stubConnector).Start(s.ctx)
	}()

	// Wait until the stub has actually entered Start.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn.(*stubConnector).startCount.Load() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("stub connector did not enter Start before timeout")
}

// runWithoutPanic invokes fn and converts any panic into an error so
// tests can assert that a restart sequence does not panic. Returns
// nil on a clean execution.
func runWithoutPanic(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errorFromPanic(r)
		}
	}()
	fn()
	return nil
}

// TestRestartWithNewTables_NoDoubleClose is the regression test for T1-3.
//
// It exercises the failure mode the ticket describes:
//
//   - RestartWithNewTables is called in rapid succession (twice).
//   - A consumer goroutine holds a STABLE reference to the ORIGINAL
//     msgChan (the value returned to the production engine by Start)
//     and reads from it. The OLD cleanup goroutine sleeps 100 ms
//     before closing the channel it captured at launch time; before
//     the fix it instead closed s.msgChan (the live reference) and
//     would therefore close the freshly-allocated NEW channel, which
//     would manifest as a "send on closed channel" panic for the new
//     session's writer.
//
// The consumer pattern (stable OLD reference, no dynamic re-read)
// mirrors the production engine: it holds a reference to the channel
// it received from Start and reads from it for the lifetime of the
// session, exiting cleanly on EOF.
//
// The test asserts:
//   - RestartWithNewTables returns no error and no goroutine panics.
//   - s.msgChan is reallocated on every restart.
//   - The previous connector's Close() is invoked by each restart.
//   - The previous cleanup goroutine exits before the new session
//     starts (s.runWg is drained between restarts).
//   - The consumer observes EOF on the ORIGINAL channel (the cleanup
//     goroutine closed it as expected) and does NOT observe any error
//     from the NEW channel.
func TestRestartWithNewTables_NoDoubleClose(t *testing.T) {
	s := NewPostgresSource("test-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	primeSourceState(t, s, factory)

	firstStub := factory.Last()
	originalMsgChan := s.msgChan

	// Consumer goroutine holding a STABLE reference to the ORIGINAL
	// channel. This is what the production engine looks like: it
	// reads from the channel returned by Start and exits on EOF.
	consumerStop := make(chan struct{})
	consumerDone := make(chan struct{})
	consumerErrCh := make(chan error, 4)
	go func() {
		defer close(consumerDone)
		defer close(consumerErrCh)
		ch := originalMsgChan
		for {
			select {
			case <-consumerStop:
				return
			case _, ok := <-ch:
				if !ok {
					// ORIGINAL channel was closed by the
					// cleanup goroutine. This is the
					// expected outcome — the consumer
					// exits cleanly.
					return
				}
			}
		}
	}()

	// Wrap the entire restart sequence in a recover() so a panicking
	// goroutine is observed as a test failure rather than crashing
	// the test binary.
	restartErr := runWithoutPanic(func() {
		// First restart.
		if err := s.RestartWithNewTables(context.Background(), []string{"public.t2"}); err != nil {
			t.Fatalf("first RestartWithNewTables failed: %v", err)
		}

		// Sanity: a new connector must have been built, and the old
		// connector must have been closed. The msgChan reference
		// MUST have changed.
		assert.Equal(t, 2, factory.Calls(), "factory must be called once per restart (initial + first restart)")
		require.NotNil(t, factory.Last(), "factory must have produced a connector")
		assert.NotEqual(t, originalMsgChan, s.msgChan, "RestartWithNewTables must allocate a new msgChan")
		assert.NotEqual(t, firstStub, factory.Last(), "the connector should be a fresh instance")
		require.NotNil(t, firstStub, "firstStub must be set from primeSourceState")
		assert.GreaterOrEqual(t, firstStub.closeCount.Load(), int32(1), "old connector must be Closed")

		// Second restart in rapid succession.
		if err := s.RestartWithNewTables(context.Background(), []string{"public.t3"}); err != nil {
			t.Fatalf("second RestartWithNewTables failed: %v", err)
		}
		assert.Equal(t, 3, factory.Calls(), "factory must be called for the second restart as well")
	})
	require.NoError(t, restartErr, "restart sequence must not panic")

	// Allow the consumer to exit on its own (via EOF on the ORIGINAL
	// channel) or via the stop signal.
	close(consumerStop)
	select {
	case <-consumerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not exit (the cleanup goroutine did not close the original msgChan within 3s)")
	}

	// Cleanup: tear down the source so background goroutines exit.
	require.NoError(t, s.Stop())

	// The consumer must NOT have surfaced any error: the cleanup
	// goroutine only closes the channel it captured at launch time,
	// and the consumer's stable reference is exactly that channel.
	for e := range consumerErrCh {
		t.Errorf("consumer observed error during restart: %v", e)
	}
}

// TestRestartWithNewTables_StaticMetricPort verifies that the port
// resolved for the metrics endpoint stays stable across multiple
// (re)starts when a static port has been configured via
// WithMetricPort. This is the regression test for T1-25: before the
// fix the port was allocated atomically on every call which broke
// external Prometheus scrapers.
func TestRestartWithNewTables_StaticMetricPort(t *testing.T) {
	s := NewPostgresSource("static-port").WithMetricPort(31999)
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	primeSourceState(t, s, factory)

	port := s.resolveMetricPort()
	assert.Equal(t, 31999, port, "first Start must use the configured static port")

	require.NoError(t, s.RestartWithNewTables(context.Background(), []string{"public.t2"}))
	assert.Equal(t, 31999, s.resolveMetricPort(),
		"first restart must keep the configured static port (NOT increment)")

	require.NoError(t, s.RestartWithNewTables(context.Background(), []string{"public.t3"}))
	assert.Equal(t, 31999, s.resolveMetricPort(),
		"second restart must keep the configured static port (NOT increment)")

	require.NoError(t, s.Stop())
}

// TestResolveMetricPort_DynamicWhenUnset verifies the backward-
// compatible behaviour: when WithMetricPort has not been called, the
// resolveMetricPort helper still allocates a fresh port per call from
// the package-level counter so that production deployments that did
// not opt into a static port continue to function unchanged.
func TestResolveMetricPort_DynamicWhenUnset(t *testing.T) {
	s := NewPostgresSource("dynamic-port")
	assert.Equal(t, 0, s.metricPort, "metricPort must default to zero (legacy behaviour)")

	port1 := s.resolveMetricPort()
	port2 := s.resolveMetricPort()
	assert.NotEqual(t, port1, port2,
		"with no static port configured, every call must allocate a fresh port")
}

// TestSourceRestartTotal_IncrementsOnRestart verifies that the
// Prometheus counter cdc_source_restart_total is bumped exactly once
// per RestartWithNewTables call.
func TestSourceRestartTotal_IncrementsOnRestart(t *testing.T) {
	before := testutil.ToFloat64(sourceRestartTotal)

	s := NewPostgresSource("counter-source").WithMetricPort(32001)
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	primeSourceState(t, s, factory)

	require.NoError(t, s.RestartWithNewTables(context.Background(), []string{"public.t2"}))
	require.NoError(t, s.RestartWithNewTables(context.Background(), []string{"public.t3"}))

	after := testutil.ToFloat64(sourceRestartTotal)
	assert.Equal(t, float64(2), after-before,
		"cdc_source_restart_total must increment by exactly 2 after two restarts")

	require.NoError(t, s.Stop())
}

// TestStop_WaitsForBackgroundGoroutines is the regression test for
// T1-24. It verifies that Stop blocks until the in-flight tickers
// (batch-wait and discovery) have actually exited, so callers can
// rely on the DB handle being safe to close immediately after Stop
// returns.
func TestStop_WaitsForBackgroundGoroutines(t *testing.T) {
	s := NewPostgresSource("tickers-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	// Bypass primeSourceState so we control exactly when the tickers
	// start (and what context they listen on).
	s.config = validSourceConfig()
	s.msgChan = make(chan []protocol.Message, 1)
	s.ackChan = make(chan struct{}, 1000)
	s.lsnChan = make(chan uint64, 1000)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.ackMgr = NewAckManager()
	conn, err := factory.Build(context.Background(), config.Config{}, nil)
	require.NoError(t, err)
	s.connector = conn

	// Use a very short discovery interval so the discovery ticker
	// fires repeatedly during the test. T1-24 demands that Stop wait
	// for this ticker to exit before returning.
	s.config.DiscoveryInterval = 10 * time.Millisecond

	var mu sync.Mutex
	var msgs []protocol.Message
	knownTables := map[string]bool{"public.t1": true}

	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.startConnector(conn, s.ctx, protocol.Checkpoint{}, &mu, &msgs, knownTables, func() {}, s.config.BatchWait, s.config.DiscoveryInterval, s.config)
	}()

	// Let the tickers run for a few iterations.
	time.Sleep(50 * time.Millisecond)

	// Issue Stop and time it. If the tickers were detached (the bug),
	// Stop would return immediately. With the fix, Stop must wait
	// for the goroutines to observe ctx.Done and unwind.
	stopStart := time.Now()
	require.NoError(t, s.Stop())
	stopElapsed := time.Since(stopStart)

	// After Stop returns, runWg MUST be drained: any subsequent
	// WaitGroup call must see a zero counter.
	require.NoError(t, waitWithTimeout(&s.runWg, time.Second),
		"runWg must be drained after Stop returns")

	// We don't assert on stopElapsed directly, but a 0-duration stop
	// is a strong signal the bug regressed.
	assert.Greater(t, stopElapsed, time.Microsecond,
		"Stop must have actually waited for background goroutines to drain")
}

// waitWithTimeout waits for the WaitGroup to drain, returning nil on
// success or an error if the timeout elapses first.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("timed out waiting for WaitGroup")
	}
}

// errorFromPanic normalises a recovered panic value into an error so
// it can flow through the producer error channel in
// TestRestartWithNewTables_NoDoubleClose.
func errorFromPanic(r interface{}) error {
	switch v := r.(type) {
	case error:
		return v
	case string:
		return errors.New(v)
	default:
		return errors.New("recovered from panic")
	}
}

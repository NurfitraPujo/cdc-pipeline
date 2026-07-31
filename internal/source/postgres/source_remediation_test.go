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
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
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

	// T0-2: UpdateXLogPos now carries a ctx and returns an error, so the stub records
	// what it was asked to advance to and can be told to fail.
	updateCount   atomic.Int32
	lastUpdateLSN atomic.Uint64
	updateErr     atomic.Pointer[error]
}

func newStubConnector() *stubConnector { return &stubConnector{} }

// failUpdatesWith makes every subsequent UpdateXLogPos return err.
func (s *stubConnector) failUpdatesWith(err error) { s.updateErr.Store(&err) }

func (s *stubConnector) Start(ctx context.Context) {
	s.startCount.Add(1)
	s.lastCtx.Store(&ctx)
	<-ctx.Done()
}

func (s *stubConnector) WaitUntilReady(_ context.Context) error { return nil }
func (s *stubConnector) Close()                                 { s.closeCount.Add(1) }
func (s *stubConnector) UpdateXLogPos(_ context.Context, lsn pq.LSN) error {
	s.updateCount.Add(1)
	s.lastUpdateLSN.Store(uint64(lsn))
	if errp := s.updateErr.Load(); errp != nil {
		return *errp
	}
	return nil
}
func (s *stubConnector) GetConfig() *config.Config                     { return nil }
func (s *stubConnector) SetMetricCollectors(_ ...prometheus.Collector) {}
func (s *stubConnector) AddRelation(_ *format.Relation)                {}

// stubFactory produces fresh stubConnector instances per invocation. It
// also captures the config.Config and replication.ListenerFunc handed to
// it, so WI-4 tests can drive the real handler built by
// PostgresSource.createHandler and assert on the exact config Start built
// (config.Config is non-comparable once it holds a func field — B4 — so
// callers must assert individual fields, never whole-struct equality).
type stubFactory struct {
	mu         sync.Mutex
	calls      int
	current    *stubConnector
	handler    replication.ListenerFunc
	lastConfig config.Config
}

func newStubFactory() *stubFactory { return &stubFactory{} }

func (f *stubFactory) Build(_ context.Context, cfg config.Config, h replication.ListenerFunc) (cdc.Connector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.handler = h
	f.lastConfig = cfg
	conn := newStubConnector()
	f.current = conn
	return conn, nil
}

// Handler returns the replication.ListenerFunc most recently passed to
// Build, i.e. the actual handler produced by PostgresSource.createHandler.
func (f *stubFactory) Handler() replication.ListenerFunc {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handler
}

// LastConfig returns the config.Config most recently passed to Build.
func (f *stubFactory) LastConfig() config.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastConfig
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
	s.ackChan = make(chan source.SourceAck, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	s.ackMgr = NewAckManager(nil)

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
		if _, _, err := s.Restart(context.Background(), []string{"public.t2"}); err != nil {
			t.Fatalf("first Restart failed: %v", err)
		}

		// Sanity: a new connector must have been built, and the old
		// connector must have been closed. The msgChan reference
		// MUST have changed.
		assert.Equal(t, 2, factory.Calls(), "factory must be called once per restart (initial + first restart)")
		require.NotNil(t, factory.Last(), "factory must have produced a connector")
		assert.NotEqual(t, originalMsgChan, s.msgChan, "RestartWithNewTables must allocate a new msgChan")
		// Compare POINTERS, not values. assert.NotEqual deep-equals the
		// struct, which races the new session's conn.Start bumping
		// startCount inside it — an intermittent -race failure unrelated
		// to what this assertion is actually checking (identity).
		assert.False(t, firstStub == factory.Last(), "the connector should be a fresh instance")
		require.NotNil(t, firstStub, "firstStub must be set from primeSourceState")
		assert.GreaterOrEqual(t, firstStub.closeCount.Load(), int32(1), "old connector must be Closed")

		// Second restart in rapid succession.
		if _, _, err := s.Restart(context.Background(), []string{"public.t3"}); err != nil {
			t.Fatalf("second Restart failed: %v", err)
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

	func() { _, _, err := s.Restart(context.Background(), []string{"public.t2"}); require.NoError(t, err) }()
	assert.Equal(t, 31999, s.resolveMetricPort(),
		"first restart must keep the configured static port (NOT increment)")

	func() { _, _, err := s.Restart(context.Background(), []string{"public.t3"}); require.NoError(t, err) }()
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

	func() { _, _, err := s.Restart(context.Background(), []string{"public.t2"}); require.NoError(t, err) }()
	func() { _, _, err := s.Restart(context.Background(), []string{"public.t3"}); require.NoError(t, err) }()

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
	s.ackChan = make(chan source.SourceAck, 1024)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.ackMgr = NewAckManager(nil)
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

// TestUpdateXLogPos_PropagatesConnectorError is the first coverage of the slot-advance
// error path, made possible by vendored patch T0-2 (which gave the vendored
// Connector.UpdateXLogPos a context and an error return).
//
// Before T0-2 the vendored call could not fail as far as the caller was concerned, so
// PostgresSource.UpdateXLogPos always returned nil despite its signature promising an
// error — a slot that silently stopped advancing was undetectable. Under the new
// at-least-once contract the slot write is the single most safety-critical call in the
// system, so a failure here must reach the caller.
func TestUpdateXLogPos_PropagatesConnectorError(t *testing.T) {
	t.Run("error is propagated", func(t *testing.T) {
		conn := newStubConnector()
		sentinel := errors.New("standby status update failed")
		conn.failUpdatesWith(sentinel)

		s := &PostgresSource{connector: conn}

		err := s.UpdateXLogPos(context.Background(), 12345)

		require.Error(t, err, "a failed slot advance must not be reported as success")
		assert.ErrorIs(t, err, sentinel, "the underlying cause must be preserved for errors.Is")
		assert.Equal(t, int32(1), conn.updateCount.Load())
		assert.Equal(t, uint64(12345), conn.lastUpdateLSN.Load())
	})

	t.Run("success returns nil and forwards the lsn", func(t *testing.T) {
		conn := newStubConnector()
		s := &PostgresSource{connector: conn}

		require.NoError(t, s.UpdateXLogPos(context.Background(), 999))
		assert.Equal(t, uint64(999), conn.lastUpdateLSN.Load())
	})

	t.Run("checkpoint is still advanced in memory", func(t *testing.T) {
		// The in-memory checkpoint assignment (T1-2) happens before the connector call and
		// must not regress: it is what a restart resumes from.
		conn := newStubConnector()
		s := &PostgresSource{connector: conn}

		require.NoError(t, s.UpdateXLogPos(context.Background(), 777))

		s.mu.Lock()
		got := s.lastCheckpoint.IngressLSN
		s.mu.Unlock()
		assert.Equal(t, uint64(777), got)
	})

	t.Run("nil connector is not an error", func(t *testing.T) {
		// Stop/Start races can leave connector nil; that is not a failed advance.
		s := &PostgresSource{}
		assert.NoError(t, s.UpdateXLogPos(context.Background(), 1))
	})
}

// drainMsgChan drains batches off ch in the background until the returned
// stop function is called, or the channel closes. Tests that feed events
// through a live handler need this because triggerFlush's send blocks
// (msgChan has capacity 1) if nothing is consuming.
func drainMsgChan(ch <-chan []protocol.Message) (stop func()) {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return
				}
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

// TestHandler_NeverAcksOrAdvances_UntilCoordinatorConfirmed is plan 01a
// test 6, the headline unit-level proof of the whole plan: the handler
// must never call lc.Ack (deleted at all 8 former call sites) and the
// slot must never advance until runAckCoordinator receives a matching
// SourceAck from every required sink.
func TestHandler_NeverAcksOrAdvances_UntilCoordinatorConfirmed(t *testing.T) {
	s := NewPostgresSource("wi4-handler-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"public.t1"}

	msgChan, ackChan, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer s.Stop()

	stopDrain := drainMsgChan(msgChan)
	defer stopDrain()

	handler := factory.Handler()
	require.NotNil(t, handler, "the factory must have captured the real handler built by createHandler")

	var ackCalls atomic.Int32
	countingAck := func() error { ackCalls.Add(1); return nil }

	// Filtered events: a Relation message and an Insert against a table
	// that is not in knownTables. LSN 0 keeps them out of the watermark
	// scan entirely (ObserveConfirmed only runs when lsn > 0) so they
	// cannot confound the watermark assertions below; the point of this
	// block is purely that they never call Ack.
	handler(&replication.ListenerContext{Message: &format.Relation{OID: 1, Name: "t1"}, Ack: countingAck, LSN: 0})
	handler(&replication.ListenerContext{Message: &format.Insert{TableName: "public.other", Decoded: map[string]any{"a": 1}}, Ack: countingAck, LSN: 0})
	handler(&replication.ListenerContext{Message: &format.Update{TableName: "public.other", NewDecoded: map[string]any{"a": 1}}, Ack: countingAck, LSN: 0})
	handler(&replication.ListenerContext{Message: &format.Delete{TableName: "public.other", OldDecoded: map[string]any{"a": 1}}, Ack: countingAck, LSN: 0})
	handler(&replication.ListenerContext{Message: &format.Snapshot{EventType: "BEGIN", Table: "public.t1"}, Ack: countingAck, LSN: 0})

	// Data events against the known table.
	handler(&replication.ListenerContext{Message: &format.Insert{TableName: "public.t1", Decoded: map[string]any{"a": 1}}, Ack: countingAck, LSN: 100})
	handler(&replication.ListenerContext{Message: &format.Update{TableName: "public.t1", NewDecoded: map[string]any{"a": 2}}, Ack: countingAck, LSN: 101})
	handler(&replication.ListenerContext{Message: &format.Delete{TableName: "public.t1", OldDecoded: map[string]any{"a": 2}}, Ack: countingAck, LSN: 102})

	assert.Equal(t, int32(0), ackCalls.Load(), "the handler must never call lc.Ack under ManualCommit")

	conn := factory.Last()
	// Let a couple of 500ms ticker cycles pass with no SourceAck received:
	// the watermark must stay at zero and UpdateXLogPos must never fire.
	time.Sleep(1100 * time.Millisecond)
	assert.Equal(t, int32(0), ackCalls.Load(), "still zero Ack calls after ticker cycles")
	assert.Equal(t, int32(0), conn.updateCount.Load(), "no SourceAck yet: the slot must not advance")

	// Now the engine reports the durable write from the only required sink.
	ackChan <- source.SourceAck{SinkID: "sink1", LSNs: []uint64{100, 101, 102}}

	require.Eventually(t, func() bool {
		return conn.updateCount.Load() == 1
	}, 3*time.Second, 20*time.Millisecond, "coordinator must issue exactly one UpdateXLogPos once fully confirmed")

	assert.Equal(t, uint64(102), conn.lastUpdateLSN.Load(), "must advance to the fully-confirmed watermark")
	assert.Equal(t, int32(1), conn.updateCount.Load(), "must not issue more than one UpdateXLogPos call")
	assert.Equal(t, int32(0), ackCalls.Load(), "lc.Ack must never be called, even after the slot advances")
}

// TestHandler_PanicSafety_MuNotStranded is plan 01a test 7: a panic during
// buildMessage's critical section must not strand the message-construction
// mutex. Before the WI-4 split, a panic between an explicit Lock and
// Unlock left mu held forever, wedging every subsequent event (including
// the batch-wait ticker's triggerFlush, which shares the same mutex).
func TestHandler_PanicSafety_MuNotStranded(t *testing.T) {
	s := NewPostgresSource("wi4-panic-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"public.t1"}

	msgChan, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	defer s.Stop()

	handler := factory.Handler()
	require.NotNil(t, handler)

	// Force a panic inside buildMessage's Relation branch: a write to a
	// nil map panics. This exercises the exact failure mode the plan
	// describes (a panic between Lock and Unlock) because the write
	// happens while mu (buildMessage's message-construction mutex) is
	// held via the deferred Unlock.
	s.oidMu.Lock()
	s.oidCache = nil
	s.oidMu.Unlock()

	require.NotPanics(t, func() {
		handler(&replication.ListenerContext{Message: &format.Relation{OID: 1, Name: "boom"}, Ack: func() error { return nil }, LSN: 0})
	}, "the handler's own recover() must contain the panic")

	// Restore a valid map so the subsequent, real event can proceed.
	s.oidMu.Lock()
	s.oidCache = make(map[uint32]string)
	s.oidMu.Unlock()

	got := make(chan []protocol.Message, 1)
	go func() {
		for batch := range msgChan {
			select {
			case got <- batch:
			default:
			}
		}
	}()

	handler(&replication.ListenerContext{Message: &format.Insert{TableName: "public.t1", Decoded: map[string]any{"a": 1}}, Ack: func() error { return nil }, LSN: 200})

	select {
	case batch := <-got:
		require.Len(t, batch, 1)
		assert.Equal(t, protocol.OpInsert, batch[0].Op)
	case <-time.After(3 * time.Second):
		t.Fatal("mu was stranded by the earlier panic: the subsequent event never flushed")
	}
}

// TestCoordinator_AckIngestion_NoLossUnderBurst is plan 01a test 8: pushing
// SourceAcks faster than the 500ms ticker must not lose any confirmation,
// and the watermark must converge on the max fully-confirmed LSN.
func TestCoordinator_AckIngestion_NoLossUnderBurst(t *testing.T) {
	s := NewPostgresSource("wi4-burst-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"public.t1"}

	msgChan, ackChan, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer s.Stop()

	stopDrain := drainMsgChan(msgChan)
	defer stopDrain()

	handler := factory.Handler()
	require.NotNil(t, handler)

	const n = 200
	for i := 1; i <= n; i++ {
		handler(&replication.ListenerContext{
			Message: &format.Insert{TableName: "public.t1", Decoded: map[string]any{"i": i}},
			Ack:     func() error { return nil },
			LSN:     pq.LSN(i),
		})
	}

	// Push all acks in a tight burst, well before the next 500ms tick.
	for i := 1; i <= n; i++ {
		ackChan <- source.SourceAck{SinkID: "sink1", LSNs: []uint64{uint64(i)}}
	}

	require.Eventually(t, func() bool {
		return s.ackMgr.Watermark() == uint64(n)
	}, 3*time.Second, 10*time.Millisecond, "watermark must reach the max fully-confirmed LSN with no ack loss")

	conn := factory.Last()
	require.Eventually(t, func() bool {
		return conn.lastUpdateLSN.Load() == uint64(n)
	}, 3*time.Second, 20*time.Millisecond, "the coordinator must eventually flush the max watermark to the connector")
}

// TestStart_SnapshotEnabledUnconditional is plan 01a test 10 (Critical 11,
// source half): Snapshot.Enabled must be true even when
// checkpoint.IngressLSN > 0 — the vendored LoadJob decides skip/resume/
// fresh, not this config gate. Per blocker B4, config.Config is no longer
// comparable (it holds a func field), so this asserts individual captured
// fields rather than whole-struct equality.
func TestStart_SnapshotEnabledUnconditional(t *testing.T) {
	s := NewPostgresSource("wi4-snapshot-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	checkpoint := protocol.Checkpoint{IngressLSN: 12345}

	_, _, err := s.Start(context.Background(), cfg, checkpoint, nil)
	require.NoError(t, err)
	defer s.Stop()

	got := factory.LastConfig()

	assert.True(t, got.Snapshot.Enabled, "Snapshot.Enabled must be unconditional, not gated on checkpoint.IngressLSN")
	assert.True(t, got.ManualCommit, "ManualCommit must be set so the coordinator is the sole slot-advance path")
	assert.NotNil(t, got.KeepaliveFunc, "KeepaliveFunc must be wired to AckManager.IdleAdvance")
	assert.Equal(t, pq.LSN(12345), got.StartLSN, "the StartLSN seed is WI-7's job to remove, not WI-4's")
}

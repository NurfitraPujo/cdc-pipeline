package postgres

import (
	"context"
	"database/sql"
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

	// WI-7 B3: firstStartNano/firstUpdateNano record the wall-clock time
	// (UnixNano, 0 == not yet occurred) of the FIRST call to Start /
	// UpdateXLogPos respectively, so tests can assert ordering (the B3
	// pre-Start seed must happen strictly before conn.Start) rather than
	// just eventually observing a value that a later periodic coordinator
	// flush could equally have produced.
	firstStartNano  atomic.Int64
	firstUpdateNano atomic.Int64

	// WI-7 B3 post-ready path: readyGate, when non-nil, makes
	// WaitUntilReady block until the gate is closed (simulating the
	// vendored connector not becoming ready until slot/publication/stream
	// setup completes) or ctx is cancelled first (shutdown), whichever
	// comes first. readyErr, when non-nil, is returned once the gate
	// unblocks (or immediately, if readyGate is nil). By default (both
	// nil) WaitUntilReady returns immediately with a nil error, matching
	// pre-B3 stub behaviour for every other test in this file.
	readyGate atomic.Pointer[chan struct{}]
	readyErr  atomic.Pointer[error]
}

// gateReady installs a fresh, closed-by-caller gate for WaitUntilReady and
// returns the release func. Tests that want to observe "seed happens only
// after WaitUntilReady unblocks" call this before Start, then call the
// returned release func once they want WaitUntilReady to return.
func (s *stubConnector) gateReady() (release func()) {
	gate := make(chan struct{})
	s.readyGate.Store(&gate)
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// failReadyWith makes WaitUntilReady return err once it unblocks (immediately
// if no gate was installed via gateReady).
func (s *stubConnector) failReadyWith(err error) { s.readyErr.Store(&err) }

func newStubConnector() *stubConnector { return &stubConnector{} }

// failUpdatesWith makes every subsequent UpdateXLogPos return err.
func (s *stubConnector) failUpdatesWith(err error) { s.updateErr.Store(&err) }

func (s *stubConnector) Start(ctx context.Context) {
	s.firstStartNano.CompareAndSwap(0, time.Now().UnixNano())
	s.startCount.Add(1)
	s.lastCtx.Store(&ctx)
	<-ctx.Done()
}

func (s *stubConnector) WaitUntilReady(ctx context.Context) error {
	if gatep := s.readyGate.Load(); gatep != nil {
		select {
		case <-*gatep:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if errp := s.readyErr.Load(); errp != nil {
		return *errp
	}
	return nil
}
func (s *stubConnector) Close() { s.closeCount.Add(1) }
func (s *stubConnector) UpdateXLogPos(_ context.Context, lsn pq.LSN) error {
	s.firstUpdateNano.CompareAndSwap(0, time.Now().UnixNano())
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

	// presetReadyGate, when non-nil, is installed on every connector this
	// factory builds AT CONSTRUCTION TIME -- i.e. before Start's
	// startConnector goroutine can possibly reach WaitUntilReady. This
	// closes an otherwise-inherent race in tests that need to observe
	// "nothing happened yet" before releasing the gate: installing the
	// gate via the connector AFTER Start returns is too late, since the
	// startConnector goroutine may already have called (and an ungated
	// stub WaitUntilReady returns from) WaitUntilReady by then.
	presetReadyGate atomic.Pointer[chan struct{}]
}

func newStubFactory() *stubFactory { return &stubFactory{} }

func (f *stubFactory) Build(_ context.Context, cfg config.Config, h replication.ListenerFunc) (cdc.Connector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.handler = h
	f.lastConfig = cfg
	conn := newStubConnector()
	if gatep := f.presetReadyGate.Load(); gatep != nil {
		conn.readyGate.Store(gatep)
	}
	f.current = conn
	return conn, nil
}

// gateReady installs a WaitUntilReady gate on every connector this factory
// subsequently builds, closed only when the returned release func is
// called. Race-free by construction: call it BEFORE s.Start, so the gate
// is already in place on the connector the moment it's constructed.
func (f *stubFactory) gateReady() (release func()) {
	gate := make(chan struct{})
	f.presetReadyGate.Store(&gate)
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
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
	var flushWg sync.WaitGroup
	var flushClosed bool

	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.startConnector(conn, s.ctx, protocol.Checkpoint{}, &mu, &msgs, knownTables, func() {}, s.config.BatchWait, s.config.DiscoveryInterval, s.config, &flushWg, &flushClosed)
	}()

	// Let the tickers run for a few iterations.
	time.Sleep(50 * time.Millisecond)

	// Issue Stop and time it. If the tickers were detached (the bug),
	// Stop would return immediately. With the fix, Stop must wait
	// for the goroutines to observe ctx.Done and unwind.
	stopStart := time.Now()
	require.NoError(t, s.Stop())
	_ = time.Since(stopStart)

	// The only assertion that actually distinguishes "Stop blocked until
	// drained" from "Stop returned early and the goroutines happened to
	// exit a moment later" is that runWg is ALREADY at zero the instant
	// Stop returns -- not merely within some generous timeout. Use a
	// near-zero timeout so a regression to the old detached-goroutine
	// behaviour (where Stop returns immediately while tickers are still
	// unwinding) fails this test instead of slipping through.
	require.NoError(t, waitWithTimeout(&s.runWg, time.Millisecond),
		"runWg must already be drained immediately after Stop returns, not merely eventually")
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
	t.Setenv(strictAckEnvVar, "true") // pin strict_ack ON regardless of the ambient ENV default
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
	t.Setenv(strictAckEnvVar, "true") // pin strict_ack ON regardless of the ambient ENV default
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
	// WI-7: StartLSN is no longer seeded from the checkpoint at all. The
	// replication slot's own confirmed_flush_lsn is the sole resume
	// authority; the hydrated watermark floor is applied via
	// AckManager.Hydrate and (on resume) the B3 mitigation in
	// startConnector, never via cfg.StartLSN.
	assert.Equal(t, pq.LSN(0), got.StartLSN, "StartLSN must always be 0; the slot's confirmed_flush_lsn is the sole resume authority")
}

// TestRunSlotLagProbe_EmitsGaugeValues is plan 01a WI-5a: the periodic
// slot-lag probe must export cdc_source_slot_lag_bytes from the injected
// slotLagBytes seam, and cdc_source_ack_watermark from the AckManager,
// without touching a live database.
func TestRunSlotLagProbe_EmitsGaugeValues(t *testing.T) {
	s := NewPostgresSource("wi5a-probe-source")
	s.pipelineID = "pipe-1"
	s.ackMgr = NewAckManager([]string{"sink-a"})
	s.ackMgr.Observe(100)
	s.ackMgr.Confirm(100, "sink-a")
	require.Equal(t, uint64(100), s.ackMgr.Watermark())

	fakeDB := &sql.DB{}
	s.db = fakeDB
	s.slotLagBytes = func(_ context.Context, gotDB *sql.DB, slotName string) (int64, bool) {
		assert.Same(t, fakeDB, gotDB)
		assert.Equal(t, "test_slot", slotName)
		return 4096, true
	}

	prevInterval := slotLagProbeInterval
	slotLagProbeInterval = 20 * time.Millisecond
	defer func() { slotLagProbeInterval = prevInterval }()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSlotLagProbe(ctx, "test_slot")
	}()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(slotLagBytesGauge.WithLabelValues("pipe-1", "wi5a-probe-source", "test_slot")) == 4096
	}, time.Second, 5*time.Millisecond, "cdc_source_slot_lag_bytes must reflect the injected slot lag")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(ackWatermarkGauge.WithLabelValues("pipe-1", "wi5a-probe-source", "test_slot")) == 100
	}, time.Second, 5*time.Millisecond, "cdc_source_ack_watermark must reflect the AckManager watermark")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(slotLagProbeLastSuccessGauge.WithLabelValues("pipe-1", "wi5a-probe-source", "test_slot")) > 0
	}, time.Second, 5*time.Millisecond, "cdc_source_slot_lag_probe_last_success_timestamp_seconds must be set on a successful probe")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runSlotLagProbe did not exit after ctx cancellation")
	}
}

// TestRunSlotLagProbe_StaleOnFailureDoesNotAdvanceSuccessTimestamp is plan
// 01a WI-5a review Defect 3: slotLagBytesGauge is a plain Prometheus gauge,
// so on a probe failure (query error, degraded DB) it silently keeps its
// last-known value forever -- a healthy-looking number during exactly the
// kind of DB-connection degradation most likely to co-occur with a real
// source-primary problem, which would otherwise silence both slot-lag
// alerts. cdc_source_slot_lag_probe_last_success_timestamp_seconds exists
// to make that staleness independently observable: it must advance on
// success and MUST NOT advance while the probe is failing, so
// CDCSourceSlotLagProbeStale can detect the gap.
func TestRunSlotLagProbe_StaleOnFailureDoesNotAdvanceSuccessTimestamp(t *testing.T) {
	s := NewPostgresSource("wi5a-stale-probe-source")
	s.pipelineID = "pipe-stale"
	s.ackMgr = NewAckManager(nil)
	s.db = &sql.DB{}

	var succeed atomic.Bool
	s.slotLagBytes = func(context.Context, *sql.DB, string) (int64, bool) {
		if succeed.Load() {
			return 777, true
		}
		return 0, false
	}

	prevInterval := slotLagProbeInterval
	slotLagProbeInterval = 15 * time.Millisecond
	defer func() { slotLagProbeInterval = prevInterval }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSlotLagProbe(ctx, "stale-slot")
	}()

	// While every probe call fails, the success-timestamp gauge must stay
	// at its zero value (never initialized) even though several ticks
	// have elapsed.
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, float64(0),
		testutil.ToFloat64(slotLagProbeLastSuccessGauge.WithLabelValues("pipe-stale", "wi5a-stale-probe-source", "stale-slot")),
		"the success-timestamp gauge must not advance while every probe call fails")

	succeed.Store(true)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(slotLagProbeLastSuccessGauge.WithLabelValues("pipe-stale", "wi5a-stale-probe-source", "stale-slot")) > 0
	}, time.Second, 5*time.Millisecond, "the success-timestamp gauge must advance once the probe starts succeeding")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runSlotLagProbe did not exit after ctx cancellation")
	}
}

// TestWI5aGauges_ShareIdenticalLabelSet is plan 01a WI-5a review Defect 1:
// cdc_source_pending_lsns, cdc_source_slot_lag_bytes, and
// cdc_source_ack_watermark MUST share an identical label set, because the
// CDCSourcePendingLSNsStuck alert joins the first and third with PromQL's
// `and` operator, which matches on the full label set by default. A
// mismatched set (the original bug: pendingLSNsGauge had only {"source"})
// means the join can never match any series and the alert silently never
// fires. This test exercises the real runAckCoordinator + runSlotLagProbe
// goroutines together and asserts all three gauges are queryable under the
// SAME (pipeline, source, slot) label tuple.
func TestWI5aGauges_ShareIdenticalLabelSet(t *testing.T) {
	s := NewPostgresSource("wi5a-label-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.pipelineID = "pipe-labels"

	cfg := validSourceConfig()
	cfg.SlotName = "labels_slot"
	cfg.Tables = []string{"public.t1"}

	msgChan, ackChan, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer s.Stop()

	stopDrain := drainMsgChan(msgChan)
	defer stopDrain()

	handler := factory.Handler()
	require.NotNil(t, handler)
	handler(&replication.ListenerContext{
		Message: &format.Insert{TableName: "public.t1", Decoded: map[string]any{"a": 1}},
		Ack:     func() error { return nil },
		LSN:     pq.LSN(1),
	})
	ackChan <- source.SourceAck{SinkID: "sink1", LSNs: []uint64{1}}

	// pendingLSNsGauge only updates on runAckCoordinator's 500ms ticker;
	// give it a couple of ticks.
	require.Eventually(t, func() bool {
		g, err := pendingLSNsGauge.GetMetricWithLabelValues("pipe-labels", "wi5a-label-source", "labels_slot")
		return err == nil && g != nil
	}, 3*time.Second, 20*time.Millisecond, "cdc_source_pending_lsns must be observable under {pipeline, source, slot}")

	_, err = ackWatermarkGauge.GetMetricWithLabelValues("pipe-labels", "wi5a-label-source", "labels_slot")
	assert.NoError(t, err, "cdc_source_ack_watermark must share the same label set")

	_, err = slotLagBytesGauge.GetMetricWithLabelValues("pipe-labels", "wi5a-label-source", "labels_slot")
	assert.NoError(t, err, "cdc_source_slot_lag_bytes must share the same label set")
}

// TestRunSlotLagProbe_MissingSlotIsNonFatal is plan 01a WI-5a: on a fresh
// deployment the replication slot does not exist yet (conn.Start has not
// created it), so slotLagBytes reports ok=false. The probe must not crash,
// must not spam, and must still update the watermark gauge (which needs no
// DB) and keep running until ctx is cancelled.
func TestRunSlotLagProbe_MissingSlotIsNonFatal(t *testing.T) {
	s := NewPostgresSource("wi5a-missing-slot-source")
	s.ackMgr = NewAckManager(nil)
	s.db = &sql.DB{}

	prevInterval := slotLagProbeInterval
	slotLagProbeInterval = 20 * time.Millisecond
	defer func() { slotLagProbeInterval = prevInterval }()

	var calls int32
	s.slotLagBytes = func(context.Context, *sql.DB, string) (int64, bool) {
		atomic.AddInt32(&calls, 1)
		return 0, false // simulates "slot does not exist yet" / query error
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Should not panic despite every probe call reporting ok=false.
		s.runSlotLagProbe(ctx, "not-yet-created-slot")
	}()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&calls) >= 1
	}, 2*time.Second, 5*time.Millisecond, "the probe must still attempt the query on a missing slot")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runSlotLagProbe did not exit after ctx cancellation on the missing-slot path")
	}
}

// TestRunSlotLagProbe_ExitsOnCtxCancel is plan 01a WI-5a: the probe
// goroutine must be a well-behaved s.runWg member -- it exits promptly on
// ctx cancellation and never leaks, even when slotLagBytes is nil (e.g. a
// test-constructed PostgresSource that never called NewPostgresSource).
func TestRunSlotLagProbe_ExitsOnCtxCancel(t *testing.T) {
	s := &PostgresSource{name: "wi5a-nil-seam-source", ackMgr: NewAckManager(nil)}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runSlotLagProbe(ctx, "some-slot")
	}()

	cancel()
	require.NoError(t, waitWithTimeout(&wg, time.Second),
		"runSlotLagProbe must exit promptly on ctx cancellation, even with a nil slotLagBytes seam")
}

// TestResolveStrictAck_DefaultsAndOverride is plan 01a §6: the strict_ack
// flag must default ON in dev/test and OFF in prod (both keyed off the same
// "ENV" var logger.Init already reads), and an explicit CDC_STRICT_ACK must
// win over that default in both directions.
func TestResolveStrictAck_DefaultsAndOverride(t *testing.T) {
	t.Run("dev/test default is ON", func(t *testing.T) {
		t.Setenv(strictAckEnvVar, "")
		t.Setenv("ENV", "")
		assert.True(t, resolveStrictAck())
	})

	t.Run("ENV=production default is OFF", func(t *testing.T) {
		t.Setenv(strictAckEnvVar, "")
		t.Setenv("ENV", "production")
		assert.False(t, resolveStrictAck())
	})

	t.Run("explicit false wins even in dev", func(t *testing.T) {
		t.Setenv(strictAckEnvVar, "false")
		t.Setenv("ENV", "")
		assert.False(t, resolveStrictAck())
	})

	t.Run("explicit true wins even in prod", func(t *testing.T) {
		t.Setenv(strictAckEnvVar, "true")
		t.Setenv("ENV", "production")
		assert.True(t, resolveStrictAck())
	})

	t.Run("unparseable value falls back to the ENV default", func(t *testing.T) {
		t.Setenv(strictAckEnvVar, "not-a-bool")
		t.Setenv("ENV", "production")
		assert.False(t, resolveStrictAck())
	})
}

// TestHandler_StrictAckOff_RestoresLegacyPerEventAck is plan 01a §6: with
// the flag OFF, cfg.ManualCommit must be false and every one of WI-4's 8
// deleted lc.Ack() call sites must fire again -- for both filtered and data
// events -- restoring the pre-WI-4 legacy per-event ack contract exactly
// (commit 0dbb895). The AckManager/coordinator/metrics plumbing must still
// run underneath, since keeping it live under OFF is the whole point of the
// §6 bake period.
func TestHandler_StrictAckOff_RestoresLegacyPerEventAck(t *testing.T) {
	t.Setenv(strictAckEnvVar, "false")

	s := NewPostgresSource("strict-ack-off-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"public.t1"}

	msgChan, ackChan, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer s.Stop()
	_ = ackChan

	stopDrain := drainMsgChan(msgChan)
	defer stopDrain()

	got := factory.LastConfig()
	assert.False(t, got.ManualCommit, "strict_ack off must leave ManualCommit false, restoring the legacy per-event slot advance")

	handler := factory.Handler()
	require.NotNil(t, handler)

	var ackCalls atomic.Int32
	countingAck := func() error { ackCalls.Add(1); return nil }

	// Filtered event (unknown table): must still ack, same as pre-WI-4.
	handler(&replication.ListenerContext{Message: &format.Insert{TableName: "public.other", Decoded: map[string]any{"a": 1}}, Ack: countingAck, LSN: 50})
	require.Eventually(t, func() bool { return ackCalls.Load() == 1 }, time.Second, 5*time.Millisecond,
		"a filtered event must call lc.Ack under strict_ack off")

	// Data event against the known table: must also ack immediately,
	// without waiting for any SourceAck from the engine -- that is
	// precisely the legacy (pre-plan-01a) contract this flag restores.
	handler(&replication.ListenerContext{Message: &format.Insert{TableName: "public.t1", Decoded: map[string]any{"a": 1}}, Ack: countingAck, LSN: 100})
	require.Eventually(t, func() bool { return ackCalls.Load() == 2 }, time.Second, 5*time.Millisecond,
		"a data event must call lc.Ack under strict_ack off, without waiting for a SourceAck")

	// Snapshot data event: also acks under strict_ack off. This branch is
	// asserted explicitly because the WI-4 refactor collapsed pre-WI-4's 8
	// lc.Ack() sites into 3 (one per handlerKind), and handlerKindSnapshot
	// is one of the two collapse points where a site could be silently
	// dropped -- without this case, deleting that ack would leave the test
	// green while the escape hatch was quietly broken for snapshot rows.
	handler(&replication.ListenerContext{
		Message: &format.Snapshot{EventType: format.SnapshotEventTypeData, Table: "public.t1", Data: map[string]any{"a": 1}},
		Ack:     countingAck,
		LSN:     150,
	})
	require.Eventually(t, func() bool { return ackCalls.Load() == 3 }, time.Second, 5*time.Millisecond,
		"a snapshot data event must call lc.Ack under strict_ack off")

	// The AckManager bookkeeping must still be running underneath (the §6
	// bake-period requirement): the data event's LSN was Observed, so the
	// watermark advances once the engine's SourceAck arrives, exactly as
	// under strict_ack on -- it is simply redundant with the legacy ack
	// above, not disabled.
	ackChan <- source.SourceAck{SinkID: "sink1", LSNs: []uint64{100}}
	require.Eventually(t, func() bool { return s.ackMgr.Watermark() == 100 }, time.Second, 5*time.Millisecond,
		"AckManager/coordinator bookkeeping must keep running under strict_ack off so cdc_source_ack_watermark stays observable")
}

// TestRunAckCoordinator_SkipsWireWriteUnderStrictAckOff is HIGH-1: under
// strict_ack off (the production ENV=production default per
// resolveStrictAck), ManualCommit is false so the vendored lc.Ack closure
// is the live, un-semaphored writer of the replication slot position
// (pq/replication/stream.go SendStandbyStatusUpdate). standbySem only
// serializes UpdateXLogPos calls against each other, not against that
// legacy send, so runAckCoordinator issuing UpdateXLogPos concurrently
// with lc.Ack risks interleaving two standby-status writes on the wire and
// corrupting the replication protocol stream.
//
// The fix: runAckCoordinator must never call UpdateXLogPos when strictAck
// is false, while everything else (watermark computation via
// ObserveConfirmed/Confirm, PendingCount, persistWatermark) keeps running.
// Under strictAck true, UpdateXLogPos must still fire exactly as before.
func TestRunAckCoordinator_SkipsWireWriteUnderStrictAckOff(t *testing.T) {
	t.Run("strict_ack off: never calls UpdateXLogPos", func(t *testing.T) {
		conn := newStubConnector()
		s := NewPostgresSource("high1-off-source")
		s.connector = conn
		s.ackChan = make(chan source.SourceAck, 4)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s.runWg.Add(1)
		go func() {
			defer s.runWg.Done()
			s.runAckCoordinator(ctx, "high1-off-slot", false)
		}()

		// Feed a fully-confirmed LSN so the watermark advances above zero.
		s.ackMgr.ObserveConfirmed(100)
		s.ackChan <- source.SourceAck{SinkID: "sink1", LSNs: []uint64{100}}

		require.Eventually(t, func() bool {
			return s.ackMgr.Watermark() == 100
		}, time.Second, 5*time.Millisecond, "watermark bookkeeping must keep running under strict_ack off")

		// Give the 500ms ticker a couple of cycles to fire.
		time.Sleep(1200 * time.Millisecond)

		assert.Equal(t, int32(0), conn.updateCount.Load(),
			"strict_ack off must never issue UpdateXLogPos: the legacy lc.Ack send is the sole slot-advance path")

		cancel()
		require.NoError(t, waitWithTimeout(&s.runWg, time.Second))
	})

	t.Run("strict_ack on: advances via UpdateXLogPos", func(t *testing.T) {
		conn := newStubConnector()
		s := NewPostgresSource("high1-on-source")
		s.connector = conn
		s.ackChan = make(chan source.SourceAck, 4)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s.runWg.Add(1)
		go func() {
			defer s.runWg.Done()
			s.runAckCoordinator(ctx, "high1-on-slot", true)
		}()

		s.ackMgr.ObserveConfirmed(200)
		s.ackChan <- source.SourceAck{SinkID: "sink1", LSNs: []uint64{200}}

		require.Eventually(t, func() bool {
			return conn.updateCount.Load() >= 1
		}, 2*time.Second, 20*time.Millisecond, "strict_ack on must advance the slot via UpdateXLogPos")
		assert.Equal(t, uint64(200), conn.lastUpdateLSN.Load())

		cancel()
		require.NoError(t, waitWithTimeout(&s.runWg, time.Second))
	})
}

// TestR9_TriggerFlushDoesNotRaceChannelClose is the regression test for R9:
// the cleanup goroutine spawned by Start used to do
// triggerFlush() -> time.Sleep(100ms) -> close(msgChan), a fixed sleep that
// is not synchronisation. A sender parked in triggerFlush's
// `select { case s.msgChan <- mCopy: ...; case <-sourceCtx.Done(): }` can
// still pick the send case after close(msgChan) runs, which panics (send on
// closed channel) and kills the process.
//
// This test drives many concurrent handler invocations (each of which calls
// triggerFlush synchronously for data events, see createHandler) against a
// session that is torn down mid-flight, under -race, without draining
// msgChan as fast as it fills -- maximising the odds that a sender is still
// in-flight exactly when Stop cancels the context and the cleanup goroutine
// closes the channel. Before the fix (flushWg/flushClosed), this reliably
// panics within a handful of iterations under `go test -race`.
func TestR9_TriggerFlushDoesNotRaceChannelClose(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		s := NewPostgresSource("r9-source")
		factory := newStubFactory()
		s.SetConnectorFactory(factory.Build)

		cfg := validSourceConfig()
		cfg.Tables = []string{"public.t1"}

		msgChan, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
		require.NoError(t, err)

		handler := factory.Handler()
		require.NotNil(t, handler)

		// Drain msgChan lazily (slower than production) so the buffered
		// channel and the ctx.Done() select cases both stay live/ready
		// concurrently, which is exactly the window R9 needs to race.
		drainDone := make(chan struct{})
		go func() {
			defer close(drainDone)
			for range msgChan {
			}
		}()

		var wg sync.WaitGroup
		stop := make(chan struct{})
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				lsn := uint64(id + 1)
				for {
					select {
					case <-stop:
						return
					default:
					}
					handler(&replication.ListenerContext{
						Message: &format.Insert{TableName: "public.t1", Decoded: map[string]any{"a": id}},
						Ack:     func() error { return nil },
						LSN:     pq.LSN(lsn),
					})
					lsn += 8
				}
			}(w)
		}

		// Let senders build up contention, then tear the session down
		// concurrently with them still hammering triggerFlush.
		time.Sleep(5 * time.Millisecond)
		require.NoError(t, s.Stop())
		close(stop)
		wg.Wait()

		// msgChan being closed and drained to completion (drainDone
		// closes only once the range loop observes the close) proves no
		// panic occurred anywhere in this iteration.
		<-drainDone
	}
}

// TestIdleAdvanceRefusedCounter_IncrementsViaHook is OPS-2: a T0-3 regression
// must be ALERTABLE, not merely loggable. The AckManager guard latches after a
// single refusal, so without this counter a recurrence would present exactly
// as the original bug did -- pending_lsns healthy, watermark advancing, one
// Error line buried in the log stream.
//
// This asserts the full chain: refusal -> hook -> Prometheus counter.
func TestIdleAdvanceRefusedCounter_IncrementsViaHook(t *testing.T) {
	const pipeline, srcName, slot = "p-canary", "src-canary", "slot-canary"
	idleAdvanceRefusedCounter.DeleteLabelValues(pipeline, srcName, slot)

	m := NewAckManager([]string{"sinkA"})
	m.SetIdleAdvanceRefusedHook(func() {
		idleAdvanceRefusedCounter.WithLabelValues(pipeline, srcName, slot).Inc()
	})

	// Establish data flow, then drain it, so the next IdleAdvance is the
	// first-after-backlog call the canary inspects.
	m.Observe(100)
	m.Confirm(100, "sinkA")
	require.Equal(t, uint64(100), m.Watermark())
	require.Equal(t, 0, m.PendingCount())

	before := testutil.ToFloat64(idleAdvanceRefusedCounter.WithLabelValues(pipeline, srcName, slot))

	// serverWALEnd far beyond anything ever Observed: precisely the
	// fast-forward-past-un-Observed-backlog shape T0-3 was about.
	advanced := m.IdleAdvance(999999)

	assert.False(t, advanced, "IdleAdvance must refuse to jump past highestSeen on the first post-drain call")
	after := testutil.ToFloat64(idleAdvanceRefusedCounter.WithLabelValues(pipeline, srcName, slot))
	assert.Equal(t, before+1, after,
		"the refusal must increment cdc_source_idle_advance_refused_total -- otherwise a T0-3 regression is invisible again")
	assert.Equal(t, uint64(1), m.IdleAdvanceRefusedCount())
}

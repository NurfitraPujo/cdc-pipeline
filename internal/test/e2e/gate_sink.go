package e2e

import (
	"context"
	"sync"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/sink"
)

// gateSink is a test-only Sink that can be gated open/closed on demand. It
// exists to give plan 01a's e2e invariant tests (WI-10, §5 tests 20-23) a way
// to hold a sink's BatchUpload in flight indefinitely while the test polls
// PostgreSQL's replication slot, without depending on any specific downstream
// database's failure modes. Rows that make it through BatchUpload are kept
// in memory so tests can assert on exactly which LSNs were durably "written".
type gateSink struct {
	name string

	mu      sync.Mutex
	blocked bool
	waiters []chan struct{}

	rowsMu      sync.Mutex
	rows        []protocol.Message
	uploadCalls int
}

var gateRegistry = struct {
	sync.Mutex
	m             map[string]*gateSink
	blockOnCreate map[string]bool
}{m: make(map[string]*gateSink), blockOnCreate: make(map[string]bool)}

func init() {
	sink.Register("gate", func(sinkID string, _ string, _ map[string]interface{}) (sink.Sink, error) {
		gateRegistry.Lock()
		startBlocked := gateRegistry.blockOnCreate[sinkID]
		delete(gateRegistry.blockOnCreate, sinkID)
		gs := &gateSink{name: sinkID, blocked: startBlocked}
		gateRegistry.m[sinkID] = gs
		gateRegistry.Unlock()
		return gs, nil
	})
}

// BlockOnConstruct arranges for the next "gate" sink built under sinkID to
// start life already blocked, closing the race where a worker could flush
// its first batch before the test has a chance to call Block(). Call this
// before StartWorker().
func BlockOnConstruct(sinkID string) {
	gateRegistry.Lock()
	gateRegistry.blockOnCreate[sinkID] = true
	gateRegistry.Unlock()
}

// GetGateSink returns the gateSink instance registered under sinkID. It is
// only valid after the worker has actually constructed the sink (i.e. after
// the pipeline has started and loaded its config), so callers should poll
// for a non-nil result if calling immediately after StartWorker().
func GetGateSink(id string) *gateSink {
	gateRegistry.Lock()
	defer gateRegistry.Unlock()
	return gateRegistry.m[id]
}

func (g *gateSink) Name() string { return g.name }

// Block makes future BatchUpload calls wait until Unblock is called.
func (g *gateSink) Block() {
	g.mu.Lock()
	g.blocked = true
	g.mu.Unlock()
}

// Unblock releases any BatchUpload calls currently waiting, and lets future
// ones proceed immediately.
func (g *gateSink) Unblock() {
	g.mu.Lock()
	g.blocked = false
	waiters := g.waiters
	g.waiters = nil
	g.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
}

func (g *gateSink) waitUnblocked(ctx context.Context) error {
	for {
		g.mu.Lock()
		if !g.blocked {
			g.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		g.waiters = append(g.waiters, ch)
		g.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *gateSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if err := g.waitUnblocked(ctx); err != nil {
		return err
	}

	g.rowsMu.Lock()
	g.rows = append(g.rows, messages...)
	g.uploadCalls++
	g.rowsMu.Unlock()
	return nil
}

func (g *gateSink) ApplySchema(ctx context.Context, m protocol.Message) error {
	return nil
}

func (g *gateSink) Stop() error { return nil }

// Rows returns a snapshot copy of every message durably "written" so far.
func (g *gateSink) Rows() []protocol.Message {
	g.rowsMu.Lock()
	defer g.rowsMu.Unlock()
	out := make([]protocol.Message, len(g.rows))
	copy(out, g.rows)
	return out
}

func (g *gateSink) Count() int {
	g.rowsMu.Lock()
	defer g.rowsMu.Unlock()
	return len(g.rows)
}

func (g *gateSink) UploadCalls() int {
	g.rowsMu.Lock()
	defer g.rowsMu.Unlock()
	return g.uploadCalls
}

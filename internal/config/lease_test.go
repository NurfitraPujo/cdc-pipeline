package config

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaseFakeEntry is a minimal nats.KeyValueEntry backing leaseFakeKV below.
// Unlike pause_expiry_test.go's fakeEntry (hardcoded Revision()==1), this
// one carries a real per-write revision so leaseFakeKV can implement
// Update's CAS semantics faithfully -- exactly what
// tryAcquireOrRenewLease's contention tests need.
type leaseFakeEntry struct {
	key      string
	value    []byte
	revision uint64
}

func (e leaseFakeEntry) Key() string                { return e.key }
func (e leaseFakeEntry) Value() []byte              { return e.value }
func (e leaseFakeEntry) Revision() uint64           { return e.revision }
func (e leaseFakeEntry) Created() time.Time         { return time.Now() }
func (e leaseFakeEntry) Delta() uint64              { return 0 }
func (e leaseFakeEntry) Operation() nats.KeyValueOp { return 0 }
func (e leaseFakeEntry) Bucket() string             { return "test" }

var _ nats.KeyValueEntry = leaseFakeEntry{}

// leaseFakeKV is a tiny in-process nats.KeyValue good enough for lease.go's
// CAS contention tests: real Create/Get/Update/Delete semantics (including
// Update's revision check), shared by however many *ConfigManager instances
// a test constructs, so two managers racing tryAcquireOrRenewLease against
// the SAME leaseFakeKV genuinely contend the way two pods racing the same
// NATS KV bucket would. Every method beyond those four panics: nothing in
// lease.go calls them, and a lease test that starts calling them wants a
// loud failure, not a silent no-op.
type leaseFakeKV struct {
	mu      sync.Mutex
	data    map[string][]byte
	rev     map[string]uint64
	nextRev uint64
}

func newLeaseFakeKV() *leaseFakeKV {
	return &leaseFakeKV{data: map[string][]byte{}, rev: map[string]uint64{}}
}

func (f *leaseFakeKV) Get(key string) (nats.KeyValueEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return leaseFakeEntry{key: key, value: v, revision: f.rev[key]}, nil
}

func (f *leaseFakeKV) Create(key string, value []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.data[key]; ok {
		return 0, nats.ErrKeyExists
	}
	f.nextRev++
	f.data[key] = value
	f.rev[key] = f.nextRev
	return f.nextRev, nil
}

func (f *leaseFakeKV) Update(key string, value []byte, last uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.rev[key]
	if !ok || cur != last {
		return 0, errors.New("wrong last sequence")
	}
	f.nextRev++
	f.data[key] = value
	f.rev[key] = f.nextRev
	return f.nextRev, nil
}

func (f *leaseFakeKV) Delete(key string, _ ...nats.DeleteOpt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.data[key]; !ok {
		return nats.ErrKeyNotFound
	}
	delete(f.data, key)
	delete(f.rev, key)
	return nil
}

func (f *leaseFakeKV) GetRevision(string, uint64) (nats.KeyValueEntry, error) {
	panic("not used by lease.go")
}
func (f *leaseFakeKV) Put(string, []byte) (uint64, error)       { panic("not used by lease.go") }
func (f *leaseFakeKV) PutString(string, string) (uint64, error) { panic("not used by lease.go") }
func (f *leaseFakeKV) Purge(string, ...nats.DeleteOpt) error    { panic("not used by lease.go") }
func (f *leaseFakeKV) Watch(string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	panic("not used by lease.go")
}
func (f *leaseFakeKV) WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error) {
	panic("not used by lease.go")
}
func (f *leaseFakeKV) Keys(...nats.WatchOpt) ([]string, error) { panic("not used by lease.go") }
func (f *leaseFakeKV) ListKeys(...nats.WatchOpt) (nats.KeyLister, error) {
	panic("not used by lease.go")
}
func (f *leaseFakeKV) History(string, ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	panic("not used by lease.go")
}
func (f *leaseFakeKV) Bucket() string                       { return "test" }
func (f *leaseFakeKV) PurgeDeletes(...nats.PurgeOpt) error  { panic("not used by lease.go") }
func (f *leaseFakeKV) Status() (nats.KeyValueStatus, error) { panic("not used by lease.go") }

var _ nats.KeyValue = (*leaseFakeKV)(nil)

func TestLease_TwoManagersContend_ExactlyOneAcquires(t *testing.T) {
	kv := newLeaseFakeKV()
	m1 := newTestManager(kv)
	m2 := newTestManager(kv)
	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m1.SetClock(clock)
	m2.SetClock(clock)

	ttl := time.Minute
	ok1 := m1.tryAcquireOrRenewLease("worker-1", ttl)
	ok2 := m2.tryAcquireOrRenewLease("worker-2", ttl)

	require.True(t, ok1, "the first contender to CAS-create the lease must win it")
	require.False(t, ok2, "the second contender must lose: the lease already exists and has not expired")

	// Renewal: worker-1 (the holder) can keep renewing indefinitely.
	assert.True(t, m1.tryAcquireOrRenewLease("worker-1", ttl), "the current holder must be able to renew its own lease")
	// worker-2 still cannot steal a live lease.
	assert.False(t, m2.tryAcquireOrRenewLease("worker-2", ttl), "a non-holder must not acquire a still-live lease")
}

func TestLease_ExpiredLeaseIsStolenByAnotherManager(t *testing.T) {
	kv := newLeaseFakeKV()
	m1 := newTestManager(kv)
	m2 := newTestManager(kv)
	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m1.SetClock(clock)
	m2.SetClock(clock)

	ttl := time.Minute
	require.True(t, m1.tryAcquireOrRenewLease("worker-1", ttl))

	// worker-1 "dies": it stops renewing. Advance the clock past the TTL.
	clock.now = clock.now.Add(2 * time.Minute)

	require.True(t, m2.tryAcquireOrRenewLease("worker-2", ttl), "an expired lease must be stealable by another replica within ~one TTL")

	// worker-1 trying to renew its now-superseded lease must fail: worker-2
	// owns a newer revision.
	assert.False(t, m1.tryAcquireOrRenewLease("worker-1", ttl), "the original holder must not be able to renew after losing the lease to a steal")
}

func TestLease_GatesTickPauseExpirySweepToTheLeader(t *testing.T) {
	kv := newLeaseFakeKV()
	m := newTestManager(kv)
	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	// Before StartLeaseLoop is ever called, leaseGatingEnabled is false and
	// the sweep must run unconditionally (pre-RM-3 single-manager
	// behavior); tickPauseExpiry on an empty bucket is a cheap no-op we can
	// call directly without setting up any pipeline fixtures.
	assert.False(t, m.leaseGatingEnabled())
	assert.False(t, m.IsLeader())

	// Simulate having lost an election: leaseGatingEnabled true, isLeader
	// false. tickPauseExpiry must return before touching kv.Keys() (which
	// this leaseFakeKV does not even implement, so a call would panic).
	m.leaseMu.Lock()
	m.leaseEnabled = true
	m.isLeader = false
	m.leaseMu.Unlock()

	assert.NotPanics(t, func() { m.tickPauseExpiry(context.Background()) }, "a non-leader's tick must be a cheap no-op, never touching kv.Keys()")
}

func TestPutLifecycleRecordCAS_RejectsStaleRevision(t *testing.T) {
	kv := newLeaseFakeKV()
	m := newTestManager(kv)

	id := "p1"
	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, UpdatedAt: time.Now()}
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	rev, err := kv.Create(protocol.LifecycleStateKey(id), data)
	require.NoError(t, err)

	// Someone else writes in between our (hypothetical) read and write --
	// simulated by writing again directly through the fake KV, advancing
	// the revision past what our CAS call below still thinks is current.
	rec2 := rec
	rec2.State = protocol.StatePaused
	data2, err := json.Marshal(rec2)
	require.NoError(t, err)
	_, err = kv.Update(protocol.LifecycleStateKey(id), data2, rev)
	require.NoError(t, err)

	// Now attempt a CAS write against the stale revision `rev`: must be
	// rejected, and must NOT clobber the record the concurrent writer just
	// wrote.
	staleRec := rec
	staleRec.State = protocol.StateStopped
	err = m.putLifecycleRecordCAS(id, staleRec, rev)
	require.Error(t, err, "a CAS write against a superseded revision must be rejected")

	got, _, ok := m.getLifecycleRecordRev(id)
	require.True(t, ok)
	assert.Equal(t, protocol.StatePaused, got.State, "the concurrent writer's state must survive a rejected stale CAS, not be overwritten")
}

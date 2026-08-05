// Package config manages pipeline configuration, lifecycle state and the
// supervisor that reconciles desired versus actual pipeline workers.
package config

import (
	"context"
	"encoding/json"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// RM-3: production runs 3-20 ConfigManager replicas (one per pod,
// deploy/helm-chart/values.production.yml minReplicas/maxReplicas), but
// StartPauseExpiryTicker's sweep (WS-3/WS-4/WS-6/WS-7) was written assuming
// a single manager -- every replica ticks the full KV bucket, so every
// sweep runs 3-20x concurrently. That violates WS-7's "one chunk of
// source+sink I/O per tick" rate limit by a factor of N, and lets one
// replica's blind Put clobber another's concurrent write.
//
// leaseRecord is the payload stored at protocol.KeyManagerSweepLease. Only
// the current leaseholder (Owner, while Now() < ExpiresAt) may run the
// ticker's sweep body; every other replica's tick is a no-op. This is a
// lease, not a lock: it is acquired/renewed/stolen via CAS against the NATS
// KV revision (the same idiom putLifecycleRecordCAS already uses against
// this bucket), never via an external coordination service.
type leaseRecord struct {
	// Owner is the holder's workerID (the same hostname-derived ID
	// cmd/pipeline/main.go already mints for heartbeats/logging -- reused
	// here rather than minting a second identity).
	Owner string `json:"owner"`
	// ExpiresAt is when this lease lapses if not renewed. A dead leader's
	// lease is stolen by the next replica to observe Now() >= ExpiresAt,
	// bounding failover time to roughly one TTL.
	ExpiresAt time.Time `json:"expires_at"`
}

// StartLeaseLoop launches the leader-election goroutine RM-3 adds: workerID
// (this replica's identity) contends for protocol.KeyManagerSweepLease
// every ttl/2, and IsLeader() reports the outcome of the most recent
// attempt. Until this is called, tickPauseExpiry runs its sweep
// unconditionally (leaseEnabled stays false) -- the pre-RM-3 single-manager
// behavior every existing pause_expiry/wal_guard/resnapshot_watcher/
// reconciliation test relies on. cmd/pipeline/main.go calls this alongside
// StartPauseExpiryTicker in production.
//
// ttl <= 0 defaults to 2 minutes; the renewal cadence is ttl/2 so a live
// leader renews well before its own lease could be stolen out from under
// it by clock/scheduling jitter alone.
func (m *ConfigManager) StartLeaseLoop(ctx context.Context, workerID string, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	interval := ttl / 2
	if interval <= 0 {
		interval = time.Second
	}

	m.leaseMu.Lock()
	m.leaseEnabled = true
	m.leaseMu.Unlock()

	go func() {
		// Attempt immediately so leadership is not delayed by the first
		// full interval -- a freshly (re)started pod should not wait
		// ttl/2 before it can even try to lead.
		m.setLeader(m.tryAcquireOrRenewLease(workerID, ttl))

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				m.releaseLease(workerID)
				return
			case <-ticker.C:
				m.setLeader(m.tryAcquireOrRenewLease(workerID, ttl))
			}
		}
	}()
}

// IsLeader reports whether this ConfigManager currently believes it holds
// the manager-sweep lease, as of the most recent acquire/renew attempt.
// tickPauseExpiry is the only production caller; exported so tests outside
// the package (none currently) and operators (a future /healthz field)
// could consult it too.
func (m *ConfigManager) IsLeader() bool {
	m.leaseMu.RLock()
	defer m.leaseMu.RUnlock()
	return m.isLeader
}

// leaseGatingEnabled reports whether StartLeaseLoop has ever been called on
// this manager. tickPauseExpiry uses this (not IsLeader alone) to decide
// whether lease gating applies at all -- see StartLeaseLoop's doc comment.
func (m *ConfigManager) leaseGatingEnabled() bool {
	m.leaseMu.RLock()
	defer m.leaseMu.RUnlock()
	return m.leaseEnabled
}

func (m *ConfigManager) setLeader(leader bool) {
	m.leaseMu.Lock()
	wasLeader := m.isLeader
	m.isLeader = leader
	m.leaseMu.Unlock()
	if leader != wasLeader {
		if leader {
			log.Info().Msg("manager sweep lease: acquired, this replica now runs the pause-expiry sweep")
		} else {
			log.Warn().Msg("manager sweep lease: lost or not held, this replica's pause-expiry sweep is now a no-op")
		}
	}
}

// tryAcquireOrRenewLease is the CAS create-or-renew-or-steal-if-expired
// primitive at the heart of the lease: exactly one of Create (nothing
// exists yet) or Update-with-matching-revision (renewing our own lease, or
// stealing an expired one) can win per KV revision, so concurrent replicas
// racing this function never both believe they hold the lease. Mirrors the
// update-if-revision idiom putLifecycleRecordCAS (reconciliation.go) already
// uses against this same KV bucket.
func (m *ConfigManager) tryAcquireOrRenewLease(workerID string, ttl time.Duration) bool {
	now := m.getClock().Now()
	next := leaseRecord{Owner: workerID, ExpiresAt: now.Add(ttl)}
	data, err := json.Marshal(next)
	if err != nil {
		log.Error().Err(err).Msg("manager sweep lease: failed to marshal lease record")
		return false
	}

	entry, err := m.kv.Get(protocol.KeyManagerSweepLease)
	if err != nil {
		// Nothing there yet (or the key was purged): try to create it.
		// Create only succeeds if no other replica beat us to it -- if one
		// did, we simply are not the leader this round.
		if _, err := m.kv.Create(protocol.KeyManagerSweepLease, data); err != nil {
			return false
		}
		return true
	}

	var cur leaseRecord
	if err := json.Unmarshal(entry.Value(), &cur); err != nil {
		// Corrupt record: treat it as expired and try to steal it via CAS
		// rather than trusting it.
		cur = leaseRecord{}
	}

	if cur.Owner != workerID && cur.ExpiresAt.After(now) {
		// Someone else holds a still-live lease: not our turn.
		return false
	}

	// Either we already own it (renewal) or it has expired (steal). Either
	// way, CAS against the revision we just read: if another replica wrote
	// in between, this fails and we simply are not the leader this round --
	// never fall back to an unconditional Put, which would defeat the
	// whole point of the CAS.
	if _, err := m.kv.Update(protocol.KeyManagerSweepLease, data, entry.Revision()); err != nil {
		return false
	}
	return true
}

// releaseLease is StartLeaseLoop's shutdown path: best-effort, and only
// acts when we still appear to be the recorded owner (never blind-deletes
// a lease some other replica may have already stolen after our last
// renewal lapsed). A missed release is not a correctness problem -- the
// lease simply expires on its own within one TTL, exactly as if this pod
// had crashed instead of shutting down cleanly.
func (m *ConfigManager) releaseLease(workerID string) {
	entry, err := m.kv.Get(protocol.KeyManagerSweepLease)
	if err != nil {
		return
	}
	var cur leaseRecord
	if err := json.Unmarshal(entry.Value(), &cur); err != nil || cur.Owner != workerID {
		return
	}
	if err := m.kv.Delete(protocol.KeyManagerSweepLease); err != nil && err != nats.ErrKeyNotFound {
		log.Warn().Err(err).Msg("manager sweep lease: failed to release on shutdown; will expire on its own")
	}
}

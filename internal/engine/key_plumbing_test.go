package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestRecoverEvoStates_NormalizesQualifiedConfigEntry is the regression test
// for the §1.1 "recovered table state is silently discarded" /
// "buffer writes and drains target different streams" bug family
// (MULTI_SCHEMA_PLAN.md §1.1, §3 Stage 1). recoverEvoStates used to key KV
// lookups and the in-memory evoStates/tableStates maps off the RAW
// p.config.Tables entry ("public.orders"), while the hot path
// (detectSchemaChange, publishBufferBatch, transitionTableToCDC, ...) keys
// off the bare m.Table ("orders"). For any config written qualified, the two
// identities never met: recovery silently found nothing, and a table stuck
// mid-evolution silently resumed as STABLE.
//
// This calls the real production recoverEvoStates. The mock KV is wired to
// respond ONLY to the KeyToken()-normalised keys ("orders", not
// "public.orders"); gomock fails on any unexpected call, so if
// recoverEvoStates is reverted to query raw config strings, the KV calls it
// issues no longer match these expectations and the test fails with a
// "missing call" / unexpected-call error rather than silently passing.
func TestRecoverEvoStates_NormalizesQualifiedConfigEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	const pipelineID = "pipeline-1"
	const sourceID = "source-1"
	bareRef := protocol.TableRef{Schema: "public", Table: "orders"}

	evoKey := protocol.SchemaEvolutionKey(pipelineID, bareRef)
	stateKey := protocol.TableStateKey(pipelineID, sourceID, bareRef)
	cpKey := protocol.IngressCheckpointKey(pipelineID, sourceID, bareRef)

	evoState := tableEvolution{
		Status:            protocol.SchemaStatusFrozen, // Frozen, not Draining, so no flushBuffer goroutine (needs live NATS) is launched
		CachedSchema:      map[string]string{"id": "bigint"},
		AcknowledgedSinks: map[string]bool{},
	}
	evoData, err := json.Marshal(evoState)
	require.NoError(t, err)

	kv.EXPECT().Get(evoKey).Return(remediationKVEntry{key: evoKey, value: evoData, revision: 7}, nil)
	kv.EXPECT().Get(stateKey).Return(remediationKVEntry{key: stateKey, value: []byte(protocol.TableStateSnapshotting)}, nil)
	kv.EXPECT().Get(cpKey).Return(nil, errors.New("not found"))

	producer := &Producer{
		pipelineID:  pipelineID,
		kv:          kv,
		evoStates:   make(map[string]*tableEvolution),
		tableStates: make(map[string]string),
	}
	producer.config.Tables = []string{"public.orders"} // config-shaped, qualified with the default schema
	producer.sourceConfig.ID = sourceID

	producer.recoverEvoStates(context.Background())

	// Recovered state must be keyed identically to how the hot path would
	// key the SAME table (m.Table == "orders", bare) -- not under the raw
	// config string "public.orders".
	st, ok := producer.evoStates["orders"]
	require.True(t, ok, "evolution state must be recovered under the bare KeyToken, matching the hot path's m.Table key")
	assert.Equal(t, protocol.SchemaStatusFrozen, st.Status)
	assert.Equal(t, protocol.TableStateSnapshotting, producer.tableStates["orders"])

	_, wrongKey := producer.evoStates["public.orders"]
	assert.False(t, wrongKey, "must not also (or instead) key state under the raw qualified config string")

	// Bug #2 ("buffer writes and drains target different streams"): both
	// publishBufferBatch (write side, keyed by the hot path's bare m.Table)
	// and flushBuffer (drain side, keyed by whatever recoverEvoStates just
	// recovered) must derive the SAME buffer topic for this table. Since
	// recoverEvoStates now stores state under "orders" (see above), a
	// hot-path publish for m.Table=="orders" finds the SAME recovered
	// Snapshotting state and buffers -- proving the write and drain sides
	// agree on the table's identity.
	err = producer.publishBufferBatch(context.Background(), "orders", protocol.MessageBatch{{
		SourceID: sourceID,
		Table:    "orders",
		Op:       protocol.OpInsert,
	}}, 0)
	// publishWithRetry with maxRetries<=0 returns errPublishRetriesExhausted
	// immediately without touching the network -- irrelevant here, this call
	// exists only to prove publishBufferBatch reads producer.tableStates["orders"]
	// (shouldBuffer==true) rather than finding an empty map because recovery
	// used a different key.
	require.ErrorIs(t, err, errPublishRetriesExhausted)
}

// TestConsumerLoadStats_NormalizesQualifiedConfigEntry is the regression test
// for the §1.1 "restored stats are orphaned" bug (MULTI_SCHEMA_PLAN.md §1.1,
// §3 Stage 1). LoadStats used to seed c.stats[sourceID+"."+table] from the
// raw config.Tables string, while the hot path (updateStats/handleSinkError)
// keys off sourceID+"."+m.Table (bare). TotalSynced would silently reset to
// zero on every restart for any qualified config entry.
//
// Calls the real production LoadStats. The mock KV only answers the
// KeyToken()-normalised TableStatsKey; if LoadStats is reverted to build the
// key from the raw config string, this call no longer matches and the test
// fails.
func TestConsumerLoadStats_NormalizesQualifiedConfigEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	const pipelineID = "p1"
	const sourceID = "source-1"
	const sinkID = "sink1"
	bareRef := protocol.TableRef{Schema: "public", Table: "orders"}

	statsKey := protocol.TableStatsKey(pipelineID, sourceID, sinkID, bareRef)
	stats := protocol.TableStats{Status: "ACTIVE", TotalSynced: 42}
	statsData, err := json.Marshal(stats)
	require.NoError(t, err)

	kv.EXPECT().Get(statsKey).Return(remediationKVEntry{key: statsKey, value: statsData}, nil)

	c := NewConsumer(pipelineID, sinkID, nil, nil, nil, nil, kv, 10, 0, protocol.RetryConfig{}, nil, nil)

	// public.orders is the config-shaped, qualified form; the hot path only
	// ever sees the bare "orders" (m.Table), the same identity produced by
	// TableRef{public,"orders"}.KeyToken().
	c.LoadStats(sourceID, []string{"public.orders"})

	restored, ok := c.stats[sourceID+".orders"]
	require.True(t, ok, "restored stats must be findable under sourceID+\".\"+m.Table, matching the hot path")
	assert.Equal(t, uint64(42), restored.TotalSynced)

	_, wrongKey := c.stats[sourceID+".public.orders"]
	assert.False(t, wrongKey, "must not key restored stats under the raw qualified config string")
}

package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	stream_nats "github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"go.uber.org/mock/gomock"
)

// TestDetectSchemaChange_CrossSchemaSameTableName_IndependentEvolutionState is
// the Stage 2b regression test for MULTI_SCHEMA_PLAN.md §11.2 requirement 5
// on the HOT PATH (detectSchemaChange/performSchemaEvolution), complementing
// key_plumbing_test.go's recoverEvoStates coverage. Before this stage,
// detectSchemaChange keyed p.evoStates off bare m.Table -- so a
// "public.orders" row and a "sales.orders" row shared ONE evolution-state
// entry ("orders"). Freezing one table for a schema change would silently
// also freeze (or be silently freed by) the other, unrelated table.
//
// This calls the real production detectSchemaChange/performSchemaEvolution.
// Reverting detectSchemaChange's key from msgTableRef(m).KeyToken() back to
// bare m.Table collapses both tables onto a single "orders" evoStates entry:
// the second warm-up call below would find (and silently share) the first
// table's state instead of creating its own, and the two
// require.True(t, ok, ...) key-existence assertions after the freeze would
// fail because the "sales=orders" entry would never have been created.
func TestDetectSchemaChange_CrossSchemaSameTableName_IndependentEvolutionState(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	const pipelineID = "pipeline-1"
	publicRef := protocol.TableRef{Schema: "public", Table: "orders"}
	evoKey := protocol.SchemaEvolutionKey(pipelineID, publicRef)
	kv.EXPECT().Put(evoKey, gomock.Any()).Return(uint64(1), nil)

	producer := &Producer{
		pipelineID: pipelineID,
		kv:         kv,
		evoStates:  make(map[string]*tableEvolution),
		config:     protocol.PipelineConfig{Sinks: []string{"sink1"}},
	}

	// Warm both tables' evolution cache with an identical column set. Bare
	// Table is "orders" for BOTH messages -- only TableSchema distinguishes
	// them.
	publicMsg := protocol.Message{SourceID: "s1", Table: "orders", TableSchema: "public", Data: map[string]interface{}{"id": 1}}
	salesMsg := protocol.Message{SourceID: "s1", Table: "orders", TableSchema: "sales", Data: map[string]interface{}{"id": 1}}

	_, changed := producer.detectSchemaChange(publicMsg)
	require.False(t, changed, "first sighting of a table only warms the cache")
	_, changed = producer.detectSchemaChange(salesMsg)
	require.False(t, changed, "first sighting of a table only warms the cache")

	producer.muEvo.RLock()
	_, publicExists := producer.evoStates["orders"]
	_, salesExists := producer.evoStates["sales=orders"]
	producer.muEvo.RUnlock()
	require.True(t, publicExists, "public.orders must be tracked under the bare KeyToken")
	require.True(t, salesExists, "sales.orders must be tracked under its OWN KeyToken, not share public.orders' entry")

	// Now evolve ONLY the public table -- add a new column.
	publicMsgV2 := protocol.Message{SourceID: "s1", Table: "orders", TableSchema: "public", Data: map[string]interface{}{"id": 1, "new_col": "x"}}
	diff, changed := producer.detectSchemaChange(publicMsgV2)
	require.True(t, changed)
	require.Equal(t, "public", diff.TableSchema, "diff must carry the sibling schema field so a Stage-1b writer downstream is not blind to it")

	producer.muEvo.RLock()
	publicState := producer.evoStates["orders"]
	salesState := producer.evoStates["sales=orders"]
	producer.muEvo.RUnlock()

	require.Equal(t, protocol.SchemaStatusFrozen, publicState.Status, "the table that actually changed must freeze")
	require.Equal(t, protocol.SchemaStatusStable, salesState.Status, "an UNRELATED table sharing the bare table name must be untouched by the other table's freeze")
}

// TestRecoverEvoStates_QualifiedNonPublicSchema_RoundTripsToHotPath extends
// key_plumbing_test.go's TestRecoverEvoStates_NormalizesQualifiedConfigEntry
// (which only exercises the "public.orders" bare-equivalent case) to a
// genuinely non-public schema, and additionally proves the round trip reaches
// the HOT PATH (detectSchemaChange), not just recoverEvoStates' own map.
// This is the "state-key round-tripping across a restart" regression: it
// persists frozen evolution state for "sales.orders" (as a prior process
// session would have via performSchemaEvolution/persistEvoState), then
// starts a FRESH Producer (simulating a restart) that recovers it from KV,
// and finally proves a live CDC message for that table
// (Table:"orders",TableSchema:"sales") is buffered rather than silently
// treated as a brand-new, un-frozen table -- which is what happens if the
// hot path's key (msgTableRef(m).KeyToken()) and recoverEvoStates' key
// (ref.KeyToken()) ever diverge again.
func TestRecoverEvoStates_QualifiedNonPublicSchema_RoundTripsToHotPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	const pipelineID = "pipeline-1"
	const sourceID = "source-1"
	salesRef := protocol.TableRef{Schema: "sales", Table: "orders"}

	evoKey := protocol.SchemaEvolutionKey(pipelineID, salesRef)
	stateKey := protocol.TableStateKey(pipelineID, sourceID, salesRef)
	cpKey := protocol.IngressCheckpointKey(pipelineID, sourceID, salesRef)

	priorSession := tableEvolution{
		Status:            protocol.SchemaStatusFrozen,
		CorrelationID:     "corr-1",
		CachedSchema:      map[string]string{"id": "bigint"},
		AcknowledgedSinks: map[string]bool{},
	}
	evoData, err := json.Marshal(priorSession)
	require.NoError(t, err)

	kv.EXPECT().Get(evoKey).Return(remediationKVEntry{key: evoKey, value: evoData, revision: 3}, nil)
	kv.EXPECT().Get(stateKey).Return(nil, errors.New("not found"))
	kv.EXPECT().Get(cpKey).Return(nil, errors.New("not found"))

	producer := &Producer{
		pipelineID:  pipelineID,
		kv:          kv,
		evoStates:   make(map[string]*tableEvolution),
		tableStates: make(map[string]string),
	}
	producer.config.Tables = []string{"sales.orders"}
	producer.sourceConfig.ID = sourceID

	producer.recoverEvoStates(context.Background())

	producer.muEvo.RLock()
	recovered, ok := producer.evoStates["sales=orders"]
	producer.muEvo.RUnlock()
	require.True(t, ok, "recovery must land under the qualified KeyToken \"sales=orders\"")
	require.Equal(t, protocol.SchemaStatusFrozen, recovered.Status)

	// The "restart" is complete. Now drive the hot path with a live message
	// for the SAME table and prove it finds the recovered Frozen state
	// (buffers, does not re-treat the table as new/unfrozen).
	liveMsg := protocol.Message{SourceID: sourceID, Table: "orders", TableSchema: "sales", Data: map[string]interface{}{"id": 1, "another_new_col": "z"}}
	diff, changed := producer.detectSchemaChange(liveMsg)
	require.Nil(t, diff)
	require.False(t, changed, "a table recovered as Frozen must not detect (and re-emit) another schema change on the hot path")

	// The weaker (diff==nil, changed==false) assertions above pass for the
	// WRONG reason too: if the hot path keyed off bare m.Table ("orders")
	// instead of msgTableRef(m).KeyToken() ("sales=orders"), it would find
	// no entry under "orders", silently initialize a FRESH Stable state
	// there, and return (nil, false) from the "first sighting" branch --
	// same return values, completely different (and wrong) behavior: the
	// recovered Frozen state would sit inert and unreachable forever. Assert
	// directly against the map to rule that out.
	producer.muEvo.RLock()
	stillFrozen, ok := producer.evoStates["sales=orders"]
	_, bareKeyCreated := producer.evoStates["orders"]
	producer.muEvo.RUnlock()
	require.True(t, ok, "the recovered state must still live under \"sales=orders\"")
	require.Equal(t, protocol.SchemaStatusFrozen, stillFrozen.Status, "the hot path must have found and used the recovered entry, not replaced it")
	require.False(t, bareKeyCreated, "the hot path must not fall back to creating a bare-keyed entry for a qualified-schema table")
}

// TestBufferWriteAndDrain_SameStream_QualifiedSchema is the Stage 2b
// regression test for MULTI_SCHEMA_PLAN.md §11.2 requirement 4 (buffer
// topic and durable name MUST derive from the same TableRef on write and
// drain) for a NON-public schema -- attempt 1's exact failure mode: the
// drain subscribed to an empty subject, saw zero pending, declared success,
// and flipped to CDC while buffered rows sat in another stream forever.
//
// This calls the REAL production publishBufferBatch (write side) and the
// REAL production flushBuffer (drain side) against a live JetStream server
// (testcontainers), NOT a recomputed topic string (§11.3: a test that just
// asserts two independently-recomputed strings are equal would pass even if
// publishBufferBatch and flushBuffer used different derivations, as long as
// each one internally happened to be self-consistent -- see attempt 1's
// multi_schema_key_plumbing_test.go:87, called out by name in the plan).
// Needs docker; skipped under -short.
func TestBufferWriteAndDrain_SameStream_QualifiedSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-JetStream test in short mode")
	}

	ctx := context.Background()
	natsC, err := tcnats.Run(ctx, "nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	require.NoError(t, err)
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	require.NoError(t, err)

	realPub, err := stream_nats.NewNatsPublisher(natsURL)
	require.NoError(t, err)
	defer realPub.Close()

	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	kv.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	const pipelineID = "buf-drain-p1"
	const sourceID = "source-1"
	ref := protocol.TableRef{Schema: "sales", Table: "orders"}
	key := ref.KeyToken()
	require.Equal(t, "sales=orders", key)

	producer := &Producer{
		pipelineID:      pipelineID,
		natsURL:         natsURL,
		publisher:       realPub,
		kv:              kv,
		cb:              &remediationCircuitBreaker{},
		circuitCoolDown: time.Millisecond,
		evoStates:       make(map[string]*tableEvolution),
		// Draining (rather than Snapshotting) so flushBuffer's
		// transitionTableToCDC below actually flips the table once the
		// drain proves empty, matching how handleDynamicTables/handleSchemaAck
		// drive this in production (they set Draining immediately before
		// calling flushBuffer).
		tableStates: map[string]string{key: protocol.TableStateDraining},
	}
	producer.sourceConfig.ID = sourceID

	// Subscribe to the MAIN ingest topic BEFORE draining, so we can observe
	// whatever flushBuffer republishes there.
	mainTopic := "cdc_pipeline_" + pipelineID + "_ingest"
	mainSub, err := stream_nats.NewNatsSubscriber(natsURL, "main-observer", mainTopic, 10, 5*time.Second)
	require.NoError(t, err)
	defer mainSub.Close()
	mainChan, err := mainSub.Subscribe(ctx, mainTopic)
	require.NoError(t, err)

	// Write side: publishBufferBatch, for the qualified table, while the
	// table is Draining (so it must route to the BUFFER stream, not the
	// main one).
	writeBatch := protocol.MessageBatch{{
		SourceID:    sourceID,
		Table:       "orders",
		TableSchema: "sales",
		Op:          protocol.OpInsert,
		Data:        map[string]interface{}{"id": float64(1)},
	}}
	require.NoError(t, producer.publishBufferBatch(ctx, key, writeBatch, 5))

	// Prove nothing landed on the main topic from the write alone.
	select {
	case <-mainChan:
		t.Fatal("buffered write must not appear on the main ingest topic before a drain")
	case <-time.After(500 * time.Millisecond):
	}

	// Drain side: flushBuffer, for the SAME key. If write and drain derive
	// the topic/durable name from different identities, this subscribes to
	// an empty/different stream, observes zero pending immediately, and the
	// buffered batch above is never seen on mainChan.
	producer.flushBuffer(ctx, key)

	select {
	case wmMsg := <-mainChan:
		wmMsg.Ack()
		var got []protocol.Message
		_, err := protocol.UnmarshalMessageBatch(wmMsg.Payload, &got)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "orders", got[0].Table)
		require.Equal(t, "sales", got[0].TableSchema)
	case <-time.After(15 * time.Second):
		t.Fatal("flushBuffer never republished the buffered row to the main topic -- write and drain diverged on stream identity")
	}

	producer.muTableStates.RLock()
	finalState := producer.tableStates[key]
	producer.muTableStates.RUnlock()
	require.Equal(t, protocol.TableStateCDC, finalState, "a fully-drained Snapshotting table must flip to CDC")
}

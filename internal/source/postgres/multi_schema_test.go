package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// TestBuildMessage_NonPublicSchema_Insert_NotFiltered is the
// MULTI_SCHEMA_PLAN.md §5 "Filtering regression (Stage 2's silent
// failure)" test: a table in a whitelisted NON-public schema must be
// treated as handlerKindData, never handlerKindFiltered. A filtered event
// self-acks and advances the watermark with no error -- a partial fix here
// is invisible to any public-only test, which is exactly why plan §3
// Stage 2 calls this the highest-risk edit.
//
// This calls the real, unexported PostgresSource.buildMessage directly --
// not a re-implementation of its filtering logic -- so it can only pass if
// the production knownTables lookup actually keys on TableRef (schema AND
// table), not a bare table-name string.
func TestBuildMessage_NonPublicSchema_Insert_NotFiltered(t *testing.T) {
	s := NewPostgresSource("multi-schema-filter-source")
	var mu sync.Mutex
	var msgs []protocol.Message
	knownTables := map[protocol.TableRef]bool{{Schema: "sales", Table: "orders"}: true}

	lc := &replication.ListenerContext{
		Message: &format.Insert{TableNamespace: "sales", TableName: "orders", Decoded: map[string]any{"id": 1}},
		LSN:     pq.LSN(42),
	}

	res := s.buildMessage(lc, &mu, &msgs, knownTables)

	require.Equal(t, handlerKindData, res.kind,
		"a table in a whitelisted non-public schema must be handlerKindData, not silently handlerKindFiltered")
	require.Len(t, msgs, 1)
	assert.Equal(t, "orders", msgs[0].Table,
		"Message.Table MUST stay bare -- MULTI_SCHEMA_PLAN.md §2.2 / §11.2 requirement 2")
	assert.Equal(t, "sales", msgs[0].TableSchema,
		"the schema must be stamped on the sibling field, not folded into Table")
}

// TestBuildMessage_SameTableName_DifferentSchema_Distinguished is the plan
// §5 "cross-schema collision" case at the filtering layer: "public.users"
// and "sales.users" must not be treated as the same table.
func TestBuildMessage_SameTableName_DifferentSchema_Distinguished(t *testing.T) {
	s := NewPostgresSource("multi-schema-collision-source")
	var mu sync.Mutex
	var msgs []protocol.Message
	// Only "sales.users" is whitelisted.
	knownTables := map[protocol.TableRef]bool{{Schema: "sales", Table: "users"}: true}

	publicLC := &replication.ListenerContext{
		Message: &format.Insert{TableNamespace: "public", TableName: "users", Decoded: map[string]any{"id": 1}},
		LSN:     pq.LSN(1),
	}
	salesLC := &replication.ListenerContext{
		Message: &format.Insert{TableNamespace: "sales", TableName: "users", Decoded: map[string]any{"id": 2}},
		LSN:     pq.LSN(2),
	}

	publicRes := s.buildMessage(publicLC, &mu, &msgs, knownTables)
	salesRes := s.buildMessage(salesLC, &mu, &msgs, knownTables)

	assert.Equal(t, handlerKindFiltered, publicRes.kind, "public.users is not whitelisted and must be filtered")
	assert.Equal(t, handlerKindData, salesRes.kind, "sales.users IS whitelisted and must not be filtered")
	require.Len(t, msgs, 1, "only the sales.users event should have been appended")
	assert.Equal(t, "sales", msgs[0].TableSchema)
}

// TestStart_NonPublicTable_EventsNotFiltered exercises the full wiring --
// Start's knownTables construction feeding createHandler's real handler,
// which calls the real buildMessage -- not buildMessage in isolation. A
// bug confined to Start's knownTables construction (e.g. reverting to a
// bare-string key while buildMessage still expects a TableRef) would slip
// past the two buildMessage-only tests above but fails this one.
func TestStart_NonPublicTable_EventsNotFiltered(t *testing.T) {
	t.Setenv(strictAckEnvVar, "true") // pin strict_ack ON regardless of the ambient ENV default
	s := NewPostgresSource("multi-schema-start-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"sales.orders"}

	msgChan, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer s.Stop()

	got := make(chan []protocol.Message, 1)
	go func() {
		for batch := range msgChan {
			select {
			case got <- batch:
			default:
			}
		}
	}()

	handler := factory.Handler()
	require.NotNil(t, handler, "the factory must have captured the real handler built by createHandler")

	handler(&replication.ListenerContext{
		Message: &format.Insert{TableNamespace: "sales", TableName: "orders", Decoded: map[string]any{"a": 1}},
		Ack:     func() error { return nil },
		LSN:     pq.LSN(1),
	})

	select {
	case batch := <-got:
		require.Len(t, batch, 1)
		assert.Equal(t, protocol.OpInsert, batch[0].Op)
		assert.Equal(t, "orders", batch[0].Table, "Message.Table MUST stay bare")
		assert.Equal(t, "sales", batch[0].TableSchema)
	case <-time.After(3 * time.Second):
		t.Fatal("event against a whitelisted non-public table was never flushed -- it was silently filtered forever")
	}
}

// TestStart_SearchPathPinnedFromSchemas asserts the vendored connector is
// handed a config.Config.SearchPath derived from srcConfig.Schemas
// (MULTI_SCHEMA_PLAN.md §3 Stage 2 "pin search_path", vendored-patch MS-1),
// and that an empty/nil Schemas pins "public" -- never "" (which would pin
// nothing) and never all schemas (§8 item 4's "empty means public only").
func TestStart_SearchPathPinnedFromSchemas(t *testing.T) {
	s := NewPostgresSource("multi-schema-searchpath-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Schemas = nil
	cfg.Tables = []string{"public.t1"}

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	defer s.Stop()

	assert.Equal(t, "public", factory.LastConfig().SearchPath,
		"empty Schemas must pin search_path to \"public\" only, not all schemas")
}

func TestStart_SearchPathPinnedFromSchemas_NonPublic(t *testing.T) {
	s := NewPostgresSource("multi-schema-searchpath-source-2")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Schemas = []string{"sales", "public"}
	cfg.Tables = []string{"sales.orders"}

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	defer s.Stop()

	assert.Equal(t, "sales,public", factory.LastConfig().SearchPath)
}

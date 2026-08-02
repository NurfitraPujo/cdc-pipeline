package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

// TestBuildMessage_Update_ToastedColumn_SurfacedAsColumnKind is the WS-7
// regression test at the source.go layer: buildMessage's Update case must
// turn format.Update.NewToastedColumns into a
// protocol.ColumnKindToastedUnchanged entry in the emitted message's
// ColumnKinds, not let the column simply vanish from Data with no trace.
// This drives the real handler built by createHandler (through Start),
// exactly the "real path, not the function in isolation" the project's
// verification standard requires -- not a call to buildMessage directly.
func TestBuildMessage_Update_ToastedColumn_SurfacedAsColumnKind(t *testing.T) {
	s := NewPostgresSource("toast-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"public.widgets"}

	msgChan, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer func() { _ = s.Stop() }()

	handler := factory.Handler()
	require.NotNil(t, handler)

	countingAck := func() error { return nil }

	// An UPDATE where "name" changed but "long_bio" is an unchanged
	// TOASTed value Postgres elided from the wire tuple -- NewDecoded has
	// no key for it at all (matching what DecodeWithColumn/format.Update
	// actually produce, verified against the real wire format by
	// data_test.go / update_test.go in the vendored package), and
	// NewToastedColumns names it.
	handler(&replication.ListenerContext{
		Message: &format.Update{
			TableNamespace:    "public",
			TableName:         "widgets",
			NewDecoded:        map[string]any{"id": 1, "name": "new-name"},
			NewToastedColumns: []string{"long_bio"},
		},
		Ack: countingAck,
		LSN: 100,
	})

	select {
	case batch := <-msgChan:
		require.Len(t, batch, 1)
		m := batch[0]
		require.Equal(t, protocol.OpUpdate, m.Op)

		_, present := m.Data["long_bio"]
		require.False(t, present, "Data[long_bio] must be absent -- an unchanged TOASTed column carries no value on the wire")

		require.NotNil(t, m.ColumnKinds, "ColumnKinds must be populated to carry the WS-7 signal")
		require.Equal(t, protocol.ColumnKindToastedUnchanged, m.ColumnKinds["long_bio"],
			"ColumnKinds[long_bio] must flag the column as toasted-unchanged so a downstream consumer never treats its absence from Data as NULL")

		require.Equal(t, "new-name", m.Data["name"], "an ordinary changed column must be unaffected")
		_, nameHasKind := m.ColumnKinds["name"]
		require.False(t, nameHasKind, "an ordinary column must not get a ColumnKinds entry")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the update batch on msgChan")
	}
}

// TestBuildMessage_Update_NoToastedColumns is the configuration that does
// NOT exercise TOAST at all -- an ordinary update where NewToastedColumns
// is empty. ColumnKinds must stay nil, matching the pre-WS-7 message shape
// exactly for a kind-unaware consumer (the sanitize_transport_test.go
// suite establishes the same "nil when nothing needs it" contract for
// ColumnKindDecimal).
func TestBuildMessage_Update_NoToastedColumns(t *testing.T) {
	s := NewPostgresSource("toast-source-2")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	cfg.Tables = []string{"public.widgets"}

	msgChan, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, []string{"sink1"})
	require.NoError(t, err)
	defer func() { _ = s.Stop() }()

	handler := factory.Handler()
	require.NotNil(t, handler)

	countingAck := func() error { return nil }

	handler(&replication.ListenerContext{
		Message: &format.Update{
			TableNamespace: "public",
			TableName:      "widgets",
			NewDecoded:     map[string]any{"id": 1, "name": "new-name"},
		},
		Ack: countingAck,
		LSN: 100,
	})

	select {
	case batch := <-msgChan:
		require.Len(t, batch, 1)
		require.Nil(t, batch[0].ColumnKinds, "no TOASTed column in this update -- ColumnKinds must stay nil")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the update batch on msgChan")
	}
}

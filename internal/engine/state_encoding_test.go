package engine

import (
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestTableStatsSurviveWriterToReaderRoundTrip is the regression test for the
// KV state encoding split (ADR-0017 "Consequences"). The same TableStats key
// was written as JSON by updateTableError and as msgp by updateStats/
// handleSinkError, while LoadStats read it back as JSON -- so whichever writer
// ran last decided whether restore succeeded, and TotalSynced silently reset
// to zero on restart.
//
// This drives the REAL production writers and the REAL production reader,
// piping the exact bytes one Put()s into the entry the other Get()s. It does
// not assert an encoding, so it stays valid if the chosen encoding changes --
// it only asserts that writer and reader agree.
func TestTableStatsSurviveWriterToReaderRoundTrip(t *testing.T) {
	const pipelineID = "p1"
	const sourceID = "s1"
	const sinkID = "sink1"
	ref := protocol.TableRef{Schema: "public", Table: "orders"}
	statsKey := protocol.TableStatsKey(pipelineID, sourceID, sinkID, ref)

	// Each writer that targets a TableStats key, named as the production path
	// that reaches it. Every one must produce bytes LoadStats can read.
	writers := map[string]func(c *Consumer){
		"updateStats/hot path": func(c *Consumer) {
			c.updateStats([]protocol.Message{{
				SourceID:  sourceID,
				TableSchema: ref.Schema,
				Table:     ref.Table,
				Op:        protocol.OpInsert,
				Timestamp: time.Now(),
			}})
		},
		"updateTableError": func(c *Consumer) {
			c.updateTableError(sourceID, ref)
		},
	}

	for name, write := range writers {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			kv := mocks.NewMockKeyValue(ctrl)

			var written []byte
			kv.EXPECT().Put(statsKey, gomock.Any()).DoAndReturn(
				func(_ string, value []byte) (uint64, error) {
					// Copy: msgp buffers are reused by the caller.
					written = append([]byte(nil), value...)
					return 1, nil
				}).AnyTimes()
			// The hot path also writes an egress checkpoint; irrelevant here.
			kv.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

			writer := NewConsumer(pipelineID, sinkID, nil, nil, nil, nil, kv, 10, 0, protocol.RetryConfig{}, nil, nil)
			write(writer)
			require.NotEmpty(t, written, "production writer produced no bytes for %s", statsKey)

			// A fresh Consumer, as after a restart, reading what was written.
			reader := NewConsumer(pipelineID, sinkID, nil, nil, nil, nil, kv, 10, 0, protocol.RetryConfig{}, nil, nil)
			kv.EXPECT().Get(statsKey).Return(mockEntry{key: statsKey, value: written}, nil)
			reader.LoadStats(sourceID, []string{ref.Table})

			restored, ok := reader.stats[sourceID+"."+ref.KeyToken()]
			require.True(t, ok, "LoadStats could not decode what the production writer wrote")
			assert.NotEmpty(t, restored.Status, "decoded stats are zero-valued; reader and writer disagree on encoding")
		})
	}
}

// TotalSynced resetting to zero on restart was the user-visible symptom.
func TestTotalSyncedSurvivesRestart(t *testing.T) {
	const pipelineID = "p1"
	const sourceID = "s1"
	const sinkID = "sink1"
	ref := protocol.TableRef{Schema: "public", Table: "orders"}
	statsKey := protocol.TableStatsKey(pipelineID, sourceID, sinkID, ref)

	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	var written []byte
	kv.EXPECT().Put(statsKey, gomock.Any()).DoAndReturn(
		func(_ string, value []byte) (uint64, error) {
			written = append([]byte(nil), value...)
			return 1, nil
		}).AnyTimes()
	kv.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	writer := NewConsumer(pipelineID, sinkID, nil, nil, nil, nil, kv, 10, 0, protocol.RetryConfig{}, nil, nil)
	batch := make([]protocol.Message, 3)
	for i := range batch {
		batch[i] = protocol.Message{
			SourceID: sourceID, TableSchema: ref.Schema, Table: ref.Table,
			Op: protocol.OpInsert, Timestamp: time.Now(),
		}
	}
	writer.updateStats(batch)
	require.Equal(t, uint64(3), writer.stats[sourceID+"."+ref.KeyToken()].TotalSynced)

	reader := NewConsumer(pipelineID, sinkID, nil, nil, nil, nil, kv, 10, 0, protocol.RetryConfig{}, nil, nil)
	kv.EXPECT().Get(statsKey).Return(mockEntry{key: statsKey, value: written}, nil)
	reader.LoadStats(sourceID, []string{ref.Table})

	restored, ok := reader.stats[sourceID+"."+ref.KeyToken()]
	require.True(t, ok)
	assert.Equal(t, uint64(3), restored.TotalSynced, "TotalSynced must not reset across restart")
}

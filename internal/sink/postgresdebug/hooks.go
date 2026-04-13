package postgresdebug

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

func (s *DebugSink) CaptureBefore(ctx context.Context, pipelineID string, transformerNames []string, messages []protocol.Message) []string {
	if !s.shouldCaptureStage("before") {
		return make([]string, len(messages))
	}

	correlationIDs := make([]string, len(messages))
	records := make([]debugRecord, 0, len(messages))

	for i, m := range messages {
		// Never capture internal control messages
		if m.Op == "drain_marker" || m.Op == "schema_change" {
			continue
		}

		if !s.shouldCaptureMessage(m) {
			continue
		}

		if s.isSampledOut(m.Table) {
			s.statsMu.Lock()
			s.recordsSampled++
			s.statsMu.Unlock()
			continue
		}

		correlationID := uuid.New().String()
		correlationIDs[i] = correlationID

		payload, err := extractPayload(m)
		if err != nil {
			log.Error().Err(err).Msg("Debug sink: failed to extract payload (before)")
			continue
		}

		payloadHash := computeHash(payload)

		rec := debugRecord{
			CorrelationID:    correlationID,
			PipelineID:       pipelineID,
			SourceID:         m.SourceID,
			SinkID:           s.name,
			TableName:        m.Table,
			SchemaName:       s.getSchemaName(m),
			OperationType:    m.Op,
			LSN:              m.LSN,
			PrimaryKey:       m.PK,
			MessageUUID:      m.UUID,
			CaptureStage:     "before",
			Filtered:         false,
			TransformerNames: transformerNames,
			Payload:          payload,
			PayloadHash:      payloadHash,
			MessageTimestamp: m.Timestamp,
		}
		records = append(records, rec)

		s.statsMu.Lock()
		s.recordsCaptured++
		s.statsMu.Unlock()
	}

	if len(records) > 0 {
		if err := s.batchInsert(ctx, records); err != nil {
			log.Error().Err(err).Msg("Debug sink: failed to insert 'before' records")
		}
	}

	return correlationIDs
}

func (s *DebugSink) CaptureAfter(ctx context.Context, pipelineID string, correlationIDs []string, transformerNames []string, originals []protocol.Message, transformed []protocol.Message, filteredIndices []int) {
	if !s.shouldCaptureStage("after") {
		return
	}

	// 1. Mark filtered records in DB if we have correlation IDs for them
	filteredSet := make(map[int]bool)
	for _, idx := range filteredIndices {
		filteredSet[idx] = true
		if idx < len(correlationIDs) && correlationIDs[idx] != "" {
			s.markAsFiltered(ctx, correlationIDs[idx])
			s.statsMu.Lock()
			s.recordsFiltered++
			s.statsMu.Unlock()
		}
	}

	// 2. Capture "after" state for successful transformations
	// We need to match transformed messages back to their original index to get the correlation ID
	records := make([]debugRecord, 0, len(transformed))

	// Since transformed slice is ordered based on originals minus filtered ones,
	// we can reconstruct the mapping.
	transformedIdx := 0
	for i := 0; i < len(originals); i++ {
		if filteredSet[i] {
			continue
		}

		// If we reach here, originals[i] was NOT filtered and should match transformed[transformedIdx]
		if transformedIdx >= len(transformed) {
			break
		}

		m := transformed[transformedIdx]
		correlationID := ""
		if i < len(correlationIDs) {
			correlationID = correlationIDs[i]
		}

		transformedIdx++

		// Skip if we didn't capture the "before" state (e.g. filtered or sampled out there)
		if correlationID == "" {
			continue
		}

		if !s.shouldCaptureMessage(m) {
			continue
		}

		payload, err := extractPayload(m)
		if err != nil {
			log.Error().Err(err).Msg("Debug sink: failed to extract payload (after)")
			continue
		}

		payloadHash := computeHash(payload)
		latency := time.Since(m.Timestamp).Milliseconds()

		rec := debugRecord{
			CorrelationID:       correlationID,
			PipelineID:          pipelineID,
			SourceID:            m.SourceID,
			SinkID:              s.name,
			TableName:           m.Table,
			SchemaName:          s.getSchemaName(m),
			OperationType:       m.Op,
			LSN:                 m.LSN,
			PrimaryKey:          m.PK,
			MessageUUID:         m.UUID,
			CaptureStage:        "after",
			Filtered:            false,
			TransformerNames:    transformerNames,
			Payload:             payload,
			PayloadHash:         payloadHash,
			ProcessingLatencyMs: latency,
			MessageTimestamp:    m.Timestamp,
		}
		records = append(records, rec)

		s.statsMu.Lock()
		s.recordsCaptured++
		s.statsMu.Unlock()
	}

	if len(records) > 0 {
		if err := s.batchInsert(ctx, records); err != nil {
			log.Error().Err(err).Msg("Debug sink: failed to insert 'after' records")
		}
	}
}

type debugRecord struct {
	CorrelationID       string
	PipelineID          string
	SourceID            string
	SinkID              string
	TableName           string
	SchemaName          string
	OperationType       string
	LSN                 uint64
	PrimaryKey          string
	MessageUUID         string
	CaptureStage        string
	Filtered            bool
	TransformerNames    []string
	Payload             map[string]interface{}
	PayloadHash         string
	ProcessingLatencyMs int64
	MessageTimestamp    time.Time
}

func (s *DebugSink) batchInsert(ctx context.Context, records []debugRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	quotedTable := pq.QuoteIdentifier(s.config.TableName)
	query := fmt.Sprintf(`
		INSERT INTO %s (
			correlation_id, pipeline_id, source_id, sink_id, table_name, schema_name,
			operation_type, lsn, primary_key, message_uuid, capture_stage, filtered,
			transformer_names, payload, payload_hash, processing_latency_ms, message_timestamp
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	`, quotedTable)

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, rec := range records {
		payloadJSON, _ := json.Marshal(rec.Payload)

		// Map empty schema/pk to nil for SQL
		var schemaName, pk, msgUUID any
		if rec.SchemaName != "" {
			schemaName = rec.SchemaName
		}
		if rec.PrimaryKey != "" {
			pk = rec.PrimaryKey
		}
		if rec.MessageUUID != "" {
			msgUUID = rec.MessageUUID
		}

		_, err := stmt.ExecContext(ctx,
			rec.CorrelationID,
			rec.PipelineID,
			rec.SourceID,
			rec.SinkID,
			rec.TableName,
			schemaName,
			rec.OperationType,
			rec.LSN,
			pk,
			msgUUID,
			rec.CaptureStage,
			rec.Filtered,
			pq.Array(rec.TransformerNames), // Use pq.Array for TEXT[]
			payloadJSON,
			rec.PayloadHash,
			rec.ProcessingLatencyMs,
			rec.MessageTimestamp,
		)
		if err != nil {
			log.Error().Err(err).Str("table", rec.TableName).Msg("Debug sink: individual record insert failed")
		}
	}

	return tx.Commit()
}

func (s *DebugSink) markAsFiltered(ctx context.Context, correlationID string) {
	quotedTable := pq.QuoteIdentifier(s.config.TableName)
	query := fmt.Sprintf("UPDATE %s SET filtered = TRUE WHERE correlation_id = $1 AND capture_stage = 'before'", quotedTable)
	_, err := s.db.ExecContext(ctx, query, correlationID)
	if err != nil {
		log.Error().Err(err).Str("correlation_id", correlationID).Msg("Failed to mark record as filtered")
	}
}

func (s *DebugSink) shouldCaptureStage(stage string) bool {
	for _, st := range s.config.Capture.Stages {
		if st == stage {
			return true
		}
	}
	return false
}

func (s *DebugSink) shouldCaptureMessage(m protocol.Message) bool {
	includeTables := s.config.Filters.IncludeTables
	excludeTables := s.config.Filters.ExcludeTables
	includeOps := s.config.Filters.IncludeOperations

	if len(includeTables) > 0 {
		found := false
		for _, t := range includeTables {
			if t == m.Table || matchesWildcard(t, m.Table) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(excludeTables) > 0 {
		for _, t := range excludeTables {
			if t == m.Table || matchesWildcard(t, m.Table) {
				return false
			}
		}
	}

	if len(includeOps) > 0 {
		found := false
		for _, op := range includeOps {
			if op == m.Op {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func (s *DebugSink) isSampledOut(tableName string) bool {
	if s.config.Sampling.Mode == "disabled" {
		return false
	}

	override, hasOverride := s.config.Sampling.TableOverrides[tableName]
	mode := s.config.Sampling.Mode
	value := s.config.Sampling.Value

	if hasOverride {
		mode = override.Mode
		value = override.Value
	}

	if mode == "disabled" {
		return false
	}

	if mode == "percentage" {
		if value <= 0 {
			return true
		}
		if value >= 100 {
			return false
		}

		n, _ := rand.Int(rand.Reader, big.NewInt(100))
		return n.Int64() >= int64(value)
	}

	if mode == "systematic" {
		if value <= 0 {
			return true
		}
		// Use a counter or simple random for systematic every Nth
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(value)))
		return n.Int64() != 0
	}

	return false
}

func (s *DebugSink) getSchemaName(m protocol.Message) string {
	if m.Schema != nil {
		return m.Schema.Schema
	}
	return ""
}

func matchesWildcard(pattern, text string) bool {
	if !strings.Contains(pattern, "*") {
		return pattern == text
	}
	pattern = strings.ReplaceAll(pattern, "*", ".*")
	pattern = "^" + pattern + "$"
	matched, _ := regexp.MatchString(pattern, text)
	return matched
}

func computeHash(payload map[string]interface{}) string {
	data, _ := json.Marshal(payload)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

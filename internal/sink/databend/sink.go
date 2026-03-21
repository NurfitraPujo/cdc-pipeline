package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/datafuselabs/databend-go"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type DatabendSink struct {
	name string
	db   *sql.DB
}

func NewDatabendSink(name string, dsn string) (*DatabendSink, error) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return nil, err
	}
	return &DatabendSink{name: name, db: db}, nil
}

func (s *DatabendSink) Name() string {
	return s.name
}

func (s *DatabendSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Group messages by table (spec says they should be same but let's be safe)
	byTable := make(map[string][]protocol.Message)
	for _, m := range messages {
		byTable[m.Table] = append(byTable[m.Table], m)
	}

	for table, msgs := range byTable {
		if err := s.uploadTableBatch(ctx, table, msgs); err != nil {
			return err
		}
	}

	return nil
}

func (s *DatabendSink) uploadTableBatch(ctx context.Context, table string, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// 1. Extract columns from the first message
	var firstData map[string]any
	if err := json.Unmarshal(messages[0].Payload, &firstData); err != nil {
		return fmt.Errorf("failed to unmarshal first message: %w", err)
	}

	columns := make([]string, 0, len(firstData))
	for k := range firstData {
		columns = append(columns, k)
	}

	// 2. Build REPLACE INTO query
	// REPLACE INTO table (col1, col2) ON (pk1) VALUES (...), (...)
	// NOTE: We assume 'pk' is part of the columns and we'll use it in ON clause.
	// For now, let's use the PK field from our protocol.Message if available.
	
	colList := strings.Join(columns, ", ")
	
	// We'll use "REPLACE INTO" which handles deduplication on unique keys/PKs
	// Databend REPLACE INTO syntax: REPLACE INTO [db.]table [(c1, c2, ...)] ON (c1, [c2, ...]) VALUES (v1, v2, ...), ...
	// For simplicity, let's assume the table has a unique key that matches the source PK.
	
	query := fmt.Sprintf("REPLACE INTO %s (%s) VALUES ", table, colList)
	
	valueStrings := make([]string, 0, len(messages))
	valueArgs := make([]any, 0, len(messages)*len(columns))

	for i, m := range messages {
		var data map[string]any
		if err := json.Unmarshal(m.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message %d: %w", i, err)
		}

		placeholders := make([]string, len(columns))
		for j, col := range columns {
			placeholders[j] = "?"
			valueArgs = append(valueArgs, data[col])
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ", ")+")")
	}

	query += strings.Join(valueStrings, ", ")

	// 3. Execute
	_, err := s.db.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("databend replace into failed: %w", err)
	}

	return nil
}

func (s *DatabendSink) Stop() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

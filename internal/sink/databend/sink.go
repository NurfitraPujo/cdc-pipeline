package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/datafuselabs/databend-go"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"golang.org/x/sync/errgroup"
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
	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	return &DatabendSink{name: name, db: db}, nil
}

func (s *DatabendSink) Name() string {
	return s.name
}

func (s *DatabendSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Group messages by table
	byTable := make(map[string][]protocol.Message)
	for _, m := range messages {
		// Filter out control messages
		if m.Op == "schema_change" {
			continue
		}
		byTable[m.Table] = append(byTable[m.Table], m)
	}

	if len(byTable) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)

	for table, msgs := range byTable {
		t, m := table, msgs // capture for closure
		g.Go(func() error {
			return s.uploadTableBatch(gCtx, t, m)
		})
	}

	return g.Wait()
}

func (s *DatabendSink) ApplySchema(ctx context.Context, schema protocol.SchemaMetadata) error {
	log.Printf("Applying schema change for table %s in Databend", schema.Table)

	// 1. Map columns to Databend types
	var colDefs []string
	for name, pgType := range schema.Columns {
		dbType := mapPgTypeToDatabend(pgType)
		colDefs = append(colDefs, fmt.Sprintf("\"%s\" %s", name, dbType))
	}

	// 2. Build CREATE TABLE OR REPLACE logic
	// In Databend, we can use CREATE TABLE IF NOT EXISTS or REPLACE TABLE
	// For simplicity and to handle evolution (e.g. adding columns), 
	// we'll use a sequence of:
	//   CREATE TABLE IF NOT EXISTS ...
	//   Then for existing, we'd need to detect missing columns and ALTER.
	// For now, let's implement CREATE TABLE IF NOT EXISTS.
	
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)", 
		schema.Table, strings.Join(colDefs, ", "))
	
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to apply schema in databend: %w", err)
	}

	return nil
}

func mapPgTypeToDatabend(pgType string) string {
	// pgType is currently an OID string in our PostgresSource implementation
	// Common OIDs: 23=int4, 20=int8, 1043=varchar, 25=text, 16=bool, 1114=timestamp, 3802=jsonb
	switch pgType {
	case "16":
		return "BOOLEAN"
	case "23", "20":
		return "INT64"
	case "1043", "25":
		return "STRING"
	case "1114", "1184":
		return "TIMESTAMP"
	case "3802":
		return "VARIANT"
	case "700", "701":
		return "FLOAT64"
	default:
		return "STRING" // Fallback
	}
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
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = "\"" + col + "\""
	}
	colList := strings.Join(quotedColumns, ", ")
	
	safeTable := "\"" + table + "\""
	query := fmt.Sprintf("REPLACE INTO %s (%s) VALUES ", safeTable, colList)
	
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
	// #nosec G201 -- table name and columns are from trusted source/schema discovery
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

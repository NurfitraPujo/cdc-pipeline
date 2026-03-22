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
	log.Printf("Syncing schema for table %s in Databend", schema.Table)

	// 1. Check if table exists and get current columns
	existingCols, err := s.getCurrentColumns(ctx, schema.Table)
	if err != nil {
		return fmt.Errorf("failed to check existing columns: %w", err)
	}

	if len(existingCols) == 0 {
		// 2. Create Table
		var colDefs []string
		for name, pgType := range schema.Columns {
			dbType := mapPgTypeToDatabend(pgType)
			colDefs = append(colDefs, fmt.Sprintf("\"%s\" %s", name, dbType))
		}
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)", 
			schema.Table, strings.Join(colDefs, ", "))
		
		log.Printf("Executing DDL: %s", query)
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		return nil
	}

	// 3. Table exists, check for new columns (Schema Evolution)
	for name, pgType := range schema.Columns {
		if !existingCols[strings.ToLower(name)] {
			dbType := mapPgTypeToDatabend(pgType)
			query := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s", 
				schema.Table, name, dbType)
			
			log.Printf("Executing Evolution DDL: %s", query)
			if _, err := s.db.ExecContext(ctx, query); err != nil {
				// We log and continue, as concurrent workers might have added it already
				log.Printf("Warning: Failed to add column %s to %s: %v", name, schema.Table, err)
			}
		}
	}

	return nil
}

func (s *DatabendSink) getCurrentColumns(ctx context.Context, table string) (map[string]bool, error) {
	query := "SELECT column_name FROM information_schema.columns WHERE table_name = $1"
	rows, err := s.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			cols[strings.ToLower(name)] = true
		}
	}
	return cols, nil
}

func mapPgTypeToDatabend(pgType string) string {
	// Normalized type mapping (Postgres type name -> Databend type)
	// We handle both OID-fallback strings and actual type names.
	t := strings.ToLower(pgType)

	switch {
	case strings.Contains(t, "bool"):
		return "BOOLEAN"
	case strings.Contains(t, "int"):
		return "INT64"
	case strings.Contains(t, "float") || strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
		return "FLOAT64"
	case strings.Contains(t, "timestamp"):
		return "TIMESTAMP"
	case strings.Contains(t, "date"):
		return "DATE"
	case strings.Contains(t, "json") || strings.Contains(t, "variant"):
		return "VARIANT"
	case strings.Contains(t, "bytea") || strings.Contains(t, "blob"):
		return "BINARY"
	case strings.Contains(t, "uuid") || strings.Contains(t, "text") || strings.Contains(t, "varchar") || strings.Contains(t, "char"):
		return "STRING"
	default:
		// Check OIDs for backward compatibility
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
		default:
			return "STRING" // Global fallback
		}
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

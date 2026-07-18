package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/sink"
	_ "github.com/datafuselabs/databend-go"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultMaxPlaceholders bounds the number of `?` placeholders emitted per
	// REPLACE INTO statement. Databend itself supports up to 65,535
	// placeholders per prepared statement, but we use a conservative ceiling to
	// keep memory and parse cost predictable for large CDC batches.
	DefaultMaxPlaceholders = 10000

	// DefaultDecimalPrecision is the precision used when mapping PostgreSQL
	// numeric / decimal columns to Databend DECIMAL columns. 38 is the maximum
	// precision supported by Databend's DECIMAL type.
	DefaultDecimalPrecision = 38

	// DefaultDecimalScale is the default scale used for numeric / decimal
	// mappings. 9 covers most financial and scientific use cases.
	DefaultDecimalScale = 9
)

// reasonDeserializationFailed is the Prometheus label used for messages that
// could not be deserialized from their wire payload.
const reasonDeserializationFailed = "deserialization_failed"

// sinkPKRegex extracts the column list from a `PRIMARY KEY (...)` clause in
// SHOW CREATE TABLE output. The regex is case-insensitive and tolerant of
// whitespace and trailing commas.
var sinkPKRegex = regexp.MustCompile(`(?is)PRIMARY\s+KEY\s*\(([^)]+)\)`)

// DatabendSink writes CDC records into a Databend cluster. The instance is
// safe for concurrent use.
type DatabendSink struct {
	name string
	db   DBExec

	dlqPublisher DLQPublisher
	dlqSubject   string

	pkMu     sync.RWMutex
	pkCache  map[string][]string // table -> pk columns
	pkLoaded map[string]struct{} // tables we've already attempted SHOW CREATE TABLE on

	maxPlaceholders  int
	decimalPrecision int
	decimalScale     int
}

// NewDatabendSink opens a new Databend sink backed by a real *sql.DB connection
// pool. The pool is sized for the typical CDC workload (25 idle/open, 5m
// lifetime) and may be tuned via WithOptions after construction.
func NewDatabendSink(name string, dsn string) (*DatabendSink, error) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &DatabendSink{
		name:             name,
		db:               sqlDBAdapter{DB: db},
		pkCache:          make(map[string][]string),
		pkLoaded:         make(map[string]struct{}),
		maxPlaceholders:  DefaultMaxPlaceholders,
		decimalPrecision: DefaultDecimalPrecision,
		decimalScale:     DefaultDecimalScale,
		dlqSubject:       DefaultSinkDeadLetterSubject(name),
	}, nil
}

// WithOptions applies configuration values from a sink options map. Supported
// keys (all optional):
//
//   - "max_placeholders"  (int)            upper bound for `?` placeholders per
//                                              REPLACE INTO statement.
//   - "decimal_precision" (int)            precision used by DECIMAL mappings.
//   - "decimal_scale"     (int)            scale used by DECIMAL mappings.
//   - "dlq_publisher"     (DLQPublisher)   publisher used to emit sink DLQ events.
//   - "dlq_subject"       (string)         NATS subject used for DLQ events.
//
// Returns the receiver for chaining with the registry factory.
func (s *DatabendSink) WithOptions(options map[string]interface{}) *DatabendSink {
	if options == nil {
		return s
	}
	if v, ok := options["max_placeholders"]; ok {
		if n, ok := asInt(v); ok && n > 0 {
			s.maxPlaceholders = n
		}
	}
	if v, ok := options["decimal_precision"]; ok {
		if n, ok := asInt(v); ok && n > 0 {
			s.decimalPrecision = n
		}
	}
	if v, ok := options["decimal_scale"]; ok {
		if n, ok := asInt(v); ok && n >= 0 {
			if s.decimalPrecision == 0 {
				s.decimalPrecision = DefaultDecimalPrecision
			}
			if n > s.decimalPrecision {
				s.decimalScale = s.decimalPrecision
			} else {
				s.decimalScale = n
			}
		}
	}
	if v, ok := options["dlq_publisher"]; ok {
		if pub, ok := v.(DLQPublisher); ok {
			s.dlqPublisher = pub
		}
	}
	if v, ok := options["dlq_subject"]; ok {
		if subject, ok := v.(string); ok && subject != "" {
			s.dlqSubject = subject
		}
	}
	return s
}

// asInt coerces an arbitrary options value (typically int, int32, int64 or
// float64 from YAML/JSON unmarshalling) into a Go int.
func asInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case uint:
		return int(n), true
	case uint32:
		return int(n), true
	case uint64:
		return int(n), true
	}
	return 0, false
}

func init() {
	sink.Register("databend", func(sinkID string, dsn string, options map[string]interface{}) (sink.Sink, error) {
		snk, err := NewDatabendSink(sinkID, dsn)
		if err != nil {
			return nil, err
		}
		return snk.WithOptions(options), nil
	})
}

func (s *DatabendSink) Name() string {
	return s.name
}

func (s *DatabendSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	upserts := make(map[string][]protocol.Message)
	deletes := make(map[string][]protocol.Message)

	for _, m := range messages {
		if m.Op == protocol.OpSchemaChange || m.Op == "drain_marker" {
			continue
		}
		if m.Op == protocol.OpDelete {
			deletes[m.Table] = append(deletes[m.Table], m)
		} else {
			upserts[m.Table] = append(upserts[m.Table], m)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	for table, msgs := range upserts {
		t, m := table, msgs
		g.Go(func() error {
			return s.uploadTableBatch(gCtx, t, m)
		})
	}

	for table, msgs := range deletes {
		t, m := table, msgs
		g.Go(func() error {
			return s.deleteTableBatch(gCtx, t, m)
		})
	}

	return g.Wait()
}

// splitQualified splits a qualified table name into schema and table parts.
// For "schema.table" returns ("schema", "table").
// For unqualifed names returns ("", "name").
func splitQualified(name string) (schema, table string) {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", name
}

func quoteIdentifier(name string) string {
	// Quote each dot-separated component individually for proper SQL quoting
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = "\"" + strings.ReplaceAll(p, "\"", "\"\"") + "\""
	}
	return strings.Join(parts, ".")
}

// validateIdentifier checks if the identifier contains only allowed characters
// Returns error if the identifier contains potentially dangerous characters
func validateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("identifier cannot be empty")
	}
	// Only allow alphanumeric characters, underscores, and dots (for schema.table)
	for _, r := range name {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_' && r != '.' {
			return fmt.Errorf("invalid character in identifier: %q", r)
		}
	}
	return nil
}

func (s *DatabendSink) ApplySchema(ctx context.Context, m protocol.Message) error {
	schema := m.Schema
	if schema == nil {
		return fmt.Errorf("schema metadata is nil in message")
	}

	log.Info().Str("table", schema.Table).Msg("Syncing schema in Databend")

	if err := validateIdentifier(schema.Table); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	s.pkMu.Lock()
	s.pkCache[schema.Table] = schema.PKColumns
	s.pkLoaded[schema.Table] = struct{}{}
	s.pkMu.Unlock()

	existingCols, err := s.getCurrentColumns(ctx, schema.Table)
	if err != nil {
		return fmt.Errorf("failed to check existing columns: %w", err)
	}

	quotedTable := quoteIdentifier(schema.Table)

	if len(existingCols) == 0 {
		var colDefs []string
		var colNames []string
		for name := range schema.Columns {
			colNames = append(colNames, name)
		}
		sort.Strings(colNames)

		for _, name := range colNames {
			if err := validateIdentifier(name); err != nil {
				return fmt.Errorf("invalid column name %q: %w", name, err)
			}
			pgType := schema.Columns[name]
			dbType := s.mapPgTypeToDatabend(pgType)
			colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdentifier(name), dbType))
		}
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
			quotedTable, strings.Join(colDefs, ", "))

		log.Info().Str("table", schema.Table).Str("query", query).Msg("Executing DDL")
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		return nil
	}

	for name, pgType := range schema.Columns {
		if err := validateIdentifier(name); err != nil {
			log.Warn().Str("column", name).Err(err).Msg("Skipping invalid column name")
			continue
		}
		if !existingCols[strings.ToLower(name)] {
			dbType := s.mapPgTypeToDatabend(pgType)
			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				quotedTable, quoteIdentifier(name), dbType)

			log.Info().Str("table", schema.Table).Str("column", name).Str("query", query).Msg("Executing Evolution DDL")
			if _, err := s.db.ExecContext(ctx, query); err != nil {
				log.Warn().Err(err).Str("table", schema.Table).Str("column", name).Msg("Failed to add column")
			}
		}
	}

	return nil
}

func (s *DatabendSink) getCurrentColumns(ctx context.Context, table string) (map[string]bool, error) {
	schema, tbl := splitQualified(table)
	var query string
	var args []any
	if schema != "" {
		query = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?"
		args = []any{schema, tbl}
	} else {
		query = "SELECT column_name FROM information_schema.columns WHERE table_name = ?"
		args = []any{table}
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
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

// mapPgTypeToDatabend translates a PostgreSQL type description (either a
// canonical name or an OID number) into the closest Databend column type. The
// DECIMAL precision and scale honour the sink's decimalPrecision /
// decimalScale configuration so operators can tune financial columns.
func (s *DatabendSink) mapPgTypeToDatabend(pgType string) string {
	t := strings.ToLower(pgType)

	// PostgreSQL arrays: `_int`, `int[]`, `text[]`, `numeric[]`, etc.
	// Databend does not have a first-class ARRAY type, so we encode arrays as
	// VARIANT (JSON) to preserve fidelity without lossy scalar conversion.
	if strings.HasSuffix(t, "[]") || strings.HasPrefix(t, "_") || strings.Contains(t, "array") {
		return "VARIANT"
	}

	switch {
	case strings.Contains(t, "bool"):
		return "BOOLEAN"
	case strings.Contains(t, "int"):
		return "INT64"
	case strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
		precision := s.decimalPrecision
		if precision <= 0 {
			precision = DefaultDecimalPrecision
		}
		scale := s.decimalScale
		if scale < 0 {
			scale = 0
		}
		if scale > precision {
			scale = precision
		}
		if scale == 0 {
			return fmt.Sprintf("DECIMAL(%d)", precision)
		}
		return fmt.Sprintf("DECIMAL(%d, %d)", precision, scale)
	case strings.Contains(t, "float") || strings.Contains(t, "double") || strings.Contains(t, "real"):
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
			return "STRING"
		}
	}
}

func (s *DatabendSink) uploadTableBatch(ctx context.Context, table string, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	if err := validateIdentifier(table); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// T1-17: lazily resolve PK from Databend the first time we see this table.
	if err := s.ensurePrimaryKey(ctx, table); err != nil {
		log.Warn().Err(err).Str("table", table).Msg("ensurePrimaryKey failed; continuing with current cache")
	}

	// GROUP BY COLUMN SET
	// CDC batches might contain records with different column sets (evolution or different sources)
	groups := make(map[string][]map[string]any)
	groupCols := make(map[string][]string)

	for _, m := range messages {
		data, err := decodePayload(m)
		if err != nil {
			// T1-1: surface deserialization failures instead of silently dropping.
			s.emitDLQ(ctx, m, table, err.Error())
			continue
		}

		cols := make([]string, 0, len(data))
		for k := range data {
			cols = append(cols, k)
		}
		sort.Strings(cols)

		key := strings.Join(cols, ",")
		groups[key] = append(groups[key], data)
		groupCols[key] = cols
	}

	s.pkMu.RLock()
	pks := s.pkCache[table]
	s.pkMu.RUnlock()
	if len(pks) == 0 {
		pks = []string{"id"}
	}

	// Validate primary key names
	for _, pk := range pks {
		if err := validateIdentifier(pk); err != nil {
			return fmt.Errorf("invalid primary key name %q: %w", pk, err)
		}
	}

	quotedPks := make([]string, len(pks))
	for i, pk := range pks {
		quotedPks[i] = quoteIdentifier(pk)
	}
	pkList := strings.Join(quotedPks, ", ")

	quotedTable := quoteIdentifier(table)

	for key, records := range groups {
		columns := groupCols[key]
		// Validate all column names before using them
		for _, col := range columns {
			if err := validateIdentifier(col); err != nil {
				return fmt.Errorf("invalid column name %q: %w", col, err)
			}
		}
		quotedColumns := make([]string, len(columns))
		for i, col := range columns {
			quotedColumns[i] = quoteIdentifier(col)
		}
		colList := strings.Join(quotedColumns, ", ")

		if err := s.executeReplaceIntoChunks(ctx, table, quotedTable, colList, pkList, columns, records); err != nil {
			return fmt.Errorf("uploadTableBatch for group %s failed: %w", key, err)
		}
	}

	return nil
}

// executeReplaceIntoChunks writes the records to Databend in one or more
// chunked REPLACE INTO statements. The chunker (T1-14) prevents the
// placeholder count from exceeding the configured maxPlaceholders per
// statement, which avoids hitting Databend's 65,535 placeholder ceiling when
// large heterogeneous batches arrive.
func (s *DatabendSink) executeReplaceIntoChunks(
	ctx context.Context,
	table, quotedTable, colList, pkList string,
	columns []string,
	records []map[string]any,
) error {
	if len(records) == 0 {
		return nil
	}

	maxPh := s.maxPlaceholders
	if maxPh <= 0 {
		maxPh = DefaultMaxPlaceholders
	}

	chunkSize := len(records)
	if len(columns) > 0 {
		// Compute the largest chunk that stays within the placeholder budget.
		// Floor to avoid partial-column placeholders.
		chunkSize = maxPh / len(columns)
		if chunkSize <= 0 {
			chunkSize = 1
		}
		if chunkSize > len(records) {
			chunkSize = len(records)
		}
	}

	prefix := fmt.Sprintf("REPLACE INTO %s (%s) ON (%s) VALUES ", quotedTable, colList, pkList)

	chunksEmitted := 0
	for start := 0; start < len(records); start += chunkSize {
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[start:end]

		valueStrings := make([]string, 0, len(chunk))
		valueArgs := make([]any, 0, len(chunk)*len(columns))

		for _, data := range chunk {
			placeholders := make([]string, len(columns))
			for j, col := range columns {
				placeholders[j] = "?"
				valueArgs = append(valueArgs, normalizeValue(data[col]))
			}
			valueStrings = append(valueStrings, "("+strings.Join(placeholders, ", ")+")")
		}

		query := prefix + strings.Join(valueStrings, ", ")

		log.Debug().
			Str("table", table).
			Str("query", query).
			Int("num_records", len(chunk)).
			Int("chunk", chunksEmitted).
			Msg("DatabendSink: Executing Upsert")

		if _, err := s.db.ExecContext(ctx, query, valueArgs...); err != nil {
			return fmt.Errorf("chunk %d failed: %w", chunksEmitted, err)
		}
		chunksEmitted++
	}

	if chunksEmitted > 0 {
		SinkChunksTotal.WithLabelValues(s.name, table).Add(float64(chunksEmitted))
	}
	return nil
}

// normalizeValue returns a value suitable for use as a Databend driver argument.
// Primitive numeric, string, bool and time values are passed through unchanged.
// Anything else is JSON encoded so the driver receives a stable representation.
// The expanded switch (T2-4) covers the full set of Go integer / float types so
// CDC consumers do not lose precision by accidentally marshalling int8/int16/
// uint64/etc. into JSON strings.
func normalizeValue(val any) any {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		bool,
		time.Time:
		return v
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// decodePayload extracts the CDC record data from a message, preferring the
// in-memory Data field and falling back to MessagePack or JSON deserialisation
// of the wire payload. The returned error is wrapped so callers can surface a
// stable reason to the DLQ.
func decodePayload(m protocol.Message) (map[string]any, error) {
	if m.Data != nil {
		return m.Data, nil
	}
	if len(m.Payload) == 0 {
		return nil, fmt.Errorf("payload is empty")
	}
	var data map[string]any
	if err := msgpack.Unmarshal(m.Payload, &data); err == nil {
		return data, nil
	} else if jsonErr := json.Unmarshal(m.Payload, &data); jsonErr == nil {
		return data, nil
	} else {
		return nil, fmt.Errorf("msgpack: %v; json: %v", err.Error(), jsonErr.Error())
	}
}

// emitDLQ records a deserialization (or other terminal) failure for a single
// CDC record. It increments the cdc_sink_dlq_total counter, logs the failure
// with structured context, and (when a DLQPublisher is wired in) publishes a
// SinkDeadLetterEvent to the configured NATS subject.
func (s *DatabendSink) emitDLQ(ctx context.Context, m protocol.Message, table, reason string) {
	log.Error().
		Err(fmt.Errorf("%s", reason)).
		Str("table", table).
		Str("sink_id", s.name).
		Str("msg_uuid", m.UUID).
		Uint64("lsn", m.LSN).
		Msg("failed to deserialize payload")

	SinkDLQTotal.WithLabelValues(s.name, table, reasonDeserializationFailed).Inc()

	if s.dlqPublisher == nil {
		// Without a wired publisher we can only observe via logs + metrics.
		return
	}

	event := SinkDeadLetterEvent{
		SinkID:    s.name,
		Table:     table,
		UUID:      m.UUID,
		LSN:       m.LSN,
		Op:        m.Op,
		SourceID:  m.SourceID,
		Reason:    reason,
		Payload:   m.Payload,
		Data:      m.Data,
		Timestamp: time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("failed to marshal DLQ event")
		return
	}
	dlqMsg := buildDLQMessage(m.UUID, payload)
	subject := s.dlqSubject
	if subject == "" {
		subject = DefaultSinkDeadLetterSubject(s.name)
	}
	if err := s.dlqPublisher.Publish(subject, dlqMsg); err != nil {
		log.Error().Err(err).Str("subject", subject).Msg("failed to publish sink DLQ event")
	}
	// ctx is reserved for future async-publish hooks; reference it to avoid
	// unused-parameter warnings and to make it discoverable in trace spans.
	_ = ctx
}

func (s *DatabendSink) deleteTableBatch(ctx context.Context, table string, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	if err := validateIdentifier(table); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// T1-17: lazily resolve PK from Databend the first time we see this table.
	if err := s.ensurePrimaryKey(ctx, table); err != nil {
		log.Warn().Err(err).Str("table", table).Msg("ensurePrimaryKey failed; continuing with current cache")
	}

	s.pkMu.RLock()
	pks := s.pkCache[table]
	s.pkMu.RUnlock()
	if len(pks) == 0 {
		pks = []string{"id"}
	}

	// Validate primary key names
	for _, pk := range pks {
		if err := validateIdentifier(pk); err != nil {
			return fmt.Errorf("invalid primary key name %q: %w", pk, err)
		}
	}

	for _, m := range messages {
		data, err := decodePayload(m)
		if err != nil {
			// T1-1: surface deserialization failures instead of silently dropping.
			s.emitDLQ(ctx, m, table, err.Error())
			continue
		}

		var whereClauses []string
		var args []any
		for _, pk := range pks {
			val, ok := data[pk]
			if !ok {
				continue
			}
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", quoteIdentifier(pk)))
			args = append(args, normalizeValue(val))
		}

		if len(whereClauses) == 0 {
			continue
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdentifier(table), strings.Join(whereClauses, " AND "))
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

// ensurePrimaryKey lazily resolves the primary key columns for a table. It
// performs at most one SHOW CREATE TABLE per table per process lifetime; after
// that, lookups are served from the in-memory cache.
func (s *DatabendSink) ensurePrimaryKey(ctx context.Context, table string) error {
	return s.refreshPrimaryKey(ctx, table)
}

// refreshPrimaryKey executes SHOW CREATE TABLE on the sink-side datastore,
// parses the PRIMARY KEY clause out of the resulting DDL, and updates the
// cache. Failures are non-fatal: we log + record a metric, fall back to the
// existing cache (or a default of "id") and continue. The pkLoaded gate
// ensures SHOW CREATE TABLE is invoked at most once per table per process
// lifetime, even when called from concurrent goroutines.
func (s *DatabendSink) refreshPrimaryKey(ctx context.Context, table string) error {
	// Double-checked locking: skip the query if we have already attempted
	// resolution for this table.
	s.pkMu.RLock()
	if _, loaded := s.pkLoaded[table]; loaded {
		s.pkMu.RUnlock()
		return nil
	}
	s.pkMu.RUnlock()

	// Reserve the slot BEFORE issuing the query so concurrent callers
	// short-circuit on their second-check.
	s.pkMu.Lock()
	if _, loaded := s.pkLoaded[table]; loaded {
		s.pkMu.Unlock()
		return nil
	}
	s.pkLoaded[table] = struct{}{}
	s.pkMu.Unlock()

	quotedTable := quoteIdentifier(table)
	query := fmt.Sprintf("SHOW CREATE TABLE %s", quotedTable)

	var ddl string
	scanErr := s.db.QueryRowScan(ctx, query, nil, &ddl)

	if scanErr != nil {
		log.Warn().Err(scanErr).Str("table", table).Msg("SHOW CREATE TABLE failed; falling back to default PK")
		s.ensureFallbackPK(table)
		SinkPKResolved.WithLabelValues(s.name, table).Set(0)
		return scanErr
	}

	pks := parsePKFromDDL(ddl)
	if len(pks) == 0 {
		log.Warn().Str("table", table).Str("ddl", ddl).Msg("no PRIMARY KEY clause found in SHOW CREATE TABLE; falling back")
		s.ensureFallbackPK(table)
		SinkPKResolved.WithLabelValues(s.name, table).Set(0)
		return nil
	}

	s.pkMu.Lock()
	s.pkCache[table] = pks
	s.pkMu.Unlock()
	SinkPKResolved.WithLabelValues(s.name, table).Set(1)
	log.Info().Str("table", table).Strs("pks", pks).Msg("resolved primary key from Databend")
	return nil
}

// ensureFallbackPK installs the default "id" primary key for a table if no PK
// has been recorded yet. This matches the legacy behaviour but is now scoped
// to tables we have actually attempted to resolve.
func (s *DatabendSink) ensureFallbackPK(table string) {
	s.pkMu.Lock()
	defer s.pkMu.Unlock()
	if _, ok := s.pkCache[table]; ok {
		return
	}
	s.pkCache[table] = []string{"id"}
}

// parsePKFromDDL extracts the column list from a `PRIMARY KEY (...)` clause.
// It returns nil if no clause is present or the input is malformed.
func parsePKFromDDL(ddl string) []string {
	if ddl == "" {
		return nil
	}
	match := sinkPKRegex.FindStringSubmatch(ddl)
	if len(match) < 2 {
		return nil
	}
	rawCols := strings.Split(match[1], ",")
	pks := make([]string, 0, len(rawCols))
	for _, c := range rawCols {
		c = strings.TrimSpace(c)
		// Strip Databend identifier quoting (backticks / double quotes).
		c = strings.Trim(c, "\"`")
		if c == "" {
			continue
		}
		// Strip ASC/DESC qualifiers if present.
		if idx := strings.IndexAny(c, " \t"); idx > 0 {
			c = c[:idx]
		}
		if c == "" {
			continue
		}
		pks = append(pks, c)
	}
	if len(pks) == 0 {
		return nil
	}
	return pks
}

func (s *DatabendSink) Stop() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

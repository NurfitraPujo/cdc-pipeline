package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// reasonDeserializationFailed is the Prometheus/DLQ label used for messages
// that could not be deserialized from their wire payload.
const reasonDeserializationFailed = "deserialization_failed"

// reasonMissingPKColumns is the Prometheus/DLQ label used when a delete is
// dropped because none of the resolved primary key columns are present in
// the decoded payload (MULTI_SCHEMA_PLAN.md §7.4 item 6 audit).
const reasonMissingPKColumns = "missing_pk_columns"

// sinkPKRegex extracts the column list from a `PRIMARY KEY (...)` clause in
// SHOW CREATE TABLE output. The regex is case-insensitive and tolerant of
// whitespace and trailing commas.
var sinkPKRegex = regexp.MustCompile(`(?is)PRIMARY\s+KEY\s*\(([^)]+)\)`)

// DatabendSink writes CDC records into a Databend cluster. The instance is
// safe for concurrent use.
//
// Table identity, throughout this file, is a protocol.TableRef qualified by
// NormalizeSchema: a Postgres schema maps to a Databend database
// (MULTI_SCHEMA_PLAN.md §7.4), so "orders" in schema "sales" targets
// Databend database "sales", table "orders". pkCache/pkLoaded/provisionedDB/
// validatedDB are all keyed by TableRef.String() (e.g. "sales.orders" or
// "public.orders") -- the *same* derivation is used whether the identity
// arrives via ApplySchema's *protocol.SchemaMetadata (refFromSchemaMeta) or
// via BatchUpload's protocol.Message (refFromMessage). Qualifying only one
// of those two paths was attempt 1's critical bug: the real PK is never
// found under the mismatched key, silently falls back to ["id"], and
// REPLACE INTO merges distinct rows.
type DatabendSink struct {
	name string
	db   DBExec

	dlqPublisher DLQPublisher
	dlqSubject   string

	pkMu     sync.RWMutex
	pkCache  map[string][]string // TableRef.String() -> pk columns
	pkLoaded map[string]struct{} // TableRef.String() we've already attempted SHOW CREATE TABLE on

	// autoCreateSchema controls whether ensureDatabase issues CREATE
	// DATABASE IF NOT EXISTS for a target database before DDL/DML, or
	// instead validates the database already exists and refuses with a
	// permanent, actionable error otherwise. Defaults to true.
	//
	// MULTI_SCHEMA_PLAN.md §7.4: attempt 1 shipped this opt-in and
	// default-off, which failed the e2e suite on the *ordinary public*
	// path -- `databend database "public" does not exist and
	// auto-provisioning is disabled`, redelivering in a hot loop, because
	// nothing had ever created database "public" for it. Default-on makes
	// the common case work out of the box; the option remains so it can be
	// flipped off once this reaches production and DDL privileges need to
	// be kept out of the pipeline credential.
	autoCreateSchema bool

	dbMu          sync.Mutex
	provisionedDB map[string]struct{} // databases we've already issued CREATE DATABASE IF NOT EXISTS for (autoCreateSchema == true)
	validatedDB   map[string]struct{} // databases confirmed to exist (autoCreateSchema == false)

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
		autoCreateSchema: true,
		provisionedDB:    make(map[string]struct{}),
		validatedDB:      make(map[string]struct{}),
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
//   - "auto_create_schema" (bool)          CREATE DATABASE IF NOT EXISTS before
//                                              DDL/DML when true (default);
//                                              validate-only when false.
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
	if v, ok := options["auto_create_schema"]; ok {
		if b, ok := v.(bool); ok {
			s.autoCreateSchema = b
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

// refFromSchemaMeta derives the canonical TableRef for a schema-change
// message. See the DatabendSink doc comment: this MUST use the same
// derivation as refFromMessage.
func refFromSchemaMeta(schema *protocol.SchemaMetadata) protocol.TableRef {
	return protocol.TableRef{Schema: protocol.NormalizeSchema(schema.Schema), Table: schema.Table}
}

// refFromMessage derives the canonical TableRef for a data (upsert/delete)
// message. Message.Table stays bare by design (MULTI_SCHEMA_PLAN.md §2.2);
// the schema travels in the sibling TableSchema field. See the DatabendSink
// doc comment: this MUST use the same derivation as refFromSchemaMeta.
func refFromMessage(m protocol.Message) protocol.TableRef {
	return protocol.TableRef{Schema: protocol.NormalizeSchema(m.TableSchema), Table: m.Table}
}

func (s *DatabendSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Group by the fully-qualified TableRef, not the bare m.Table. A single
	// batch can carry rows for same-named tables in different schemas (e.g.
	// public.orders and sales.orders); grouping by bare Table would merge
	// them into one REPLACE INTO/DELETE run against whichever database
	// happens to be resolved for the group, silently corrupting the other.
	// See MULTI_SCHEMA_PLAN.md §5 "Cross-schema collision".
	upserts := make(map[protocol.TableRef][]protocol.Message)
	deletes := make(map[protocol.TableRef][]protocol.Message)

	for _, m := range messages {
		if m.Op == protocol.OpSchemaChange || m.Op == "drain_marker" {
			continue
		}
		ref := refFromMessage(m)
		if m.Op == protocol.OpDelete {
			deletes[ref] = append(deletes[ref], m)
		} else {
			upserts[ref] = append(upserts[ref], m)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	for ref, msgs := range upserts {
		r, m := ref, msgs
		g.Go(func() error {
			return s.uploadTableBatch(gCtx, r, m)
		})
	}

	for ref, msgs := range deletes {
		r, m := ref, msgs
		g.Go(func() error {
			return s.deleteTableBatch(gCtx, r, m)
		})
	}

	return g.Wait()
}

// splitQualified splits a name into schema and table components. Accepts
// exactly one ("table") or two ("schema.table") non-empty parts. Anything
// else -- 3+ parts, "a..b", a leading or trailing dot, or the empty string
// -- is rejected rather than silently reinterpreted.
//
// Attempt 1's version treated "a.b.c" as the unqualified table "a.b.c";
// quoteIdentifier then happily rendered that as a 3-part `"a"."b"."c"` DDL
// fragment, which Databend accepts as catalog.database.table (§6) -- valid
// SQL, but never what the caller meant, and no error told anyone.
// See MULTI_SCHEMA_PLAN.md §1.1, §7.4 item 8.
func splitQualified(name string) (schema, table string, err error) {
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return "", "", fmt.Errorf("identifier is empty")
		}
		return "", parts[0], nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return "", "", fmt.Errorf("qualified identifier %q has an empty component", name)
		}
		return parts[0], parts[1], nil
	default:
		return "", "", fmt.Errorf("identifier %q has more than one \".\": expected at most one schema-qualifier", name)
	}
}

func quoteIdentifier(name string) string {
	// Quote each dot-separated component individually for proper SQL quoting
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = "\"" + strings.ReplaceAll(p, "\"", "\"\"") + "\""
	}
	return strings.Join(parts, ".")
}

// quoteQualified renders a TableRef's Schema and Table as a quoted 2-part
// identifier, e.g. "sales"."orders". Composing it from the already-validated
// components -- rather than quoting ref.String() as one string -- avoids
// re-parsing a string we just built and keeps composition and validation
// orthogonal (see splitQualified's doc comment).
func quoteQualified(ref protocol.TableRef) string {
	return quoteIdentifier(ref.Schema) + "." + quoteIdentifier(ref.Table)
}

// validateIdentifier checks that name is a single identifier ("table") or a
// schema-qualified pair ("schema.table") built from safe characters only:
// alphanumeric and underscore per component. It reuses splitQualified so the
// two functions agree on what counts as a validly-shaped identifier --
// previously validateIdentifier accepted "a..b", ".", and leading/trailing
// dots purely because its character loop allowed "." unconditionally with no
// arity check (MULTI_SCHEMA_PLAN.md §1.1, §7.4 item 8).
func validateIdentifier(name string) error {
	schema, table, err := splitQualified(name)
	if err != nil {
		return fmt.Errorf("invalid identifier %q: %w", name, err)
	}
	for _, part := range []string{schema, table} {
		if part == "" {
			continue // unqualified form: schema is legitimately empty
		}
		for _, r := range part {
			if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_' {
				return fmt.Errorf("invalid character in identifier: %q", r)
			}
		}
	}
	return nil
}

// ensureDatabase makes sure the target database exists before DDL/DML runs
// against it.
//
// autoCreateSchema == true: issue CREATE DATABASE IF NOT EXISTS. Idempotent
// server-side, but also cached per-process (provisionedDB) so it is not
// re-issued on every ApplySchema/upload call for a table we have already
// provisioned.
//
// autoCreateSchema == false: validate the database exists (cached in
// validatedDB once confirmed) and return a permanent, actionably-worded
// DDLError if it is missing. MULTI_SCHEMA_PLAN.md §7.4 item 2 requires this
// path to never fall into per-message retry; classifying the error
// Permanent is how the sink tells its caller "do not redeliver this" (see
// errors.go / IsPermanentDDLError). A failure of the existence check itself
// (connection, timeout) is transient and safe to retry.
func (s *DatabendSink) ensureDatabase(ctx context.Context, database string) error {
	if s.autoCreateSchema {
		s.dbMu.Lock()
		_, done := s.provisionedDB[database]
		s.dbMu.Unlock()
		if done {
			return nil
		}

		query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdentifier(database))
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return classifyDDLError(database, fmt.Errorf("failed to create database: %w", err))
		}

		s.dbMu.Lock()
		s.provisionedDB[database] = struct{}{}
		s.dbMu.Unlock()
		return nil
	}

	s.dbMu.Lock()
	_, done := s.validatedDB[database]
	s.dbMu.Unlock()
	if done {
		return nil
	}

	exists, err := s.databaseExists(ctx, database)
	if err != nil {
		return classifyDDLError(database, fmt.Errorf("failed to verify database %q exists: %w", database, err))
	}
	if !exists {
		return &DDLError{
			Target:    database,
			Permanent: true,
			Err: fmt.Errorf(
				"databend database %q does not exist and auto-provisioning is disabled (sink option %q); create it manually or enable auto-provisioning",
				database, "auto_create_schema"),
		}
	}

	s.dbMu.Lock()
	s.validatedDB[database] = struct{}{}
	s.dbMu.Unlock()
	return nil
}

// databaseExists checks Databend's system.databases table for the named
// database. Unlike probing information_schema.columns, this correctly
// reports existence for a database that has no tables yet.
func (s *DatabendSink) databaseExists(ctx context.Context, database string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT name FROM system.databases WHERE name = ?", database)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}

// ValidateSchemas checks that every database targeted by tables is usable,
// per the current auto_create_schema setting: it eagerly provisions
// (autoCreateSchema == true) or eagerly validates (autoCreateSchema ==
// false) every distinct target database up front, rather than deferring to
// the first ApplySchema/BatchUpload call for that database.
//
// This implements MULTI_SCHEMA_PLAN.md §7.4 item 2 ("validate at STARTUP ...
// refuse to start"). PipelineFactory.CreateWorker (internal/engine/factory.go)
// calls it via an optional-interface check -- the same pattern used there for
// sink.DebugCapturer -- after sink.New and before the worker is returned, so a
// missing database fails startup rather than surfacing lazily on the first
// ApplySchema, where the schema path's unbounded Nack would loop on it.
func (s *DatabendSink) ValidateSchemas(ctx context.Context, tables []protocol.TableRef) error {
	seen := make(map[string]struct{})
	var missing []string

	for _, t := range tables {
		database := protocol.NormalizeSchema(t.Schema)
		if _, ok := seen[database]; ok {
			continue
		}
		seen[database] = struct{}{}

		if err := s.ensureDatabase(ctx, database); err != nil {
			if IsPermanentDDLError(err) {
				missing = append(missing, database)
				continue
			}
			return fmt.Errorf("failed to validate database %q: %w", database, err)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf(
			"databend: %d target database(s) missing and auto-provisioning is disabled (sink option %q): %s; create them manually or enable auto-provisioning",
			len(missing), "auto_create_schema", strings.Join(missing, ", "))
	}
	return nil
}

func (s *DatabendSink) ApplySchema(ctx context.Context, m protocol.Message) error {
	schema := m.Schema
	if schema == nil {
		return fmt.Errorf("schema metadata is nil in message")
	}

	ref := refFromSchemaMeta(schema)
	qualified := ref.String()

	log.Info().Str("table", qualified).Msg("Syncing schema in Databend")

	if err := validateIdentifier(ref.Schema); err != nil {
		return fmt.Errorf("invalid schema name %q: %w", ref.Schema, err)
	}
	if err := validateIdentifier(ref.Table); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	if err := s.ensureDatabase(ctx, ref.Schema); err != nil {
		return fmt.Errorf("failed to ensure database %q: %w", ref.Schema, err)
	}

	// pkCache/pkLoaded MUST be keyed identically to the upload path
	// (uploadTableBatch/deleteTableBatch via refFromMessage) -- both derive
	// the key from TableRef.String() here. Qualifying only one side is
	// attempt 1's critical bug: the real PK is never found under the
	// mismatched key, silently falls back to ["id"], and REPLACE INTO
	// merges distinct rows (MULTI_SCHEMA_PLAN.md §7.4 item 5).
	//
	// Only record a PK -- and only mark pkLoaded -- when this message
	// actually carries one. schema.PKColumns is nil for schema-change
	// events reconstructed from a SchemaDiff (the consumer's inline
	// SchemaMetadata copy has no PK source to draw from). Setting pkLoaded
	// unconditionally here would permanently short-circuit
	// refreshPrimaryKey's SHOW CREATE TABLE lookup for this table --
	// reviving the sticky-wrong-PK bug through exactly this door, which is
	// how attempt 1 reintroduced it.
	if len(schema.PKColumns) > 0 {
		s.pkMu.Lock()
		s.pkCache[qualified] = schema.PKColumns
		s.pkLoaded[qualified] = struct{}{}
		s.pkMu.Unlock()
	}

	existingCols, err := s.getCurrentColumns(ctx, ref)
	if err != nil {
		return fmt.Errorf("failed to check existing columns: %w", err)
	}

	quotedTable := quoteQualified(ref)

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

		log.Info().Str("table", qualified).Str("query", query).Msg("Executing DDL")
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %w", classifyDDLError(qualified, err))
		}
		return nil
	}

	// §7.4 item 6 audit: attempt 1 (and the code before it) logged a warning
	// on ALTER failure and returned nil, so ApplySchema reported success
	// while every subsequent write against the missing column failed
	// forever. Attempt every missing column (best effort -- a failure on
	// one column should not block the others), then surface every failure
	// to the caller as a single joined, classified error.
	var alterErrs []error
	for name, pgType := range schema.Columns {
		if err := validateIdentifier(name); err != nil {
			log.Warn().Str("column", name).Err(err).Msg("Skipping invalid column name")
			continue
		}
		if !existingCols[strings.ToLower(name)] {
			dbType := s.mapPgTypeToDatabend(pgType)
			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				quotedTable, quoteIdentifier(name), dbType)

			log.Info().Str("table", qualified).Str("column", name).Str("query", query).Msg("Executing Evolution DDL")
			if _, err := s.db.ExecContext(ctx, query); err != nil {
				classified := classifyDDLError(qualified, fmt.Errorf("failed to add column %q: %w", name, err))
				log.Warn().Err(classified).Str("table", qualified).Str("column", name).Msg("ALTER TABLE failed")
				alterErrs = append(alterErrs, classified)
			}
		}
	}
	if len(alterErrs) > 0 {
		return errors.Join(alterErrs...)
	}

	return nil
}

func (s *DatabendSink) getCurrentColumns(ctx context.Context, ref protocol.TableRef) (map[string]bool, error) {
	// Always predicate on schema. ref.Schema is never empty here -- both
	// callers (ApplySchema via refFromSchemaMeta, the upload path via
	// refFromMessage) run the schema through NormalizeSchema first. An
	// unqualified lookup was verified live to return the union of
	// same-named tables across databases (MULTI_SCHEMA_PLAN.md §6, §7.4
	// item 7): with "orders" in both "default" and "sales", a query with no
	// table_schema predicate returned rows from both. Requiring the schema
	// argument here removes that hazard structurally instead of patching
	// around an optional branch.
	rows, err := s.db.QueryContext(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
		ref.Schema, ref.Table)
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

func (s *DatabendSink) uploadTableBatch(ctx context.Context, ref protocol.TableRef, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	qualified := ref.String()
	if err := validateIdentifier(ref.Schema); err != nil {
		return fmt.Errorf("invalid schema name %q: %w", ref.Schema, err)
	}
	if err := validateIdentifier(ref.Table); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	if err := s.ensureDatabase(ctx, ref.Schema); err != nil {
		return fmt.Errorf("failed to ensure database %q: %w", ref.Schema, err)
	}

	// T1-17: lazily resolve PK from Databend the first time we see this table.
	if err := s.ensurePrimaryKey(ctx, ref); err != nil {
		log.Warn().Err(err).Str("table", qualified).Msg("ensurePrimaryKey failed; continuing with current cache")
	}

	// GROUP BY COLUMN SET
	// CDC batches might contain records with different column sets (evolution or different sources)
	groups := make(map[string][]map[string]any)
	groupCols := make(map[string][]string)

	for _, m := range messages {
		data, err := decodePayload(m)
		if err != nil {
			// T1-1: surface deserialization failures instead of silently dropping.
			s.emitDLQ(ctx, m, qualified, reasonDeserializationFailed, err.Error())
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
	pks := s.pkCache[qualified]
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

	quotedTable := quoteQualified(ref)

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

		if err := s.executeReplaceIntoChunks(ctx, qualified, quotedTable, colList, pkList, columns, records); err != nil {
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
			return fmt.Errorf("chunk %d failed: %w", chunksEmitted, classifyDDLError(table, err))
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

// emitDLQ records a terminal per-record failure (deserialization, or a
// delete with no resolvable primary key). It increments the
// cdc_sink_dlq_total counter under reasonLabel, logs the failure with
// structured context, and (when a DLQPublisher is wired in) publishes a
// SinkDeadLetterEvent to the configured NATS subject.
func (s *DatabendSink) emitDLQ(ctx context.Context, m protocol.Message, table, reasonLabel, detail string) {
	log.Error().
		Err(fmt.Errorf("%s", detail)).
		Str("table", table).
		Str("sink_id", s.name).
		Str("msg_uuid", m.UUID).
		Uint64("lsn", m.LSN).
		Str("reason", reasonLabel).
		Msg("sink write failed")

	SinkDLQTotal.WithLabelValues(s.name, table, reasonLabel).Inc()

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
		Reason:    detail,
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

func (s *DatabendSink) deleteTableBatch(ctx context.Context, ref protocol.TableRef, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	qualified := ref.String()
	if err := validateIdentifier(ref.Schema); err != nil {
		return fmt.Errorf("invalid schema name %q: %w", ref.Schema, err)
	}
	if err := validateIdentifier(ref.Table); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	if err := s.ensureDatabase(ctx, ref.Schema); err != nil {
		return fmt.Errorf("failed to ensure database %q: %w", ref.Schema, err)
	}

	// T1-17: lazily resolve PK from Databend the first time we see this table.
	if err := s.ensurePrimaryKey(ctx, ref); err != nil {
		log.Warn().Err(err).Str("table", qualified).Msg("ensurePrimaryKey failed; continuing with current cache")
	}

	s.pkMu.RLock()
	pks := s.pkCache[qualified]
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

	quotedTable := quoteQualified(ref)

	for _, m := range messages {
		data, err := decodePayload(m)
		if err != nil {
			// T1-1: surface deserialization failures instead of silently dropping.
			s.emitDLQ(ctx, m, qualified, reasonDeserializationFailed, err.Error())
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
			// §7.4 item 6 audit: this used to be a silent `continue`. If none
			// of the resolved PK columns are present in the decoded payload
			// we cannot build a WHERE clause and the delete is dropped --
			// surface that the same way a deserialization failure is
			// surfaced (DLQ + metric) instead of letting it disappear.
			s.emitDLQ(ctx, m, qualified, reasonMissingPKColumns,
				fmt.Sprintf("delete skipped: none of the primary key columns %v present in decoded payload", pks))
			continue
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s", quotedTable, strings.Join(whereClauses, " AND "))
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return classifyDDLError(qualified, err)
		}
	}
	return nil
}

// ensurePrimaryKey lazily resolves the primary key columns for a table. It
// performs at most one SHOW CREATE TABLE per table per process lifetime; after
// that, lookups are served from the in-memory cache.
func (s *DatabendSink) ensurePrimaryKey(ctx context.Context, ref protocol.TableRef) error {
	return s.refreshPrimaryKey(ctx, ref)
}

// refreshPrimaryKey executes SHOW CREATE TABLE on the sink-side datastore,
// parses the PRIMARY KEY clause out of the resulting DDL, and updates the
// cache. Failures are non-fatal: we log + record a metric, fall back to the
// existing cache (or a default of "id") and continue. The pkLoaded gate
// ensures SHOW CREATE TABLE is invoked at most once per table per process
// lifetime, even when called from concurrent goroutines.
func (s *DatabendSink) refreshPrimaryKey(ctx context.Context, ref protocol.TableRef) error {
	qualified := ref.String()

	// Double-checked locking: skip the query if we have already attempted
	// resolution for this table.
	s.pkMu.RLock()
	if _, loaded := s.pkLoaded[qualified]; loaded {
		s.pkMu.RUnlock()
		return nil
	}
	s.pkMu.RUnlock()

	// Reserve the slot BEFORE issuing the query so concurrent callers
	// short-circuit on their second-check.
	s.pkMu.Lock()
	if _, loaded := s.pkLoaded[qualified]; loaded {
		s.pkMu.Unlock()
		return nil
	}
	s.pkLoaded[qualified] = struct{}{}
	s.pkMu.Unlock()

	quotedTable := quoteQualified(ref)
	query := fmt.Sprintf("SHOW CREATE TABLE %s", quotedTable)

	var ddl string
	scanErr := s.db.QueryRowScan(ctx, query, nil, &ddl)

	if scanErr != nil {
		s.pkMu.Lock()
		delete(s.pkLoaded, qualified)
		s.pkMu.Unlock()

		log.Warn().Err(scanErr).Str("table", qualified).Msg("SHOW CREATE TABLE failed; falling back to default PK")
		s.ensureFallbackPK(qualified)
		SinkPKResolved.WithLabelValues(s.name, qualified).Set(0)
		return scanErr
	}

	pks := parsePKFromDDL(ddl)
	if len(pks) == 0 {
		log.Warn().Str("table", qualified).Str("ddl", ddl).Msg("no PRIMARY KEY clause found in SHOW CREATE TABLE; falling back")
		s.ensureFallbackPK(qualified)
		SinkPKResolved.WithLabelValues(s.name, qualified).Set(0)
		return nil
	}

	s.pkMu.Lock()
	s.pkCache[qualified] = pks
	s.pkMu.Unlock()
	SinkPKResolved.WithLabelValues(s.name, qualified).Set(1)
	log.Info().Str("table", qualified).Strs("pks", pks).Msg("resolved primary key from Databend")
	return nil
}

// ensureFallbackPK installs the default "id" primary key for a table (keyed
// by TableRef.String()) if no PK has been recorded yet. This matches the
// legacy behaviour but is now scoped to tables we have actually attempted to
// resolve.
func (s *DatabendSink) ensureFallbackPK(qualified string) {
	s.pkMu.Lock()
	defer s.pkMu.Unlock()
	if _, ok := s.pkCache[qualified]; ok {
		return
	}
	s.pkCache[qualified] = []string{"id"}
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

package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

	// deletedAtColumn is the column every synced table's soft-delete UPDATE
	// path (deleteTableBatch) targets unconditionally. WS-4/PIPE-OQ-5 is
	// "soft delete everywhere", but the plan itself documents that several
	// real satellite tables (business_entity_addresses,
	// business_entity_contacts, visitation_contacts,
	// business_entity_industry -- see PIPE-OQ-4's satellite table list)
	// have no deleted_at column at the source. ApplySchema below always
	// synthesizes this column on the Databend side regardless of whether
	// the source schema declares it, so a delete against any of those
	// tables succeeds instead of failing with an unknown-column error
	// (which, left unclassified, would retry-loop forever against a frozen
	// replication slot -- see permanentDDLMarkers below).
	deletedAtColumn = "deleted_at"

	// pkMetaDatabase/pkMetaTable name the sink-owned metadata table that
	// persists PkColumns durably (WS-4.6). Databend's CREATE TABLE emits no
	// PRIMARY KEY / CLUSTER BY clause we can rely on -- SHOW CREATE TABLE
	// after a process restart finds nothing, and both write paths silently
	// fell back to pks = []string{"id"}, which does not dedup a sidecar
	// keyed on record_id and silently duplicates rows on every redelivered
	// or updated row. Persisting the declared PkColumns here, and treating
	// this table as authoritative ahead of SHOW CREATE TABLE, survives a
	// restart because it is read back from Databend itself rather than
	// process memory.
	pkMetaDatabase = "cdc_meta"
	pkMetaTable    = "pk_columns"
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
	pkLoaded map[string]struct{} // TableRef.String() we've already attempted resolution for (metadata table + SHOW CREATE TABLE)

	pkMetaMu      sync.Mutex
	pkMetaEnsured bool // whether cdc_meta.pk_columns has been created this process

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

	// colTypeMu/colTypeCache back WS-6's schema-type-divergence detection.
	// ApplySchema is add-only -- it never issues ALTER ... MODIFY COLUMN --
	// so a schema_change that redeclares an *existing* column with a
	// different Databend type can only be surfaced, never silently
	// applied. Rather than compare against information_schema.columns'
	// reported data_type (which uses Databend's own canonical type names,
	// e.g. VARCHAR, and would false-positive against every column purely
	// from naming skew with mapPgTypeToDatabend's STRING/INT64/etc
	// vocabulary), this remembers the dbType string *this sink itself*
	// last computed and applied for that column, so the comparison is
	// self-consistent and only fires on a genuine change in what the
	// source declares. In-memory only: a process restart clears it, so the
	// column already existing but with an as-yet-unknown Databend type is
	// correctly not flagged.
	colTypeMu    sync.Mutex
	colTypeCache map[string]map[string]string // qualified table -> column -> last-applied dbType
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
		colTypeCache:     make(map[string]map[string]string),
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

	for ref, upsertMsgs := range upserts {
		r, um := ref, upsertMsgs
		if delMsgs, hasDelete := deletes[ref]; hasDelete {
			// Round-5c review MEDIUM: the same ref appears in both maps --
			// this flush contains a delete AND a (superseded) upsert for
			// the same table. Running them as independent concurrent
			// goroutines is exactly how the round-5b tombstone-preservation
			// fix reopens itself: fetchCurrentDeletedAt (inside
			// uploadTableBatch) can read the pre-delete deleted_at before
			// the concurrent UPDATE lands, then REPLACE INTO writes that
			// stale (nil) value, silently erasing the delete this same
			// flush was supposed to apply. Serialise the pair instead of
			// giving them independent goroutines: run the upsert, THEN the
			// delete, in one goroutine, so fetchCurrentDeletedAt (if it
			// runs at all for other rows in the upsert) can only observe
			// pre-delete state, and the delete -- being strictly last --
			// always wins for this ref. This does not reorder anything
			// relative to today's behaviour beyond removing the race: the
			// two were never ordered before, so "delete wins" is a
			// deliberate, documented choice, not an accident.
			dm := delMsgs
			g.Go(func() error {
				if err := s.uploadTableBatch(gCtx, r, um); err != nil {
					return err
				}
				return s.deleteTableBatch(gCtx, r, dm)
			})
			continue
		}
		g.Go(func() error {
			return s.uploadTableBatch(gCtx, r, um)
		})
	}

	for ref, delMsgs := range deletes {
		if _, hasUpsert := upserts[ref]; hasUpsert {
			// Already scheduled (serialised after its upsert) above.
			continue
		}
		r, dm := ref, delMsgs
		g.Go(func() error {
			return s.deleteTableBatch(gCtx, r, dm)
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

		// WS-4.6: persist durably so a sink restart does not lose the PK and
		// fall back to pks = []string{"id"}. Best effort logged loudly rather
		// than failing the whole ApplySchema call -- the in-memory cache set
		// above already makes this process instance correct; persistence is
		// what makes the *next* process instance correct too.
		if err := s.persistPKMetadata(ctx, ref, schema.PKColumns); err != nil {
			log.Error().Err(err).Str("table", qualified).Strs("pks", schema.PKColumns).
				Msg("failed to persist primary key metadata; a sink restart before this succeeds will fall back to an incorrect PK for this table")
		}
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

		hasDeletedAt := false
		for _, name := range colNames {
			if err := validateIdentifier(name); err != nil {
				return fmt.Errorf("invalid column name %q: %w", name, err)
			}
			if strings.EqualFold(name, deletedAtColumn) {
				hasDeletedAt = true
			}
			pgType := schema.Columns[name]
			dbType := s.mapPgTypeToDatabend(pgType)
			colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdentifier(name), dbType))
			s.recordColumnType(qualified, name, dbType)
		}
		if !hasDeletedAt {
			// WS-4: soft delete everywhere requires deleted_at on every
			// synced table, but several real satellite tables have no such
			// column at the source (PIPE-OQ-4). Synthesize it here so
			// deleteTableBatch's unconditional UPDATE ... SET deleted_at
			// never hits an unknown-column error for those tables.
			colDefs = append(colDefs, fmt.Sprintf("%s TIMESTAMP", quoteIdentifier(deletedAtColumn)))
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
	// addedDeletedAt tracks whether the main loop below already issued (and
	// the underlying Databend already applied) an ADD COLUMN for
	// deleted_at -- round-5 review MEDIUM: existingCols is a snapshot taken
	// once at the top of ApplySchema and never updated as this function
	// adds columns. Without this flag, a table whose Databend columns lack
	// deleted_at but whose schema.Columns *does* declare it (exactly
	// PIPE-OQ-4's satellite-table remediation direction: the source gains
	// the column and a schema_change carries it) would have it ALTERed
	// twice in the same ApplySchema call -- once by the loop below, once by
	// the backstop that follows -- and Databend has no ADD COLUMN IF NOT
	// EXISTS, so the second ALTER fails with "column already exists",
	// misclassified transient (no permanentDDLMarkers match), which errors
	// the table and Nacks the batch until the next redelivery self-heals it.
	addedDeletedAt := false
	for name, pgType := range schema.Columns {
		if err := validateIdentifier(name); err != nil {
			log.Warn().Str("column", name).Err(err).Msg("Skipping invalid column name")
			continue
		}
		dbType := s.mapPgTypeToDatabend(pgType)
		if !existingCols[strings.ToLower(name)] {
			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				quotedTable, quoteIdentifier(name), dbType)

			log.Info().Str("table", qualified).Str("column", name).Str("query", query).Msg("Executing Evolution DDL")
			if _, err := s.db.ExecContext(ctx, query); err != nil {
				classified := classifyDDLError(qualified, fmt.Errorf("failed to add column %q: %w", name, err))
				log.Warn().Err(classified).Str("table", qualified).Str("column", name).Msg("ALTER TABLE failed")
				alterErrs = append(alterErrs, classified)
			} else if strings.EqualFold(name, deletedAtColumn) {
				addedDeletedAt = true
			}
			s.recordColumnType(qualified, name, dbType)
			continue
		}

		// WS-6: the column already exists in Databend. ApplySchema never
		// ALTERs an existing column's type (custom objects don't permit
		// type changes app-side, and a non-custom-object type change has
		// no safe automatic remediation -- see the plan's PIPE-OQ-1). What
		// it must not do is stay silent about a divergence: detect it,
		// count it, and log it loudly so it surfaces on a dashboard/alert
		// instead of only being discoverable by noticing stale data.
		if s.checkColumnTypeDivergence(qualified, name, dbType) {
			log.Error().Str("table", qualified).Str("column", name).Str("declared_type", dbType).
				Msg("schema_change declared a column type that differs from the previously applied type; ApplySchema is add-only and will NOT alter the existing column -- this table's column is now out of sync with the source and requires manual remediation (see WS-6 / PIPE-OQ-1)")
			SinkSchemaTypeDivergenceTotal.WithLabelValues(s.name, qualified, name).Inc()
		}
		s.recordColumnType(qualified, name, dbType)
	}

	// WS-4: backstop for a table created before this fix, or whose source
	// schema.Columns has never declared deleted_at (e.g. a satellite table
	// per PIPE-OQ-4). Without this, such a table's deleteTableBatch UPDATE
	// hits an unknown-column error on every delete, forever -- see the
	// deletedAtColumn doc comment. Guarded by addedDeletedAt (see above) so
	// this never re-issues an ALTER the loop above already applied
	// successfully this call.
	if !addedDeletedAt && !existingCols[strings.ToLower(deletedAtColumn)] {
		query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s TIMESTAMP", quotedTable, quoteIdentifier(deletedAtColumn))
		log.Info().Str("table", qualified).Str("column", deletedAtColumn).Str("query", query).Msg("Executing Evolution DDL (soft-delete backstop)")
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			classified := classifyDDLError(qualified, fmt.Errorf("failed to add column %q: %w", deletedAtColumn, err))
			log.Warn().Err(classified).Str("table", qualified).Str("column", deletedAtColumn).Msg("ALTER TABLE failed")
			alterErrs = append(alterErrs, classified)
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

// recordColumnType remembers dbType as the last type this sink applied (or
// observed applied) for qualified.name, for checkColumnTypeDivergence to
// compare future schema_change declarations against. Always overwrites --
// once a divergence has been logged for a change, repeating the exact same
// (now-divergent) declaration on every redelivered/replayed schema_change
// must not re-alert forever; only an actual *further* change should.
func (s *DatabendSink) recordColumnType(qualified, name, dbType string) {
	s.colTypeMu.Lock()
	defer s.colTypeMu.Unlock()
	if s.colTypeCache == nil {
		s.colTypeCache = make(map[string]map[string]string)
	}
	cols, ok := s.colTypeCache[qualified]
	if !ok {
		cols = make(map[string]string)
		s.colTypeCache[qualified] = cols
	}
	cols[strings.ToLower(name)] = dbType
}

// checkColumnTypeDivergence reports whether dbType differs from the type
// this sink last recorded for qualified.name. No prior recording (nil map
// entry -- e.g. first ApplySchema call for this table since process start,
// column pre-existed from before this sink ever saw it) is NOT treated as
// a divergence: there is nothing to compare against, and flagging it would
// false-positive on every restart.
func (s *DatabendSink) checkColumnTypeDivergence(qualified, name, dbType string) bool {
	s.colTypeMu.Lock()
	defer s.colTypeMu.Unlock()
	cols, ok := s.colTypeCache[qualified]
	if !ok {
		return false
	}
	prev, ok := cols[strings.ToLower(name)]
	if !ok {
		return false
	}
	return prev != dbType
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

	s.pkMu.RLock()
	pks := s.pkCache[qualified]
	s.pkMu.RUnlock()
	if len(pks) == 0 {
		// WS-4.6: a custom_objects table (generated custom table or
		// built-in sidecar) with no resolved PK is not survivable via the
		// ["id"] default -- sidecars are keyed on record_id, not id, and an
		// id-keyed REPLACE INTO ON ("id") does not dedup against the real
		// key, silently duplicating every redelivered or updated row. Make
		// this a hard error instead of a silent default so the batch
		// retries (or is DLQ'd loudly) rather than corrupting the table.
		if protocol.NormalizeSchema(ref.Schema) == "custom_objects" {
			return fmt.Errorf("no primary key resolved for custom_objects table %q; refusing to fall back to [\"id\"] (would silently duplicate rows for a non-id-keyed table); ensure a schema_change with PkColumns has been applied", qualified)
		}
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

	// Decode every message's payload first (pks must already be resolved,
	// since the deleted_at-preservation step below needs them).
	type decodedRow struct {
		msg  protocol.Message
		data map[string]any
	}
	decoded := make([]decodedRow, 0, len(messages))
	for _, m := range messages {
		data, err := decodePayload(m)
		if err != nil {
			// T1-1: surface deserialization failures instead of silently dropping.
			s.emitDLQ(ctx, m, qualified, reasonDeserializationFailed, err.Error())
			continue
		}
		decoded = append(decoded, decodedRow{msg: m, data: data})
	}

	// Round-5 review MEDIUM: "a later upsert resurrects a soft-deleted
	// row." REPLACE INTO's column list comes from the payload's own keys.
	// For any table where deleted_at is *synthesized* (not a real source
	// column -- PIPE-OQ-4's satellite tables), no upsert payload ever
	// mentions it, so a later REPLACE INTO on the same PK would silently
	// null out an existing tombstone -- exactly what at-least-once
	// redelivery of an update that was already superseded by a delete does.
	// Fix: for any row whose decoded payload omits deleted_at, look up the
	// row's *current* deleted_at from Databend and carry it forward
	// explicitly, so the REPLACE INTO preserves rather than erases it. Rows
	// that already carry their own deleted_at (a real source column) are
	// left untouched -- the bug does not apply to them.
	needsPreserve := make([]decodedRow, 0)
	for _, dr := range decoded {
		if _, ok := dr.data[deletedAtColumn]; !ok {
			needsPreserve = append(needsPreserve, dr)
		}
	}
	if len(needsPreserve) > 0 {
		tuples := make([][]any, len(needsPreserve))
		for i, dr := range needsPreserve {
			tuple := make([]any, len(pks))
			for j, pk := range pks {
				tuple[j] = normalizeValue(dr.data[pk])
			}
			tuples[i] = tuple
		}
		current, err := s.fetchCurrentDeletedAt(ctx, ref, pks, tuples)
		if err != nil {
			// Non-fatal: log loudly and proceed without preservation for
			// this batch rather than failing the whole upload over a
			// best-effort read. Worst case reproduces the pre-fix
			// behaviour for this one batch; it does not make anything
			// worse than today, and the fetch failure itself is visible.
			log.Warn().Err(err).Str("table", qualified).Msg("failed to fetch current deleted_at for tombstone-preserving upsert; proceeding without preservation for this batch")
			SinkDeletedAtPreservationFailuresTotal.WithLabelValues(s.name, qualified).Inc()
		} else {
			for i, dr := range needsPreserve {
				key := pkTupleKey(tuples[i])
				// Explicitly set deleted_at (nil if the row doesn't exist
				// yet, i.e. a genuine first insert) rather than leaving it
				// absent, so every row in this table's batch carries a
				// consistent column set for grouping below.
				dr.data[deletedAtColumn] = current[key]
			}
		}
	}

	// WS-7: any row whose message flagged a column as
	// protocol.ColumnKindToastedUnchanged has that column absent from
	// dr.data not because it is NULL, but because Postgres elided an
	// unchanged TOASTed value from the WAL tuple. REPLACE INTO's column
	// list comes from dr.data's own keys (below), so left alone that
	// column would be omitted from the statement and Databend would
	// default it to NULL on replace -- silently truncating a large column
	// on every update that doesn't touch it. Fetch and carry forward the
	// current value, the same pattern as the deleted_at preservation step
	// above, generalized to an arbitrary per-row column set.
	tupleFor := func(dr decodedRow) []any {
		tuple := make([]any, len(pks))
		for j, pk := range pks {
			tuple[j] = normalizeValue(dr.data[pk])
		}
		return tuple
	}

	// Opus-validation-review MEDIUM: fetchCurrentColumns (below) reads
	// pre-flush database state. If THIS SAME batch already contains an
	// earlier row (decoded preserves message/event order) for the same PK
	// that carries a real value for a column a later row toast-needs, that
	// in-batch value -- not the stale pre-batch DB value -- is what the
	// later row's write must preserve. Without this, a batch containing two
	// updates to the same PK, where the second toast-elides a column the
	// first just set, would silently overwrite the first update's real
	// value with an older one read back from Databend -- strictly better
	// than the pre-WS-7 bug (which nulled it outright) but still wrong.
	// Track each PK's resulting column values as we walk the batch in
	// order, and resolve a later row's toast-need from it before ever
	// falling back to a DB read.
	inBatchValues := make(map[string]map[string]any) // pkTupleKey -> column -> value

	toastNeeds := make(map[string][]decodedRow) // column name -> rows still needing a DB-fetched value
	for _, dr := range decoded {
		key := pkTupleKey(tupleFor(dr))
		if len(dr.msg.ColumnKinds) > 0 {
			for col, kind := range dr.msg.ColumnKinds {
				if kind != protocol.ColumnKindToastedUnchanged {
					continue
				}
				if _, present := dr.data[col]; present {
					// Already has a value (e.g. from the deleted_at
					// preservation step, or simply present some other way) --
					// nothing to resolve.
					continue
				}
				if prior, ok := inBatchValues[key]; ok {
					if v, ok2 := prior[col]; ok2 {
						dr.data[col] = v
						continue
					}
				}
				toastNeeds[col] = append(toastNeeds[col], dr)
			}
		}
		// Record this row's own resulting values (including anything just
		// resolved from an earlier row above) as the new in-batch state for
		// its PK, so any LATER row for the same PK in this batch sees it.
		m := inBatchValues[key]
		if m == nil {
			m = make(map[string]any)
			inBatchValues[key] = m
		}
		for col, v := range dr.data {
			m[col] = v
		}
	}
	if len(toastNeeds) > 0 {
		toastCols := make([]string, 0, len(toastNeeds))
		for col := range toastNeeds {
			toastCols = append(toastCols, col)
		}
		sort.Strings(toastCols)

		seen := make(map[string]bool)
		tuples := make([][]any, 0)
		for _, rows := range toastNeeds {
			for _, dr := range rows {
				key := pkTupleKey(tupleFor(dr))
				if seen[key] {
					continue
				}
				seen[key] = true
				tuples = append(tuples, tupleFor(dr))
			}
		}

		current, err := s.fetchCurrentColumns(ctx, ref, pks, tuples, toastCols)
		if err != nil {
			log.Warn().Err(err).Str("table", qualified).Strs("columns", toastCols).Msg("failed to fetch current values for TOAST-unchanged columns; proceeding without preservation for this batch")
			SinkToastPreservationFailuresTotal.WithLabelValues(s.name, qualified).Inc()
		} else {
			for col, rows := range toastNeeds {
				for _, dr := range rows {
					key := pkTupleKey(tupleFor(dr))
					if vals, ok := current[key]; ok {
						if v, ok2 := vals[col]; ok2 {
							dr.data[col] = v
						}
					}
					// A pk miss (row not found in Databend yet) means this
					// is a genuine first write for that pk -- there is
					// nothing to preserve, so the column stays absent and
					// is correctly excluded from this row's REPLACE INTO
					// column list (it lands in a different GROUP BY COLUMN
					// SET bucket below than rows that do have it).
				}
			}
		}
	}

	// GROUP BY COLUMN SET
	// CDC batches might contain records with different column sets (evolution or different sources)
	groups := make(map[string][]map[string]any)
	groupCols := make(map[string][]string)

	for _, dr := range decoded {
		data := dr.data
		cols := make([]string, 0, len(data))
		for k := range data {
			cols = append(cols, k)
		}
		sort.Strings(cols)

		key := strings.Join(cols, ",")
		groups[key] = append(groups[key], data)
		groupCols[key] = cols
	}

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
		// WS-4.6: same reasoning as uploadTableBatch -- an id-keyed default
		// against a non-id-keyed custom_objects table silently no-ops or
		// targets the wrong row on a soft delete. Fail loudly instead.
		if protocol.NormalizeSchema(ref.Schema) == "custom_objects" {
			return fmt.Errorf("no primary key resolved for custom_objects table %q; refusing to fall back to [\"id\"]; ensure a schema_change with PkColumns has been applied", qualified)
		}
		pks = []string{"id"}
	}

	// Validate primary key names
	for _, pk := range pks {
		if err := validateIdentifier(pk); err != nil {
			return fmt.Errorf("invalid primary key name %q: %w", pk, err)
		}
	}

	quotedTable := quoteQualified(ref)

	// WS-4 item 3: batch by (deletedAt, pk-tuple) rather than one UPDATE per
	// message. Under the ratified merge-on-write model (WS-4B) every UPDATE
	// is copy-on-write and produces a new table snapshot version, so N
	// per-row deletes emit N snapshots for what should be one batched
	// statement -- exactly the write amplification WS-4B flagged. Grouping
	// by deletedAt (rather than a single shared value for the whole
	// message batch) preserves per-message event-time idempotency: a
	// redelivered delete still sets the same deleted_at it always would,
	// it just now shares a statement with any other delete in this flush
	// that happens to carry the identical timestamp.
	groups := make(map[time.Time][][]any) // deletedAt -> ordered pk-value tuples (in pks order)

	for _, m := range messages {
		data, err := decodePayload(m)
		if err != nil {
			// T1-1: surface deserialization failures instead of silently dropping.
			s.emitDLQ(ctx, m, qualified, reasonDeserializationFailed, err.Error())
			continue
		}

		tuple := make([]any, 0, len(pks))
		missing := false
		for _, pk := range pks {
			val, ok := data[pk]
			if !ok {
				missing = true
				break
			}
			tuple = append(tuple, normalizeValue(val))
		}

		if missing || len(tuple) == 0 {
			// §7.4 item 6 audit: this used to be a silent `continue`. If any
			// of the resolved PK columns are absent from the decoded
			// payload we cannot build a WHERE clause and the delete is
			// dropped -- surface that the same way a deserialization
			// failure is surfaced (DLQ + metric) instead of letting it
			// disappear.
			s.emitDLQ(ctx, m, qualified, reasonMissingPKColumns,
				fmt.Sprintf("delete skipped: not all primary key columns %v present in decoded payload", pks))
			continue
		}

		// WS-4 / PIPE-OQ-5: soft delete everywhere. No synced table is ever
		// hard-deleted from the warehouse -- deleteTableBatch's DELETE FROM
		// path is retired for every table, transformed or not, uniformly.
		// deleted_at comes from the message's own event Timestamp, never
		// server time, so a replayed/redelivered delete is idempotent and
		// reflects when the delete actually happened in Postgres.
		deletedAt := m.Timestamp
		if deletedAt.IsZero() {
			deletedAt = time.Now().UTC()
		}
		groups[deletedAt] = append(groups[deletedAt], tuple)
	}

	// LOW (round-5 review): Go map iteration order is randomized. If the
	// same PK appears twice in one flush under two different delete
	// timestamps (a genuine possibility -- e.g. a delete-recreate-delete
	// sequence collapsed into one batch), iterating groups in random order
	// makes the winning deleted_at arbitrary from run to run. Sort
	// ascending so the LATEST timestamp always wins deterministically (it
	// is applied last), matching "the most recent delete event is the one
	// that sticks."
	orderedTimestamps := make([]time.Time, 0, len(groups))
	for deletedAt := range groups {
		orderedTimestamps = append(orderedTimestamps, deletedAt)
	}
	sort.Slice(orderedTimestamps, func(i, j int) bool { return orderedTimestamps[i].Before(orderedTimestamps[j]) })

	for _, deletedAt := range orderedTimestamps {
		tuples := groups[deletedAt]
		if err := s.executeSoftDeleteChunks(ctx, qualified, quotedTable, pks, deletedAt, tuples); err != nil {
			return err
		}
	}
	return nil
}

// pkTupleKey renders a pk-value tuple (already normalized, in pks order) as
// a stable map key, joining with "|" the same way deleteTableBatch's
// composite-PK grouping does.
func pkTupleKey(tuple []any) string {
	parts := make([]string, len(tuple))
	for i, v := range tuple {
		parts[i] = canonicalPKValueString(v)
	}
	return strings.Join(parts, "|")
}

// canonicalPKValueString renders a single PK value the same way regardless
// of which side of the wire it came from (round-5c review MEDIUM). The
// write side keys on normalizeValue(payload[pk]); when decodePayload falls
// back to its JSON path (sink.go decodePayload), a JSON-encoded integer PK
// decodes to float64 -- msgpack decodes integers natively, so this is
// specifically the msgpack-decode-failed / JSON-fallback path, not the
// common case. The read side keys on values scanned out of Databend's
// driver into *any, which for an INT64 column comes back as a Go int64 (or
// similar integral type), never float64. A plain fmt.Sprintf("%v") on both
// renders these differently for a large PK -- float64(1234567890123)
// formats as "1.234567890123e+12" via %v's default %g-like verb, while
// int64(1234567890123) formats as "1234567890123" -- so the two sides would
// never agree on the key for exactly the PK values most likely to matter
// (large bigint ids), causing fetchCurrentDeletedAt's map lookup to silently
// miss and reintroduce the tombstone-resurrection bug with no error at all.
//
// Canonicalize the two known-divergent shapes:
//   - an integral float64 (no fractional part, in int64 range) renders via
//     %d, matching how the same logical value renders as an int64;
//   - []byte (a driver possibility for some column types, and never
//     produced by the write side, which already converts binary PKs to
//     string/base64 well before this point) renders as its string form
//     rather than %v's default (which for []byte is a bracketed list of
//     byte values, matching nothing on the write side).
//
// Every other type (string, int64, bool, time.Time, ...) is assumed to
// already render identically on both sides and falls through to %v.
func canonicalPKValueString(v any) string {
	switch val := v.(type) {
	case float64:
		if val == math.Trunc(val) && val >= math.MinInt64 && val <= math.MaxInt64 {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%v", val)
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// fetchCurrentDeletedAt reads back the current deleted_at value for each of
// tuples (pk-value tuples in pks order) from ref, keyed by pkTupleKey. A pk
// with no row in Databend yet (a genuine first insert) simply has no entry
// in the returned map -- callers should treat a map miss as "no tombstone",
// i.e. nil. This is the read half of the round-5 "upsert resurrects a
// soft-deleted row" fix: an upsert whose payload omits deleted_at must
// carry forward whatever value is already there instead of letting
// REPLACE INTO default it to NULL.
// pkWhereChunkSize returns how many pk-tuples fit in a single query's WHERE
// clause without exceeding maxPh placeholders, reserving `reserved`
// placeholders for use outside the per-row predicates (e.g. a shared SET
// value in an UPDATE). Always returns at least 1, capped at numTuples.
// Shared by fetchCurrentDeletedAt, fetchCurrentColumns and
// executeSoftDeleteChunks, which all chunk the same pk-tuple batch shape
// against the same placeholder budget.
func pkWhereChunkSize(numTuples, numPKs, maxPh, reserved int) int {
	if numPKs <= 0 {
		return numTuples
	}
	budget := maxPh - reserved
	if budget < numPKs {
		budget = numPKs
	}
	chunkSize := budget / numPKs
	if chunkSize <= 0 {
		chunkSize = 1
	}
	if chunkSize > numTuples {
		chunkSize = numTuples
	}
	return chunkSize
}

// buildPKWhereClause renders the WHERE predicate (and its bind args) for one
// chunk of pk-value tuples: a single-column PK gets the compact `pk IN
// (?, ...)` form, a composite PK gets `(pk1 = ? AND pk2 = ?) OR (...)`.
func buildPKWhereClause(quotedPKs []string, chunk [][]any) (where string, args []any) {
	if len(quotedPKs) == 1 {
		placeholders := make([]string, len(chunk))
		args = make([]any, 0, len(chunk))
		for i, tuple := range chunk {
			placeholders[i] = "?"
			args = append(args, tuple[0])
		}
		return fmt.Sprintf("%s IN (%s)", quotedPKs[0], strings.Join(placeholders, ", ")), args
	}

	predicates := make([]string, len(chunk))
	args = make([]any, 0, len(chunk)*len(quotedPKs))
	for i, tuple := range chunk {
		clauses := make([]string, len(quotedPKs))
		for j, col := range quotedPKs {
			clauses[j] = fmt.Sprintf("%s = ?", col)
			args = append(args, tuple[j])
		}
		predicates[i] = "(" + strings.Join(clauses, " AND ") + ")"
	}
	return strings.Join(predicates, " OR "), args
}

func (s *DatabendSink) fetchCurrentDeletedAt(ctx context.Context, ref protocol.TableRef, pks []string, tuples [][]any) (map[string]any, error) {
	result := make(map[string]any, len(tuples))
	if len(tuples) == 0 || len(pks) == 0 {
		return result, nil
	}

	quotedTable := quoteQualified(ref)
	quotedPks := make([]string, len(pks))
	for i, pk := range pks {
		quotedPks[i] = quoteIdentifier(pk)
	}
	selectList := strings.Join(quotedPks, ", ") + ", " + quoteIdentifier(deletedAtColumn)

	maxPh := s.maxPlaceholders
	if maxPh <= 0 {
		maxPh = DefaultMaxPlaceholders
	}
	chunkSize := pkWhereChunkSize(len(tuples), len(pks), maxPh, 0)

	for start := 0; start < len(tuples); start += chunkSize {
		end := start + chunkSize
		if end > len(tuples) {
			end = len(tuples)
		}
		chunk := tuples[start:end]

		where, args := buildPKWhereClause(quotedPks, chunk)

		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectList, quotedTable, where)
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch current deleted_at: %w", err)
		}

		scanErr := func() error {
			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					log.Warn().Err(closeErr).Str("table", ref.Table).Msg("failed to close rows after fetching current deleted_at")
				}
			}()
			for rows.Next() {
				dest := make([]any, len(pks)+1)
				vals := make([]any, len(pks)+1)
				for i := range dest {
					dest[i] = &vals[i]
				}
				if err := rows.Scan(dest...); err != nil {
					return fmt.Errorf("failed to scan current deleted_at row: %w", err)
				}
				key := pkTupleKey(vals[:len(pks)])
				result[key] = vals[len(pks)]
			}
			return rows.Err()
		}()
		if scanErr != nil {
			return nil, scanErr
		}
	}

	return result, nil
}

// fetchCurrentColumns reads back the current values of cols for each
// pk-value tuple, keyed by pkTupleKey then column name. This is the WS-7
// TOAST-preservation counterpart to fetchCurrentDeletedAt: where that
// function always reads one well-known column, different rows in the same
// batch may have different TOASTed columns omitted, so this supports an
// arbitrary, per-call column set. A pk with no row in Databend yet has no
// entry in the returned map; callers should treat a map miss (or a miss on
// a specific column within an entry) as "nothing to preserve".
func (s *DatabendSink) fetchCurrentColumns(ctx context.Context, ref protocol.TableRef, pks []string, tuples [][]any, cols []string) (map[string]map[string]any, error) {
	result := make(map[string]map[string]any, len(tuples))
	if len(tuples) == 0 || len(pks) == 0 || len(cols) == 0 {
		return result, nil
	}

	quotedTable := quoteQualified(ref)
	quotedPks := make([]string, len(pks))
	for i, pk := range pks {
		quotedPks[i] = quoteIdentifier(pk)
	}
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quoteIdentifier(c)
	}
	selectList := strings.Join(quotedPks, ", ") + ", " + strings.Join(quotedCols, ", ")

	maxPh := s.maxPlaceholders
	if maxPh <= 0 {
		maxPh = DefaultMaxPlaceholders
	}
	chunkSize := pkWhereChunkSize(len(tuples), len(pks), maxPh, 0)

	for start := 0; start < len(tuples); start += chunkSize {
		end := start + chunkSize
		if end > len(tuples) {
			end = len(tuples)
		}
		chunk := tuples[start:end]

		where, args := buildPKWhereClause(quotedPks, chunk)

		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectList, quotedTable, where)
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch current columns: %w", err)
		}

		scanErr := func() error {
			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					log.Warn().Err(closeErr).Str("table", ref.Table).Msg("failed to close rows after fetching current columns")
				}
			}()
			return scanCurrentColumnsChunk(rows, pks, cols, result)
		}()
		if scanErr != nil {
			return nil, scanErr
		}
	}

	return result, nil
}

// scanCurrentColumnsChunk drains one query's result rows into result, keyed
// by pk-tuple then column name. Split out of fetchCurrentColumns purely to
// keep the chunking loop's cyclomatic complexity down -- the scan loop
// carries its own branching (Scan error, per-column assembly) that doesn't
// need to share a stack frame with the WHERE-clause/query construction above.
func scanCurrentColumnsChunk(rows DBRows, pks, cols []string, result map[string]map[string]any) error {
	for rows.Next() {
		dest := make([]any, len(pks)+len(cols))
		vals := make([]any, len(pks)+len(cols))
		for i := range dest {
			dest[i] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("failed to scan current columns row: %w", err)
		}
		key := pkTupleKey(vals[:len(pks)])
		colVals := make(map[string]any, len(cols))
		for i, c := range cols {
			colVals[c] = vals[len(pks)+i]
		}
		result[key] = colVals
	}
	return rows.Err()
}

// executeSoftDeleteChunks issues one or more batched soft-delete UPDATEs for
// tuples sharing a single deletedAt value, chunked against maxPlaceholders
// (WS-4 item 3 / T1-14's chunker, mirrored here for the delete path). A
// tombstone for a row that never landed is a no-op UPDATE affecting 0 rows
// -- correct, logged at debug rather than treated as an error.
//
// Single-column PK tables (the common case) get the plan's literal
// `WHERE pk IN (?, ?, ...)` form. A composite PK (e.g. visitation_contacts,
// business_entity_contacts -- PIPE-OQ-4) has no portable single-IN
// equivalent, so those use `WHERE (pk1 = ? AND pk2 = ?) OR (...)` chunks
// instead; the placeholder-budget chunking applies identically either way.
func (s *DatabendSink) executeSoftDeleteChunks(
	ctx context.Context,
	table, quotedTable string,
	pks []string,
	deletedAt time.Time,
	tuples [][]any,
) error {
	if len(tuples) == 0 {
		return nil
	}

	maxPh := s.maxPlaceholders
	if maxPh <= 0 {
		maxPh = DefaultMaxPlaceholders
	}

	// Each row consumes len(pks) placeholders for its WHERE predicate, plus
	// one shared placeholder for the SET deleted_at = ? value (bound once
	// per statement, not once per row).
	chunkSize := pkWhereChunkSize(len(tuples), len(pks), maxPh, 1)

	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = quoteIdentifier(pk)
	}

	chunksEmitted := 0
	for start := 0; start < len(tuples); start += chunkSize {
		end := start + chunkSize
		if end > len(tuples) {
			end = len(tuples)
		}
		chunk := tuples[start:end]

		where, pkArgs := buildPKWhereClause(quotedPKs, chunk)
		args := make([]any, 0, 1+len(pkArgs))
		args = append(args, normalizeValue(deletedAt))
		args = append(args, pkArgs...)

		query := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s", quotedTable, quoteIdentifier(deletedAtColumn), where)

		log.Debug().
			Str("table", table).
			Str("query", query).
			Int("num_records", len(chunk)).
			Int("chunk", chunksEmitted).
			Msg("DatabendSink: Executing soft-delete UPDATE")

		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("soft-delete chunk %d failed: %w", chunksEmitted, classifyDDLError(table, err))
		}
		chunksEmitted++
	}

	if chunksEmitted > 0 {
		SinkChunksTotal.WithLabelValues(s.name, table).Add(float64(chunksEmitted))
	}
	return nil
}

// ensurePrimaryKey lazily resolves the primary key columns for a table. It
// performs at most one SHOW CREATE TABLE per table per process lifetime; after
// that, lookups are served from the in-memory cache.
func (s *DatabendSink) ensurePrimaryKey(ctx context.Context, ref protocol.TableRef) error {
	return s.refreshPrimaryKey(ctx, ref)
}

// ensurePKMetaTable creates the cdc_meta.pk_columns table if it does not
// already exist. Guarded by pkMetaEnsured so it is issued at most once per
// process lifetime -- CREATE TABLE IF NOT EXISTS is idempotent server-side
// regardless, but this avoids a redundant DDL round trip on every ApplySchema
// call.
//
// RUNBOOK NOTE (round-5 review LOW): with sink option auto_create_schema:
// false, ensureDatabase refuses to auto-provision cdc_meta and returns a
// permanent DDLError instead, so loadPKMetadata errors and
// refreshPrimaryKey falls through to SHOW CREATE TABLE (and, for a
// custom_objects table, on to the WS-4.6 hard error rather than a silently
// wrong PK -- which is correct, not a bug). But this means a deployment
// running with auto_create_schema: false MUST provision the cdc_meta
// database out-of-band before first use, or every custom_objects table's
// first write in every process lifetime pays the SHOW CREATE TABLE
// fallback and, until a schema_change has been applied, hard-errors. This
// is not automated by this change; document it in the deployment runbook.
func (s *DatabendSink) ensurePKMetaTable(ctx context.Context) error {
	s.pkMetaMu.Lock()
	defer s.pkMetaMu.Unlock()
	if s.pkMetaEnsured {
		return nil
	}

	if err := s.ensureDatabase(ctx, pkMetaDatabase); err != nil {
		return fmt.Errorf("failed to ensure %s database: %w", pkMetaDatabase, err)
	}

	quoted := quoteIdentifier(pkMetaDatabase) + "." + quoteIdentifier(pkMetaTable)
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (table_ref STRING, pk_columns STRING)",
		quoted,
	)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create %s.%s: %w", pkMetaDatabase, pkMetaTable, classifyDDLError(pkMetaDatabase+"."+pkMetaTable, err))
	}
	s.pkMetaEnsured = true
	return nil
}

// persistPKMetadata durably records ref's primary key columns in
// cdc_meta.pk_columns (WS-4.6), keyed by ref.String() -- the same identity
// used everywhere else in this file. REPLACE INTO ... ON (table_ref) makes
// this idempotent: a later ApplySchema for the same table (e.g. an
// incremental ADD COLUMN that still carries PKColumns) overwrites the row
// rather than duplicating it.
func (s *DatabendSink) persistPKMetadata(ctx context.Context, ref protocol.TableRef, pks []string) error {
	if err := s.ensurePKMetaTable(ctx); err != nil {
		return err
	}
	quoted := quoteIdentifier(pkMetaDatabase) + "." + quoteIdentifier(pkMetaTable)
	query := fmt.Sprintf(
		"REPLACE INTO %s (table_ref, pk_columns) ON (table_ref) VALUES (?, ?)",
		quoted,
	)
	if _, err := s.db.ExecContext(ctx, query, ref.String(), strings.Join(pks, ",")); err != nil {
		return fmt.Errorf("failed to persist pk metadata for %s: %w", ref.String(), err)
	}
	return nil
}

// loadPKMetadata reads back a table's primary key columns from
// cdc_meta.pk_columns. Returns (nil, false, nil) when no row exists for ref
// -- a genuinely missing entry, not an error, e.g. for a table synced before
// WS-4.6 landed or a direct-class table that never carried PKColumns.
func (s *DatabendSink) loadPKMetadata(ctx context.Context, ref protocol.TableRef) ([]string, bool, error) {
	if err := s.ensurePKMetaTable(ctx); err != nil {
		return nil, false, err
	}
	quoted := quoteIdentifier(pkMetaDatabase) + "." + quoteIdentifier(pkMetaTable)
	query := fmt.Sprintf("SELECT pk_columns FROM %s WHERE table_ref = ?", quoted)

	var joined string
	err := s.db.QueryRowScan(ctx, query, []any{ref.String()}, &joined)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if joined == "" {
		return nil, false, nil
	}
	return strings.Split(joined, ","), true, nil
}

// refreshPrimaryKey resolves the primary key columns for ref, durably first.
// WS-4.6: cdc_meta.pk_columns (persistPKMetadata's target) is consulted
// before SHOW CREATE TABLE and, when it holds an entry, is authoritative --
// it is never overridden by SHOW CREATE TABLE or the ["id"] default, which
// is exactly the "three sources race for pkCache" hazard the durability fix
// closes. SHOW CREATE TABLE remains as a fallback for tables synced before
// this metadata table existed. The pkLoaded gate ensures resolution runs at
// most once per table per process lifetime, even from concurrent goroutines.
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

	if pks, found, err := s.loadPKMetadata(ctx, ref); err != nil {
		// Metadata lookup itself failed (connectivity, etc.) -- release the
		// pkLoaded reservation so a later call can retry, and fall through to
		// the SHOW CREATE TABLE path below rather than giving up outright.
		s.pkMu.Lock()
		delete(s.pkLoaded, qualified)
		s.pkMu.Unlock()
		log.Warn().Err(err).Str("table", qualified).Msg("pk metadata lookup failed; falling back to SHOW CREATE TABLE")
	} else if found {
		s.pkMu.Lock()
		s.pkCache[qualified] = pks
		s.pkLoaded[qualified] = struct{}{}
		s.pkMu.Unlock()
		SinkPKResolved.WithLabelValues(s.name, qualified).Set(1)
		log.Info().Str("table", qualified).Strs("pks", pks).Msg("resolved primary key from durable pk metadata (post-restart safe)")
		return nil
	}

	// No durable metadata entry (found == false, err == nil): fall through
	// to SHOW CREATE TABLE. pkLoaded is already reserved for qualified from
	// the double-checked-locking block above -- do NOT re-check/re-set it
	// here, since it is only ever cleared on an actual failure (the
	// metadata-lookup-error branch above, or the SHOW CREATE TABLE failure
	// branch below), not on a clean "no entry found" miss.
	quotedTable := quoteQualified(ref)
	query := fmt.Sprintf("SHOW CREATE TABLE %s", quotedTable)

	var ddl string
	scanErr := s.db.QueryRowScan(ctx, query, nil, &ddl)

	// WS-4.6: a custom_objects table (generated custom table or sidecar)
	// with no PK resolvable from durable metadata nor SHOW CREATE TABLE
	// must NOT get the ["id"] default installed here -- that default is
	// exactly the silent-duplication hazard the durability fix closes for
	// non-id-keyed sidecars. Leave pkCache empty for it instead, so
	// uploadTableBatch/deleteTableBatch's own len(pks)==0 guard turns this
	// into a loud, non-silent error rather than a wrong write. Every other
	// table class keeps the legacy ["id"] default, unchanged.
	isCustomObjects := protocol.NormalizeSchema(ref.Schema) == "custom_objects"

	if scanErr != nil {
		s.pkMu.Lock()
		delete(s.pkLoaded, qualified)
		s.pkMu.Unlock()

		log.Warn().Err(scanErr).Str("table", qualified).Msg("SHOW CREATE TABLE failed; falling back to default PK")
		if !isCustomObjects {
			s.ensureFallbackPK(qualified)
		}
		SinkPKResolved.WithLabelValues(s.name, qualified).Set(0)
		return scanErr
	}

	pks := parsePKFromDDL(ddl)
	if len(pks) == 0 {
		log.Warn().Str("table", qualified).Str("ddl", ddl).Msg("no PRIMARY KEY clause found in SHOW CREATE TABLE; falling back")
		if !isCustomObjects {
			s.ensureFallbackPK(qualified)
		}
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

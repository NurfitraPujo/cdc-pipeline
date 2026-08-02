package nats

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
)

type NatsProtoTransformer struct {
	pipelineID string
	natsURL    string
	subject    string
	timeout    time.Duration
	schemas    []string
	tables     []string
	conn       *nats.Conn
	// maxPayload is the NATS-negotiated max payload for t.conn (WS-3), read
	// once at connect time via conn.MaxPayload() rather than hardcoded --
	// NATS' commonly-cited 1MB default is a server config, not a wire
	// constant, and a misconfigured guess would either reject legal batches
	// or admit ones that fail server-side.
	maxPayload int64
}

// chunkSafetyFraction is the fraction of maxPayload a chunk's encoded size
// budget targets (WS-3.2). proto.Size gives an exact wire size (unlike a
// guessed 80% margin for an approximate measurement), so the margin here
// only needs to cover the request's own non-record envelope bytes (already
// subtracted explicitly, see chunkRecords) and general safety slack -- 95%
// per the plan (PIPE-OQ-3: "resolved... target ~95%").
const chunkSafetyFraction = 0.95

// defaultMaxPayloadFallback is used only if conn.MaxPayload() reports <= 0
// (e.g. a fake/test connection), matching NATS' own commonly-shipped
// server default so a guard still exists rather than silently disabling
// chunking.
const defaultMaxPayloadFallback = 1024 * 1024

// DefaultTimeoutMs computes this transformer's default per-request timeout
// (WS-5 item 4) when its processor config sets no explicit timeout_ms:
// max(15000, 5*batchSize) milliseconds. WS-3 chunking means a single
// pipeline batch can fan out into up to batchSize serial requests in the
// worst case (one oversized record per chunk), so the default scales with
// batch size instead of staying flat regardless of how large a batch this
// transformer might have to process. batchSize <= 0 (unknown, e.g. a
// transformer built directly in a test) returns the floor.
//
// engine/factory.go's deriveAckWait uses this same function to size a
// sink's NATS AckWait around this transformer's worst-case per-batch wall
// clock, so the two must stay in agreement -- this is the single source of
// truth for "how long can one chunk legitimately take".
func DefaultTimeoutMs(batchSize int) int {
	const floorMs = 15000
	const perRecordMs = 5
	d := perRecordMs * batchSize
	if d < floorMs {
		return floorMs
	}
	return d
}

func NewNatsProtoTransformer(options map[string]interface{}) (transformer.Transformer, error) {
	natsURL, ok := options["nats_url"].(string)
	if !ok || natsURL == "" {
		return nil, fmt.Errorf("nats transformer requires 'nats_url' option")
	}

	subject, ok := options["subject"].(string)
	if !ok || subject == "" {
		return nil, fmt.Errorf("nats transformer requires 'subject' option")
	}

	pipelineID, _ := options["pipeline_id"].(string)

	// WS-5 item 4: the flat 5000ms default did not scale with how large a
	// batch this transformer might have to process. WS-3 chunking can fan
	// a single pipeline batch out into up to BatchSize serial requests
	// (worst case: one oversized record per chunk) all sharing this same
	// per-request timeout, so a 5s timeout against a batch of any real
	// size left almost no margin before the very first chunk could time
	// out under normal, non-degraded latency. batch_size is plumbed in by
	// the factory (engine/factory.go) alongside pipeline_id; a
	// transformer constructed directly in a test without it simply gets
	// DefaultTimeoutMs(0), which is the floor.
	batchSize := 0
	if bs, ok := options["batch_size"].(float64); ok && bs > 0 {
		batchSize = int(bs)
	}
	timeoutMs := DefaultTimeoutMs(batchSize)

	// WS-8 item 3: config values arrive JSON-decoded (numbers as float64,
	// arrays as []interface{}), so a scalar like `schemas: "custom_objects"`
	// or a stringly-typed `timeout_ms: "5000"` silently type-asserts to the
	// zero value / empty filter instead of erroring. Log loudly rather than
	// degrade silently, so a misconfigured processor is visible in the logs
	// even though it does not fail the type assertion outright.
	if raw, exists := options["timeout_ms"]; exists {
		if to, ok := raw.(float64); ok {
			timeoutMs = int(to)
		} else {
			log.Printf("WARNING: nats/protobuf transformer option 'timeout_ms' has unexpected type %T (value %v); falling back to default %dms", raw, raw, timeoutMs)
		}
	}

	schemas := decodeStringList("schemas", options)
	tables := decodeStringList("tables", options)

	// WS-8 item 4: an unfiltered nats/protobuf instance sends every table in
	// the pipeline to daya-core, where all of them fail metadata lookup --
	// require an explicit filter at construction time instead of letting it
	// start and silently misbehave.
	if len(schemas) == 0 && len(tables) == 0 {
		return nil, fmt.Errorf("nats/protobuf transformer requires at least one of 'schemas' or 'tables' to be set; an unfiltered instance would forward every table to the responder")
	}

	// WS-5 item 5: an unnamed connection with no reconnect policy previously
	// meant a NATS blip that outlasted the client's default reconnect
	// attempts (finite by default) simply never came back for the
	// lifetime of the pipeline process -- every subsequent record for this
	// processor fails forever, indistinguishable from daya-core being
	// permanently down. nats.Name identifies this connection on the NATS
	// server side (`nats server list connections` / monitoring) for
	// exactly this transformer instance; MaxReconnects(-1) means "retry
	// forever" instead of giving up.
	connName := fmt.Sprintf("cdc-pipeline-transformer-nats-protobuf-%s", pipelineID)
	conn, err := nats.Connect(natsURL,
		nats.Name(connName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	maxPayload := conn.MaxPayload()
	if maxPayload <= 0 {
		maxPayload = defaultMaxPayloadFallback
	}

	return &NatsProtoTransformer{
		pipelineID: pipelineID,
		natsURL:    natsURL,
		subject:    subject,
		timeout:    time.Duration(timeoutMs) * time.Millisecond,
		schemas:    schemas,
		tables:     tables,
		conn:       conn,
		maxPayload: maxPayload,
	}, nil
}

// decodeStringList reads a `[]interface{}` of strings out of options[key],
// logging loudly (rather than silently degrading to "no filter") when the
// option is present but not shaped as a list, or contains a non-string
// element -- both are easy JSON-config mistakes (e.g. `schemas: "public"`
// instead of `schemas: ["public"]`) that used to type-assert away quietly.
func decodeStringList(key string, options map[string]interface{}) []string {
	raw, exists := options[key]
	if !exists {
		return nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		log.Printf("WARNING: nats/protobuf transformer option '%s' has unexpected type %T (value %v); expected a list of strings, treating as unset", key, raw, raw)
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		str, ok := v.(string)
		if !ok {
			log.Printf("WARNING: nats/protobuf transformer option '%s' contains a non-string element %v (%T); skipping it", key, v, v)
			continue
		}
		out = append(out, str)
	}
	return out
}

func (t *NatsProtoTransformer) Name() string {
	return "nats/protobuf"
}

// Transform is the single-message fallback used by the engine only for
// transformers that do not implement BatchTransformer (internal/engine/
// consumer.go:~200-222); NatsProtoTransformer implements TransformBatch, so
// in production this path is never taken. It still needs to behave
// correctly rather than being a landmine for any future direct caller: on
// error it must fail closed (drop, do not forward untransformed data) to
// stay consistent with TransformBatch/doTransform, which always returns an
// error rather than passing matching rows through unchanged (WS-10).
// Previously this returned (m, true, err) -- "keep the original message" --
// on error, which contradicted that fail-closed batch behaviour.
func (t *NatsProtoTransformer) Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error) {
	transformed, err := t.TransformBatch(ctx, []protocol.Message{*m})
	if err != nil {
		return nil, false, err
	}
	if len(transformed) == 0 {
		return nil, false, nil
	}
	return &transformed[0], true, nil
}

// transformOutcome values for the transform_records_total metric (WS-9).
const (
	outcomeTransformed = "transformed"
	outcomePassthrough = "passthrough"
	outcomeDropped     = "dropped"  // responder returned Success:true, Keep:false
	outcomeRejected    = "rejected" // responder returned Success:false, Retryable:false (malformed record)
	outcomeFailed      = "failed"
)

func (t *NatsProtoTransformer) TransformBatch(ctx context.Context, msgs []protocol.Message) ([]protocol.Message, error) {
	matchingIndices := make([]int, 0)
	for i, m := range msgs {
		if t.matchesFilter(m) {
			matchingIndices = append(matchingIndices, i)
		}
	}

	passthroughCount := len(msgs) - len(matchingIndices)
	if passthroughCount > 0 {
		metrics.TransformRecordsTotal.WithLabelValues(t.pipelineID, t.Name(), outcomePassthrough).Add(float64(passthroughCount))
	}

	if len(matchingIndices) == 0 {
		return msgs, nil
	}

	matching := make([]protocol.Message, 0, len(matchingIndices))
	for _, idx := range matchingIndices {
		matching = append(matching, msgs[idx])
	}

	start := time.Now()
	result, err := t.doTransform(ctx, msgs, matchingIndices, matching)
	metrics.TransformDurationSeconds.WithLabelValues(t.pipelineID, t.Name()).Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.TransformRequestsTotal.WithLabelValues(t.pipelineID, t.Name(), "error").Inc()
		metrics.TransformRecordsTotal.WithLabelValues(t.pipelineID, t.Name(), outcomeFailed).Add(float64(len(matching)))
		return nil, err
	}
	metrics.TransformRequestsTotal.WithLabelValues(t.pipelineID, t.Name(), "success").Inc()

	return result, nil
}

// doTransform is the part of TransformBatch that actually round-trips to the
// responder, split out so TransformBatch can wrap it uniformly with the
// duration/outcome metrics above regardless of which step fails.
func (t *NatsProtoTransformer) doTransform(ctx context.Context, msgs []protocol.Message, matchingIndices []int, matching []protocol.Message) ([]protocol.Message, error) {
	req, err := t.buildTransformRequest(matching)
	if err != nil {
		return nil, fmt.Errorf("failed to build transform request: %w", err)
	}

	// WS-3: chunk by encoded size, not record count. A wide batch (the
	// shipped example config's BatchSize:2000, or any batch with many/large
	// columns) can exceed the NATS payload limit deterministically -- every
	// retry fails identically, so the batch DLQs. proto.Size gives the exact
	// wire size, so this never needs to guess.
	chunks := t.chunkRequest(req)
	if len(chunks) > 1 {
		log.Printf("INFO: nats/protobuf transformer chunked a %d-record batch into %d requests (max_payload=%d bytes)", len(matching), len(chunks), t.maxPayload)
	}
	metrics.TransformChunksPerBatch.WithLabelValues(t.pipelineID, t.Name()).Observe(float64(len(chunks)))

	resp, err := t.sendChunks(ctx, chunks)
	if err != nil {
		return nil, fmt.Errorf("failed to send transform request: %w", err)
	}

	results, err := t.parseResponseWithOrder(matching, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transform response: %w", err)
	}

	result := make([]protocol.Message, 0, len(msgs))
	matchIdx := 0
	transformedCount := 0
	droppedCount := 0
	rejectedCount := 0
	for i, m := range msgs {
		if matchIdx < len(matchingIndices) && i == matchingIndices[matchIdx] {
			r := results[matchIdx]
			if r.msg != nil {
				result = append(result, *r.msg)
				transformedCount++
			} else if r.rejected {
				// Already logged with its failure reason in
				// parseResponseWithOrder; counted separately from a clean
				// Keep:false drop so a malformed-record rate is
				// distinguishable from a deliberate-drop rate.
				rejectedCount++
			} else {
				log.Printf("WARNING: matched record dropped by transformer: UUID=%s", m.UUID)
				droppedCount++
			}
			matchIdx++
		} else {
			result = append(result, m)
		}
	}
	if transformedCount > 0 {
		metrics.TransformRecordsTotal.WithLabelValues(t.pipelineID, t.Name(), outcomeTransformed).Add(float64(transformedCount))
	}
	if droppedCount > 0 {
		metrics.TransformRecordsTotal.WithLabelValues(t.pipelineID, t.Name(), outcomeDropped).Add(float64(droppedCount))
	}
	if rejectedCount > 0 {
		metrics.TransformRecordsTotal.WithLabelValues(t.pipelineID, t.Name(), outcomeRejected).Add(float64(rejectedCount))
	}

	return result, nil
}

func (t *NatsProtoTransformer) matchesFilter(m protocol.Message) bool {
	// WS-1 (schema filter, "the silent showstopper"): the schema of a
	// data-plane message (insert/update/delete/snapshot) lives in the sibling
	// field m.TableSchema, not m.Schema. m.Schema (*protocol.SchemaMetadata)
	// is populated only for OpSchemaChange events, so a filter that read
	// m.Schema.Schema for ordinary rows always fell into the "no schema"
	// branch and either matched everything (when "" was configured) or
	// nothing (any real schema name) -- the filter never actually filtered.
	// See internal/protocol/message.go and MULTI_SCHEMA_PLAN.md §2.2.
	schemaMatch := len(t.schemas) > 0 && t.matchesSchema(m)
	tableMatch := len(t.tables) > 0 && t.matchesTable(m)

	// WS-1B: when both schemas and tables are configured, they OR together,
	// not AND. This processor carries a third replication class beyond
	// "transformed by schema" -- public.visitations, which is transformed
	// for the checked_in/checked_out enrichment (PIPE-OQ-4) despite living
	// in the public schema alongside untransformed built-ins. Configuring
	// schemas:["custom_objects"] plus tables:["visitations"] must admit
	// *both* custom_objects rows (any table) and visitations rows
	// (regardless of schema) -- not only rows that are simultaneously in
	// schema "custom_objects" and named "visitations", which is empty by
	// construction and would silently drop every visitations row. When only
	// one of the two options is set, behaviour is unchanged from before:
	// it alone gates the match.
	switch {
	case len(t.schemas) > 0 && len(t.tables) > 0:
		return schemaMatch || tableMatch
	case len(t.schemas) > 0:
		return schemaMatch
	case len(t.tables) > 0:
		return tableMatch
	default:
		return true
	}
}

func (t *NatsProtoTransformer) matchesSchema(m protocol.Message) bool {
	schema := protocol.NormalizeSchema(m.TableSchema)
	for _, s := range t.schemas {
		if schema == protocol.NormalizeSchema(s) {
			return true
		}
	}
	return false
}

func (t *NatsProtoTransformer) matchesTable(m protocol.Message) bool {
	for _, tbl := range t.tables {
		if m.Table == tbl {
			return true
		}
	}
	return false
}

func (t *NatsProtoTransformer) buildTransformRequest(msgs []protocol.Message) (*cdctransformv1.TransformRequest, error) {
	records := make([]*cdctransformv1.TransformRecord, 0, len(msgs))

	for _, m := range msgs {
		var data map[string]*cdctransformv1.TypedValue
		if m.Data != nil {
			data = make(map[string]*cdctransformv1.TypedValue, len(m.Data))
			for k, v := range m.Data {
				data[k] = encodeTypedValue(v, m.ColumnKinds[k])
			}
		}

		var schemaMeta *cdctransformv1.SchemaMetadata
		if m.Schema != nil {
			schemaMeta = &cdctransformv1.SchemaMetadata{
				Table:     m.Schema.Table,
				Schema:    m.Schema.Schema,
				Columns:   m.Schema.Columns,
				PkColumns: m.Schema.PKColumns,
			}
		}

		var ts *timestamppb.Timestamp
		if !m.Timestamp.IsZero() {
			ts = timestamppb.New(m.Timestamp)
		}

		// WS-1.2: TransformRecord.schema carries the *sibling* schema of the
		// row itself -- protocol.Message.TableSchema (empty normalises to
		// "public") -- not m.Schema, which is the structured DDL metadata
		// populated only for OpSchemaChange events and is nil for every data
		// row. Populating this from m.Schema is why daya-core has always
		// seen an empty schema for insert/update/delete/snapshot records.
		records = append(records, &cdctransformv1.TransformRecord{
			SourceId:       m.SourceID,
			SinkId:         m.SinkID,
			Table:          m.Table,
			Schema:         protocol.NormalizeSchema(m.TableSchema),
			Op:             string(m.Op),
			Lsn:            m.LSN,
			Pk:             m.PK,
			Uuid:           m.UUID,
			Data:           data,
			Timestamp:      ts,
			SchemaMetadata: schemaMeta,
		})
	}

	return &cdctransformv1.TransformRequest{
		PipelineId: t.pipelineID,
		Records:    records,
	}, nil
}

// chunkRequest splits req.Records into one or more TransformRequests, each
// sized to stay within t.maxPayload * chunkSafetyFraction bytes of encoded
// wire size (WS-3.2). Uses proto.Size(rec), not incremental proto.Marshal of
// a growing batch -- re-marshalling to measure would be O(n^2) (roughly
// 1000x the cost of a single marshal for a 2000-record batch), burned inside
// the request timeout, for no benefit: proto.Size walks the message
// computing the exact wire size without allocating or serialising.
//
// Each record contributes recordWireSize(rec) = tag + varint-length-prefix +
// payload bytes to the running total, exactly matching how proto.Marshal
// encodes a repeated embedded message field. The running total is seeded
// with the exact size of the "envelope" -- everything in the request other
// than the records themselves (currently just pipeline_id) -- so the budget
// reflects the real wire size of the request that will actually be sent, not
// just the sum of the records.
//
// A lone record whose own wire size already exceeds the budget still gets
// its own single-record chunk rather than being dropped or erroring here --
// the responder's own WS-3.3 "response too large" signal is the correct
// place to reject a record that is unsendable even alone.
func (t *NatsProtoTransformer) chunkRequest(req *cdctransformv1.TransformRequest) []*cdctransformv1.TransformRequest {
	if len(req.Records) == 0 {
		return []*cdctransformv1.TransformRequest{req}
	}

	budget := int64(float64(t.maxPayload) * chunkSafetyFraction)
	if budget <= 0 {
		budget = 1
	}

	envelope := &cdctransformv1.TransformRequest{PipelineId: req.PipelineId}
	envelopeSize := int64(proto.Size(envelope))

	var chunks []*cdctransformv1.TransformRequest
	var current []*cdctransformv1.TransformRecord
	currentSize := envelopeSize

	flush := func() {
		if len(current) == 0 {
			return
		}
		chunks = append(chunks, &cdctransformv1.TransformRequest{
			PipelineId: req.PipelineId,
			Records:    current,
		})
		current = nil
		currentSize = envelopeSize
	}

	for _, rec := range req.Records {
		recSize := recordWireSize(rec)
		// If adding this record would exceed the budget AND the current
		// chunk already has at least one record, flush first -- a single
		// oversized record still gets its own chunk (see doc comment) rather
		// than being force-fit or dropped.
		if len(current) > 0 && currentSize+recSize > budget {
			flush()
		}
		current = append(current, rec)
		currentSize += recSize
	}
	flush()

	if len(chunks) == 0 {
		// Defensive: req.Records was non-empty but nothing got chunked (should
		// be unreachable given the loop above always appends). Fall back to
		// the whole request as a single chunk rather than losing records.
		return []*cdctransformv1.TransformRequest{req}
	}
	return chunks
}

// recordWireSize returns the number of bytes rec contributes to its parent
// message's wire encoding as a repeated embedded-message field: a 1-byte tag
// (valid for TransformRequest.records, field number 2, well under the
// 1-byte-tag ceiling of field 15) + the varint length prefix + the payload
// itself.
func recordWireSize(rec *cdctransformv1.TransformRecord) int64 {
	size := proto.Size(rec)
	return 1 + int64(protowire.SizeVarint(uint64(size))) + int64(size) //nolint:gosec // proto.Size never returns negative; conversion to uint64 is safe
}

// sendChunks sends every chunk concurrently (WS-3.2: "chunks go out
// concurrently, not serially -- N chunks x a 5s serial timeout is exactly
// what pushes a batch past AckWait") and reassembles their Results back into
// chunk order, then record order within each chunk, so the merged response
// lines up 1:1 with the original (unchunked) matching slice that
// parseResponseWithOrder expects. A transport error on any chunk fails the
// whole call -- matching the documented semantic that a chunk failure fails
// the whole batch, since flushWithFilter acks wmMsgs atomically for the
// batch and redelivery re-transforms every chunk regardless.
func (t *NatsProtoTransformer) sendChunks(ctx context.Context, chunks []*cdctransformv1.TransformRequest) (*cdctransformv1.TransformResponse, error) {
	if len(chunks) == 1 {
		return t.sendRequest(ctx, chunks[0])
	}

	responses := make([]*cdctransformv1.TransformResponse, len(chunks))
	g, gCtx := errgroup.WithContext(ctx)
	for i, chunk := range chunks {
		i, chunk := i, chunk
		g.Go(func() error {
			resp, err := t.sendRequest(gCtx, chunk)
			if err != nil {
				return fmt.Errorf("chunk %d/%d failed: %w", i+1, len(chunks), err)
			}
			responses[i] = resp
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	merged := &cdctransformv1.TransformResponse{}
	for _, resp := range responses {
		merged.Results = append(merged.Results, resp.Results...)
	}
	return merged, nil
}

func (t *NatsProtoTransformer) sendRequest(ctx context.Context, req proto.Message) (*cdctransformv1.TransformResponse, error) {
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}
	metrics.TransformRequestBytes.WithLabelValues(t.pipelineID, t.Name()).Observe(float64(len(reqBytes)))

	reqCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	msg, err := t.conn.RequestWithContext(reqCtx, t.subject, reqBytes)
	if err != nil {
		return nil, fmt.Errorf("NATS request failed: %w", err)
	}
	metrics.TransformResponseBytes.WithLabelValues(t.pipelineID, t.Name()).Observe(float64(len(msg.Data)))

	var resp cdctransformv1.TransformResponse
	if err := proto.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &resp, nil
}

// transformedResult holds a transformed message or nil if dropped.
type transformedResult struct {
	msg *protocol.Message
	// rejected is true when msg is nil because daya-core returned a
	// non-retryable failure (the record is malformed), as opposed to a
	// clean Keep=false drop (the responder chose not to keep it). Kept
	// separate so the two are counted under distinct metric outcomes rather
	// than both landing on "dropped" -- a malformed-record rate and a
	// deliberate-drop rate are different signals to alert on.
	rejected bool
}

func (t *NatsProtoTransformer) parseResponseWithOrder(msgs []protocol.Message, resp *cdctransformv1.TransformResponse) ([]transformedResult, error) {
	if len(resp.Results) != len(msgs) {
		return nil, fmt.Errorf("response result count (%d) does not match request count (%d)", len(resp.Results), len(msgs))
	}

	results := make([]transformedResult, len(msgs))
	for i, res := range resp.Results {
		if !res.Success {
			// WS-0/WS-4: retryable distinguishes "daya-core is degraded, try
			// this batch again" (propagate the error so the caller retries)
			// from "this record is malformed, drop it" (do not poison the
			// rest of the batch over one bad record).
			//
			// WS-2 item 3: a schema_change record is the one exception to
			// "malformed, drop it". Dropping a schema_change is not a lost
			// row -- the consumer's ApplySchema is skipped entirely
			// (consumer.go's "Schema change filtered out by transformer"
			// path), leaving the table permanently missing a column, after
			// which every subsequent write of that column fails or silently
			// omits it. So treat any non-success on an OpSchemaChange record
			// as retryable regardless of the wire retryable flag, and never
			// route it to the per-record drop/DLQ path.
			if res.Retryable || msgs[i].Op == protocol.OpSchemaChange {
				return nil, fmt.Errorf("transform failed for record %d (retryable=%v, op=%s): %s", i, res.Retryable, msgs[i].Op, res.Error)
			}
			log.Printf("WARNING: record %d dropped by transformer, non-retryable failure: UUID=%s reason=%s", i, msgs[i].UUID, res.Error)
			results[i] = transformedResult{msg: nil, rejected: true}
			continue
		}

		if !res.Keep {
			// Mark as dropped (nil msg indicates drop)
			results[i] = transformedResult{msg: nil}
			continue
		}

		original := msgs[i]
		if res.TransformedData != nil {
			// Rebuild ColumnKinds from scratch (not merged with the
			// pre-transform kinds) -- the transformed data is a new set of
			// columns/values the responder chose, so a stale kind entry
			// from before the transform could misroute a column the
			// responder repurposed for something else.
			original.Data, original.ColumnKinds = decodeTypedValueMap(res.TransformedData)
		}
		// If TransformedData is nil (a legal pure-filter response that only
		// sets Keep), original.Data and original.ColumnKinds are left as
		// the request's own values -- correct, since nothing about the
		// columns changed.
		if res.TransformedSchema != nil {
			original.Schema = &protocol.SchemaMetadata{
				Table:     res.TransformedSchema.Table,
				Schema:    res.TransformedSchema.Schema,
				Columns:   res.TransformedSchema.Columns,
				PKColumns: res.TransformedSchema.PkColumns,
			}
		}
		results[i] = transformedResult{msg: &original}
	}

	return results, nil
}

func (t *NatsProtoTransformer) Close() error {
	if t.conn != nil {
		t.conn.Close()
	}
	return nil
}

func (t *NatsProtoTransformer) Stop() error {
	return t.Close()
}

func sanitizeMapForStructPB(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	res := make(map[string]interface{}, len(m))
	for k, v := range m {
		res[k] = sanitizeValueForStructPB(v)
	}
	return res
}

func sanitizeValueForStructPB(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case *time.Time:
		if val == nil {
			return nil
		}
		return val.Format(time.RFC3339Nano)
	case uuid.UUID:
		return val.String()
	case *uuid.UUID:
		if val == nil {
			return nil
		}
		return val.String()
	case []byte:
		if len(val) == 16 {
			if u, err := uuid.FromBytes(val); err == nil {
				return u.String()
			}
		}
		return base64.StdEncoding.EncodeToString(val)
	case map[string]interface{}:
		return sanitizeMapForStructPB(val)
	}

	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Map:
		res := make(map[string]interface{}, val.Len())
		for _, key := range val.MapKeys() {
			kStr := fmt.Sprintf("%v", key.Interface())
			res[kStr] = sanitizeValueForStructPB(val.MapIndex(key).Interface())
		}
		return res
	case reflect.Slice, reflect.Array:
		res := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			res[i] = sanitizeValueForStructPB(val.Index(i).Interface())
		}
		return res
	case reflect.Struct:
		if stringer, ok := v.(fmt.Stringer); ok {
			return stringer.String()
		}
		return fmt.Sprintf("%v", v)
	default:
		return v
	}
}

// encodeTypedValue is the pipeline-side half of WS-0: it builds the
// map<string, TypedValue> that replaces google.protobuf.Struct for
// TransformRecord.data. structpb.Struct's only numeric representation is
// float64, which cannot carry an int64 above 2^53 or an exact decimal --
// TypedValue's oneof preserves the source representation instead of
// collapsing everything through a float.
//
// This encoder is deliberately generic: it dispatches on the Go types that
// pgx's TextFormatCode codecs actually decode WAL tuples into (see
// internal/vendor/go-pq-cdc/pq/message/tuple/data.go), not on daya-core's
// custom-field type system -- every custom_objects column is CITEXT on the
// wire regardless of its logical field type (WS-7 note in the companion
// plan), so it always arrives here as a plain string and is carried as
// string_value verbatim. daya-core's cdcvalue.Parse (WS-2) is what recovers
// the richer type from that string.
//
// kind is the optional protocol.Message.ColumnKinds hint for this value
// (currently only protocol.ColumnKindDecimal, or "" for none). A NUMERIC
// column's pgtype.Numeric cannot itself cross the internal NATS JetStream
// transport (it's a struct; msgpack's WriteIntf reflection fallback only
// supports Ptr/Slice/Map) and is collapsed to a plain, unmarked, exact
// decimal-text string by internal/source/postgres/source.go:sanitizeValue
// -- identical to what the string_value path would otherwise produce.
// ColumnKinds is the out-of-band signal that recovers the decimal_value
// routing here without requiring the Data value itself to carry any
// transformer-private encoding (an earlier revision tried an in-band NUL
// marker on the string; rejected on review because every consumer of
// Data -- both sinks, transformer/builtin.go, the delete-PK path -- reads
// it unconditionally and none of them know about a marker convention).
//nolint:gocyclo // exhaustive type switch over every Go type pgx/msgpack can hand back; splitting it up would scatter the dispatch across helpers without reducing real complexity.
func encodeTypedValue(v interface{}, kind string) *cdctransformv1.TypedValue {
	if v == nil {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
	}

	if kind == protocol.ColumnKindDecimal {
		if s, ok := v.(string); ok {
			return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_DecimalValue{DecimalValue: s}}
		}
		// Unexpected: a decimal kind hint on a non-string value. Fall
		// through to the generic switch below rather than silently
		// dropping the hint or panicking -- an untyped value should still
		// get *some* correct routing.
	}

	switch val := v.(type) {
	case string:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val}}
	case bool:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_BoolValue{BoolValue: val}}
	case int:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case int8:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case int16:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case int32:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case int64:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: val}}
	case uint:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}} //nolint:gosec // uint is not used for any WAL/pgx-decoded column value in this pipeline; no observed source can produce one above math.MaxInt64
	case uint8:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case uint16:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case uint32:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}}
	case uint64:
		// Genuine truncation risk: a uint64 above math.MaxInt64 would wrap to
		// a negative IntValue. TypedValue has no uint64 oneof kind, so route
		// values that don't fit into StringValue (exact decimal text)
		// instead of silently corrupting them -- mirrors the decimal_value
		// fallback-to-string-value pattern used elsewhere in this encoder.
		if val > math.MaxInt64 {
			return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: strconv.FormatUint(val, 10)}}
		}
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_IntValue{IntValue: int64(val)}} //nolint:gosec // guarded above: val <= math.MaxInt64
	case float32:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_DoubleValue{DoubleValue: float64(val)}}
	case float64:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_DoubleValue{DoubleValue: val}}
	case time.Time:
		// KNOWN LIMITATION, explicit decision (not silent): pgx's DateCodec
		// decodes a genuine `date` column to the exact same Go type,
		// time.Time (pgtype.Date.Time), as timestamp/timestamptz do. This
		// generic, column-type-blind encoder has no signal to tell "this
		// came from a `date` column" apart from "this came from a
		// `timestamp[tz]` column" -- both arrive here as time.Time -- so it
		// cannot honor the normative table's date -> date_value (YYYY-MM-DD)
		// row; every time.Time becomes timestamp_value. This is a real type
		// demotion for a genuine passthrough `date` column (e.g.
		// public.visitations, a built-in sidecar) but not for
		// custom_objects fields, whose `date` columns are CITEXT and arrive
		// as plain strings, not time.Time, and are unaffected. If a real
		// `date` passthrough column needs date_value fidelity, the caller
		// must supply column-type metadata to this encoder (e.g. from
		// SchemaMetadata.Columns) and branch on it before falling into this
		// generic type switch -- not attempted here since protocol.Message
		// does not carry that context down to individual values today.
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_TimestampValue{TimestampValue: timestamppb.New(val)}}
	case *time.Time:
		if val == nil {
			return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_TimestampValue{TimestampValue: timestamppb.New(*val)}}
	case uuid.UUID:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val.String()}}
	case *uuid.UUID:
		if val == nil {
			return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val.String()}}
	case []byte:
		if len(val) == 16 {
			if u, err := uuid.FromBytes(val); err == nil {
				return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: u.String()}}
			}
		}
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: base64.StdEncoding.EncodeToString(val)}}
	case [16]byte:
		// pgtype.UUIDCodec.DecodeValue's TextFormatCode path returns
		// pgtype.UUID.Bytes, which is [16]byte -- NOT []byte and NOT
		// google/uuid.UUID -- this is the type a real WAL uuid column
		// decodes to (internal/vendor/go-pq-cdc/pq/message/tuple/data.go:99).
		// Without this case, [16]byte falls through to the reflect.Array
		// branch below and becomes a json_value array of 16 small integers.
		// NOTE on reachability: the real postgres source path
		// (internal/source/postgres/source.go:sanitizeValue) already
		// converts [16]byte to this exact string form *before*
		// encodeTypedValue runs, because the value must survive the
		// internal NATS JetStream msgpack transport hop first (msgpack has
		// no case for a fixed-size array and would hard-fail encoding it).
		// So through the shipped pipeline this case is dead; it is
		// reachable, tested and correct for any direct caller of
		// encodeTypedValue (see TestEncodeTypedValue_RealPgtypeCodecDecode),
		// and is the right behavior if that transport constraint is ever
		// lifted or a different source feeds this encoder directly.
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: uuid.UUID(val).String()}}
	case pgtype.Numeric:
		// pgtype.Numeric is NOT a fmt.Stringer (it has no String() method)
		// -- do not route this through a generic Stringer branch, it will
		// silently miss and dump the struct via %v
		// ("{12345678 -4 false finite true}") into a DECIMAL column.
		// Value() renders the exact text form the codec would itself write
		// on the wire (TextFormatCode), never through a float.
		// NOTE on reachability: same caveat as [16]byte above -- a raw
		// pgtype.Numeric is a struct, which also cannot cross the msgpack
		// transport hop, so sanitizeValue converts it to a plain (unmarked)
		// decimal-text string *before* it reaches this switch, alongside a
		// protocol.ColumnKindDecimal entry in ColumnKinds. The real
		// decimal_value routing for the shipped pipeline happens via the
		// `kind` parameter check at the top of this function, not this
		// case. This case is exercised directly by
		// TestEncodeTypedValue_RealPgtypeCodecDecode and is the correct
		// behavior for any caller that hands this encoder an
		// already-decoded pgtype.Numeric without going through
		// sanitizeValue's transport-safety conversion.
		return encodeNumeric(val)
	case *netip.Prefix:
		if val == nil {
			return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val.String()}}
	case netip.Prefix:
		// inet/cidr decode to netip.Prefix, which implements Stringer --
		// explicitly string_value, never allowed to fall into a generic
		// Stringer-to-decimal branch (that misrouting was blocker 2).
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val.String()}}
	case netip.Addr:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val.String()}}
	case net.HardwareAddr:
		// macaddr/macaddr8 decode to net.HardwareAddr, also a Stringer --
		// same rule, explicit string_value.
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: val.String()}}
	case pgtype.Interval:
		// pgtype.Interval has no interval_value oneof kind to preserve, so
		// (unlike pgtype.Numeric) there is no routing distinction worth
		// protecting across the msgpack transport hop -- its own
		// driver.Valuer.Value() is already the exact canonical Postgres
		// text form ("1 mon 2 day 00:00:00"), so sanitizeValue lets it
		// collapse via the generic driver.Valuer path upstream, same as
		// timestamptz/date/etc. This case exists for direct callers of
		// encodeTypedValue (e.g. the codec test, or a future caller not
		// behind sanitizeValue) and mirrors Value()'s own text exactly,
		// rather than a hand-rolled, non-Postgres-syntax format.
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: intervalText(val)}}
	case pgtype.Bits:
		// Same reasoning as Interval: no bits_value kind, Value() is
		// already the canonical "0101..." text, sanitizeValue lets the
		// generic driver.Valuer path handle it in the real pipeline. Kept
		// here for direct callers.
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: bitsText(val)}}
	}

	// No fmt.Stringer fallback here on purpose: pgtype.Numeric proves a
	// Stringer-shaped branch cannot be trusted to mean "this is a decimal",
	// and netip.Prefix/net.HardwareAddr prove it cannot even be trusted to
	// mean "safe to treat generically" -- both are legitimate Stringers that
	// must land in string_value, not a decimal or a generic string picked by
	// accident of interface satisfaction. Every case that needs a specific
	// TypedValue kind is listed explicitly above; everything else is a
	// plain string via %v below. A closed switch with a string default,
	// never a decimal one.
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		sanitized := sanitizeValueForStructPB(v)
		b, err := json.Marshal(sanitized)
		if err != nil {
			return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
		}
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_JsonValue{JsonValue: string(b)}}
	default:
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

// encodeNumeric renders a pgtype.Numeric as the exact decimal text the
// TextFormatCode codec would itself produce -- never via float64. Value()
// drives NumericCodec's own text encoder, so "1500.50" stays "1500.50", not
// 1500.5 or a binary-float approximation. NaN and the two infinities are not
// decimal numbers, so they fall back to string_value rather than being
// forced into decimal_value.
func encodeNumeric(n pgtype.Numeric) *cdctransformv1.TypedValue {
	if !n.Valid {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
	}
	if n.NaN {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "NaN"}}
	}
	if n.InfinityModifier == pgtype.Infinity {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "Infinity"}}
	}
	if n.InfinityModifier == pgtype.NegativeInfinity {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "-Infinity"}}
	}
	dv, err := n.Value()
	if err != nil {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: fmt.Sprintf("%v", n)}}
	}
	s, ok := dv.(string)
	if !ok {
		return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: fmt.Sprintf("%v", dv)}}
	}
	return &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_DecimalValue{DecimalValue: s}}
}

// encodeBits renders a pgtype.Bits (bit/varbit) as its canonical "0101..."
// bit-string text form.
// intervalText renders a pgtype.Interval via its own driver.Valuer, which
// is the codec's exact TextFormatCode encoding ("1 mon 2 day 00:00:00") --
// real Postgres interval syntax, not a hand-rolled approximation.
func intervalText(val pgtype.Interval) string {
	v, err := val.Value()
	if err != nil || v == nil {
		return fmt.Sprintf("%v", val)
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// bitsText renders a pgtype.Bits via its own driver.Valuer, which is the
// codec's exact canonical "0101..." text encoding.
func bitsText(val pgtype.Bits) string {
	v, err := val.Value()
	if err != nil || v == nil {
		return fmt.Sprintf("%v", val)
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// decodeTypedValueMap is the inverse of encodeTypedValue's callers: it turns
// a TransformRecordResult.transformed_data map back into the plain
// map[string]interface{} that the rest of the pipeline (sinks, debug output,
// existing tests) already knows how to handle. It is intentionally lossy in
// the same direction sanitizeValueForStructPB always was -- everything comes
// back as a Go primitive, never as a *TypedValue -- because nothing
// downstream of the transformer understands the wire type.
// decodeTypedValueMap turns a TransformRecordResult.transformed_data map
// back into the plain map[string]interface{} the rest of the pipeline
// (sinks, debug output, existing tests) already knows how to handle, plus
// a protocol.Message.ColumnKinds-shaped side-channel for any entry whose
// TypedValue kind needs a routing hint to survive a further hop -- HIGH
// finding from review: without this, a decimal_value round-tripped through
// the transformer decodes to a plain Go string with the "this is a decimal"
// signal gone, so a chained second nats/protobuf processor (or any other
// future re-encode of the same Message) would see an ordinary string and
// silently emit string_value on its own turn. Returning the kinds map here,
// for the caller to attach to protocol.Message.ColumnKinds, keeps that
// signal alive across arbitrarily many hops the same way it survives the
// first one.
func decodeTypedValueMap(m map[string]*cdctransformv1.TypedValue) (map[string]interface{}, map[string]string) {
	if m == nil {
		return nil, nil
	}
	res := make(map[string]interface{}, len(m))
	var kinds map[string]string
	for k, v := range m {
		val, kind := decodeTypedValue(v)
		res[k] = val
		if kind != "" {
			if kinds == nil {
				kinds = make(map[string]string, len(m))
			}
			kinds[k] = kind
		}
	}
	return res, kinds
}

// decodeTypedValue returns the plain Go value for v, and a
// protocol.Message.ColumnKinds hint ("" for none) for callers that need to
// preserve routing information that the plain value alone can't carry
// (currently only DecimalValue -> protocol.ColumnKindDecimal).
func decodeTypedValue(v *cdctransformv1.TypedValue) (interface{}, string) {
	if v == nil {
		return nil, ""
	}
	switch kind := v.Kind.(type) {
	case *cdctransformv1.TypedValue_NullValue:
		return nil, ""
	case *cdctransformv1.TypedValue_StringValue:
		return kind.StringValue, ""
	case *cdctransformv1.TypedValue_IntValue:
		return kind.IntValue, ""
	case *cdctransformv1.TypedValue_DecimalValue:
		return kind.DecimalValue, protocol.ColumnKindDecimal
	case *cdctransformv1.TypedValue_DoubleValue:
		return kind.DoubleValue, ""
	case *cdctransformv1.TypedValue_BoolValue:
		return kind.BoolValue, ""
	case *cdctransformv1.TypedValue_TimestampValue:
		if kind.TimestampValue == nil {
			return nil, ""
		}
		return kind.TimestampValue.AsTime(), ""
	case *cdctransformv1.TypedValue_DateValue:
		return kind.DateValue, ""
	case *cdctransformv1.TypedValue_JsonValue:
		var out interface{}
		if err := json.Unmarshal([]byte(kind.JsonValue), &out); err != nil {
			return kind.JsonValue, ""
		}
		return out, ""
	default:
		return nil, ""
	}
}

func init() {
	transformer.RegisterTransformer("nats/protobuf", NewNatsProtoTransformer)
}

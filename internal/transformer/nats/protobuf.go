package nats

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/gen/go/cdc/transform/v1"
)

type NatsProtoTransformer struct {
	natsURL string
	subject string
	timeout time.Duration
	schemas []string
	tables  []string
	conn    *nats.Conn
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

	timeoutMs := 5000
	if to, ok := options["timeout_ms"].(float64); ok {
		timeoutMs = int(to)
	}

	schemasRaw, _ := options["schemas"].([]interface{})
	schemas := make([]string, 0, len(schemasRaw))
	for _, s := range schemasRaw {
		if str, ok := s.(string); ok {
			schemas = append(schemas, str)
		}
	}

	tablesRaw, _ := options["tables"].([]interface{})
	tables := make([]string, 0, len(tablesRaw))
	for _, t := range tablesRaw {
		if str, ok := t.(string); ok {
			tables = append(tables, str)
		}
	}

	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return &NatsProtoTransformer{
		natsURL: natsURL,
		subject: subject,
		timeout: time.Duration(timeoutMs) * time.Millisecond,
		schemas: schemas,
		tables:  tables,
		conn:    conn,
	}, nil
}

func (t *NatsProtoTransformer) Name() string {
	return "nats/protobuf"
}

func (t *NatsProtoTransformer) Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error) {
	transformed, err := t.TransformBatch(ctx, []protocol.Message{*m})
	if err != nil {
		return m, true, err
	}
	if len(transformed) == 0 {
		return nil, false, nil
	}
	return &transformed[0], true, nil
}

func (t *NatsProtoTransformer) TransformBatch(ctx context.Context, msgs []protocol.Message) ([]protocol.Message, error) {
	matchingIndices := make([]int, 0)
	for i, m := range msgs {
		if t.matchesFilter(m) {
			matchingIndices = append(matchingIndices, i)
		}
	}

	if len(matchingIndices) == 0 {
		return msgs, nil
	}

	matching := make([]protocol.Message, 0, len(matchingIndices))
	for _, idx := range matchingIndices {
		matching = append(matching, msgs[idx])
	}

	req, err := t.buildTransformRequest(matching)
	if err != nil {
		return nil, fmt.Errorf("failed to build transform request: %w", err)
	}

	resp, err := t.sendRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send transform request: %w", err)
	}

	results, err := t.parseResponseWithOrder(matching, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transform response: %w", err)
	}

	result := make([]protocol.Message, 0, len(msgs))
	matchIdx := 0
	for i, m := range msgs {
		if matchIdx < len(matchingIndices) && i == matchingIndices[matchIdx] {
			if results[matchIdx].msg != nil {
				result = append(result, *results[matchIdx].msg)
			} else {
				log.Printf("WARNING: matched record dropped by transformer: UUID=%s", m.UUID)
			}
			matchIdx++
		} else {
			result = append(result, m)
		}
	}

	return result, nil
}

func (t *NatsProtoTransformer) filterMessages(msgs []protocol.Message) (matching []protocol.Message, passthrough []protocol.Message) {
	for _, m := range msgs {
		if t.matchesFilter(m) {
			matching = append(matching, m)
		} else {
			passthrough = append(passthrough, m)
		}
	}
	return
}

func (t *NatsProtoTransformer) matchesFilter(m protocol.Message) bool {
	if len(t.schemas) > 0 {
		found := false
		for _, s := range t.schemas {
			if m.Schema != nil && m.Schema.Schema == s {
				found = true
				break
			}
			if m.Schema == nil && s == "" {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(t.tables) > 0 {
		found := false
		for _, tbl := range t.tables {
			if m.Table == tbl {
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

func (t *NatsProtoTransformer) buildTransformRequest(msgs []protocol.Message) (*cdctransformv1.TransformRequest, error) {
	records := make([]*cdctransformv1.TransformRecord, 0, len(msgs))

	for _, m := range msgs {
		var data *structpb.Struct
		if m.Data != nil {
			var err error
			sanitized := sanitizeMapForStructPB(m.Data)
			data, err = structpb.NewStruct(sanitized)
			if err != nil {
				return nil, fmt.Errorf("failed to convert data to Struct: %w", err)
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

		schemaStr := ""
		if m.Schema != nil {
			schemaStr = m.Schema.Schema
		}

		records = append(records, &cdctransformv1.TransformRecord{
			SourceId:       m.SourceID,
			SinkId:         m.SinkID,
			Table:          m.Table,
			Schema:         schemaStr,
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
		PipelineId: "",
		Records:    records,
	}, nil
}

func (t *NatsProtoTransformer) sendRequest(ctx context.Context, req proto.Message) (*cdctransformv1.TransformResponse, error) {
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	msg, err := t.conn.RequestWithContext(reqCtx, t.subject, reqBytes)
	if err != nil {
		return nil, fmt.Errorf("NATS request failed: %w", err)
	}

	var resp cdctransformv1.TransformResponse
	if err := proto.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &resp, nil
}

// transformedResult holds a transformed message or nil if dropped.
type transformedResult struct {
	msg *protocol.Message
}

func (t *NatsProtoTransformer) parseResponseWithOrder(msgs []protocol.Message, resp *cdctransformv1.TransformResponse) ([]transformedResult, error) {
	if len(resp.Results) != len(msgs) {
		return nil, fmt.Errorf("response result count (%d) does not match request count (%d)", len(resp.Results), len(msgs))
	}

	results := make([]transformedResult, len(msgs))
	for i, res := range resp.Results {
		if !res.Success {
			return nil, fmt.Errorf("transform failed for record %d: %s", i, res.Error)
		}

		if !res.Keep {
			// Mark as dropped (nil msg indicates drop)
			results[i] = transformedResult{msg: nil}
			continue
		}

		original := msgs[i]
		if res.TransformedData != nil {
			original.Data = res.TransformedData.AsMap()
		}
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

func init() {
	transformer.RegisterTransformer("nats/protobuf", NewNatsProtoTransformer)
}

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

// MaskTransformer hashes specific fields in the message data.
type MaskTransformer struct {
	fields []string
	salt   string
}

func NewMaskTransformer(options map[string]interface{}) (Transformer, error) {
	fieldsRaw, ok := options["fields"]
	if !ok {
		return nil, fmt.Errorf("mask transformer requires 'fields' option")
	}

	fieldsSlice, ok := fieldsRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("'fields' option must be a list of strings")
	}

	fields := make([]string, 0, len(fieldsSlice))
	for _, f := range fieldsSlice {
		if s, ok := f.(string); ok {
			fields = append(fields, s)
		}
	}

	salt, _ := options["salt"].(string)

	return &MaskTransformer{
		fields: fields,
		salt:   salt,
	}, nil
}

func (t *MaskTransformer) Name() string {
	return "mask"
}

func (t *MaskTransformer) Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error) {
	if m.Data == nil {
		return m, true, nil
	}

	for _, field := range t.fields {
		if val, ok := m.Data[field]; ok {
			if strVal, ok := val.(string); ok {
				hash := sha256.Sum256([]byte(strVal + t.salt))
				m.Data[field] = hex.EncodeToString(hash[:])
			}
		}
	}

	return m, true, nil
}

func init() {
	RegisterTransformer("mask", NewMaskTransformer)
}

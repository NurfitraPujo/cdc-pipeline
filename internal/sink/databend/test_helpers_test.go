package databend

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

// jsonMarshal is a thin wrapper used by tests so they can construct payloads
// without re-importing encoding/json everywhere.
func jsonMarshal(v map[string]any) ([]byte, error) {
	return json.Marshal(v)
}

// jsonUnmarshal is the symmetric decode helper used by tests.
func jsonUnmarshal(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
}

// msgpackMarshal encodes a value as MessagePack so tests can verify the
// "msgpack first, json fallback" decode path.
func msgpackMarshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

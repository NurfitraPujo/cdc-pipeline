package protocol

import (
	"encoding/json"
)

// StateValue is a KV *state* struct: one that msgp generates codec methods for
// and that ADR-0017 assigns to MessagePack (checkpoints, table stats, table
// metadata). Control-plane *config* values are deliberately JSON and must not
// go through these helpers -- see docs/decisions/0017-msgpack-for-state-json-for-config.md.
type StateValue interface {
	MarshalMsg(b []byte) ([]byte, error)
	UnmarshalMsg(b []byte) ([]byte, error)
}

// MarshalState encodes a state value for KV. Every writer of a state key must
// use this rather than calling json.Marshal or MarshalMsg directly: the defect
// this replaces was one key written by three call sites in two encodings, where
// whichever writer ran last decided whether restore succeeded.
func MarshalState(v StateValue) ([]byte, error) {
	return v.MarshalMsg(nil)
}

// UnmarshalState decodes a state value written by MarshalState.
//
// It also accepts JSON, because deployments that ran before the encodings were
// unified still hold JSON-written stats under keys this now reads with msgp.
// Sniffing is unambiguous: msgp encodes these structs as a map, whose leading
// byte is a fixmap (0x80-0x8f), map16 (0xde) or map32 (0xdf) -- never '{'.
//
// Unlike the call sites it replaces, a decode failure here is returned, not
// swallowed. The old `if err := json.Unmarshal(...); err == nil` guards dropped
// every msgp-encoded entry without a log, an error or a metric.
func UnmarshalState(data []byte, v StateValue) error {
	if looksLikeJSON(data) {
		return json.Unmarshal(data, v)
	}
	_, err := v.UnmarshalMsg(data)
	return err
}

// looksLikeJSON reports whether data begins with a JSON object. Leading
// whitespace is skipped because encoding/json tolerates it.
func looksLikeJSON(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

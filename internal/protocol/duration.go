package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Duration is a time.Duration that marshals to JSON as a Go duration string
// ("5s", "100ms") instead of a bare nanosecond integer.
//
// Why this exists: every duration field in the config structs is a plain
// time.Duration, which encoding/json renders as an int64 nanosecond count.
// The published contract (docs/openapi.yaml, and the `swaggertype:"string"`
// struct tags) has always claimed these are strings, so the spec and the wire
// disagreed on ten fields. The frontend believed the spec: it rendered raw
// nanosecond integers into duration text boxes, and every duration it sent
// back was rejected with a 400 because encoding/json cannot unmarshal a string
// into a time.Duration.
//
// Duration is deliberately NOT used as the field type on the config structs.
// Those fields are read at 600+ call sites that treat them as time.Duration;
// retyping them would force a cast at every one. Instead the containing
// structs implement MarshalJSON/UnmarshalJSON and convert through Duration at
// the boundary (see config.go). The Go-facing types are unchanged.
type Duration time.Duration

// MarshalJSON renders the duration as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts either a duration string ("5s") or a bare nanosecond
// number.
//
// The numeric form is the legacy on-disk representation: pipeline, source,
// sink and global configs are persisted to NATS KV as JSON (see
// internal/config/manager.go and internal/api/handler.go), so every config
// written before this type existed holds integers. Rejecting them would brick
// an existing deployment on upgrade. Numbers are accepted on read forever;
// only writes are normalised to strings.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	switch v := raw.(type) {
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", v, err)
		}
		*d = Duration(parsed)
		return nil
	case float64:
		// Legacy nanosecond integer. json decodes all numbers as float64;
		// durations that large are not representable exactly, but the values
		// in play here (seconds to minutes) are far below 2^53 ns.
		*d = Duration(time.Duration(v))
		return nil
	case nil:
		*d = 0
		return nil
	default:
		return errors.New("duration must be a string or a number")
	}
}

// String makes Duration printable in error messages and logs.
func (d Duration) String() string { return time.Duration(d).String() }

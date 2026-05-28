# NATS Protobuf Transformer Interface & Sanitization Contracts

This document specifies the exact data types and serialization conventions used by the `nats/protobuf` transformer when mapping database logical records into Protobuf payloads.

Domain microservices acting as NATS Responders on the configured subject **MUST** adhere to these type contracts.

---

## Data Type Mappings

To support dynamic PostgreSQL database structures without requiring Go CDC worker redeployments, the `nats/protobuf` transformer utilizes `google.protobuf.Struct` for record payloads. 

Because `google.protobuf.Struct` only supports standard JSON primitives, the CDC engine recursively sanitizes native database and driver types using the following conventions:

| PostgreSQL Data Type | Go CDC Representation | Protobuf `google.protobuf.Struct` Field | Format / Example |
| :--- | :--- | :--- | :--- |
| **BOOLEAN** | `bool` | `BoolValue` | `true` or `false` |
| **NUMERIC / SMALLINT / INTEGER / BIGINT / REAL / DOUBLE** | `float64` / `int64` | `NumberValue` | `12345.67` |
| **TEXT / VARCHAR** | `string` | `StringValue` | `"some text"` |
| **TIMESTAMP / TIMESTAMPTZ / DATE** | `time.Time` | `StringValue` (RFC3339 formatted) | `"2023-01-01T12:00:00Z"` |
| **UUID** | `[]byte` (16 bytes) or `uuid.UUID` | `StringValue` (Standard UUID string) | `"550e8400-e29b-41d4-a716-446655440000"` |
| **BYTEA (Binary)** | `[]byte` (length != 16) | `StringValue` (Base64 encoded string) | `"3q2+7w=="` |
| **ARRAYS (e.g. `INTEGER[]`, `TEXT[]`)** | `[]T` (concrete slice) | `ListValue` (containing values) | `[1.0, 2.0, 3.0]` |
| **JSON / JSONB** | `map[string]interface{}` | `StructValue` (nested map) | `{"key": "value"}` |
| **CUSTOM STRUCTS (e.g. pgtypes)** | Custom struct | `StringValue` (String representation) | `"12345"` (parsed via `.String()`) |

---

## Guidelines for NATS Responders (External Services)

### 1. Parsing UUIDs
If the responder is acting on a table that has a UUID column (e.g. `user_id`), it will receive a standard **hex-hyphenated UUID string** (e.g. `"550e8400-e29b-41d4-a716-446655440000"`). The responder does *not* need to decode base64.

### 2. Parsing Binary (`BYTEA`)
If the responder is acting on a binary payload column (e.g. `c_bytea`), the value is transmitted as a **standard Base64-encoded string**. The responder **MUST** decode the Base64 payload to retrieve the raw byte slice.

### 3. Returning Transformed Values
When the responder replies with `TransformRecordResult`, the mutated data inside `transformed_data` **MUST** also adhere to JSON-safe types. If the responder wants to modify binary columns, it should return them as Base64 strings.

# TODO: Fix Lossy Decimal and Array Type Mapping in Databend Sink

## Context
The Databend Analytical Sink ingests data from PostgreSQL databases and replicates schema configurations to Databend analytical tables.

## The Problem (Threat)
1. High-precision decimal fields (such as `numeric` and `decimal` in PostgreSQL, often used for financial tracking) are mapped directly to Databend `FLOAT64` fields. This incurs floating-point rounding errors and loss of precision during replication.
2. PostgreSQL array data types (e.g., `integer[]` or `text[]`) are matched using substring matching rules, causing them to fall back to scalar types (like `INT64` or `VARCHAR`). When structured array payloads are inserted into these fields, the sink throws driver errors and fails to ingest the records.

## Action Items
- [ ] Implement robust type resolution in `mapPgTypeToDatabend` to map PostgreSQL `decimal` and `numeric` to Databend `DECIMAL` types.
- [ ] Map PostgreSQL array types (ending in `[]`) to Databend `ARRAY` or `VARIANT` types.
- [ ] Update sink deserialization logic to serialize Go slice data into JSON strings or arrays suitable for Databend `VARIANT` or `ARRAY` ingestion.
- [ ] Write integration unit tests verifying no precision loss on decimal mappings.

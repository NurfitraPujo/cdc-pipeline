# Existing E2E Testing Plan

This document outlines the currently implemented End-to-End (E2E) testing scenarios for the CDC Data Pipeline.

## 1. Initial Snapshot (`TestE2E_InitialSnapshot`)
**GIVEN** a source PostgreSQL database with pre-existing data in tracked tables
**WHEN** the CDC pipeline is initialized for the first time
**THEN** the pipeline should perform an initial snapshot and all pre-existing rows should be successfully written to the Databend sink.

## 2. Live CDC (`TestE2E_LiveCDC`)
**GIVEN** an active CDC pipeline tracking specific PostgreSQL tables
**WHEN** new `INSERT`, `UPDATE`, and `DELETE` operations are executed on the source tables
**THEN** these changes must be captured, processed, and accurately reflected in the Databend sink in near real-time.

## 3. Dynamic Discovery (`TestE2E_DynamicDiscovery`)
**GIVEN** a running CDC pipeline with a configured publication
**WHEN** a new table is created in PostgreSQL and added to the publication
**THEN** the pipeline must dynamically discover the new table, perform a chunked snapshot of any existing data, and seamlessly transition to real-time CDC without dropping any in-flight transactions (Production Chaos testing).

## 4. Schema Evolution (`TestE2E_SchemaEvolution`)
**GIVEN** an active CDC pipeline processing data
**WHEN** a DDL operation (e.g., `ALTER TABLE ADD COLUMN`) is executed on a tracked PostgreSQL table
**THEN** the pipeline must pause CDC for that table, orchestrate a safe schema evolution at the Databend sink via NATS KV CAS fencing, and resume CDC with the new schema structure.

## 5. Dead Letter Queue (`TestE2E_DLQ`)
**GIVEN** an active CDC pipeline
**WHEN** a "poison-pill" message (e.g., malformed data that fails validation at the sink) is encountered repeatedly
**THEN** the pipeline should switch to isolation mode, isolate the failing message, and route it to a designated NATS Dead Letter Queue (DLQ) topic without halting the entire pipeline.

## 6. PostgreSQL Data Types (`TestE2E_PostgresTypes`)
**GIVEN** an active CDC pipeline
**WHEN** rows containing various complex PostgreSQL data types (e.g., JSONB, arrays, timestamps, numerics) are inserted
**THEN** the pipeline must accurately serialize, deserialize, and map these types to their correct Databend equivalents without data loss or truncation.

## 7. Provider Lifecycle (`TestProviderLifecycle`)
**GIVEN** a running pipeline engine
**WHEN** lifecycle transition commands (Start, Drain, Stop) are issued
**THEN** the underlying Source, Sink, and Stream providers must cleanly allocate and release resources, adhering strictly to the Cancel->Sleep->Close protocol.

## 8. Multi-Sink & Sink Isolation (`TestE2E_MultiSink_Debug`, `TestE2E_SinkIsolation`)
**GIVEN** a CDC pipeline configured with multiple heterogeneous sinks (e.g., Databend and a Debug logger)
**WHEN** data is processed through the pipeline, or one sink encounters a transient failure
**THEN** data must be written to all sinks, and a failure in one sink must not disrupt the processing flow or checkpointing of the other healthy sinks.

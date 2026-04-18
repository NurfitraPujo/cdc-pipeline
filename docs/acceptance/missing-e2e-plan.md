# Missing E2E Testing Scenarios

This document details the critical missing End-to-End (E2E) testing scenarios identified for the CDC Data Pipeline. These scenarios focus on stability, fault tolerance, and edge cases under load.

## Priority 1: Graceful Shutdown Under Load (CRITICAL)
**Description:** Validates the "Drain → Shutdown → Restart" zero-downtime protocol under heavy traffic.
**GIVEN** a CDC pipeline actively processing thousands of messages per second
**WHEN** a shutdown signal (SIGTERM) or configuration reload is triggered mid-stream
**THEN** the pipeline must:
- Successfully complete the drain phase by processing all messages buffered up to the drain marker.
- Guarantee zero message loss during the shutdown process.
- Guarantee exactly-once delivery semantics (no duplicates) when the pipeline is subsequently restarted.

## Priority 2: Checkpoint/Offset Resume (CRITICAL)
**Description:** Validates consumer restart behavior and accurate LSN resumption.
**GIVEN** a consumer processing a large batch of messages from NATS
**WHEN** the consumer process is forcibly killed (SIGKILL) mid-batch before the egress LSN is checkpointed
**THEN** upon restart, the consumer must:
- Resume from the last successfully persisted egress LSN.
- Re-process the interrupted batch without skipping any messages.
- Rely on the sink's idempotency (UPSERT/REPLACE) to ensure the sink state remains consistent.

## Priority 3: NATS Reconnection After Partition
**Description:** Validates pipeline resilience to message broker connectivity loss.
**GIVEN** an active CDC pipeline processing data
**WHEN** the network connection to the NATS JetStream cluster is severed, then restored after a delay
**THEN** the pipeline must:
- Trigger circuit breakers and safely pause publishing/consuming.
- Automatically reconnect to NATS once the partition is resolved.
- Resume message processing and redelivery from the exact point of failure without data loss.

## Priority 4: Backpressure Scenario
**Description:** Validates system behavior when the producer outpaces the consumer.
**GIVEN** a CDC pipeline where the PostgreSQL source generates data much faster than the Databend sink can ingest
**WHEN** the NATS JetStream buffers begin to fill up and the consumer falls significantly behind
**THEN** the pipeline must:
- Properly apply backpressure to the PostgreSQL replication slot.
- Ensure the JetStream buffer does not exceed its configured limits.
- Allow the consumer to eventually catch up without memory exhaustion or message dropping.

## Priority 5: Concurrent Schema Evolution
**Description:** Validates handling of complex and concurrent DDL operations.
**GIVEN** an active CDC pipeline
**WHEN** multiple DDL operations are executed simultaneously (e.g., `ALTER` on multiple tables, `DROP COLUMN`, `RENAME COLUMN` while data is in-flight)
**THEN** the pipeline must:
- Safely queue or isolate the schema evolution state machine for each affected table.
- Use NATS KV CAS fencing to prevent split-brain issues during concurrent updates.
- Apply the DDL changes to the sink sequentially and correctly resume CDC for all tables.

## Priority 6: Exactly-Once / Duplication Testing
**Description:** Validates that retries do not result in duplicated data at the sink.
**GIVEN** an active CDC pipeline processing inserts
**WHEN** transient failures force the pipeline to retry processing specific batches or restart entirely
**THEN** the sink database must not contain duplicated records, proving that idempotent operations (`UPSERT` / `REPLACE INTO`) are functioning correctly across restarts.

## Priority 7: Timezone/Date Boundary Edge Cases
**Description:** Validates data integrity for tricky temporal values.
**GIVEN** an active CDC pipeline
**WHEN** records containing timestamps across Daylight Saving Time (DST) transitions, Unix epoch boundaries, or leap-year boundaries are inserted
**THEN** the data must be accurately replicated to Databend without timezone shifting, truncation, or conversion errors.

## Priority 8: Large Payload Handling
**Description:** Validates pipeline limits with unusually large records.
**GIVEN** an active CDC pipeline
**WHEN** rows containing large payloads (e.g., Multi-Megabyte `TEXT` or `BYTEA` columns, deeply nested `JSONB`, or arrays with thousands of elements) are inserted
**THEN** the pipeline must successfully serialize, chunk (if necessary), transmit over NATS, and insert the large payload into Databend without OOM errors or timeouts.

## Priority 9: Multi-Table Transactions
**Description:** Validates the preservation of relational integrity during transactional processing.
**GIVEN** an active CDC pipeline tracking multiple related tables (e.g., Orders and OrderItems)
**WHEN** a PostgreSQL transaction spanning multiple tables is committed, or partially executed and then rolled back
**THEN** the pipeline must:
- Ensure that rolled-back transactions are never written to the sink.
- Ensure that records committed together in PostgreSQL are eventually consistent in the sink, maintaining referential integrity.

## Priority 10: Config Hot-Reload
**Description:** Validates dynamic updates to the pipeline configuration.
**GIVEN** a running CDC pipeline
**WHEN** the configuration is dynamically updated in the NATS KV store (e.g., adding a new tracked table or changing a transformer rule) without restarting the worker process
**THEN** the pipeline's ConfigManager must detect the change, execute a graceful two-phase reload (Drain -> Shutdown -> Restart), and apply the new configuration seamlessly.

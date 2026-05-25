# TODO: Implement PostgreSQL WAL Monitoring and Alerting

## Context
The CDC pipeline reads from a PostgreSQL logical replication slot. 

## The Problem (Threat)
If the pipeline goes down for an extended period, or if the consumer cannot keep up with the producer, the unacknowledged LSNs will cause PostgreSQL to retain WAL (Write-Ahead Log) files indefinitely. This will eventually fill up the PostgreSQL server's disk space, leading to a catastrophic database outage.

## Action Items
- [ ] Add explicit Prometheus metrics for PostgreSQL replication slot size/lag (e.g., measuring the distance between the current LSN and the confirmed flush LSN).
- [ ] Surface these WAL retention metrics in the Control Plane dashboard.
- [ ] Implement configurable threshold alerts (e.g., warning if WAL size exceeds 50GB) to notify users before their source database disk fills up.

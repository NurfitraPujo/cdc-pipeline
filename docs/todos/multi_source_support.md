# TODO: Complete Multi-Source Pipeline Support

## Context
The pipeline configuration currently includes a `Sources []string` field, suggesting support for multiple data sources per pipeline.

## The Problem
The documentation (`internal/engine/AGENT.md`) states that this field is designed for future use, and currently, only `Sources[0]` is utilized. This half-implemented feature can confuse users and complicate the code architecture without providing actual value.

## Action Items
- [ ] **Decision Required**: Decide whether to fully implement multi-source support or remove it.
- [ ] **Option A (Implement)**: Update the pipeline orchestrator to handle N:M topologies, ensuring that LSN checkpointing and schema evolution state machines function correctly across multiple independent sources.
- [ ] **Option B (Remove)**: Refactor the configuration schema to replace `Sources []string` with a single `Source string` field, simplifying the API and reducing ambiguity. Update all relevant documentation to reflect a 1:N (one source to many sinks) architecture.

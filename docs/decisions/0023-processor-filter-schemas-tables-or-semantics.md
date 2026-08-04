---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers (Opus validation review)
consulted: internal/transformer/nats/protobuf.go (matchesFilter), plans/cdc_custom_object_transform_remediation.md (WS-1B), summaries/ws1b_ws2b_ws4c_ws10_implementation.md
---

# A processor's `schemas` and `tables` filters OR together, not AND

## Context and Problem Statement

`NatsProtoTransformer.matchesFilter` gates which messages a processor (e.g.
`nats/protobuf`) transforms, using two independent option lists: `schemas`
and `tables`. Before this change, when both were configured, a message had
to satisfy **both** to match.

WS-1B requires a single `nats/protobuf` processor to transform two different
replication classes at once: every table in the `custom_objects` schema
(generated custom tables and built-in sidecars), **and** the `public`-schema
`visitations` table specifically (enriched with `checked_in`/`checked_out`,
PIPE-OQ-4), while leaving every other `public` built-in
(`master_contacts`, `business_entities`, `visitation_contacts`) untransformed
by construction.

Expressed under the old AND semantics, `schemas: ["custom_objects"]` +
`tables: ["visitations"]` matches nothing: no message is simultaneously in
schema `custom_objects` and named `visitations`. Every `visitations` row
would silently fall through to the passthrough path — untransformed,
un-alerted, indistinguishable from a correctly configured pipeline until
someone checked the warehouse and found `checked_in`/`checked_out` missing.

## Decision

When both `schemas` and `tables` are configured on the same processor, a
message matches if it satisfies **either** filter. A processor configured
with only one of the two is unaffected — that filter alone still gates the
match, exactly as before.

## Considered Options

* **Keep AND.** Rejected: it cannot express WS-1B's actual requirement at
  all — there is no way to write "custom_objects schema, plus one extra
  named table from a different schema" as an AND of two independent lists.
  The two-replication-classes shape this pipeline now needs to support is
  not a corner case; it is the shape described in the plan's WS-1B section
  as a structural property of the domain (built-in shared tables vs.
  per-company custom/sidecar tables).
* **A third, explicit option (e.g. `extra_tables` or `include`) instead of
  changing `tables`'s semantics.** Rejected as unnecessary complexity: the
  existing two options already cover the two axes (schema-wide, specific
  table) that WS-1B needs; adding a third option to avoid changing what OR
  vs AND means is more surface area for the same expressiveness.
* **OR the two filters (chosen).** Matches the plan's explicit requirement
  ("make sure WS-1's filter change ORs the two rather than ANDing them")
  and needs no new config surface — just a semantic fix to how the two
  existing options combine.

## Consequences

* Good: the two-replication-classes shape WS-1B requires is now
  expressible in one processor's config, matching the plan verbatim.
* Good: a single-filter processor (only `schemas` or only `tables` set,
  which is every configuration this pipeline has shipped with historically)
  is byte-for-byte unaffected — this is additive, not a breaking change to
  narrower configs.
* **Bad — over-match consequence, the unavoidable cost of OR:**
  `schemas: ["public"]` + `tables: ["orders"]` now admits **every** table in
  the `public` schema, not only `public.orders`. Pre-fix, the (broken) AND
  semantics happened to behave like an intersection-narrowing filter for
  configs that combined a broad schema with a specific table name — an
  operator relying on that behavior (even accidentally, since it never
  worked as documented) to *narrow* a schema-wide match down to one table
  will now see every table in that schema instead. There is no way to keep
  both "OR admits the two-replication-classes case" and "AND narrows a
  schema to one table" with the same two option lists — they are opposite
  operations. An operator who wants schema-wide-minus-narrowing must
  currently express it via `schemas` alone (accepting the whole schema) or
  `tables` alone (naming every table wanted, foregoing the schema
  shorthand); there is no combined "schema AND table" narrowing available
  after this change.
* Neutral: this is a behavior-only fix, not a new config field — no
  `config.example.yaml` schema change, only a documented usage example
  showing the WS-1B pattern.

## More Information

`internal/transformer/nats/protobuf_test.go`'s
`TestMatchesFilter_SchemasAndTablesOR` pins both the WS-1B admit case and
the single-filter-unaffected case. See
`summaries/ws1b_ws2b_ws4c_ws10_implementation.md` (WS-1B section) for the <!-- hygiene:ignore -->
verification method (disable-and-confirm-fails against real code, not just
the assertion).

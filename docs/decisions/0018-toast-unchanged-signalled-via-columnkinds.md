---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers
consulted: plans/cdc_custom_object_transform_remediation.md (WS-7), internal/vendor/go-pq-cdc/PATCHES.md (WS7-1), docs/decisions/0019-columnkinds-typed-side-channel.md, summaries/ws5_ws6_ws7_implementation.md
---

# An unchanged TOASTed column is signalled out-of-band via `ColumnKinds`, never inferred from absence in `Data`

## Context and Problem Statement

Under PostgreSQL's `REPLICA IDENTITY DEFAULT` with an unchanged replica-identity key, an UPDATE's
WAL tuple omits any column that is TOASTed (out-of-line storage, roughly >2KB) *and* unchanged —
Postgres sends a TOAST-pointer marker instead of a value, and (per `format.Update.decode`'s
existing backfill, which only fires when an old tuple is present) there is no fallback to recover
it from in this specific case. `Data.DecodeWithColumn` previously gave such a column no entry in
its output map at all — the exact same shape as a genuine NULL, which explicitly gets
`decoded[colName] = nil`.

Downstream, both this pipeline's Databend sink (`uploadTableBatch`'s `REPLACE INTO`, which derives
its column list from `Data`'s own keys) and daya-core's custom-object upsert (WS-4.5's
column-set-collapse null-fill) treat "column absent from the payload" as license to write
NULL/default for it. For a genuinely-unsent column that is correct. For an unchanged TOASTed
column it silently truncates the column's real value on every unrelated-field update — the exact
failure this decision exists to close, for a CITEXT-backed custom-object field that can legitimately
exceed the TOAST threshold.

The fix therefore needs two things: (1) the pipeline must be able to tell "no value because
unchanged TOAST" apart from "no value because NULL" internally, and (2) that distinction must
survive the trip across process/language boundaries to daya-core, since WS-4.5's null-fill logic
lives there, not in this repo.

## Decision Drivers

* A real NULL and an omitted-because-unchanged-TOAST column must remain distinguishable at every
  hop, not just where they are first decoded — the bug this closes is specifically about that
  distinction getting lost between the WAL decode and the final write.
* daya-core's WS-4.5 depends on this signal to know which absent columns to omit from its write
  (partial UPDATE) versus which to null-fill; this is a cross-repo contract, not an internal
  implementation detail, so whatever carries it must survive (de)serialization unchanged for a
  consumer that has not been updated to look for it.
* Custom-object CITEXT columns are the reachable case in practice (TOASTed above ~2KB); this must
  not depend on daya-core shipping before the pipeline, or vice versa.

## Considered Options

1. **Signal out-of-band**, via a side-channel keyed by column name alongside `Data`
   (`protocol.Message.ColumnKinds`), leaving `Data` itself byte-identical to today for every column
   that isn't affected.
2. **Signal in-band**, by encoding a marker into the value that would have occupied `Data[col]`
   (e.g. a reserved sentinel string, or a NUL-prefixed byte marker). See
   [0019](0019-columnkinds-typed-side-channel.md) for why an in-band marker was tried first and
   specifically rejected — this ADR assumes that rejection and focuses on the narrower "where does
   the *TOAST* signal live" question.
3. **Always populate `Data[col]` with the column's current value**, by having the pipeline itself
   read it back from the source (a live query against Postgres) before emitting the message, so no
   downstream consumer ever needs to know the difference.
4. **Do nothing pipeline-side**; require every sink/consumer to independently detect and handle a
   missing column by re-reading from its own store, on the theory that "don't write a column you
   don't have a value for" is discoverable without an explicit signal.

## Decision Outcome

Chosen: **option 1**. `protocol.ColumnKindToastedUnchanged` (`"toasted_unchanged"`) is a new value
in the pre-existing `ColumnKinds map[string]string` side-channel
(`internal/protocol/message.go`), reusing the same mechanism WS-0/WS-1 introduced for
`ColumnKindDecimal` rather than inventing a second one. `source/postgres/source.go`'s `buildMessage`
sets `ColumnKinds[col] = ColumnKindToastedUnchanged` for exactly the columns
`format.Update.NewToastedColumns` names (see `internal/vendor/go-pq-cdc/PATCHES.md`'s WS7-1 entry
for the decode-layer half of this fix); `Data` itself is never touched — the column simply stays
absent from it, which was already correct behavior for "no value to write."

**The contract**: a column named in `ColumnKinds` with value `"toasted_unchanged"` is guaranteed
absent from `Data` because Postgres elided it, never because it is NULL. A consumer doing a
wholesale row replace (this repo's `REPLACE INTO`, or daya-core's upsert) must treat that as "do
not write this column" — either a true partial-column write, or (this repo's own sink) fetch and
carry forward the column's current value before writing. A consumer that has never heard of
`ColumnKindToastedUnchanged` sees exactly the pre-WS-7 message shape and pre-WS-7 (buggy, but no
worse than before) behavior — this is what makes the fix independently deployable on the pipeline
side without daya-core shipping WS-4.5 first.

Option 3 was rejected: it reintroduces the exact "extra database round trip inside the hot decode
path" cost WS-1's `ColumnKinds` design was chosen partly to avoid for the decimal case, and it
would require the pipeline to hold a live connection capable of reading the *current* committed
value at exactly the right point in the replication stream — a correctness hazard (a concurrent
write between decode and read-back could return a value newer than the one this WAL record
represents) on top of the performance cost.

Option 4 was rejected: it pushes the entire correctness burden onto every current and future
consumer independently reconstructing "was this column really NULL," which is not recoverable from
`Data` alone by definition — that is the bug, not a missing convenience.

### Consequences

* Good: the fix is entirely pipeline-side and back-compatible — an unmodified daya-core keeps its
  current (imperfect, null-filling) behavior until it reads the new signal; nothing breaks by
  shipping this repo's half first.
* Good: `Data`'s shape for every unaffected message (the overwhelming majority) is byte-for-byte
  unchanged — no re-encoding cost, no new field to skip for a kind-unaware consumer.
* Bad: `ColumnKinds` is now serving two unrelated purposes (`decimal` routing,
  `toasted_unchanged` write-suppression) behind one string-keyed map — see
  [0019](0019-columnkinds-typed-side-channel.md) for why that shape was chosen anyway and what its
  limits are.
* Bad: this is a cross-repo contract enforced only by convention (a shared constant name and this
  ADR), not by a schema the two repos both compile against — daya-core must independently keep its
  reading of `"toasted_unchanged"` in sync with this repo's constant.

## More Information

Full daya-core-facing contract is written out in
`summaries/ws5_ws6_ws7_implementation.md`'s WS-7 section: what to read, what "absent + flagged"
must mean for the column-set-collapse null-fill, and that this repo did not change the wire/proto
schema — only what it puts into the existing `ColumnKinds` field.

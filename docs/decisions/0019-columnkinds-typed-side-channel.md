---
status: accepted
date: 2026-08-02
decision-makers: cdc-pipeline maintainers
consulted: internal/protocol/message.go, internal/source/postgres/sanitize_transport_test.go, internal/transformer/nats/typed_value_codec_test.go, docs/decisions/0018-toast-unchanged-signalled-via-columnkinds.md
---

# Type/routing hints that can't survive `Data` on their own travel in a typed `ColumnKinds` side-channel, not an in-band marker

## Context and Problem Statement

Some values a source decodes cannot cross the msgpack transport in the shape the rest of the
pipeline expects, or need a routing hint a plain value can't carry on its own — the original case
was a `pgtype.Numeric` decimal, which the generated msgp `WriteIntf` cannot marshal directly
(`msgp.ErrUnsupportedType`) and which a kind-aware encoder (the nats/protobuf transformer's
`encodeTypedValue`) needs to route to `TypedValue.decimal_value` rather than `TypedValue.string_value`.
WS-7 later needed the same kind of side information for a structurally different reason: flagging
that a column is absent from `Data` because of an unchanged TOASTed value, not a NULL (see
[0018](0018-toast-unchanged-signalled-via-columnkinds.md)).

Both needs share a shape: attach a small amount of typed information to specific named columns of
a `protocol.Message`, readable by a kind-aware consumer, invisible to everything else. The first
implementation of the decimal case took a different approach — an **in-band marker**: prefixing the
`Data[col]` string value itself with a reserved byte sequence (a NUL byte followed by a kind tag)
that a kind-aware encoder would strip and act on, and every other consumer would presumably just
see as an odd-looking string.

That in-band design was implemented and rejected in review before this pattern was adopted. The
failure modes it produced are the actual substance of this decision.

## Decision Drivers

* A kind-unaware consumer (either sink, any future processor, any external subscriber to the same
  NATS subject) must see **exactly** the value it already reads today — no reserved-byte parsing,
  no stripping logic it doesn't know it needs to run.
* The signal must not be re-derivable incorrectly by a consumer that only partially understands
  it — a marker that looks like ordinary data to code that doesn't check for it is worse than no
  signal, because it silently corrupts that code's view of the value.
* Whatever the mechanism, it must generalize to WS-7's later, structurally different need (flagging
  an *absent* column, not decorating a *present* one) without a redesign.

## Considered Options

1. **In-band marker**: prefix/suffix the affected value in `Data[col]` itself with a reserved
   sentinel a kind-aware consumer strips off.
2. **Typed side-channel**: a separate `map[string]string` (`ColumnKinds`), keyed by the same
   column names as `Data`, carrying only the kind tag — `Data[col]` stays the plain, sink-safe
   value unconditionally.
3. **A parallel typed payload**, replacing `Data map[string]interface{}` with a richer structure
   (e.g. `map[string]TypedValue`) that every consumer must decode through, whether or not it cares
   about kinds.

## Decision Outcome

Chosen: **option 2**, `protocol.Message.ColumnKinds map[string]string` (`internal/protocol/message.go`).
`Data[col]` always carries the plain value every existing consumer already expects — a decimal
still arrives as its exact decimal text in a string, unchanged from pre-`ColumnKinds` behavior.
`ColumnKinds[col]`, when present, is a hint a kind-aware consumer *may* consult; an unrecognized
value in it is defined as informational and ignorable, not an error, so a newer producer talking to
an older consumer degrades to "no kind hint" rather than breaking it. `ColumnKinds` itself is
`omitempty` and `nil` whenever no column in the message needs one, so the overwhelming majority of
messages are byte-identical to the pre-`ColumnKinds` wire format.

Option 1 (the in-band marker, tried first) was rejected in review for three concrete, independently
disqualifying reasons:

* **It leaked to sinks that never opted into the feature.** A sink with no notion of "kind" marker
  parsing received the raw marker-prefixed string as if it were the real value — writing the
  reserved bytes straight into the target table's column, corrupting the row for every consumer
  downstream of *that* sink, not just this pipeline.
* **It broke NUMERIC-PK deletes silently.** A primary key value that happened to be numeric and
  therefore kind-flagged got its marker baked into `Data[pk]`; a subsequent delete's `WHERE pk = ?`
  matched against the marker-decorated value, not the real one, so `deleteTableBatch`-style
  lookups silently matched zero rows — no error, just a delete that appeared to succeed and did
  nothing.
* **It survived pure-filter responses.** A response path that only echoed values back unmodified
  (no decoding, just pass-through/filtering) never touched the marker, so it reached whatever
  consumed that response fully intact and unexplained — the opposite of "invisible to a
  kind-unaware consumer," the property this mechanism most needed.

Option 3 was rejected without a full implementation attempt: it forces every existing and future
consumer through a new decode step regardless of whether it cares about kinds, which is a strictly
larger blast radius than option 1's failure modes for a benefit (uniform typed access) nothing in
this codebase currently needs.

### Consequences

* Good: a kind-unaware consumer's behavior is provably unchanged — `Data` is never touched by the
  side-channel, so there is no marker to leak, corrupt a PK match, or survive somewhere unexpected.
* Good: the same mechanism generalized cleanly to WS-7's absence-flagging need
  ([0018](0018-toast-unchanged-signalled-via-columnkinds.md)), which is structurally different from
  the original decimal-routing case (decorating a present value vs. explaining an absent one) —
  confirming the side-channel shape, not the specific decimal use case, was the right level of
  abstraction.
* Bad: `ColumnKinds` is a stringly-typed, single flat namespace serving multiple unrelated concerns
  (`"decimal"`, `"toasted_unchanged"`, and whatever comes next) rather than a proper sum type per
  concern — a future kind whose semantics conflict with an existing one (e.g. needing to be both
  `decimal`-routed *and* toast-flagged) has no representation today short of widening the value to
  a delimited compound string, which would partially reintroduce option 1's fragility inside the
  side-channel itself. Not needed yet; worth revisiting if it becomes needed.
* Bad: unlike option 3, a consumer that *does* want typed access still has to consult two separate
  maps (`Data`, `ColumnKinds`) and reconcile them by key, rather than one typed structure.

## More Information

`internal/source/postgres/sanitize_transport_test.go` pins the byte-identical-when-unused property
for the decimal case end-to-end through the real msgp-generated marshal/unmarshal path (not a
constructed literal). `internal/source/postgres/source_toast_test.go` and
`internal/vendor/go-pq-cdc/pq/message/{tuple,format}` do the same for the WS-7 case. See
[0018](0018-toast-unchanged-signalled-via-columnkinds.md) for the WS-7-specific contract this
mechanism now also carries.

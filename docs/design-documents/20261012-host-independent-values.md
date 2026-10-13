# Record values do not depend on the host time zone or the pgx version (#302)

## Summary

The values this connector emits for `timestamp`, `timestamptz`, `date` and
`tsvector` depended on two things that are not part of the data: the time zone of
the machine running the connector, and the version of pgx that decoded the
value. This change normalizes them in `source/types`, for snapshot and CDC,
scalars and arrays, so the same row gives the same record on every host and on
every pgx from 5.7 up. It also moves pgx to 5.11.0, which fixes GO-2026-5004
(needs pgx >= 5.9.2).

Tier 1: it changes the values, and for non-UTC hosts the Avro bytes, of records
on the data path.

## Problem

Measured on PG 17.5, pgx 5.7.5 to 5.11.0, with `TZ` set per run.

| Value | What happened | Where |
| --- | --- | --- |
| `timestamp '2024-01-02 03:04:05.123456'` | **Avro bytes encode the wall clock shifted by the host offset.** The Avro library writes a `time.Time` for `local-timestamp-micros` by converting it to `time.Local` first. A wall clock labeled UTC comes out as `19:04` on a Los Angeles host. | `logrepl.withAvroSchema` (default `true`), snapshot and CDC. This is #302: source and destination both on a CET host gave a destination row one hour later. |
| `timestamptz` | Snapshot (pgx binary decode) has always returned the value in `time.Local`, on pgx 5.7.5, 5.8.0 and 5.11.0 alike. CDC (text decode) returned UTC up to pgx 5.10 and `time.Local` since 5.11. JSON shows `-08:00` where it could show `Z`. Same instant. | JSON. Snapshot: all versions. CDC: pgx 5.11 (Conduit v0.20). |
| `tstzrange`, `tstzmultirange` | Bounds are the same `timestamptz` values: `time.Local` over CDC since pgx 5.11 (`"Lower":"2024-01-02T03:04:05Z"` became `"2024-01-01T19:04:05-08:00"` in Los Angeles). | JSON, CDC. |
| `tsvector` | pgx >= 5.9 decodes `pgtype.TSVector`, which serializes as `{"Lexemes":[...],"Valid":true}`. Older pgx gave the text form. Conduit v0.17 to v0.20 shipped pgx >= 5.9.2 in the built-in connector. | JSON. Avro: no mapping in `avroMap`, so the schema could not be built at all. |
| `infinity`, `-infinity` | `pgtype.InfinityModifier` is an `int8`; it serialized as `1` / `-1`. | JSON. Avro: `timestamp` and `timestamptz` returned an opaque encoder error; **`date` was silently written as `1970-01-02` / `1969-12-31`** (confirmed pg to pg). |
| BC dates, years above 9999 | pgx 5.11 reads them (earlier versions returned an error). `time.Time` cannot be marshalled to JSON outside years 0 to 9999, and `Record.Bytes()` in conduit-commons **panics**. | JSON. |

What did **not** reproduce: a plain `timestamp` shifted in JSON. pgx returns the
wall clock labeled UTC in every version tested (text decode on 5.7.5 to 5.11.0,
binary decode on 5.7.5, 5.8.0 and 5.11.0), and JSON prints it with `Z`.
The shift is in the Avro encoding.

## Constraints

- Values must be identical on any host and any supported pgx.
- The Avro schema does not change (`local-timestamp-micros` for `timestamp`).
  Changing it would register a new schema version for every pipeline.
- The Go values for in-range data stay what they were (`time.Time` for dates and
  timestamps). Only values that were wrong or unserializable change type.
- No change in `conduit-commons` or the SDK from this repo.

## Decision

All of it is in `types.FormatFor(encoding, oid, value)`; `types.Format` is the
`EncodingNative` form. Callers pass `EncodingAvro` when the connector attaches
an Avro schema (`logrepl.withAvroSchema`).

| Type | Native (no Avro schema) | Avro schema attached |
| --- | --- | --- |
| `timestamp` | `time.Time`, wall clock labeled UTC. Relabeled, never converted, whatever zone the decoder used. | `int64` microseconds of the wall clock since `1970-01-01T00:00:00`. |
| `timestamptz` | `time.Time`, `.UTC()` | same |
| `date` | `time.Time`, midnight UTC | same |
| `tsvector` | canonical text (`'a':1 'cat':3A,4B`), formatted in this repo, not by pgx's `String()` | same (new `tsvector` to Avro `string` mapping) |
| `infinity` / `-infinity` | the strings `infinity`, `-infinity` | error `postgres.source.temporal_not_representable`, naming table and column |
| `tsrange`, `tstzrange`, `daterange`, multiranges and arrays of them | `pgtype.Range[any]` with the bounds normalized as above; infinite bounds are strings | not supported by the Avro schema extractor |
| Go year < 0 (before 1 BC) or > 9999 | Postgres text (`0044-03-15 12:00:00 BC`, `12345-06-07`) | as above; error if outside int64 microseconds |
| arrays of the above | element by element | not supported by the Avro schema extractor, unchanged |

Why a UTC `time.Time` for `timestamp` in native form: it is what the connector
has always emitted, JSON prints it without an offset ambiguity beyond the `Z`
convention for "wall clock", and the Postgres destination writes the wall clock
of whatever `time.Time` it gets, so the values round trip.

Why `int64` for `timestamp` under Avro: the Avro encoder cannot be made to write
a wall clock from a `time.Time` (it converts to `time.Local` first, and a DST
gap has no `time.Local` representation), and the SDK, not this connector, calls
it. A plain `int64` is written as is. The record is encoded by the SDK in the
same process before it leaves the connector, so no consumer sees the `int64`.
A test replays the encoding under four host zones and pins the bytes.

Ranges are normalized bound by bound with the native form even under Avro,
which has no range type. They do not round-trip pg to pg with Avro off (the
Postgres destination cannot encode a `Range[any]` bound); that predates this
change and is tracked separately.

Why the BC and far-future values become text instead of an error: the data is
valid in Postgres and a `time.Time` cannot carry it through JSON. Text is the
Postgres form and Postgres can parse it back. An error would halt pipelines on
data that used to be only a crash risk.

## Alternatives

1. **Change the Avro type to `timestamp-micros`.** Bytes would be right on every
   host with no special value type. Rejected: it changes the registered schema
   for every pipeline with a timestamp column, and loses the "no zone" meaning.
2. **Pass `time.Time` in `time.Local` with the wall clock** so the Avro
   encoder's conversion is a no-op. Rejected: wrong for DST gaps, and JSON
   would show the host offset.
3. **Set `time.Local = time.UTC` in the connector.** Rejected: the built-in
   connector runs in the Conduit process; this changes every other component.
4. **Fix the Avro library or the SDK.** The right long-term fix, tracked
   separately; it cannot ship with this connector and the v0.15.0 stable. Both
   libraries (hamba/avro before #349, iskorotkov/avro after) convert to
   `time.Local` the same way, and the golden Avro bytes are identical under
   both.
5. **Format tsvector with pgx's `String()`.** Rejected: that ties the text form
   to the pgx version, which is the bug.

## Failure modes

- **Crash mid-batch, restart:** formatting is pure and stateless. Positions do
  not contain values. No change to ack, position or ordering paths
  (invariants 1 to 4).
- **Duplicate delivery after upgrade:** a row redelivered after the upgrade has
  a different timestamp encoding than the first delivery on non-UTC hosts. See
  Upgrade.
- **Unexpected Go type from pgx for a temporal OID:** returns an error naming
  the column rather than passing the value on.
- **Error code:** `postgres.source.temporal_not_representable`, at the start of
  the message like the numeric family (`postgres.source.numeric_not_representable`).
  The advice in it is `logrepl.withAvroSchema=false`. "Remove the value from the
  table" is not advice for CDC: the change is already in the WAL and is read
  again after every restart.
- **Values outside int64 microseconds under Avro** (after 292277 AD, Postgres
  allows to 294276): error. Before this change the Avro library wrapped around.
- **Infinity under Avro:** error. For `timestamp` and `timestamptz` it was
  already an error, with no hint. For `date` it was not: the value was silently
  written as `1970-01-02` or `1969-12-31`. Pipelines that run today therefore
  stop after the upgrade, and rows already written downstream are corrupt. The
  source stops; source errors are not dead-lettered.
- **Destination decoding Avro `local-timestamp-micros` on a DST-observing
  host:** the Avro library decodes into `time.Local`, so a wall clock inside a
  DST gap decodes shifted by an hour. Not fixable here; it only affects values
  in the one hour a year that does not exist locally. Reported separately.
- **Range boundaries:** years 0000 (1 BC) and 9999 stay `time.Time` and are
  tested at the boundary.

## Upgrade and rollback

On a UTC host, nothing changes in the Avro bytes and nothing in `timestamp`
JSON. What changes on every host: `tsvector` is text (a change for Conduit v0.17
to v0.20 built-in users, who got the `Lexemes` object), infinity is a string or,
under Avro, a halt, BC/large years are strings. `timestamptz` JSON always ends in
`Z`: on non-UTC hosts snapshot records go from the host offset to `Z` (every
version), and CDC records go from the host offset (pgx 5.11, Conduit v0.20) to
`Z`.

On a non-UTC host with `logrepl.withAvroSchema=true` (the default), every
`timestamp` value written by an earlier version was shifted by the offset of the
host zone at that wall clock (so the shift also differs across a DST change).
After the upgrade new records are correct and old records stay shifted.
Destinations that matched rows by a timestamp key will see the corrected value
as a new key; destinations with timestamp columns are wrong for rows written
before the upgrade. Re-snapshot those tables after upgrading. Pipelines that run
on UTC hosts, or with `withAvroSchema=false`, do not need a re-snapshot for
`timestamp`.

Rollback is a version downgrade; there is no stored state to migrate. Values
written after the upgrade would then be shifted again on non-UTC hosts.

## Observability

There is no metric for a value's representation. The failures are errors that
name the table and column and carry the code
`postgres.source.temporal_not_representable`; Conduit surfaces them as the
pipeline error (a source error is not dead-lettered). `make test-timezones` and the `test-timezones` job replay the value
tests under `America/Los_Angeles`, `Asia/Kolkata` and `Pacific/Auckland`.

## Release note

> **Postgres source: values no longer depend on the host time zone or the pgx
> version.**
>
> - **`timestamp` (without time zone)** is now emitted as the wall clock stored
>   in Postgres on every host. Before, on hosts not set to UTC, Avro-encoded
>   records carried the value shifted by the host's UTC offset (#302). If your
>   Conduit host is not UTC and `logrepl.withAvroSchema` is `true` (the
>   default), rows already written downstream from `timestamp` columns are
>   shifted. Upserts keyed on such a column will insert new rows for the
>   corrected values. Re-snapshot the affected tables.
> - **`timestamptz`** is always emitted in UTC (`Z`). Snapshot records carried
>   the host's offset (`-08:00`) on every version on non-UTC hosts, and CDC
>   records did on pgx 5.11 (Conduit v0.20). Same instant; anything that parses
>   or compares the string, or the offset, sees `Z` now.
> - **`tsvector`** is emitted as text (`'a':1 'cat':3`) and can now be used with
>   Avro schemas. Conduit v0.17 to v0.20 shipped pgx 5.9.2 or later in the
>   built-in connector, which emitted `{"Lexemes":[...],"Valid":true}` when
>   `logrepl.withAvroSchema` was `false` (with Avro on it failed to build the
>   schema). If you consume that object, change the consumer before upgrading.
>   Versions before v0.17 emitted text.
> - **`infinity` and `-infinity`** in date and timestamp columns:
>   - Without an Avro schema they are the strings `infinity` and `-infinity`
>     (they were the numbers 1 and -1).
>   - With an Avro schema, `timestamp` and `timestamptz` infinity already
>     stopped the pipeline with an opaque error. `date` infinity did not: it was
>     **silently written as `1970-01-02` (infinity) or `1969-12-31`
>     (-infinity)**. Now the source stops with error code
>     `postgres.source.temporal_not_representable`.
>   - **Pipelines that run today will stop after the upgrade** if the table has
>     such a row, or when one is replicated. **Rows already written downstream
>     are wrong**: a `date` column that holds `1970-01-02` or `1969-12-31` where
>     the source has infinity.
>   - What to do: set `logrepl.withAvroSchema` to `false` (the source then emits
>     the strings, which the Postgres destination accepts), or remove infinity
>     from the data. Removing a row does not help a change already in the WAL,
>     which is read again after every restart. Then find and fix the downstream
>     rows, or re-snapshot the table.
> - **Dates and timestamps before 1 BC or after 9999 AD** are emitted as
>   Postgres text instead of crashing the record serializer.
> - **Ranges and multiranges** over `timestamptz` have UTC bounds, over
>   `timestamp` and `date` the stored wall clock; infinite bounds are strings.
> - pgx is updated to 5.11.0 (GO-2026-5004).

## Related

- #302, GO-2026-5004
- #349 (Go 1.26, commons v0.7.0, iskorotkov/avro; merged). Its hamba-compat
  fixtures pin schema strings and codec output for fixed inputs, not connector
  values, and pass unchanged: they do not call `source/types`.

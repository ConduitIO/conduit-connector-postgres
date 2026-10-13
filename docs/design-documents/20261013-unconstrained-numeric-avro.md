# Unconstrained numeric is an Avro string (#375)

## Summary

A `numeric` column without precision and scale got the Avro schema
`decimal(precision 65535, scale 65531)`. That leaves four digits before the
decimal point, so any value of 10000 or more fails to encode, the source stops,
and the restart stops at the same row. This change maps an unconstrained
`numeric` to an Avro `string` that holds the exact text of the value. `numeric(p,s)`
is unchanged.

Tier 1: it changes a registered schema and the Go type of a payload field.
Target: v0.14.4 (backport) and the next minor on `main`.

## Context

`source/schema/avro.go` derives the decimal from the type modifier,
`scale = (typmod-4) & 65535` and `precision = ((typmod-4) >> 16) & 65535`. An
unconstrained numeric has `typmod = -1`, which yields 65535 and 65531.

Reproduced on Conduit v0.20.0 (embeds connector v0.14.2) with the built-in
Postgres source and default settings, on connector v0.14.3, and on `main`:

| value | `numeric` (unconstrained) | `numeric(12,3)` |
| --- | --- | --- |
| `1.50`, `9999.9999`, `0.000001` | encoded | encoded |
| `10000`, `12345.678`, `123456789.123456` | error, pipeline stops | encoded |

The error is `... as Avro bytes.decimal with precision=65535, has 65536
significant digits`. The Conduit pipeline goes to `degraded` and the rows that
were in the same batch are not delivered either.

What Postgres can store in an unconstrained `numeric`: up to 131072 digits
before the point, up to 16383 after it, plus `NaN`, `Infinity` and `-Infinity`.

## Constraints

- A fixed Avro schema is registered per table. A decimal's scale is part of that
  schema, not of the value.
- Never round, truncate or reinterpret a value (Invariant 6).
- Postgres to Postgres must round-trip exactly.
- No new config option for something that is a bug.

## Options

Sizes are measured with the encoder this connector uses (iskorotkov/avro 2.34.0;
hamba/avro on `release/v0.14.x` gives the same bytes). "Encoded size" is the
Avro-encoded size of one value.

| | Schema | Encoded size of `0` / `1.5` / `12345.678` / 1000 digits + 60 fractional | Holds every value | Notes |
| --- | --- | --- | --- | --- |
| today | `decimal(65535,65531)` | 2 / 27215 / error / error | no | The common case already costs 27 KB per value, because the unscaled integer is `value * 10^65531`. |
| (a) | `string` | 2 / 4 / 10 / 1053 bytes | yes, incl. NaN and the infinities | Exact text, scale kept (`1.50` stays `1.50`). A consumer parses the decimal itself. |
| (b) | `decimal(147455,16383)` | 2 / 6806 / 6807 / 7221 bytes | yes, except NaN and the infinities | Holds the maximum Postgres allows (131072 + 16383 digits), but every non-zero value costs about 6.8 KB, 700 to 1700 times the string, because the scale is fixed at the maximum. |
| (c) | scale chosen per value | not possible | | The scale is a schema attribute. A fixed schema cannot carry a per-value scale. Registering one schema version per possible scale (16384 of them) and switching between them per record is not a design. |
| (d) | Avro 1.12 `big-decimal` (bytes with the scale inside each value) | n/a | yes | The right shape, but neither hamba/avro nor iskorotkov/avro implements it: the logical type is parsed and ignored, and a `*big.Rat` is rejected as "unsupported for Avro bytes". Consumers older than Avro 1.12 see opaque bytes. Not usable today. |
| (e) | `double` | 9 bytes | no | Loses digits. Rejected (Invariant 6). |

Decision: (a). It is the only option that holds every value, is cheap, and works
with the codecs and consumers that exist. (b) is correct but about 6.8 KB per
value for every non-zero number. (d) is the better long-term schema; the
decision can be revisited when the Go Avro libraries support it, with the same
kind of schema version bump described below.

NaN and the infinities: with a string they are representable losslessly as
`"NaN"`, `"Infinity"` and `"-Infinity"`, so an unconstrained numeric no longer
halts on them under Avro (it did since #371). `numeric(p,s)` is still an Avro
decimal: NaN still halts there with `postgres.source.numeric_not_representable`
(`numeric(p,s)` cannot hold an infinity). This makes the two column kinds behave
differently on purpose, and each is lossless or loud.

## Decision

`extractType` returns `string` for `numeric` with type modifier -1. A new
`types.FormatColumn` (FormatFor plus the type modifier) returns, under Avro, the
exact Postgres text for such a column: the digits as stored with their scale, or
`NaN` / `Infinity` / `-Infinity`; NULL stays the untyped nil. Snapshot and CDC
use it with the field's type modifier. Key columns too.

## Consumer impact

- Go type of the payload and key field: `*big.Rat` becomes `string`, for
  unconstrained numeric columns only. Constrained columns are unchanged.
- A string keeps the scale: `1.50` stays `1.50`. Through `*big.Rat` it became
  `3/2` and the scale was lost (#345). Checked on Conduit v0.20.0 with the
  built-in file destination: before, the record shows `"v": "3/2"`; after,
  `"v": "1.50"`.
- This connector's destination: a string passes through `formatValue`
  unchanged and Postgres converts the text, so no digits are lost and the
  column's scale is applied by Postgres. Tested pg to pg, snapshot and CDC, into
  `numeric`, `numeric(900,70)` and `text`, with values from `0` to a 1060-digit
  number, `NaN` and `+/-Infinity`: all equal to the source text, or numerically
  equal where the target has a scale. Also through a real Conduit v0.20.0
  pipeline (patched standalone source, built-in v0.14.2 destination).
- Destinations that serialize the structured payload now see a decimal string
  instead of `"3/2"` (checked with the built-in file destination; log and Kafka
  were not run). Kafka consumers of the Avro
  bytes see a `string` field where there was `bytes` / `decimal`.
- `conduit-connector-postgres` consumers that read `*big.Rat` for these columns
  must parse the string.

## Schema versioning and upgrade

- The payload and key schema of a table with an unconstrained numeric column
  changes text, so the first record after the upgrade registers a new version of
  the subject (`<table>_payload`, `<table>_key`). Compatibility on the Conduit
  registry is NONE, so the registration succeeds; no policy blocks it.
- Mid-stream: records before the upgrade keep version N (decimal), records after
  carry version N+1 (string). A reader that follows the schema version per record
  copes; a reader that pins version N does not.
- Tables without an unconstrained numeric are not affected.
- The evolve schema-drift policy exists on `main` only, not on `release/v0.14.x`;
  see the main PR for how a change between `text`/`varchar` and an unconstrained
  numeric is judged there (only with `withAvroSchema` on).
- Rollback to v0.14.3: the schema goes back to the decimal and the halt for
  values of 10000 or more is back. Rows already emitted as strings stay in the
  sink.
- The registered schema text is pinned by `source/schema/testdata` fixtures;
  the unconstrained entry was changed on purpose.

## Details worth knowing

- With `withAvroSchema` off, an unconstrained numeric is a `big.Rat` as before:
  the Go type of the column depends on the setting.
- The snapshot fetches `numeric` columns in text format (pgx's binary form drops
  the display scale of a zero, so `0.000` was `"0"` in the snapshot and `"0.000"`
  in CDC). Both paths now give the same text; tested with `0.000`, `0.00000` and
  `-0.0` (Postgres stores the last as `0.0`).
- Rows written before the upgrade through the old schema were rounded by the
  destination: v0.20.0's built-in postgres destination wrote `2.50` as `3`.
  #365/#366 fix that for new rows only; existing rows stay wrong and need a
  re-sync.

## Failure modes

| | |
| --- | --- |
| Crash mid-batch | No change: the value is formatted before the record is built, nothing is acked early. |
| Huge value | 131072 digits are emitted as 131 KB of text; the Avro encoder and Postgres accept it. Tested to 1060 digits. |
| `numeric` key column | The key is the text. Snapshot position is unaffected (it needs an integer key and still does). |
| Consumer cannot parse a string | The record is delivered, the consumer fails on parsing. The alternative (a halt in the source) would lose the pipeline for everyone. |
| `ALTER COLUMN TYPE numeric(p,s)` while running | Schema differs: handled by the drift policy as any numeric change. |

## Observability

No new metric. The schema subject version in the record metadata
(`opencdc.payload.schema.version`) increases at the upgrade.

## Related

- #375 (this), #368 and #371 (non-finite numerics), #345 (scale through `big.Rat`),
  #365 (destination numeric scale), #326 (Avro fields are not nullable).

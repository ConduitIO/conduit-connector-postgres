# json text that is not valid UTF-8 is delivered as base64, named in metadata

## Summary

On a `SQL_ASCII` database a `json` or `jsonb` value can hold bytes that are not valid
UTF-8. From v0.21.0 json fields are Avro and Go strings, which cannot carry those bytes
as they are. The source now emits such a value as the standard base64 of its bytes and
names its column in the record metadata `postgres.json.raw_bytes` (key and
`payload.after`) or `postgres.json.raw_bytes.before` (`payload.before`). This
connector's destination decodes the listed columns and writes the bytes. The setting
`json.invalidUTF8` chooses between `base64` (the default) and `halt`.

Tier 1: a new metadata contract between source, consumers and destination, and a change
to what the data path emits. Target: connector v0.21.0, shipped in Conduit v0.22.
Roadmap: Conduit v0.22, Debezium Postgres output parity.

## Context

- Every server encoding except `SQL_ASCII` checks json text on input, so only a
  `SQL_ASCII` database can hold such a value (`source/types/json.go`,
  `JSONNotUTF8Error`).
- v0.20.0 emitted json as Avro `bytes` and Go `[]byte`, so the bytes passed through.
  v0.21.0 emits a string named `io.debezium.data.Json`, for Debezium parity.
- The first version of the string change halted the source on such a value, by
  default, and a restart halted on the same change again. The maintainer rejected that:
  operators often do not know their database is `SQL_ASCII`, and one row must not stop
  replication.
- **pgx changed the bytes in CDC arrays.** pgoutput sends `json[]`/`jsonb[]` as text,
  and pgx's text array parser reads runes and writes them back with `WriteRune`, so
  every invalid byte became U+FFFD before any formatter saw it
  (`source/types/json_array.go`). The halt never fired for CDC arrays: the element
  arrived changed, with no error. The snapshot reads arrays in binary and was not
  affected. `jsonArrayCodec` replaces pgx's text decoder for these two types.

## Constraints

1. Invariant 6: no byte is replaced, dropped or reinterpreted without the consumer
   being able to tell. A value that is encoded must never be emitted unmarked.
2. Invariants 1 and 3: the default must deliver the record; nothing is acked that was
   not delivered or written.
3. The Avro schema must not depend on the value. A json field is a string, or
   `["null", string]` for a nullable column (#466).
4. Valid text, which is every value on a `UTF8` database, is emitted exactly as before.

## Decision

**Source** (`source/types/json_policy.go`, `JSONPolicy.FormatColumn`):

- Valid UTF-8: the exact text, as a string. No metadata.
- Not valid UTF-8 with `json.invalidUTF8: base64` (default; any value other than
  `halt` is base64, `NewJSONPolicy`): the field holds `base64.StdEncoding` (RFC 4648,
  padded) of the raw bytes, and the column is listed in the metadata.
- `postgres.json.raw_bytes` covers the key and `payload.after`;
  `postgres.json.raw_bytes.before` covers `payload.before`. Two keys, because an
  `UPDATE` that corrects the value has base64 in Before and valid text in After.
- The metadata value is a JSON array of column names, sorted by byte order, without
  duplicates, for example `["doc","tags"]` (`JSONRawBytesMetadata`). A Postgres column
  name can contain a comma; a JSON array of strings cannot be read two ways. The key is
  absent when nothing in that part of the record was encoded.
- `json[]`/`jsonb[]` are decided per column: if any element is invalid, every non-NULL
  element is base64 and NULL elements stay NULL (`jsonBase64`, `mapArray`). A
  consumer then decodes a listed array element by element without guessing.
- CDC (`source/logrepl/handler.go`): a TOASTed column backfilled into After from a
  base64 old value is listed in `raw_bytes`. A delete has no After; its key comes from
  the old row, and a listed key column is named in `raw_bytes`. A Before that is not
  emitted (key-only old tuple) is not named.
- Snapshot: `FetchWorker.buildRecordData` returns the encoded columns in
  `FetchData.RawJSON`, and `Iterator.buildRecord` sets the metadata
  (`source/snapshot/`).

**Halt** (`json.invalidUTF8: halt`): the source stops with `postgres.source.json_not_utf8`
(table, column, row as key columns plus a salted key hash, byte offset, fix, runbook;
never the value). It halts on an insert, on the new row of an update, on a full old
row of an `UPDATE` (REPLICA IDENTITY FULL), and on a `DELETE`'s old tuple, key-only or
full, because that tuple is the emitted key. It does not halt on the key-only old tuple
of an `UPDATE` (`'K'`): that tuple is never emitted, and halting on it would stop the
source on every restart for a value it does not deliver (`handler.go`, the
`msg.OldTupleType == pglogrepl.UpdateMessageTupleTypeOld` gate).

**Destination** (`destination_json_raw.go`, called from `upsert` and `remove` in
`destination.go`): for every column listed in `postgres.json.raw_bytes`, in the key
and in the payload, `decodeJSONRawBytes` decodes the value with
`base64.StdEncoding.Strict()` and writes the bytes as a Go string (pgx sends a string
into a json, jsonb or text parameter as it is, in every exec mode; a `[]byte` would be
sent as bytea where the parameter type is not described). For a list it decodes every
element and keeps nil elements. The caller's maps are copied (#343).
`postgres.json.raw_bytes.before` is ignored: the destination never writes Before. A
record without the metadata takes the same path as before this change.

The destination fails closed, with `postgres.destination.json_raw_bytes_invalid`, on
metadata that is not a JSON array of strings (including `null` and `""`), a listed
column that neither the key nor the payload has, and a listed value that is not strict
padded base64 or a list of such strings and nils. The decision to fail on a missing
column, rather than ignore it, was the maintainer's: ignoring it could hide a processor
that rewrote the record. The decode runs in `planWrites`, before any statement is
built, so a failure returns `(0, err)` from `Write` and nothing of the batch is stored
or acked.

## Alternatives

| Alternative | Why it lost |
| --- | --- |
| Halt by default | The previous behaviour. An operator who does not know the database is `SQL_ASCII` gets a pipeline that stops on one row, and a change already in the WAL cannot be got past by correcting the row. Kept as the opt-in `halt` for consumers that cannot handle base64. |
| Avro `bytes`, or a union `["string","bytes"]`, for such values | The schema would change per value, or every json field would become a union. Breaks constraint 3, Debezium parity and every consumer that reads `string`. |
| Replace invalid bytes with U+FFFD, as Debezium does | Lossy and silent: the original bytes cannot be recovered (invariant 6). |
| Map invalid bytes to reserved code points (surrogateescape, Private Use Area) | Ambiguous: those code points can appear in valid JSON text. Lone surrogates are not valid UTF-8, and many JSON parsers reject them. |
| A separate field per column (`doc_raw` next to `doc`) | Changes the record's shape and the Avro schema per value, collides with real column names, and a sink that writes every field would create or fail on a column the table does not have. |
| One metadata key for the whole record | Wrong for an `UPDATE` that corrects the value: the consumer would decode a valid After, or the source would have to base64 a valid value. |
| Encode only the invalid elements of an array | Ambiguous with column-level metadata: a valid element such as `1234` is also valid base64. Per-element indices would fix that at the cost of a more complex contract. |

## Failure modes

- **Consumer ignores the metadata.** It reads base64 text as the JSON value: a parse
  error, or base64 stored downstream. No byte is lost; the bytes are in the base64.
  Signals: the startup warning and the first-value warning (Observability). Remedy:
  decode the listed columns (README runbook has an example), or set `halt`.
- **Upgrade order.** A destination older than v0.21.0 fed by a v0.21.0 source does
  not read the metadata. It writes the base64 text into a `text` column without an
  error; a json or jsonb column normally rejects it, because base64 text is not JSON.
  Upgrade the destination with the source or before it. Only a source on
  a `SQL_ASCII` database with such values is affected.
- **A processor rewrites a listed column and keeps the metadata.** A value that is not
  padded base64 (anything with `{`, `[`, `"`, `-`, `.` or a space) fails with
  `postgres.destination.json_raw_bytes_invalid`. A value that happens to be valid
  padded base64 (`true`, `null` as text, `1234`) is decoded and written as wrong
  bytes, without an error; the destination cannot tell it from the record. The
  contract is that a processor that changes a listed column updates or removes the
  metadata (README, destination section).
- **A processor drops or renames a listed column.** Fails closed with
  `postgres.destination.json_raw_bytes_invalid`, naming table and column. The pipeline
  stops on the record, or the record goes to the DLQ if one is configured.
- **Non-canonical base64.** `Strict()` rejects a value with non-zero trailing bits
  (`/x==`), which a lenient decoder would read as the same byte as `/w==`. The source
  only emits canonical base64; strictness keeps the destination from accepting values
  the source could not have written (`TestDecodeJSONRawBytes_Errors`, "value not
  canonical").
- **Target is `UTF8`, or the url sets `client_encoding=UTF8`.** Postgres rejects the
  bytes with SQLSTATE 22021; nothing of the write is stored or acked. This is the
  intended loud failure: such a database cannot hold the value.
- **`simple_protocol` against a `SQL_ASCII` target.** pgx refuses every
  simple-protocol query unless `client_encoding` is `UTF8` (pgx v5.11 `conn.go`), so
  every write fails, valid or not. With `client_encoding=UTF8` the listed values fail
  with 22021. This predates the change and fails loudly; the README destination
  section says to use another exec mode.
- **Target in another encoding** (LATIN1 and so on). Postgres reads the bytes in that
  encoding, as it reads every other value the destination sends there. Not tested.
- **The source misses a column in the metadata.** Would be silent mangling. Guarded by
  one metadata append site per path, guardmut markers in `source/logrepl`, and tests
  that the metadata equals exactly the invalid columns and that the bytes round-trip.
- **The array parser misreads Postgres array text.** A decode error, which stops the
  source, or wrong elements. Covered by `FuzzParseJSONTextArray` and agreement with pgx
  on valid UTF-8; binary format still uses pgx.
- **Column names that are not valid UTF-8.** `json.Marshal` of the metadata replaces
  their bytes with U+FFFD, as every JSON serializer does with the record's own field
  names. Not addressed.

## Upgrade and rollback

- **Schema.** The base64 feature does not change the schema: the field stays a
  string. The bytes-to-string change itself is `BACKWARD`, `FORWARD` and `FULL`
  compatible for a `NOT NULL` column. A nullable column also becomes `["null", string]`
  in this release (#466), which is only `BACKWARD` compatible with the v0.20 `bytes`
  version (`source/schema/avro_json_compat_test.go`,
  `TestJSONNullableSchema_BackwardOnly`).
- **Order.** Upgrade the destination with the source or first (see Failure modes).
- **Rollback to v0.20.** v0.20 emits such text as raw bytes again, without the
  metadata, and registers the `bytes` version again; for a nullable column that is not
  `BACKWARD` compatible after `["null", string]`. A v0.20 destination fed by records
  produced by v0.21 writes the base64 text of listed columns.
- Positions, checkpoints and state are not touched.

## Observability

The connector exposes no metrics, so warnings, errors and the metadata are the
signals.

- `postgres.source.json_server_encoding` (warning, `source_json_encoding.go`): at
  `Open`, when `server_encoding` is not `UTF8` and a captured table has a json,
  jsonb, json[] or jsonb[] column. Names the columns, the `json.invalidUTF8` setting
  in use and the metadata key.
- `postgres.source.json_not_utf8` (warning under `base64`, `JSONPolicy.WarnRawBytes`):
  once per table and column per source, with table, column, row, path (`snapshot` or
  `cdc`), metadata key and runbook. Never the value.
- `postgres.source.json_not_utf8` (error under `halt`, `JSONNotUTF8Error`): stops the
  source; the same row stops it after every restart.
- `postgres.destination.json_raw_bytes_invalid` (error,
  `ErrorCodeDestinationJSONRawBytesInvalid`): fails the write, naming table, column
  and metadata key, never the value.
- SQLSTATE 22021 from Postgres on a target that cannot hold the bytes.

## Related

#343, #466, `20261106-avro-nullable-columns.md`, README sections "json and jsonb
Columns", "Runbook: postgres.source.json_not_utf8" and "json text that is not valid
UTF-8" (Destination).

# Nullable columns are Avro unions with null (#326)

## Summary

The Avro schema extractor gave every column a plain type, so a `NULL` in any
nullable column failed to encode (`avro: *avro.null is unsupported for Avro <T>`)
and stopped the source, with the default `logrepl.withAvroSchema: true`, for every
mapped type. This change reads each column's nullability from the catalog and
emits `["null", T]` with a default of `null` for a column that is not `NOT NULL`,
the shape Debezium's Avro converter gives an optional column. `NOT NULL` columns,
primary keys included, keep the plain `T`.

Tier 1: it changes registered schemas on the data path. Target: v0.21.0.
Roadmap: v0.22 Debezium Postgres output parity.

## Context

- Neither input of the extractor carries nullability: `pgconn.FieldDescription`
  (snapshot) and `pglogrepl.RelationMessage` (CDC) have none.
- The connector registered `sch.String()`, the Parsing Canonical Form, which drops
  field defaults.
- Since #317 an `UPDATE` that leaves a TOASTed column unchanged omits the column
  from `After`. The Avro serde the SDK uses (`conduit-commons/schema/avro`) encodes
  a field that is missing from the data as the field's default.
- The #326 and #388 discussion fixed a constraint: nullability must describe the
  real column, never be widened to make an incomplete record encode, because an
  absent field decoded as `null` overwrites the real value downstream.

## Constraints

1. Invariant 6: `NULL` is delivered as `null` for every type; a `NOT NULL` column
   never receives a made-up value; absence stays distinguishable from `NULL`.
2. A table whose columns are all `NOT NULL` registers the same text as before
   (no new schema version on upgrade). Pinned by the hamba fixtures.
3. No new configuration.

## Options

1. **Catalog lookup (chosen).** Snapshot: `pg_attribute` for the table's OID, read
   in the snapshot transaction before the first `FETCH`. CDC: `pg_attribute` for
   the relation OID of the Relation message, on the pool, cached per relation
   until the next Relation message (pgoutput sends one after any `ALTER TABLE`).
2. **Every field optional.** No catalog read. Rejected: a `NOT NULL` column would
   accept a missing value as `null` (the #317 corruption), and the schema would not
   describe the table.
3. **Union without a default.** Keeps the canonical registration. Rejected:
   adding a nullable column later is then not `BACKWARD` compatible in a registry
   (the reader field has no default), unlike Debezium; the default is what makes
   column additions safe for consumers.

## Decision

Option 1, with three parts that the default makes necessary:

- The registered text is the full JSON form (`schema.Text`), which keeps
  `"default": null`. For a schema without defaults it is byte-identical to the
  canonical form, so constraint 2 holds.
- `NotNull` must have an entry for every included column; a missing entry is an
  error. A caller cannot forget the lookup and get optional fields.
- With Avro on, an `UPDATE` whose new row lacks a column (unchanged TOAST) stops
  the source with `postgres.avro.unchanged_toast` before the record is built.
  Without this refusal the default would encode the omitted column as `null`.
  Before this change the same record also stopped the source, with
  `missing required field`; the outcome is unchanged, the error is now coded. #388
  proposes the per-record schema that lets these records through.

A nullable `timestamp` is passed to the encoder as `*int64` instead of the int64
of #302 (`types.NullableAvroValue`): the encoder resolves a union branch by Go type,
and int64 does not name the `local-timestamp-micros` branch. Only the Go value
inside the connector changes; the encoded long is the same.

From Postgres 18, `attnotnull` is also set by a `NOT NULL NOT VALID` constraint,
whose existing rows may hold `NULL`. Such a column is treated as nullable.

## Failure modes

- **Catalog read fails** (connection, permission) or exceeds its 10s bound
  (`schema.NotNullQueryTimeout`, well under the default `wal_sender_timeout`): the
  change or snapshot fetch returns the error; nothing is emitted or acked; the
  restart retries. A failed read is not cached.
- **Catalog newer than the row**, CDC behind a `SET NOT NULL` (or a snapshot whose
  catalog read sees a `SET NOT NULL` committed after its data snapshot): before the
  record is built, a `NULL` in a column whose field is plain widens that field to
  `["null", T]`, registers the new version and delivers `null`. A warning coded
  `postgres.avro.null_in_not_null_column` names the table and column once per
  process (CDC) or worker (snapshot). The widening holds for the rest of the process,
  so the subject does not flip between two versions. If the registry rejects the
  widened schema (an external subject with `FORWARD` or `FULL`), the change fails
  with the same code and a fix (allow `BACKWARD`, or turn Avro off); nothing is
  emitted or acked. A key-only old tuple's `NULL`s are not row values and are not
  checked. Rejected alternative: fail closed with the code; it stopped a pipeline
  for a value the schema can represent truthfully.
- **Catalog newer than the WAL**, column dropped or renamed since: not in the
  catalog, treated as nullable, which accepts every value. A rename that reuses
  another column's old name picks up that column's nullability; the worst case is
  the widening above.
- **Unchanged TOAST under Avro**: coded stop, as before (see Decision).
- **Snapshot**: the lookup runs in the snapshot's `REPEATABLE READ` transaction, so
  it describes the rows being read. Each field's table OID is checked against it.

## Upgrade and rollback

Payload subjects of tables with a nullable column get a new version on the first
record after the upgrade. `T` to `["null", T]` is `BACKWARD` compatible and not
`FORWARD` compatible (checked with `avro.NewSchemaCompatibility` in
`TestAvroExtract_NullableRegistryCompatibility`). Key subjects and all-`NOT NULL`
tables are unchanged. Rolling back registers the old shape again and stops on the
first `NULL`, as before. Positions and other serialized state are not touched.

## Observability

No metrics exist in the connector. The two stops carry the SDK encode error or the
`postgres.avro.unchanged_toast` code, and are visible as a failed pipeline with a
held replication slot.

## Related

#317, #326, #383, #388, `20261013-unconstrained-numeric-avro.md`.

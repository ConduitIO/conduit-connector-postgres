# One collection name per table, in both phases (#424)

## Summary

The report: `opencdc.collection` differs between the snapshot and CDC records of a
table. The snapshot uses the configured `tables` entry, CDC the bare relation name,
so a pipeline that configured `public.orders` would see `public.orders` and then
`orders`, and anything routing on the collection would change target at the handoff.

On `main` (v0.21.0) that case cannot occur, and this change does not alter it:

- Every configured name is quoted as one identifier (`internal.WrapSQLIdent`) and
  resolved through the session's `search_path`, for the primary key lookup, the
  snapshot and the publication. `public.orders` is the name of a table that does not
  exist, so Open fails with `relation "public.orders" does not exist`. Schema-qualified
  names have been refused since #251 (February 2025); supporting them is #277.
- Because the name is resolved exactly, a configured name is the relation's
  `relname`, which is what `RelationMessage.RelationName` carries. Snapshot and CDC
  records of a configured table therefore already carry the same collection, key
  column and Avro subjects: lower-case, mixed-case, and a table in a non-public schema
  found through `search_path` (also with a same-name table in `public`). Partitioned
  parents, where pgoutput would report the leaf, are refused at Open (the snapshot
  validates the key against the parent).

What is broken is the CDC side's lookup by bare name, #424. A publication that already
exists under `logrepl.publicationName` is reused without checking its tables, and in
Debezium import mode the publication may cover more of Debezium's tables than
`tables`. A relation streamed that way with the same name as a configured table, in
another schema, gets the configured table's collection, key column, and Avro subjects
and schema cache. If both tables have the key column (`id`), the other table's rows
reach the destination keyed as the configured table's rows and upsert over them.
Invariant 6.

This change identifies each CDC relation as a configured table by its OID, or failing
that by (namespace, name). A configured table keeps the configured name, and any other
relation is named `<schema>.<table>`.

Tier 1: changes the records the source emits. Roadmap: Conduit v0.22, "Postgres Avro
schemas match the table" / correct data on the default path.

## Context

- `CDCHandler` keys `opencdc.collection`, `tableKeys`, `payloadSchemas`/`keySchemas`
  and the `<name>_payload`/`<name>_key` subjects by `rel.RelationName`.
- The snapshot names records, positions and subjects by the configured entry.
- `NewCDCIterator` creates the publication `FOR TABLE <tables>` and, on 42710
  (exists), logs a warning and uses it as it is. The subscription does not filter
  relations; pgoutput streams whatever the publication covers.
- The import preflight requires the publication to cover the pipeline's tables and
  to stream only tables Debezium captured; it can stream more than `tables`.
- The Postgres destination's default table is `{{ index .Metadata "opencdc.collection" }}`,
  quoted as one identifier (`pgx.Identifier{name}.Sanitize()`) and resolved through
  its `search_path`. A schema-qualified collection there names a table called
  `schema.table`, so changing a configured table's collection to the qualified form
  would break the default Postgres-to-Postgres path.

## Constraints

1. A configured table's records keep exactly the collection, key and subjects they
   carry today. Its snapshot and CDC records agree.
2. A relation the pipeline did not configure never takes a configured table's
   collection, key column or schema.
3. No new configuration. No change to positions or the publication the connector
   creates.

## Alternatives

1. **Match configured tables by OID, then by (namespace, name); configured tables
   keep the configured name, others are `<schema>.<table>` (chosen).** At iterator
   start each configured name is resolved (`to_regclass`, the same quoting and
   `search_path` as the rest of the connector) to its OID and schema. A Relation
   message whose `RelationID` is that OID, and whose name is still the configured one,
   is the configured table, in whatever schema it is now. One whose (namespace, name) is
   the resolved one is also the configured table: it was dropped and created again, so
   it has a new OID. Anything else gets the qualified name. If two live relations match
   one configured name in one run (the table was moved and a new one took its name),
   the source halts with `postgres.source.configured_table_ambiguous`. No change for any
   configured table, collisions removed for the rest.

   History of this rule:
   - A first version matched by (namespace, name) only. Review found that a table moved
     with `SET SCHEMA` then lost its key, and its deletes skipped the null-key check, a
     regression from v0.21.0.
   - A second version matched by OID whatever the name. Review found that in a
     rename-and-swap (T to T_old, a copy to T) the retired T_old's deletes came out as
     T with T's key, deleting live rows downstream. v0.21.0 emitted them as `T_old` with
     no key, which fails closed at the Postgres destination. Hence the name condition
     on the OID match, and the halt for two live relations.
2. **Always emit `<schema>.<table>`, in both phases.** Unambiguous and what Debezium
   does in its topic names. Lost: every existing pipeline's collection changes, the
   default Postgres destination template stops finding its table (it quotes the whole
   collection as one identifier), and the snapshot positions are keyed by the
   configured name, so a position migration would be needed. A large break to fix a
   case that needs a reused, wider publication.
3. **Refuse to start, or halt, when the publication covers a same-name table in
   another schema** (#424's minimal option). No output changes. Lost: a check at Open
   misses an `ALTER PUBLICATION ... ADD TABLE` while running, so it needs a runtime
   halt as well; a halt on a change in the WAL is replayed on every restart (pgoutput
   reads publication membership as of the change), so removing the table from the
   publication does not unstick the pipeline, and the operator must advance the slot.
   It also leaves two unconfigured same-name tables colliding with each other.
4. **Drop changes of relations that are not configured.** Matches what `tables`
   says. Lost: silently stops delivering records some pipelines receive today (a
   delivery change hidden as a fix); out of scope here.

## Decision

Alternative 1.

- `logrepl.ResolveTableNamespaces` returns configured name → (OID, schema). It runs in
  `newCDCIteratorHandler` before the subscription creates the slot; it is read-only,
  and a failure fails the start (nothing is left behind).
- `CDCHandler.tableName(rel)` returns the configured name when `rel.RelationID` is a
  resolved OID and `rel.RelationName` is still that name. Otherwise it returns `rel.RelationName` when the resolved schema of that
  name is `rel.Namespace`, and otherwise `rel.Namespace + "." + rel.RelationName`. It is
  used for the collection, the key column lookup (insert, update, delete, the
  delete-key check), the schema caches and the subjects. Without `SetTableNamespaces`
  (unit tests that build a handler directly) every relation keeps its bare name.
- `CDCHandler.checkTable(rel)` runs before each insert, update and delete. It records,
  per configured name, the schema each OID named after it was last seen in during this
  run. It refuses the change, with `postgres.source.configured_table_ambiguous` and
  nothing emitted, when another OID was seen under that name in a different schema. A
  drop and recreate in the same schema is not refused, because the old table is gone.
  The refusal does not recur on restart: the name is resolved again, so the other table
  no longer matches and is named `<schema>.<table>`. It is a marked guard
  (`logrepl-configured-table-ambiguous`), killed by its test under `tools/guardmut`.
  `checkTable` also logs the unconfigured-table `WARN`.
- The Avro record names stay `<relation>_payload`/`<relation>_key` with the bare
  relation name, as on v0.21.0. A qualified record name would make Avro read the
  schema as a record namespace and reject one that is not a valid Avro name
  (`sales-eu`, `1x`: `invalid name part`), halting the source on a table that v0.21.0
  emitted. The subject tells the tables apart; the record name does not need to. The
  connector does not sanitise table names for Avro anywhere, so a table name that is
  not a valid Avro name fails as it did before.
- The first change of an unconfigured relation logs a `WARN` with the code
  `postgres.source.unconfigured_table`, once per relation and process.
- `NewCDCIterator`'s publication step moved into `ensurePublication` (no behaviour
  change) to keep the function under the lint's complexity limit.

The value for a configured table is the form the user configured, which is the only
form `tables` accepts. When #277 adds schema-qualified names, the same rule applies:
the configured entry is the collection in both phases, and CDC maps (namespace, name)
back to it through the resolved map.

How ambiguity resolves:

- Two schemas with the same table name: only one can be configured (the one
  `search_path` finds first, for the snapshot, the publication and CDC alike). The
  other, if a reused publication streams it, is `<schema>.<table>`.
- `*`: expands to the bare names in `pg_tables` for schema `public`, which are then
  resolved like any entry. With a `search_path` that puts another schema with a
  same-name table first, `*` reads that schema's table in both phases (unchanged
  behaviour, now documented).

## Failure modes

| Case | Behaviour |
| --- | --- |
| Configured table, any case, any schema on `search_path` | Configured name in both phases, as before. Tested. |
| Schema-qualified entry (`public.orders`) | Open refused, 42P01, as before. Tested. |
| Reused publication streams a same-name table in another schema | That table's records say `<schema>.<table>`, empty key, own subjects; `WARN` once per relation. Tested (unit, integration with and without Avro, and the once-per-relation warning). Their deletes carry an empty key, as unconfigured tables' deletes already did on v0.21.0; emit-versus-refuse for unconfigured tables is a separate decision (kept as emit for now). |
| Reused publication streams an unrelated unconfigured table | Collection changes from `t` to `<schema>.t`. Release-noted. |
| Configured table moved with `ALTER TABLE ... SET SCHEMA` while running | Same OID: its changes keep the configured name and key column, and a delete with a missing key is still refused (invariant 3), as on v0.21.0. Tested (unit, and integration with an update and a delete). A restart re-resolves the name, and fails if the new schema is not on `search_path`. |
| Configured table renamed while running | Not matched by OID (its name changed): emitted as `<schema>.<new name>` with an empty key, like v0.21.0's bare new name with no key column. The `WARN` fires. A restart resolves the configured name again: it fails if no table has the name now, or picks the table that has it. |
| Rename-and-swap (T to T_old, a copy to T) | The retired T_old's changes are `<schema>.T_old` with an empty key, so its deletes never reach T's collection. The new T is matched by name if a publication streams it. The connector's own publication covers T_old, not the new T, until a restart. Tested (integration). |
| Configured table moved with `SET SCHEMA` and a new table created under its name, both streamed | Two live relations for one name: the change is refused with `postgres.source.configured_table_ambiguous` and nothing is emitted for it. After a restart the name resolves to the table that holds it, and the moved one is `<schema>.<table>`. Tested (unit in both orders, integration). |
| Configured table dropped and created again | New OID, same (namespace, name): configured name. Tested (unit). |
| On restart, a new same-name table earlier on `search_path` | The name resolves to the new table, the same as the snapshot and the primary key lookup. The original, if a publication still streams it, is `<schema>.<table>` with no key. Needs a DDL that changes what the configured name means; not handled. |
| Unconfigured table in a schema whose name is not a valid Avro name (`sales-eu`, `1x`) | Emitted: the subject is `sales-eu.<table>_payload` and the record name is the bare `<table>_payload`. Tested (unit). |
| Table dropped between Open and the iterator start | `ResolveTableNamespaces` fails the start; the pipeline retries. |
| A configured name that itself contains a dot (a table literally named `audit.users`) and an unconfigured `audit`.`users` in the same publication | Both would be `audit.users`. Needs a table name with a dot that equals another schema's qualified name; not handled. |
| Restart mid-transaction, replayed Relation messages | Naming is a function of the message and the map resolved at that start. A replay gives the same names when the resolution has not changed. When DDL changed what the configured name means (moved, renamed or swapped, then a restart), records replayed from before the restart can carry a different collection than when they were first delivered: for example a moved table's change, first delivered as `T`, replays as `<schema>.T` with no key. At-least-once redelivery of such a change is not idempotent downstream under collection routing. Not handled; it needs DDL on a configured table in the unacknowledged window. |

No ack, position or ordering path changes; records are emitted and acked exactly as
before.

## Upgrade and rollback

- Pipelines whose publication covers only `tables` (every pipeline whose publication
  the connector created): no change.
- Affected: pipelines that reuse a publication covering tables outside `tables`
  (created by hand, `FOR ALL TABLES`, `FOR TABLES IN SCHEMA`, or the import-mode
  publication when Debezium captured more tables). Those tables' CDC records change
  collection from `<table>` to `<schema>.<table>`, and their Avro subjects from
  `<table>_payload`/`_key` to `<schema>.<table>_payload`/`_key` (new subjects, version
  1). Their Avro record names do not change. Destinations routing on the collection
  see a new name; the Postgres destination with its default template will report
  `postgres.destination.table_not_found` for them.
- Configured tables moved to another schema while running keep their configured
  collection and key column, as on v0.21.0. Tables renamed while running are named
  `<schema>.<new name>` instead of the bare new name, with no key column either way.
- New stop: a pipeline whose moved configured table and a new table under the same
  name are both streamed stops with `postgres.source.configured_table_ambiguous` until
  restarted. On v0.21.0 both were delivered as the configured table.
- Rollback: install the previous version. Nothing persisted changes format; records
  return to the bare names.

Release note:

> **Source: tables outside `tables` are named `<schema>.<table>`.** A publication
> that already exists under `logrepl.publicationName` can stream tables that are not
> in `tables`. Their records now carry `opencdc.collection: <schema>.<table>` and the
> Avro subjects `<schema>.<table>_payload`/`_key`, and the connector logs
> `postgres.source.unconfigured_table` once per table. Before, they carried the bare
> table name, so a same-name table in another schema was merged into the configured
> table's collection and keyed by its key column (#424). Configured tables are
> unchanged: their snapshot and CDC records carry the configured name, also after an
> `ALTER TABLE ... SET SCHEMA` while the pipeline runs. A configured table renamed
> while the pipeline runs is named `<schema>.<new name>` from then on. If a moved
> configured table and a new table under its name are both streamed, the source now
> stops with `postgres.source.configured_table_ambiguous` (restart to resolve the name
> again) instead of delivering both as the configured table. If you rely on
> those extra tables, configure them in `tables`, or use a publication that covers
> only the configured tables.

## Observability

- `WARN` with `code: postgres.source.unconfigured_table`, `table` and `collection`,
  once per unconfigured relation per process.
- The collection and subjects themselves are visible on every record.
- The error `postgres.source.configured_table_ambiguous` names both relations (schema,
  name and relation ID) and never a value.

## Related

#424, #277 (schema-qualified table names), #251 (identifier quoting),
`20261012-debezium-offset-import.md` (import preflight, publication checks).

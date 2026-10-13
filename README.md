# Conduit Connector PostgreSQL

The PostgreSQL connector is a [Conduit](https://github.com/ConduitIO/conduit)
plugin. It provides both, a source and a destination PostgresSQL connector.

<!-- readmegen:description -->
## Source

The Postgres Source Connector connects to a database with the provided `url` and
starts creating records for each change detected in the provided tables.

Upon starting, the source takes a snapshot of the provided tables in the database,
then switches into CDC mode. In CDC mode, the plugin reads from a buffer of CDC events.

### Snapshot

When the connector first starts, snapshot mode is enabled. The connector acquires
a read-only lock on the tables, and then reads all rows of the tables into Conduit.
Once all rows in that initial snapshot are read the connector releases its lock and
switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding
`"snapshotMode": "never"` to the Source configuration.

### Change Data Capture

This connector implements Change Data Capture (CDC) features for PostgreSQL by
creating a logical replication slot and a publication that listens to changes in the
configured tables. Every detected change is converted into a record. If there are no
records available, the connector blocks until a record is available or the connector
receives a stop signal.

#### Logical Replication Configuration

When the connector switches to CDC mode, it attempts to run the initial setup commands
to create its logical replication slot and publication. It will connect to an existing
slot if one with the configured name exists.

The Postgres user specified in the connection URL must have sufficient privileges to
run all of these setup commands, or it will fail.

Example pipeline configuration that's using logical replication:

```yaml
version: 2.2
pipelines:
  - id: pg-to-log
    status: running
    connectors:
      - id: pg
        type: source
        plugin: builtin:postgres
        settings:
          url: "postgres://exampleuser:examplepass@localhost:5433/exampledb?sslmode=disable"
          tables: "users"
          cdcMode: "logrepl"
          logrepl.publicationName: "examplepub"
          logrepl.slotName: "exampleslot"
      - id: log
        type: destination
        plugin: builtin:log
        settings:
          level: info
```

:warning: When the connector or pipeline is deleted, the connector will automatically
attempt to delete the replication slot and publication. This is the default behaviour
and can be disabled by setting `logrepl.autoCleanup` to `false`.

### Key Handling

The connector will automatically look up the primary key column for the specified tables
and use them as the key value. If that can't be determined, the connector will return
an error.

The table's `REPLICA IDENTITY` must carry the primary key in deletes. `DEFAULT` and
`FULL` always do. `USING INDEX` does only if the index includes the primary-key
column; otherwise Postgres sends deletes without the key, and they cannot be applied
downstream. The source refuses to open on such a table with the error code
`postgres.replica_identity.excludes_key`. Fix it with
`ALTER TABLE <table> REPLICA IDENTITY FULL`, or use a unique index that includes the
primary key.

### Before Image of Updates and Deletes

`Payload.Before` is set only when Postgres sends the whole old row, which it does
under `REPLICA IDENTITY FULL`. Under `DEFAULT` and `USING INDEX`, Postgres sends only
the identity columns of the old row, so deletes and key-changing updates carry no
`Before`. The deleted row's key is in the record `Key`. A key-changing update carries
the new key in `Key`; the old key is not carried. If you need the old row or the old
key, set `REPLICA IDENTITY FULL` on the table.

:warning: The Postgres destination does not delete the old row when a primary key
changes. It upserts on the new key, so a primary-key-changing `UPDATE` leaves the row
under its old key in the destination next to the new one. This happens with or without
Avro and under `REPLICA IDENTITY FULL` as well. It is a known destination bug
([#386](https://github.com/ConduitIO/conduit-connector-postgres/issues/386)). Until it
is fixed, avoid changing primary keys on tables replicated to Postgres, or reconcile
the old rows by hand.

### Delete With a Missing Key

**Symptom.** The pipeline fails and keeps failing after restarts with
`postgres.replica_identity.excludes_key: delete on table "<table>" (WAL LSN <a>, in the
transaction that commits at LSN <b>) arrived without a value for key column ...`.
The replication slot retains WAL while it is stuck.

**Diagnosis.** The table's `REPLICA IDENTITY` was changed while the pipeline was
running, to `USING INDEX` on an index whose key columns do not include the primary key
(columns in `INCLUDE` do not count), or the identity index was dropped. Postgres then
sends the delete without the key. Check with
`SELECT relreplident FROM pg_class WHERE oid = '<table>'::regclass` (`i` is
`USING INDEX`).

**Remediation.**

1. Fix the identity so that new deletes carry the key:
   `ALTER TABLE <table> REPLICA IDENTITY FULL`, or `REPLICA IDENTITY DEFAULT`, or
   `REPLICA IDENTITY USING INDEX <index>` on a unique index that has the primary key
   among its key columns. This does not repair the change already in the WAL. A restart
   alone fails on it again.
2. Stop the pipeline. Postgres refuses to read or advance a slot while a connection
   uses it.
3. Find the row. The error carries no row values, because they would end up in logs.
   Read the stuck transaction from the slot instead, as a superuser or a role with
   `REPLICATION`. Peeking does not consume anything. `<c>` is the commit LSN `<b>`
   from the error with its low hex part incremented by one (`0/40C5318` becomes
   `0/40C5319`); the error prints it:

   ```sql
   SELECT xid, lsn, encode(data, 'escape') AS message
   FROM pg_logical_slot_peek_binary_changes(
          '<slot name>', '<c>', NULL,
          'proto_version', '1', 'publication_names', '<publication name>');
   ```

   Use the values of `logrepl.slotName` and `logrepl.publicationName` (defaults
   `conduitslot` and `conduitpub`).
   The output can start with earlier transactions that the pipeline already delivered
   but Postgres has not yet been told about (the connector reports its position
   periodically). They can contain the same value as the stuck delete, so do not read
   them. Read only the transaction (`xid`) that contains the `D` row whose `lsn` is the
   WAL LSN `<a>` in the error.
   Each row is one pgoutput message. `B` and `C` begin and commit the transaction. `R`
   names a table and its columns. `I`, `U` and `D` are the changes. A `K` tuple follows
   the table's object id: a 2-byte column count, then one entry per column, `n` for NULL
   or `t`, a 4-byte length and the text value. For this error the only non-null columns
   are those of the identity index, so the readable text is the value to look for, for
   example `222` in a `D` row that shows `K ... n t ... 222 n`. Postgres only accepts a
   unique index as a replica identity, so that value identifies the row in the
   destination. Only tables in the publication appear, which are the tables the pipeline
   captures.
4. Delete that row from the destination yourself. The change has no key, so the pipeline
   cannot do it. The other changes in the same transaction are in the same output
   (`I` and `U` carry the full new row as text). Apply the ones you need to the
   destination too, because the next step skips them. Step 5 also skips the earlier
   transactions the peek listed, so check that the destination has them.
5. Skip the transaction. Use the same `<c>` as in step 3. Writing it as a literal is
   deliberate: `'<b>'::pg_lsn + 1` only works from Postgres 14.
   - Postgres 11 or later:
     `SELECT pg_replication_slot_advance('<slot name>', '<c>');`
   - Postgres 10 has no such function. Consume the changes up to `<c>` instead. This
     skips exactly the same changes and needs no new snapshot:
     `SELECT count(*) FROM pg_logical_slot_get_binary_changes('<slot name>', '<c>', NULL, 'proto_version', '1', 'publication_names', '<publication name>');`

   Advancing to `<c>` is what skips the transaction. Advancing to the change LSN
   (`<a>`) or to the commit LSN `<b>` itself does not: Postgres replays the transaction
   and the pipeline fails on the same delete.
6. Start the pipeline.

**Skipping the transaction skips that change, and it skips the whole transaction,
including its other changes to captured tables.** Skipping data is your decision and the
connector cannot check it for you.

### Unchanged TOASTed Columns

Postgres stores large field values (`text`, `bytea`, `jsonb`, and other
variable-length types past a size threshold) out-of-line using
[TOAST](https://www.postgresql.org/docs/current/storage-toast.html). When a row is
updated but a TOASTed column is **not** changed, Postgres omits that column's value
from the logical-replication message to avoid re-transmitting large unchanged data.

For such columns, this connector **omits the field from the record payload** rather
than emitting it as `NULL`. Emitting `NULL` would be silent data corruption — a
downstream sink cannot distinguish "the value was set to NULL" from "the value did
not change". Omission preserves that distinction: absence means "unchanged".

What this means for your destination:

- **Partial-update / upsert sinks** (including this connector's own Postgres
  destination, which builds its `ON CONFLICT DO UPDATE SET` clause only from the
  fields present in the payload) handle omission correctly — an absent field is left
  untouched in the target row.
- **Full-row / full-document replace sinks** must treat an absent field as
  "unchanged" and preserve the existing target value. If a sink cannot do this, set
  `REPLICA IDENTITY FULL` on the source table. Under `REPLICA IDENTITY FULL` the old
  row image carries every column, and this connector backfills unchanged TOASTed
  values from it so the payload is always complete.

With the default `logrepl.withAvroSchema: true` and the default
`REPLICA IDENTITY`, an update that leaves a TOASTed column unchanged still stops the
source: the field is omitted, but the Avro payload schema requires it, so the record
cannot be encoded
([#383](https://github.com/ConduitIO/conduit-connector-postgres/issues/383)). Set
`REPLICA IDENTITY FULL` on such tables.

### Limitations

* **`TRUNCATE` is not propagated.** OpenCDC has no truncate operation, so a
  `TRUNCATE` on a published table produces no record: rows it removed remain in
  the destination. The connector logs a `WARN` ("TRUNCATE was NOT propagated")
  naming the table and the LSN, at most once a minute per table, with a
  `truncates_total` field (the connector exposes no metrics, so count these log
  lines) and a `suppressed` field for warnings the rate limit swallowed. Inserts,
  updates and deletes around the `TRUNCATE` are delivered as usual. A
  `truncate.policy` setting is planned for v0.21: `halt` (the default, stops the
  pipeline with an error naming the tables) or an explicit `skip` that only
  warns. Until then, treat the warning as a signal to reconcile the destination
  by hand.

## Destination

The Postgres Destination takes a Conduit record and stores it using a SQL statement.
The Destination is designed to handle different payloads and keys. Because of this,
each record is individually parsed and upserted.

### Handling record operations

Based on the `Operation` field in the record, the destination will either insert,
update or delete the record in the target table. Snapshot records are always inserted.

If the target table already contains a record with the same key as a record being
inserted, the record will be updated (upserted). This can overwrite and thus potentially
lose data, so keys should be assigned correctly from the Source.

If the target table does not contain a record with the same key as a record being
deleted, the record will be ignored.

If there is no key, the record will be simply appended.
<!-- /readmegen:description -->

## Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "postgres"
        settings:
          # Tables is a List of table names to read from, separated by a comma,
          # e.g.:"table1,table2". Use "*" if you'd like to listen to all tables.
          # Type: string
          # Required: yes
          tables: ""
          # URL is the connection string for the Postgres database.
          # Type: string
          # Required: yes
          url: ""
          # CDCMode determines how the connector should listen to changes.
          # Type: string
          # Required: no
          cdcMode: "auto"
          # LogreplAutoCleanup determines if the replication slot and
          # publication should be removed when the connector is deleted.
          # Type: bool
          # Required: no
          logrepl.autoCleanup: "true"
          # LogreplPublicationName determines the publication name in case the
          # connector uses logical replication to listen to changes (see
          # CDCMode).
          # Type: string
          # Required: no
          logrepl.publicationName: "conduitpub"
          # LogreplSlotName determines the replication slot name in case the
          # connector uses logical replication to listen to changes (see
          # CDCMode). Can only contain lower-case letters, numbers, and the
          # underscore character.
          # Type: string
          # Required: no
          logrepl.slotName: "conduitslot"
          # WithAvroSchema determines whether the connector should attach an
          # avro schema on each record.
          # Type: bool
          # Required: no
          logrepl.withAvroSchema: "true"
          # Snapshot fetcher size determines the number of rows to retrieve at a
          # time.
          # Type: int
          # Required: no
          snapshot.fetchSize: "50000"
          # SnapshotMode is whether the plugin will take a snapshot of the
          # entire table before starting cdc mode.
          # Type: string
          # Required: no
          snapshotMode: "initial"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "false"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "false"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

## Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "postgres"
        settings:
          # URL is the connection string for the Postgres database.
          # Type: string
          # Required: yes
          url: ""
          # Table is used as the target table into which records are inserted.
          # Type: string
          # Required: no
          table: "{{ index .Metadata "opencdc.collection" }}"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## Testing

Run `make test` to run all the unit and integration tests, which require Docker
to be installed and running. The command will handle starting and stopping
docker containers for you.

## References

- https://github.com/bitnami/bitnami-docker-postgresql-repmgr
- https://github.com/Masterminds/squirrel

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=1423de19-24e7-4d64-91cf-0b893ca28cc6)

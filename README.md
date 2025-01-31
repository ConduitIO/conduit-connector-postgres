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
          logrepl.slotName": "exampleslot"
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
          # URL is the connection string for the Postgres database.
          # Type: string
          url: ""
          # CDCMode determines how the connector should listen to changes.
          # Type: string
          cdcMode: "auto"
          # LogreplAutoCleanup determines if the replication slot and
          # publication should be removed when the connector is deleted.
          # Type: bool
          logrepl.autoCleanup: "true"
          # LogreplPublicationName determines the publication name in case the
          # connector uses logical replication to listen to changes (see
          # CDCMode).
          # Type: string
          logrepl.publicationName: "conduitpub"
          # LogreplSlotName determines the replication slot name in case the
          # connector uses logical replication to listen to changes (see
          # CDCMode).
          # Type: string
          logrepl.slotName: "conduitslot"
          # WithAvroSchema determines whether the connector should attach an
          # avro schema on each record.
          # Type: bool
          logrepl.withAvroSchema: "false"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          sdk.schema.extract.key.enabled: "false"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          sdk.schema.extract.payload.enabled: "false"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          sdk.schema.extract.type: "avro"
          # Snapshot fetcher size determines the number of rows to retrieve at a
          # time.
          # Type: int
          snapshot.fetchSize: "50000"
          # SnapshotMode is whether the plugin will take a snapshot of the
          # entire table before starting cdc mode.
          # Type: string
          snapshotMode: "initial"
          # Deprecated: use `tables` instead.
          # Type: string
          table: ""
          # Tables is a List of table names to read from, separated by a comma,
          # e.g.:"table1,table2". Use "*" if you'd like to listen to all tables.
          # Type: string
          tables: ""
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
          url: ""
          # Key represents the column name for the key used to identify and
          # update existing rows.
          # Type: string
          key: ""
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          sdk.schema.extract.payload.enabled: "true"
          # Table is used as the target table into which records are inserted.
          # Type: string
          table: "{{ index .Metadata "opencdc.collection" }}"
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

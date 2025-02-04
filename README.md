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

<!-- readmegen:source.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Required</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

`url`

</td>
<td>

string

</td>
<td>

✅

</td>
<td>

``

</td>
<td>

URL is the connection string for the Postgres database.

</td>
  </tr>
  <tr>
<td>

`cdcMode`

</td>
<td>

string

</td>
<td>



</td>
<td>

`auto`

</td>
<td>

CDCMode determines how the connector should listen to changes.

</td>
  </tr>
  <tr>
<td>

`logrepl.autoCleanup`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`true`

</td>
<td>

LogreplAutoCleanup determines if the replication slot and publication should be
removed when the connector is deleted.

</td>
  </tr>
  <tr>
<td>

`logrepl.publicationName`

</td>
<td>

string

</td>
<td>



</td>
<td>

`conduitpub`

</td>
<td>

LogreplPublicationName determines the publication name in case the
connector uses logical replication to listen to changes (see CDCMode).

</td>
  </tr>
  <tr>
<td>

`logrepl.slotName`

</td>
<td>

string

</td>
<td>



</td>
<td>

`conduitslot`

</td>
<td>

LogreplSlotName determines the replication slot name in case the
connector uses logical replication to listen to changes (see CDCMode).

</td>
  </tr>
  <tr>
<td>

`logrepl.withAvroSchema`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`true`

</td>
<td>

WithAvroSchema determines whether the connector should attach an avro schema on each
record.

</td>
  </tr>
  <tr>
<td>

`sdk.batch.delay`

</td>
<td>

duration

</td>
<td>



</td>
<td>

`0`

</td>
<td>

Maximum delay before an incomplete batch is read from the source.

</td>
  </tr>
  <tr>
<td>

`sdk.batch.size`

</td>
<td>

int

</td>
<td>



</td>
<td>

`0`

</td>
<td>

Maximum size of batch before it gets read from the source.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.context.enabled`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`true`

</td>
<td>

Specifies whether to use a schema context name. If set to false, no schema context name will
be used, and schemas will be saved with the subject name specified in the connector
(not safe because of name conflicts).

</td>
  </tr>
  <tr>
<td>

`sdk.schema.context.name`

</td>
<td>

string

</td>
<td>



</td>
<td>

``

</td>
<td>

Schema context name to be used. Used as a prefix for all schema subject names.
If empty, defaults to the connector ID.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.key.enabled`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`false`

</td>
<td>

Whether to extract and encode the record key with a schema.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.key.subject`

</td>
<td>

string

</td>
<td>



</td>
<td>

`key`

</td>
<td>

The subject of the key schema. If the record metadata contains the field
"opencdc.collection" it is prepended to the subject name and separated
with a dot.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.payload.enabled`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`false`

</td>
<td>

Whether to extract and encode the record payload with a schema.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.payload.subject`

</td>
<td>

string

</td>
<td>



</td>
<td>

`payload`

</td>
<td>

The subject of the payload schema. If the record metadata contains the
field "opencdc.collection" it is prepended to the subject name and
separated with a dot.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.type`

</td>
<td>

string

</td>
<td>



</td>
<td>

`avro`

</td>
<td>

The type of the payload schema.

</td>
  </tr>
  <tr>
<td>

`snapshot.fetchSize`

</td>
<td>

int

</td>
<td>



</td>
<td>

`50000`

</td>
<td>

Snapshot fetcher size determines the number of rows to retrieve at a time.

</td>
  </tr>
  <tr>
<td>

`snapshotMode`

</td>
<td>

string

</td>
<td>



</td>
<td>

`initial`

</td>
<td>

SnapshotMode is whether the plugin will take a snapshot of the entire table before starting cdc mode.

</td>
  </tr>
  <tr>
<td>

`table`

</td>
<td>

string

</td>
<td>



</td>
<td>

``

</td>
<td>

Deprecated: use `tables` instead.

</td>
  </tr>
  <tr>
<td>

`tables`

</td>
<td>

string

</td>
<td>



</td>
<td>

``

</td>
<td>

Tables is a List of table names to read from, separated by a comma, e.g.:"table1,table2".
Use "*" if you'd like to listen to all tables.

</td>
  </tr>
</table>
<!-- /readmegen:source.parameters.table -->

## Destination Configuration Parameters

<!-- readmegen:destination.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Required</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

`url`

</td>
<td>

string

</td>
<td>

✅

</td>
<td>

``

</td>
<td>

URL is the connection string for the Postgres database.

</td>
  </tr>
  <tr>
<td>

`key`

</td>
<td>

string

</td>
<td>



</td>
<td>

``

</td>
<td>

Key represents the column name for the key used to identify and update existing rows.

</td>
  </tr>
  <tr>
<td>

`sdk.batch.delay`

</td>
<td>

duration

</td>
<td>



</td>
<td>

`0`

</td>
<td>

Maximum delay before an incomplete batch is written to the destination.

</td>
  </tr>
  <tr>
<td>

`sdk.batch.size`

</td>
<td>

int

</td>
<td>



</td>
<td>

`0`

</td>
<td>

Maximum size of batch before it gets written to the destination.

</td>
  </tr>
  <tr>
<td>

`sdk.rate.burst`

</td>
<td>

int

</td>
<td>



</td>
<td>

`0`

</td>
<td>

Allow bursts of at most X records (0 or less means that bursts are not
limited). Only takes effect if a rate limit per second is set. Note that
if `sdk.batch.size` is bigger than `sdk.rate.burst`, the effective batch
size will be equal to `sdk.rate.burst`.

</td>
  </tr>
  <tr>
<td>

`sdk.rate.perSecond`

</td>
<td>

float

</td>
<td>



</td>
<td>

`0`

</td>
<td>

Maximum number of records written per second (0 means no rate limit).

</td>
  </tr>
  <tr>
<td>

`sdk.record.format`

</td>
<td>

string

</td>
<td>



</td>
<td>

`opencdc/json`

</td>
<td>

The format of the output record. See the Conduit documentation for a full
list of supported formats (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).

</td>
  </tr>
  <tr>
<td>

`sdk.record.format.options`

</td>
<td>

string

</td>
<td>



</td>
<td>

``

</td>
<td>

Options to configure the chosen output record format. Options are normally
key=value pairs separated with comma (e.g. opt1=val2,opt2=val2), except
for the `template` record format, where options are a Go template.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.key.enabled`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`true`

</td>
<td>

Whether to extract and decode the record key with a schema.

</td>
  </tr>
  <tr>
<td>

`sdk.schema.extract.payload.enabled`

</td>
<td>

bool

</td>
<td>



</td>
<td>

`true`

</td>
<td>

Whether to extract and decode the record payload with a schema.

</td>
  </tr>
  <tr>
<td>

`table`

</td>
<td>

string

</td>
<td>



</td>
<td>

`{{ index .Metadata "opencdc.collection" }}`

</td>
<td>

Table is used as the target table into which records are inserted.

</td>
  </tr>
</table>
<!-- /readmegen:destination.parameters.table -->

## Testing

Run `make test` to run all the unit and integration tests, which require Docker
to be installed and running. The command will handle starting and stopping
docker containers for you.

## References

- https://github.com/bitnami/bitnami-docker-postgresql-repmgr
- https://github.com/Masterminds/squirrel

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=1423de19-24e7-4d64-91cf-0b893ca28cc6)

# Conduit Connector PostgreSQL
![scarf pixel](https://static.scarf.sh/a.png?x-pxid=1423de19-24e7-4d64-91cf-0b893ca28cc6)

The PostgreSQL connector is a [Conduit](https://github.com/ConduitIO/conduit) plugin. It provides both, a source
and a destination PostgresSQL connectors.

# Source

The Postgres Source Connector connects to a database with the provided `url` and starts creating records for each change
detected in the provided tables.

Upon starting, the source takes a snapshot of the provided tables in the database, then switches into CDC mode. In CDC mode,
the plugin reads from a buffer of CDC events.

## Snapshot Capture

When the connector first starts, snapshot mode is enabled. The connector acquires a read-only lock on the tables, and
then reads all rows of the tables into Conduit. Once all rows in that initial snapshot are read the connector releases
its lock and switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding `"snapshotMode":"never"` to the Source
configuration.

## Change Data Capture

This connector implements CDC features for PostgreSQL by creating a logical replication slot and a publication that
listens to changes in the configured tables. Every detected change is converted into a record and returned in the call to
`Read`. If there is no record available at the moment `Read` is called, it blocks until a record is available or the
connector receives a stop signal.

### Logical Replication Configuration

When the connector switches to CDC mode, it attempts to run the initial setup commands to create its logical replication
slot and publication. It will connect to an existing slot if one with the configured name exists.

The Postgres user specified in the connection URL must have sufficient privileges to run all of these setup commands, or
it will fail.

Example configuration for CDC features:

```json
{
  "url": "url",
  "tables": "records",
  "cdcMode": "logrepl",
  "logrepl.publicationName": "meroxademo",
  "logrepl.slotName": "meroxademo"
}
```

:warning: When the connector or pipeline is deleted, the connector will automatically attempt to delete the replication slot and publication. This is the default behaviour and can be disabled by setting `logrepl.autoCleanup` to `false`.

## Key Handling

The connector will automatically look up the primary key column for the specified tables. If that can't be determined,
the connector will return an error.

## Configuration Options

| name                      | description                                                                                                                                | required | default       |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| `url`                     | Connection string for the Postgres database.                                                                                               | true     |               |
| `tables`                  | List of table names to read from, separated by comma. Example: `"employees,offices,payments"`. Using `*` will read from all public tables. | true     |               |
| `snapshotMode`            | Whether or not the plugin will take a snapshot of the entire table before starting cdc mode (allowed values: `initial` or `never`).        | false    | `initial`     |
| `cdcMode`                 | Determines the CDC mode (allowed values: `auto`, `logrepl` or `long_polling`).                                                             | false    | `auto`        |
| `logrepl.publicationName` | Name of the publication to listen for WAL events.                                                                                          | false    | `conduitpub`  |
| `logrepl.slotName`        | Name of the slot opened for replication events.                                                                                            | false    | `conduitslot` |
| `logrepl.autoCleanup`     | Whether or not to cleanup the replication slot and pub when connector is deleted                                                                                            | false    | `true` |
| ~~`table`~~               | List of table names to read from, separated by comma. **Deprecated: use `tables` instead.**                                                | false    |               |

# Destination

The Postgres Destination takes a `record.Record` and parses it into a valid SQL query. The Destination is designed to
handle different payloads and keys. Because of this, each record is individually parsed and upserted.

## Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can overwrite and thus potentially lose data, so keys should be assigned
correctly from the Source.

If there is no key, the record will be simply appended.

## Configuration Options

| name    | description                                                                                                                                                                           | required | default                                      |
|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------------------------------------------|
| `url`   | Connection string for the Postgres database.                                                                                                                                          | true     |                                              |
| `table` | Table name. It can contain a Go template that will be executed for each record to determine the table. By default, the table is the value of the `opencdc.collection` metadata field. | false    | `{{ index .Metadata "opencdc.collection" }}` |

# Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command
will handle starting and stopping docker containers for you.

# References

- https://github.com/bitnami/bitnami-docker-postgresql-repmgr
- https://github.com/Masterminds/squirrel

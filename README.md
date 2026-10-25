# Conduit Connector PostgreSQL

The PostgreSQL connector is a [Conduit](https://github.com/ConduitIO/conduit)
plugin. It provides both, a source and a destination PostgresSQL connector.

<!-- readmegen:description -->
## Connection and SCRAM authentication

Source and destination connect with the `url` setting. Over TLS with SCRAM
authentication, the connector does not use channel binding unless you ask for it:
`channel_binding=prefer`, the default of the pgx driver since 5.9, is mapped to
`disable`, which is how the driver behaved in earlier releases. Channel binding
(SCRAM-SHA-256-PLUS) fails against a server with an Ed25519 certificate and behind
a proxy that terminates TLS and passes the SCRAM exchange through. Add
`channel_binding=require` to the URL, or set `PGCHANNELBINDING=require` in the
environment of the Conduit process, to require it. The driver reads
`PGCHANNELBINDING` when the URL has no `channel_binding`. An explicit `prefer`,
in the URL or in the environment, is also mapped to `disable`; `require` and
`disable` are kept.

```
postgres://user:pass@host:5432/db?sslmode=require&channel_binding=require
```

### Dialer supplied by the host

If the process that runs the connector puts a dialer on the context (see
`github.com/conduitio/conduit-commons/dialer`), the connector opens every
network connection through it: the destination connection and its reconnects,
and for the source the pool connections, the snapshot reads, the replication
connection, and the slot and publication cleanup. Host
names in the URL are then passed to the dialer unresolved, so it decides how to
resolve them. Without a host dialer, nothing changes: the driver dials and
resolves the addresses itself. A connector setting cannot supply the dialer.

### Upgrading from v0.14.x: connection string parsing

This version uses pgx 5.11, which parses connection strings like libpq does.
Most URLs are unaffected. Check yours if it relies on one of these (the full
list is in the pgx 5.11.0 changelog):

- `+` in a query value is literal, no longer a space; encode a space as `%20`.
- If a query parameter is repeated, the last one wins (the first used to).
- An IPv6 host in a URL needs brackets: `postgres://[::1]/db`.
- In a `key=value` connection string a backslash escapes the next character,
  so a Windows path must be written `sslcert=C:\\path\\to\\cert`.
- Default locations of the client certificate, key and `.pgpass` file are
  resolved from `$HOME` instead of the OS account's home directory.

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

#### Resuming after the replication slot was lost

The slot is what keeps Postgres from discarding the changes the pipeline has not
acknowledged yet. If the pipeline has a saved position and its slot no longer
exists, a new slot starts at the current WAL and every change in between is gone.
This happens when the slot was dropped by hand, removed by `logrepl.autoCleanup`
while the position was kept, invalidated and then dropped (`max_slot_wal_keep_size`),
or not carried over by a failover or promotion to another server (slots are not
copied to a standby before Postgres 17, or without `failover=true`). The connector
does not carry on from there. It refuses to start with the error code
`postgres.source.slot_wal_gap`, which names the slot, the position the pipeline
stopped at and the WAL position a new slot would start at. The same applies to a
position saved during an initial snapshot. A pipeline with no position (a fresh
start) creates its slot as usual.

To recover, first check that `logrepl.slotName` is the name the pipeline has
always used. Then re-snapshot: stop the pipeline, delete the source connector and
create it again so it has no position. With `snapshotMode: initial` every table is
read again and every row is delivered again, so the destination must tolerate
duplicates (an upsert or keyed destination does).

To accept the loss instead, set `logrepl.allowWALGap` to `true`. This is data
loss: the skipped changes are never delivered, and the connector logs a warning
when it takes the setting. Remove it after the pipeline has resumed, or the next
lost slot is accepted with a warning only.

#### Resuming on a replication slot that was replaced

A slot dropped and created again under the same name exists and is healthy, but it
starts at the WAL of its creation, so the changes between the saved position and
then are gone just the same. Postgres gives a slot no identity to tell it from the
original, so the connector keeps one itself (since v0.17.5). Every position carries
a random slot identity, and the connector writes it into the WAL as a logical
decoding message under the prefix `conduit.postgres.slot_identity` (a beacon). A
beacon also names a WAL position before which the pipeline needed nothing more when
it was written: every change committed before it had been delivered and acked (or,
for the beacon written when a run opens the slot, the slot position that run
checked). The connector never advances an idle slot past that position of the last
beacon it received, so its own slot always still holds a beacon that covers it. On
a resume whose slot is past the saved position, the connector reads the slot without
consuming it (`pg_logical_slot_peek_binary_changes`) and looks for a beacon written
after the slot's `confirmed_flush_lsn` whose position is at or past it. A slot that
holds none is refused with the error code `postgres.source.slot_replaced`, which
names the slot, the saved position and the slot's `confirmed_flush_lsn`. The same
refusal covers the pipeline's own slot when another client advanced or consumed it
(`pg_replication_slot_advance`, `pg_logical_slot_get_changes`) past changes the
pipeline had not delivered, including changes the pipeline had received but not
acked when it stopped. A slot another client moved only over changes the pipeline
had already delivered and acked loses nothing and is accepted. A slot swapped between the check (or, on a fresh start, its creation) and the
start of streaming is refused once streaming holds it, also after a snapshot of hours.
A beacon is written only while this pipeline's own stream holds the slot (or, before
streaming starts, while the slot is idle and where the pipeline checked it), so a beacon
can never land in a slot that replaced it. Recover as from a lost slot: re-snapshot, or
accept the loss with `logrepl.allowWALGap: true` for one start.

The check also refuses a position written on another WAL history, with
`postgres.source.wal_history_changed`: every position names the cluster's system
identifier and timeline, so a different server at the same address, a point-in-time
recovery or failover that left the position's timeline before the position, a restore
to an earlier timeline, and a server whose WAL is behind the position (a restored backup,
snapshot or VM image) are refused instead of resuming at LSNs that now name other
changes. A restore on the same timeline whose new WAL has already passed the position
when the pipeline starts is caught by the position's transaction: every position also
names the xid and commit time of the transaction it was taken in (for a schema drift
marker, of the transaction it halted before), and a resume whose slot is at or below
the position checks that the slot still decodes that transaction, with the same xid and
commit time, at that commit LSN. A position written before v0.17.5 names no xid; the
check then requires a transaction committing at its commit LSN. On Postgres 15+, a
publication or row filter changed since the position can hide the transaction and
refuse a database that was not restored; start once with `logrepl.allowWALGap: true`
then. Re-snapshot pipelines after restoring their source database.

What it needs and what it costs:

- The connector's role must be able to call `pg_logical_emit_message`, which
  Postgres grants to `PUBLIC` by default. If it was revoked, the connector refuses to
  start with `postgres.source.slot_beacon_failed`; grant it back with
  `GRANT EXECUTE ON FUNCTION pg_logical_emit_message(boolean, text, text) TO <role>`
  (Postgres 14 to 16) or
  `GRANT EXECUTE ON FUNCTION pg_logical_emit_message(boolean, text, text, boolean) TO <role>`
  (Postgres 17+). Peeking needs `REPLICATION`, which the connector already needs.
- A beacon holds a random nonce, a WAL position and an HMAC-SHA256 made with a secret
  key that exists only in the pipeline's position (Conduit's position store). The key
  never reaches the database or a log (logs show an 8-digit fingerprint), so a role that
  can call `pg_logical_emit_message` cannot forge a beacon, and a genuine one copied into
  a replaced slot is refused. Treat the position store as sensitive. A beacon holds no
  row data and no configuration.
- Hardening: `pg_logical_emit_message` lets any role that can connect write messages into
  the WAL. Consider `REVOKE EXECUTE ON FUNCTION <signature above> FROM PUBLIC` together
  with the `GRANT` to the connector's role; the connector logs
  `postgres.source.slot_beacon_public` at start while PUBLIC still has it.
- **Who can read the key.** Records carry their positions, Conduit hands them to
  destinations, and its API returns the source's position (`sourceState.position`). So
  the key can be read wherever records or that API are, for example in a Kafka topic of
  OpenCDC records. Forging a beacon with it still needs the rights to drop and recreate
  the slot (`REPLICATION` or superuser) and to write into the WAL.
- `logrepl.allowWALGap` gives the slot it accepts a new key. **Remove `allowWALGap` only
  after a record has been delivered**: until a position with the new key is saved, a
  restart from the old position is refused again, so an attempt that created the slot and
  failed before saving never passes for the original. On an idle source that can take a
  while; write a row to a captured table to get a record through.
- Other logical consumers of the database that decode messages, such as Debezium with
  messages enabled, see the beacons; Debezium can filter them with
  `message.prefix.exclude.list`.
- One small transaction per start, and at most one per status interval (10 seconds)
  while the pipeline is idle, after it acked records or once the server has written at
  least 512 bytes of WAL since the last beacon. An idle database stays idle. An idle
  slot reaches the WAL end as of the last beacon about two status intervals after the
  last ack (the beacon itself, and WAL written after it, stay until the next one). Beacons commit
  with `synchronous_commit = local`, so a synchronous standby that is down does not block
  them.
- If beacons cannot be written while the pipeline runs (the connector logs
  `postgres.source.slot_beacon_unavailable` with `retained_wal_bytes`), the idle slot is
  held at the last beacon for at most `logrepl.slotIdentity.beaconFailureTimeout`
  (default 5 minutes; the WAL held back is at most that time's worth). Then the slot
  advances without beacons, so its WAL cannot fill the disk, and the connector logs the
  same code as an error. Until a beacon is written and delivered again, a restart is
  refused with `slot_replaced`. The connector keeps trying: fix the cause (usually the
  `GRANT` above) while the pipeline runs, and it recovers on its own (it logs that the
  slot can be proven again). If the pipeline was stopped in that state, start it once
  with `logrepl.allowWALGap: true`.
- The check reads the slot from its `restart_lsn`; a long-running transaction can hold
  that far back and make it slow, and so can a large backlog: it reads as far as it must,
  never giving up early. `logrepl.slotIdentity.checkTimeout` (default 10 minutes) bounds
  it; a check that times out or cannot read the slot refuses the start with
  `postgres.source.slot_identity_check_failed`, which is retried. Raise the timeout, or
  start once with `logrepl.allowWALGap: true` to take the slot unproven.
- Postgres 13 cannot send logical decoding messages to the connector, and a standby
  cannot write them. There positions carry no identity and the check is the one before
  v0.17.5: the connector logs `postgres.source.slot_identity_unsupported` and resumes on
  a slot past the position. A position that does carry an identity (written on a
  primary) is never accepted unproven there: it is refused with
  `postgres.source.slot_identity_unprovable`. A slot on a standby is a slot of the
  standby, never the primary's, even when it can decode the primary's beacons.

Upgrading from v0.17.4 or earlier:

- **Before the upgrade**, check that the connector's role may call
  `pg_logical_emit_message` (only databases that revoked it from PUBLIC are affected):
  `SELECT has_function_privilege('<role>', 'pg_logical_emit_message(boolean, text, text, boolean)', 'EXECUTE');`
  on Postgres 17+, or with `(boolean, text, text)` on 14 to 16. If it returns false,
  grant it, or the first start fails with `postgres.source.slot_beacon_failed`.
- A saved position from those versions has no slot identity. If the slot is not past
  it, it is proven by the position alone. Otherwise (the usual case, since older
  versions advance an idle slot to the WAL end) the connector cannot tell whether the
  slot was replaced before the upgrade. It takes the slot over and logs
  `postgres.source.slot_identity_unverified` with `unproven_wal_bytes`, the stretch of
  WAL nothing proves. **Protection begins at the first record saved after the
  upgrade**; until then a restart adopts the slot again with the same warning.
- Such a position also does not name the WAL history it was written on, so the first
  start logs `postgres.source.wal_history_unverified` (with a fingerprint of the server's
  system identifier and its timeline): a restore, point-in-time recovery or another server
  at the same address before the upgrade would go unnoticed. That protection, too, starts
  once the next position is saved.
- If anyone dropped and recreated a pipeline's slot, or restored or replaced its source
  database, since that pipeline last delivered a record, re-snapshot that pipeline: the
  changes in between may be gone, and nothing can tell after the fact.

Failover (Postgres 17+, `failover = true` slots synchronized to a standby): the
identity survives a promotion when the primary lists the standby's physical slot in
`synchronized_standby_slots`, the setting Postgres requires for a failover that keeps
logical consumers consistent. Without it the standby can lag behind the beacon the
slot was advanced to, and the first start after a promotion can be refused with
`postgres.source.slot_replaced` (no data is lost; check the slot and start once with
`logrepl.allowWALGap: true`). After a promotion, set `synchronized_standby_slots` on
the new primary to slots that exist there: decoding a failover slot waits for every
slot it names.

After a `pg_upgrade`: the new cluster can have another system identifier, and the
migrated slot is a new slot, so the start is refused with
`postgres.source.wal_history_changed` or `postgres.source.slot_replaced`. pg_upgrade
migrates logical slots only once they decoded all WAL. If the pipeline had delivered
every change before the upgrade, nothing is lost: start once with
`logrepl.allowWALGap: true` (the warning says `WAL HISTORY CHANGE ACCEPTED`), and remove
it after a record has been delivered. Otherwise re-snapshot.

Moving the slot by hand: `pg_upgrade` (which recreates logical slots), a manual
`pg_replication_slot_advance`, and a rollback to v0.17.4 or earlier that ran while
the pipeline was idle all move the slot past the last beacon. The next start is
refused with `postgres.source.slot_replaced`. When you know nothing was skipped,
start once with `logrepl.allowWALGap: true` and remove it after the pipeline resumed.
`pg_upgrade` also refuses to migrate a slot that has not decoded all WAL, and the
latest beacon is always ahead of the slot. Advance the stopped pipeline's slot with
`pg_replication_slot_advance` before the upgrade, and only once the pipeline has
delivered every change to the destination.

#### Invalidated replication slots

Postgres can invalidate a slot that fell too far behind (`max_slot_wal_keep_size`):
the slot still exists, but `wal_status` is `lost` (and, on Postgres 17+,
`invalidation_reason` is set), the WAL it needs is removed and it cannot be
streamed from. The connector refuses to start on it, with a fresh start or a saved
position, with the error code `postgres.source.slot_invalidated`. `logrepl.allowWALGap`
does not apply. Drop the slot (`SELECT pg_drop_replication_slot('<slot>')`), then
re-snapshot as above; a pipeline with no position only needs the slot dropped.

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
   cannot do it. Then work out what else step 5 would skip. It skips every change the
   peek lists before the stuck one, and the other changes of the stuck transaction.
   Postgres has not been told they are done, and the pipeline may have read them
   without the destination writing them before it stopped. Get the source connector's
   last acknowledged position from Conduit:
   `GET /v1/connectors/<source connector id>`, field `sourceState.position`. It is
   base64 of JSON, for example
   `{"version":2,"type":2,"last_lsn":"0/40845E8","tx_commit_lsn":"0/40846E8","tx_seq":1}`
   (decode it with `base64 -d`). Conduit sets it only when the destination acknowledges a
   record, never when the source reads one, so `last_lsn` is the WAL LSN of the last
   change the destination has confirmed. If Conduit was restarted since the pipeline
   stopped, it reports the last position it saved, which can be slightly older; that errs
   on the safe side. Find the peek row of kind `I`, `U` or `D` whose `lsn` equals
   `last_lsn`. Ignore `B` and `C` rows: a `C` row can show the same number as the first
   change after it. If several rows share that `lsn` (a `COPY` or another statement
   that changes many rows can do that), use `tx_seq`. It numbers the transaction's
   `I`, `U`, `D` and `T` (truncate) rows in the order listed, starting at 1 after its
   `B` row. `R` (relation), `Y` (type) and `O` (origin) rows are not counted. The row
   with that number is the one to match, and it is an `I`, `U` or `D` row. The matched row
   itself and everything listed before it are in the destination. For each change
   listed after it, up to the stuck one and including the rest of the stuck transaction,
   check the destination for its rows and apply any that are missing by hand (`I` and
   `U` carry the full new row; `D` carries the identity columns). If no row matches,
   treat every listed change as unconfirmed. A position without `last_lsn` (a pipeline
   still in its snapshot) matches nothing.
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
6. Start the pipeline. The advance moved the slot past the pipeline's saved position,
   so the connector can refuse it with `postgres.source.slot_replaced` (see "Resuming
   on a replication slot that was replaced"). If it does, start it once with
   `logrepl.allowWALGap: true`, and remove the setting once it runs.

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

### Value Representation

Values do not depend on the time zone of the machine running the connector or on the
version of the Postgres driver. The same row gives the same record everywhere, for
snapshot and CDC.

| Postgres type | Without an Avro schema | With an Avro schema (`logrepl.withAvroSchema`, the default) |
| --- | --- | --- |
| `timestamp` | The wall clock stored in Postgres, as a time with no offset information (`2024-01-02T03:04:05Z` for `2024-01-02 03:04:05`). | `local-timestamp-micros` of the same wall clock. |
| `timestamptz` | The instant, in UTC (`Z`). | `timestamp-micros`. |
| `date` | Midnight UTC of the date. | `date`. |
| `tsvector` | Postgres text form, `'a':1 'cat':3A,4B`. | `string`, same text. |
| `infinity`, `-infinity` | The strings `infinity` and `-infinity`. | The source halts with error code `postgres.source.temporal_not_representable`. |
| Years before 1 BC or after 9999 AD | Postgres text, for example `0044-03-15 12:00:00 BC`. | As above, up to what 64-bit microseconds hold; beyond that the same error. |
| Arrays, ranges, multiranges of these | Element and bound values as above. | Not supported by the Avro schema. |

A range or multirange is a JSON object `{"Lower":...,"Upper":...,"LowerType":105,
"UpperType":101,"Valid":true}`, and a multirange is an array of them. `LowerType` and
`UpperType` are the ASCII codes of the bound kind: `105` (`i`) inclusive, `101` (`e`)
exclusive, `85` (`U`) unbounded (the bound is `null`), `69` (`E`) empty range. Bounds
follow the rules above. Arrays of `tsmultirange`, `tstzmultirange` and `datemultirange`
are emitted as the Postgres text of the array.

If the source halts with `postgres.source.temporal_not_representable`, set
`logrepl.withAvroSchema` to `false`. Removing the row does not help a change that is
already in the WAL, because it is read again after every restart.

### Numeric Columns

A `numeric` column with a precision and scale, `numeric(p,s)`, is an Avro
`decimal(p,s)` and its values are `big.Rat`.

A `numeric` column **without** a precision and scale can hold any value Postgres
can store, which no Avro `decimal` can. With `logrepl.withAvroSchema: true` (the
default) it is an Avro `string` holding the exact text of the value, with its
scale (`1.50` stays `1.50`), and `NaN`, `Infinity` and
`-Infinity` as those three words: **a consumer of such a column must handle
them**, a plain decimal parser will reject them. Earlier versions used `decimal(65535, 65531)`, which could not encode a
value of 10000 or more and stopped the pipeline. The first record after an
upgrade registers a new schema version for the table; consumers must read the
field as a string. This connector's destination writes the string to a `numeric`
column exactly.

**Known issue:** a zero with a display scale reads as `0` in the snapshot and as
`0.000` (its stored text) in change data capture. The values are numerically
equal; only the text differs. Tracked in
[#391](https://github.com/ConduitIO/conduit-connector-postgres/issues/391).

So the Go type of an unconstrained `numeric` depends on the setting: a `string`
with `logrepl.withAvroSchema: true`, a `big.Rat` with `false`. Changing a column
between `text`/`varchar` and an unconstrained numeric is accepted by the
`evolve` schema-drift policy only when `withAvroSchema` is `true` (the Avro
shape does not change); with `false` the value's type changes, so it halts.

Without an Avro schema, a `numeric` is a `big.Rat`; `NaN`, `Infinity` and
`-Infinity` are the strings `"NaN"`, `"Infinity"` and `"-Infinity"`, and the same
applies to the elements of `numeric[]` (finite elements are unchanged). A `NULL`
stays `NULL`.

`NaN` in a `numeric(p,s)` column (the infinities cannot be stored there) cannot
be an Avro decimal, and the connector never replaces it with `NULL` or a number:
the source stops with the error code `postgres.source.numeric_not_representable`,
which names the table, the column, the value and the row's key. To continue:

- **Change data capture:** restart once with `logrepl.withAvroSchema: false`
  from the same position. The value is delivered as a string. **Then set it
  back to `true` as soon as that record has been delivered.** Without an Avro
  schema a finite `numeric` is a `big.Rat`, which a standalone connector
  cannot send to Conduit (#378): the next finite numeric would stop the
  pipeline. Built-in pipelines are not affected.
  On a standalone connector the SDK also fails the whole read batch on the
  first record it cannot send, so if the NaN/Infinity record or its batch
  contains any finite numeric, it is never delivered with Avro off and this
  workaround does not help (#378 is the fix). While Avro is off,
  JSON-serializing destinations receive a finite numeric as the string `"3/2"`.
- **Snapshot:** `UPDATE` the row to a finite value (or `NULL`) and restart.
- Removing the row, or adding a `CHECK` constraint, does not help for a change
  that is already in the WAL: it is read again after every restart. Do not drop
  the replication slot to get past it, that loses the position. A constraint
  only keeps new bad values out.

### Limitations

- **`TRUNCATE` is not propagated.** OpenCDC has no truncate operation, so a
  `TRUNCATE` on a published table produces no record: rows it removed remain in
  the destination. If the publication publishes `truncate` (the one the
  connector creates does by default), the connector logs a `WARN` ("TRUNCATE was NOT
  propagated") naming the table and the LSN, at most once a minute per table.
  A restricted or user-supplied publication (for example
  `publish = 'insert, update, delete'`) never sends the message, so there is
  no warning and no other signal. The `suppressed` field counts the TRUNCATEs
  of that table the rate limit swallowed since the previous warning; when
  another table's warning evicts an expired entry that still holds a count, a
  one-line summary with that count is logged. Treat the log lines as a signal,
  not a precise counter: a count still pending when the process exits is
  lost. Inserts, updates and deletes around the `TRUNCATE` are delivered as
  usual. A configurable policy for TRUNCATE is planned. Until then, treat the
  warning as a signal to reconcile the destination by hand.

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

### Keys, columns and acknowledgements

- **Upsert key.** With a structured record key (or a raw key that is a JSON object),
  a create, update or snapshot record is `INSERT ... ON CONFLICT (<key columns>) DO
  UPDATE SET` every other field of the record. The key columns must carry a primary
  key or unique constraint, otherwise Postgres rejects the statement. When a record has
  no field besides its key columns (a table whose columns are all key columns), the
  statement is `ON CONFLICT DO NOTHING`, so a redelivery is a no-op.
- **Columns.** Each record is written with exactly its own fields. A field the table has
  no column for fails the write; the destination does not create or alter tables. A
  field the record lacks is not written: an insert gets the column default, an existing
  row keeps its value. A create, update or snapshot record without fields fails with
  `postgres.destination.payload_empty`.
- **Key values** must be one non-null scalar per key column, for upserts and deletes. A
  null value fails with `postgres.destination.key_column_missing` (a delete would match
  every row with a NULL there, an upsert would append a new row on every redelivery), and
  a list or object fails with `postgres.destination.key_value_not_scalar` (a delete would
  match several rows or none).
- **Deletes** need a key and delete the rows matching every key column.
- **Raw JSON** keys and payloads are decoded without going through `float64`: an integer
  literal is an `int64` (or `uint64`), and any other number keeps its literal, so
  `numeric`, `text` and `json`/`jsonb` columns store it exactly. Integer columns take a
  whole value in any form (`1e3`, `7.0`) and reject a fraction; `real` and `float8`
  columns store the nearest float. Structured data that arrives over the plugin protocol
  has already passed through a double (ConduitIO/conduit#3100), so integers above 2^53
  sent that way are rounded before the destination sees them.
- **Names** of the table and columns are quoted as Postgres identifiers, so reserved
  words, upper case and double quotes work. A name with a NUL byte or longer than 63 bytes
  fails with `postgres.destination.invalid_identifier`, because Postgres would address a
  different table or column. The table name is one identifier: a dot is part of the name,
  not a schema separator.
- **Acknowledgements.** A write is one implicit transaction. If any statement or the
  commit fails, nothing from that write is stored and no record of it is acknowledged;
  the records are redelivered (at-least-once). Keyed upserts make redelivery
  idempotent; keyless inserts append again.

### Schema-drift markers

When the Postgres source halts on schema drift it emits a drift marker: a create
record with no key and no payload, with the metadata `postgres.schema.drift=true`.
The destination skips and acknowledges a marker without writing a row, but only
when the record's position proves the Postgres source produced it as a marker for
the table named in `postgres.schema.drift.table`. The metadata alone is not proof,
because any processor or upstream system can set it.

- A record that carries the metadata but not a marker's position, and has a key
  or a payload, is written as an ordinary record. One WARN per batch reports it.
- A record shaped like a marker (no key, no payload) without a marker's position
  stops the pipeline with `postgres.destination.drift_marker_unverified`. Nothing
  in that batch is written or acknowledged.
- A proven marker that a processor added a key or fields to is still skipped as
  a marker, with a WARN that names the added fields.

A marker loses its position when it passes through another system, such as Kafka
or a file, because records read from there carry that system's position. In a
Postgres to Kafka to Postgres setup, drop drift markers before the hop with a
filter processor on the `postgres.schema.drift` metadata.

The proof relies on the engine not letting processors change a record's
position. Conduit's classic engine refuses that; the arch-v2 engine lets a
processor rewrite positions, so there a processor could forge a marker. The
forged record would be skipped, which a processor with that power could do to
any record anyway.
### Column type changes on a running pipeline

By default the destination describes every statement on the connection that
runs it (pgx's `describe_exec` query mode), so after `ALTER TABLE ... ALTER
COLUMN ... TYPE` the next write uses the new column types. A value the new
type cannot hold exactly fails the write instead of being converted.

The cached modes, `cache_statement` and `cache_describe`, are opt-in. They
reuse prepared statements and check a per-table schema fingerprint with
every batch: after a column type change the batch is rolled back, the
statement cache is cleared, and the batch is retried with the new types. A
fired check is logged at Info with the table name and the running count
(`schema_changes_total`). The destination warns at startup when a cached
mode is set.

**The cached modes are safe only on a direct connection, or behind a pooler
that does not share prepared statements between clients.** A pooler that
shares them hands a statement one client prepared before a column type
change to every client that sends the same SQL; the fingerprint check
cannot see that, and values are converted without an error (5/2 stored as 3
in a column that became `integer`). v0.17.0 and v0.17.1 used
`cache_statement` by default; see the v0.17.2 release notes.

If pgx closes the connection after a failed batch (for example a value
that does not encode into a parameter type, on any record but the first),
the destination reconnects and, in a cached mode, retries the batch once
on an empty statement cache; nothing of a batch that fails this way was
committed. A value that does not fit fails the write, and the error names
the index of the record that holds it.

One benchi run on a dedicated machine (`benchi/destination-exec-mode`:
keyed upserts into a two-column table) measured cached statements
*without* the fingerprint check against `describe_exec`: describing every
statement cost 35% fewer rows per second at `sdk.batch.size: 1` and 16%
fewer at 1000. Cached statements with the check have not been measured
with benchi; that run is pending (the `pg-guarded` arm). The Go
microbenchmark `BenchmarkDestination_Write_Upsert` compares the two modes on
a local machine; it is indicative only, not a benchi result.

### Connection poolers

Behind PgBouncer, at any `max_prepared_statements`, use the default
(`describe_exec`) or `exec`. That likely applies to other poolers that
share or route prepared statements (Supavisor, PgCat, Odyssey), which have
not been tested. With `max_prepared_statements` above 0, PgBouncer shares
prepared statements between clients, so a cached mode can convert values
silently after a column type change. With `max_prepared_statements=0`, a
batch can reach a server that does not have its statement; the destination
retries batches that fail with SQLSTATE 26000 (prepared statement does not
exist) or 42P05 (prepared statement already exists) on an empty statement
cache, up to three times, but a cached mode still fails some batches
there. `describe_exec` was tested with PgBouncer 1.26 in both settings.

### Numeric columns

Decimal values (`big.Rat`) are sent to Postgres as exact decimal strings, and
Postgres applies the column's precision and scale itself when it stores the
value. The destination never reads or caches a column's scale, so an
`ALTER TABLE ... ALTER COLUMN ... TYPE numeric(p,s)` takes effect on the next
write without restarting the pipeline, and the table is resolved through the
connection's `search_path` like the rest of the statement.

Postgres rounds half away from zero when a value has more fractional digits
than the column's scale, and rejects a value that exceeds the column's
precision (`numeric field overflow`). A rejected batch is not acknowledged.

A decimal that has no finite expansion, such as 1/3, is truncated toward zero
after 1001 fractional digits before it is sent. So is a decimal that terminates
but needs more than 16383 fractional digits, which Postgres rejects on every
numeric column. Postgres allows a column scale of at most 1000, so the
truncation never changes the result of the column's rounding. An unconstrained
`numeric` column stores all 1001 digits.

The destination no longer checks that the target column is numeric. The general
rule: any column whose text input accepts the decimal string stores it.
Postgres converts the string, and the examples below were checked on
Postgres 17.5:

- `integer`, `smallint`, `bigint`: stored if the value is whole. A fractional
  value is rejected, also on a destination that was already running when the
  column was changed to an integer type (see "Column type changes on a running
  pipeline").
- `real`, `float8`: stored as the nearest binary float.
- `money`: rounded to the locale's fraction digits (1/3 is stored as `$0.33`).
- `oid`: stored if the value is a whole number (7 is stored as `7`).
- `text`: stored as the decimal string. `varchar(n)` and `char(n)`: rejected
  with 22001 if the string is longer than `n`.
- `boolean`: 1 is stored as `true` and 0 as `false`; other values are rejected.
- `date`: 20261012 is stored as `2026-10-12`.
- `timestamp`: 20261012 is stored as `2026-10-12 00:00:00`.
- `timestamptz`: 20261012 is stored as `2026-10-12 00:00:00+00` (session time
  zone UTC).
- `interval`: 12345 is stored as `03:25:45`.
- `json`, `jsonb`: 1/4 is stored as the JSON number `0.25`.
- `bytea`: 5/2 is stored as `\x322e35`, the ASCII bytes of `2.5`.
- `bit(n)`: stored if the decimal string is exactly `n` binary digits (101 is
  stored as `101` in a `bit(3)`).
- `time`, `inet`: rejected.

Every accepted conversion to a non-numeric type is a silent coercion if the
schema maps a numeric field to the wrong column, so check the mapping.<!-- /readmegen:description -->

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
        type: source
        settings:
          # Tables is a List of table names to read from, separated by a comma,
          # e.g.:"table1,table2". Use "*" if you'd like to listen to all tables.
          # Type: string
          # Required: yes
          tables: ""
          # URL is the connection string for the Postgres database. With TLS,
          # SCRAM authentication does not use channel binding by default
          # (`channel_binding=disable`); add `channel_binding=require` to the
          # URL to require SCRAM-SHA-256-PLUS.
          # Type: string
          # Required: yes
          url: ""
          # CDCMode determines how the connector should listen to changes.
          # Type: string
          # Required: no
          cdcMode: "auto"
          # LogreplAllowWALGap lets the connector resume from a saved position
          # when the replication slot no longer exists and has to be created
          # again, or when the slot that exists is not the one the position was
          # taken from (it was dropped and created again under the same name, or
          # another client advanced it). Such a slot starts after the saved
          # position, so every change in between is skipped and never delivered:
          # this setting accepts that DATA LOSS. Leave it false (the default)
          # and the connector refuses to start with the error
          # `postgres.source.slot_wal_gap` or `postgres.source.slot_replaced`
          # instead. Use it only after deciding that the destination can be
          # repaired some other way, and remove it once the pipeline has
          # resumed.
          # Type: bool
          # Required: no
          logrepl.allowWALGap: "false"
          # LogreplAutoCleanup determines if the replication slot and
          # publication should be removed when the connector is deleted.
          # Type: bool
          # Required: no
          logrepl.autoCleanup: "true"
          # LogreplImportDebeziumOffset starts the pipeline from a Debezium
          # Postgres connector's committed offset instead of a snapshot. It is
          # written by `conduit migrate kafka-connect --cutover`, not by hand.
          # It applies only while the pipeline has no position: the first
          # acknowledged record writes an ordinary position and the setting is
          # ignored from then on (remove it once the import is complete).
          # Requires snapshotMode "never" and cdcMode "logrepl", and an existing
          # slot and publication, which import mode never creates. Requires
          # Conduit v0.19.0 or later, which acks a record only after its
          # position is persisted.
          # Type: string
          # Required: no
          logrepl.import.debeziumOffset: ""
          # LogreplPublicationName determines the publication name in case the
          # connector uses logical replication to listen to changes (see
          # CDCMode).
          # Type: string
          # Required: no
          logrepl.publicationName: "conduitpub"
          # LogreplSchemaDriftPolicy determines how the connector handles schema
          # drift (a DDL that changes a table's shape during CDC; DBZ-3 B1): -
          # "halt" (default): emit a drift marker record, checkpoint before it,
          # and stop with a terminal error until the pipeline is restarted to
          # approve the change. - "evolve": accept additive changes silently;
          # incompatible (narrowing) changes still halt. "dlq" is reserved for a
          # future version and rejected at validation.
          # Type: string
          # Required: no
          logrepl.schemaDrift.policy: "halt"
          # LogreplSlotIdentityBeaconFailureTimeout is how long the connector
          # keeps an idle replication slot from advancing past its last identity
          # beacon while new beacons cannot be written (#442). After it, the
          # slot advances without a beacon so its WAL cannot fill the disk, and
          # the connector logs `postgres.source.slot_beacon_unavailable` as an
          # error. Until a beacon is written and delivered again, a restart is
          # refused with `postgres.source.slot_replaced` (an outage, never a
          # loss). The connector keeps trying, and recovers on its own once
          # beacons can be written. The WAL held back meanwhile is at most what
          # the server writes in this time.
          # Type: duration
          # Required: no
          logrepl.slotIdentity.beaconFailureTimeout: "5m"
          # LogreplSlotIdentityCheckTimeout bounds the replication slot identity
          # check at start (#442), which reads the slot without consuming it to
          # find the connector's identity beacon. It decodes from the slot's
          # restart_lsn, so a long-running transaction that holds restart_lsn
          # far back makes it slower; on Postgres 17+ it also waits for the
          # standbys in synchronized_standby_slots. When it times out the start
          # is refused with postgres.source.slot_identity_check_failed and
          # retried.
          # Type: duration
          # Required: no
          logrepl.slotIdentity.checkTimeout: "10m"
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
        type: destination
        settings:
          # URL is the connection string for the Postgres database. With TLS,
          # SCRAM authentication does not use channel binding by default
          # (`channel_binding=disable`); add `channel_binding=require` to the
          # URL to require SCRAM-SHA-256-PLUS. The destination describes every
          # statement before it runs it (`describe_exec`), so a column type
          # change on a running pipeline takes effect on the next write. Set
          # `default_query_exec_mode` (in the URL or in a service file) to
          # `exec`, `simple_protocol`, `cache_statement` or `cache_describe` to
          # override. The cached modes save a round trip per batch and check a
          # per-table schema fingerprint, but they are safe only on a direct
          # connection or behind a pooler that does not share prepared
          # statements between clients. Behind PgBouncer (at any
          # `max_prepared_statements`) or another statement-sharing pooler, keep
          # `describe_exec`: a cached mode can convert values silently after a
          # column type change.
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
          # (https://conduitdata.io/docs/using/connectors/configuration-parameters/output-format).
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

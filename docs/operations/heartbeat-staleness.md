# Runbook: heartbeat staleness

Applies to the Postgres source with `logrepl.heartbeat.enabled: "true"`. Background:
[`docs/design-documents/20261007-dbz3-b2-heartbeats.md`](../design-documents/20261007-dbz3-b2-heartbeats.md).

Once CDC is streaming, the connector writes its heartbeat row every
`logrepl.heartbeat.interval` and expects that change back through the replication
stream. Heartbeat staleness means one of the two halves stopped working:

- **Writes fail** (connector → database). The connector cannot update the row.
- **Delivery stops** (database → connector). The writes succeed, but the changes do not
  come back through the replication stream.

Neither one affects data correctness. Heartbeats never become records, and the flush
position the connector reports to Postgres only advances past acked data. A stale
heartbeat can mean the slot advances less often. It cannot cause loss or duplicates.

## Symptoms

- Log line `postgres.heartbeat.write_failed` (Warn, once per failed write, with
  `consecutive_failures`).
- Log line `postgres.heartbeat.stale` (Warn, once per episode) after three intervals
  with no heartbeat observed. `writes_succeeding` says which half broke.
- On the database: the heartbeat row's `beat_at` is old, or the slot's
  `confirmed_flush_lsn` lags `pg_current_wal_lsn()` and keeps growing.

## Diagnosis

Run as a user that can read the heartbeat table and `pg_replication_slots`.
Substitute your slot, publication, and heartbeat table
(defaults: `conduitslot`, `conduitpub`, `public._conduit_heartbeat`).

1. **Are writes landing?**

   ```sql
   SELECT beat, beat_at, now() - beat_at AS age
   FROM public._conduit_heartbeat WHERE slot_name = 'conduitslot';
   ```

   - `age` close to the interval: writes are fine. Go to step 2.
   - No row, the table is missing, or `age` keeps growing: writes are failing. The
     `postgres.heartbeat.write_failed` log line has the database error. See
     *Writes fail* below.

2. **Is the table still in the publication?**

   ```sql
   SELECT 1 FROM pg_publication_tables
   WHERE pubname = 'conduitpub' AND schemaname = 'public' AND tablename = '_conduit_heartbeat';
   ```

   No row: the heartbeat changes are no longer published. See
   *Table removed from the publication*.

3. **Is the slot being read?**

   ```sql
   SELECT s.active, s.confirmed_flush_lsn, s.restart_lsn, pg_current_wal_lsn(),
          r.sent_lsn, r.reply_time, s.wal_status
   FROM pg_replication_slots s
   LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid
   WHERE s.slot_name = 'conduitslot';
   ```

   - `active = false`: nothing is consuming the slot. The connector is stopped or
     disconnected. Check the pipeline status and the connector logs.
   - `sent_lsn` stuck while `pg_current_wal_lsn()` grows: the walsender is not getting
     past something. Usually that is a large transaction still being decoded, or a
     long-running transaction (step 4).
   - `reply_time` older than about 10 seconds: the connector stopped sending status
     updates. Its replication connection is wedged. Restart the pipeline.

4. **Is a long-running transaction holding the slot back?**

   ```sql
   SELECT pid, xact_start, now() - xact_start AS age, state, left(query, 80)
   FROM pg_stat_activity
   WHERE xact_start IS NOT NULL ORDER BY xact_start LIMIT 5;
   ```

   Postgres cannot move the slot's `restart_lsn` past the start of a transaction that
   is still open. This is a database-side condition, not a connector fault. Heartbeats
   cannot get around it, and `restart_lsn` stays pinned until that transaction ends.

5. **Is data in flight?** If heartbeats are fresh (step 1 is fine, and no `stale` log
   line) but `confirmed_flush_lsn` still lags, the connector is holding the flush
   position on purpose. Records have been read but the destination has not acked them
   yet, and the connector never confirms WAL past an unacked record. Look at
   destination latency and errors, not at heartbeats.

## Remediation

**Writes fail.**

- Permission denied: grant `INSERT, UPDATE, SELECT` on the heartbeat table to the
  connector's user.
- Table does not exist (dropped under the connector): restart the pipeline. Setup
  recreates the table and adds it to the publication again. That needs `CREATE` on the
  schema and ownership of the publication. Otherwise create the table as described in
  the README and run
  `ALTER PUBLICATION conduitpub ADD TABLE public._conduit_heartbeat`.
- Lock or statement timeouts: the write is bounded by `min(interval, 10s)`. Look for
  something holding locks on the heartbeat table.

**Table removed from the publication.**
Run `ALTER PUBLICATION conduitpub ADD TABLE public._conduit_heartbeat`, or restart the
pipeline so setup adds it again.

**Slot not being read, or a wedged connection.** Restart the pipeline. Positions are
crash-safe: the restart resumes from the last acked record.

**Long-running transaction.** Find the owner of the transaction and end it, or wait for
it. If WAL retention is about to fill the disk, the last resort is `max_slot_wal_keep_size`.
That invalidates the slot, and recovery then needs a full re-snapshot. Treat it as an
outage decision, not routine maintenance.

**Heartbeats no longer wanted.** Set `logrepl.heartbeat.enabled: "false"`. Then run
`ALTER PUBLICATION conduitpub DROP TABLE public._conduit_heartbeat` and drop the table
if no other connector writes to it. Otherwise its rows reach this connector as records.

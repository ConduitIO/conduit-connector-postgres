# Snapshot: read rows keyed 0 or below, and recover positions that skipped them

## Summary

Affected versions: v0.14.0 to v0.14.6 and v0.15.0 to v0.19.0. Fixed in v0.18.1 (v0.18 line) and v0.20.0. In those
versions the initial snapshot skipped every row whose key was 0 or negative. The fetch worker's cursor was
`WHERE key > lastRead AND key <= end`, and a new snapshot's `lastRead` was the zero
value of an `int64`. The slot holds only the changes made after the snapshot, so those
rows were never delivered unless they changed later. It was silent: no error and no log
line. The cdc-parity smoke run found it. Its table was keyed 0 to 499,999 and 499,999
rows were delivered.

The fix does three things:

1. A new snapshot has no lower bound.
2. A snapshot in progress on an old build repairs itself on resume. A low pass reads the
   keys at or below zero before the main pass continues, and it tracks its own progress so
   a crash resumes it.
3. A pipeline that finished its snapshot on an old build cannot be repaired from the slot.
   On its first start, the connector reports every table that holds rows keyed 0 or below,
   under a stable code.

This changes the serialized position: format version 5, with additive fields only.

## Context

- The key is always the table's single-column integer primary key (`getPrimaryKey`,
  `validateKey`): smallint, integer or bigint. NULL and duplicate keys cannot occur.
- A table's entry in `Position.Snapshots` is written only by `snapshot.Iterator.buildRecord`,
  and only from a row the worker read. An entry therefore always names a real key.
  `TestSnapshot_EntryOnlyAfterARowIsRead` pins this.
- An entry written by an old build always has `last_read >= 1`, since it was read with
  `key > 0`. Nothing in the old format separates "a new entry that read keys <= 0" from
  "an old entry that skipped them". The position contract says to key on a field's presence,
  never on the version number (`release/v0.14.x` shares the version numbers). So the fix
  needs a new field.
- A resumed snapshot runs without the exported-snapshot pin, and CDC replays from the low
  watermark (`emitResumeWarning`). Re-reading rows causes duplicates, never a gap.

## Constraints

- Invariant 2: positions are monotonic and crash-safe. No field may move backwards.
- Invariant 3: at-least-once. No path may skip a row, including a crash at any point of
  the repair.
- Backward compatibility: a position written by v0.14.0 to v0.19.0 must resume. A position
  written by this build must not make an older build lose more than it already did.
- No data in logs: keys are record data, so only hashes of key values are logged.

## Decision

### 1. New snapshots have no lower bound

`FetchWorker.passes` returns one pass, `key <= end`, for a table without an entry. The end
is `COALESCE(max(key), 0)`, read in the same REPEATABLE READ transaction as the cursor.
An empty table gets end 0 and no rows. A table whose keys are all at or below zero gets
end = max, and every row is read.

### 2. Format version 5

Two fields go on the per-table entry, `SnapshotPosition`. Both are omitempty.

- `from_min` (bool): every row keyed at or below `last_read` was read. Every entry this
  build writes from a main-pass row has it.
- `low_pass_last_read` (*int64): the last key the low pass read. It is set only on the
  positions of low-pass rows.

One field goes on `Position`:

- `low_key_gap_checked` (bool): this position's history cannot hide the gap, or the
  one-time check reported it. It is carried forward on every snapshot position (via the
  iterator's base position) and on every CDC position (`CDCHandler.positionAt`).

### 3. Resuming an old entry: the low pass

An entry without `from_min` gets two passes, in order:

1. The low pass: `key > low_pass_last_read` (with no lower bound when that field is unset)
   `AND key <= min(0, snapshot_end)`. Its rows' positions keep the old `last_read` and
   `snapshot_end` and set `low_pass_last_read`.
2. The main pass: `key > last_read AND key <= snapshot_end`. Its rows' positions set
   `from_min`, and `low_pass_last_read` is dropped.

Each field moves only forward:

- `last_read` stays fixed during the low pass, then increases.
- `low_pass_last_read` increases during the low pass.

A crash anywhere resumes from the last acked row's entry:

- **During the low pass:** the entry has `low_pass_last_read` and no `from_min`, so the
  low pass resumes after that key.
- **After the last low-pass row, before a main-pass row is acked:** the same rule applies.
  The low pass finds nothing more, and the main pass runs.
- **During the main pass:** the entry has `from_min`, so there is no low pass.

The low pass has no lower bound because the cursor ends with the last row. Bounding it by
`min(key)` would need another query and change nothing. Rows inserted after the original
snapshot point that are keyed at or below zero may be read by the low pass and also
delivered by CDC. That duplicate is at-least-once and is the documented behavior of every
resumed snapshot.

### 4. Finished snapshots: the one-time check

`CombinedIterator.checkLowKeyGap` runs before the CDC iterator is created.
`lowKeyGapNeedsCheck` decides whether it is needed, and it is not in any of these cases:

- the position is already marked;
- a fresh start (no slot implied, a Debezium offset import included): any snapshot it takes
  is from this build;
- a resumed snapshot: each entry repairs itself.

In every other case the check runs. These are a CDC position, or a snapshot position whose
snapshot is now disabled. The check queries
`SELECT EXISTS (SELECT 1 FROM t WHERE key <= 0)` per table, an index probe. Every table
with such rows gets a warning with code `postgres.source.snapshot_low_keys_skipped`, the
table, the key column, and the remediation: re-snapshot, or
`UPDATE t SET key = key WHERE key <= 0`. The position is then marked.

Each probe runs in its own read-only transaction with `SET LOCAL lock_timeout` and
`SET LOCAL statement_timeout` of 5 seconds. Without that, a probe queues behind a pending
ACCESS EXCLUSIVE lock (an `ALTER TABLE` waiting on a long transaction blocks every later
reader), and `Open` would hang on an advisory check. On a timeout the connector logs the
same code, names the table and says the check did not finish. A role without SELECT on the
table gets one warning that says so, not the raw error. Any other failure logs the same code
with the error. In all three cases the check stops, the position stays unmarked, the start
goes on, and the next start checks again. A role that can never read the table gets the
warning on every start; granting SELECT ends it.

It is a warning, not a refusal. A pipeline that never snapshotted, or whose rows changed
later, lost nothing. The connector cannot tell those cases apart, and refusing would stop
pipelines that are fine.

## Alternatives

1. **Marker only, re-read the whole lower range on a legacy resume.** Set `last_read` back
   to below the minimum and read everything again. Rejected: `last_read` would go backwards
   (from >= 1 to <= 0), breaking invariant 2. A crash would then re-read every row up to the
   old `last_read`, possibly millions of duplicates.
2. **Re-snapshot every table of a legacy snapshot position.** Simple and needs no field.
   Rejected for the same duplicate cost, and because it discards progress the old build
   made correctly.
3. **Key on `version <= 4`.** Rejected: it breaks the presence-not-version rule of the
   position contract, since the v0.14.x hotfix line writes the same version numbers.
4. **Refuse to start on a legacy CDC position with rows keyed <= 0.** Rejected: most such
   pipelines lost nothing (rows updated since, or no snapshot ever), and the connector
   cannot backfill the ones that did. A refusal stops healthy pipelines and fixes nothing.
5. **Backfill finished snapshots by reading `key <= 0` and emitting them as snapshot
   records in CDC mode.** Rejected: it delivers current row values out of stream order with
   respect to CDC changes already delivered. A destination could apply an old row value over
   a newer one. It is a correctness change to the stream, not a repair. Re-snapshot or the
   `UPDATE` touch keep ordering.

## Failure modes

| Scenario | Behavior | Data effect |
| --- | --- | --- |
| New snapshot, keys <= 0 and > 0 | one pass, no lower bound | every row read |
| New snapshot, empty table | end 0, no rows | correct |
| New snapshot, all keys <= 0 | end = max | every row read |
| SIGKILL after acking a row keyed <= 0 | resume `key > last_read` (entry has `from_min`) | no gap; the unacked tail may be re-delivered |
| Resume a v0.19.0 entry mid-table | low pass, then main pass after `last_read` | the skipped rows are delivered once |
| Crash during the low pass | low pass resumes after `low_pass_last_read` | no gap, no re-read of acked rows |
| Crash after the low pass, before a main-pass ack | low pass is empty, main pass runs | no gap |
| Resume a v0.19.0 position, table without entry | new snapshot | every row read |
| Resume a v0.19.0 CDC position | one-time check, warning per affected table | rows still missing until remediated; now visible |
| Check query fails (table dropped) | warning with the error, retried next start | none |
| Check blocked behind an ACCESS EXCLUSIVE lock | gives up after 5 s, "did not finish" warning, retried next start; `Open` continues | none |
| Role cannot SELECT from the table | one "no SELECT privilege" warning per start | none |
| Legacy entry with `last_read <= 0` (hand-edited, not written by any build) | low pass re-reads keys up to 0 | duplicates only |
| Disk full or crash mid-checkpoint | the engine keeps the last durable position | same as any record |

## Upgrade / rollback

- **Upgrade:** automatic. Old positions are read as they are, and the first record
  re-serializes them as version 5.
- **Downgrade to an older build** (it ignores the new fields):
  - Mid low pass: the old build reads `last_read` (the legacy value) and resumes
    `key > last_read`. It skips the rows keyed <= 0, as it always did. Nothing is lost that
    the old build would not have lost anyway.
  - Mid main pass with an entry whose `last_read` is <= 0: the old build resumes after
    `last_read`. If `snapshot_end` is <= 0, it recomputes the end, which widens the range
    and causes duplicates only.
  - New snapshots on the old build skip the rows keyed <= 0 again.
  - The old build drops `low_key_gap_checked` on rewrite. Upgrading again then runs the
    one-time check again.
- **Revert of this change:** the same as a downgrade.

## Observability

- `starting fetcher` logs `resumed` and `low_pass`. The start hash is logged only for a
  resumed scan.
- `low pass completed` logs the number of rows the low pass read.
- `snapshot completed` now logs `rows`.
- `postgres.source.snapshot_low_keys_skipped` is logged once per affected table on a
  legacy position.

There is no metric for "snapshot delivered fewer rows than the table had". Comparing
`count(*)` taken in the snapshot transaction with the rows read would catch the next bug of
this class. That is a follow-up.

## Testing

All of these fail on v0.19.0 behavior:

- `source/snapshot/low_keys_test.go`:
  - new snapshots of an empty, all-negative, zero-only and mixed table;
  - the entry-only-after-a-read pin;
  - a two-table crash round trip through the serialized position;
  - an upgrade from a literal v0.19.0 position, with one table mid-snapshot and one not
    started;
  - crashes inside and right after the low pass, and after the first main-pass row.
- `source/snapshot/fetch_worker_test.go`: `Test_FetcherRun_KeysAtOrBelowZero` and the
  cursor text for a new and a resumed scan.
- `source/logrepl/low_key_gap_test.go`:
  - the decision table;
  - a property test that every CDC position carries the mark;
  - the check against Postgres, including a table held under ACCESS EXCLUSIVE (the check
    gives up and Open continues) and a role without SELECT;
  - an end-to-end run that resumes a v0.19.0-shaped CDC position and warns once.
- `test/chaos/lowkeys_test.go`: SIGKILL after acking two negative keys, with the key-0 row
  in flight. Every row must be delivered exactly once.
  The low pass under SIGKILL has no chaos case. The chaos child resumes from the last
  acked ledger position and nothing else, and the low pass writes the same kind of position
  (`low_pass_last_read` in the entry) through the same ack path as the main pass, so a kill
  at any instant resumes from the last acked entry. The in-process tests crash after every
  single acked record of a legacy position (`Teardown`, which drops unacked state exactly as
  a kill does) and check exactly-once and per-table monotonicity. A real SIGKILL case would
  need a fabricated v0.19.0 position, slot identity included, and a ledger that tolerates
  the redelivery. That harness work is not cheap, and it would exercise no different code.
- `source/position`: the round-trip property covers the new fields.

## Backport to release/v0.14.x

The same bug exists there (`fetch_worker.go`). It gets its own PR off that branch. It
carries the same fix and fields and reserves format version 5 in that branch's version
history, so a position means the same on both lines.

## Related

- `docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`, "Upgrade / rollback" (the
  position compatibility contract).
- `docs/design-documents/20261021-slot-identity.md` (format version 4, the one-time check
  pattern for legacy positions).

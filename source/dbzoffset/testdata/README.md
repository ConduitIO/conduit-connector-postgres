# dbzoffset fixtures

Every `*/offsets.jsonl` holds one JSON object per line, `{"phase": ..., "offset": {...}}`.
`TestFixtures` and the fuzz seed corpus read all of them.

| Directory | Source |
| --- | --- |
| `debezium-3.2.4-pgoutput-pg16` | captured from Debezium 3.2.4 (Kafka Connect 4.0), `plugin.name=pgoutput`, Postgres 16.15 |
| `debezium-3.2.4-decoderbufs-pg16` | the same, with `plugin.name=decoderbufs` (`quay.io/debezium/postgres:16`) |
| `debezium-2.7.4-pgoutput-pg16` | Debezium 2.7.4, `pgoutput` |
| `debezium-2.7.4-decoderbufs-pg16` | Debezium 2.7.4, `decoderbufs` |
| `synthetic` | written by hand from `PostgresOffsetContext.getOffset()`; covers shapes the captures do not (a 1.x offset without `lsn_commit`, `BEGIN`) |

The captured directories come from `capture/capture.sh`. That script runs a
workload in phases (snapshot, single rows, a 60k-row transaction, COPY,
update/delete, multi-table TRUNCATE, a non-transactional logical message, an
incremental snapshot, a blocking snapshot, heartbeats, transaction metadata,
stop). It records
every distinct offset that `GET /connectors/{name}/offsets` returned, tagged
with the phase.

`verify.tsv` in each directory checks the design's LSN assumptions against
Postgres itself, through two probe slots created before Debezium started: a
pgoutput slot on Debezium's publication, and a test_decoding slot that sees
every commit, including transactions with no published change (pgoutput on
PG 15+ does not send those, decoderbufs does, and Debezium's `lsn_commit` can
be the end of one). Each row is `check|matched|total`, over the importable offsets
(streaming offsets with no snapshot or incremental-snapshot state):

- `a_lsn_commit_is_commit_end_or_message`: `lsn_commit` is the end of a
  commit record, or the LSN of a non-transactional logical message (S1).
  The message case was observed: Debezium 2.7 and 3.2 both move `lsn_commit`
  to the message.
- `b_lsn_is_change_of_txid`: for a change offset, `lsn` is the LSN of a
  change of transaction `txId`, as the probe slot decodes it.
- `b_ts_usec_is_commit_time`: `ts_usec` is that transaction's commit time.
  This holds only for importable offsets. During an incremental snapshot,
  Debezium 2.7 writes `ts_usec` a few milliseconds after the commit, but
  those offsets are refused anyway (`incremental_snapshot_in_progress`).
- `commit_offset_lsn_is_end`: a `COMMIT` offset has `lsn == lsn_commit`, the
  end of its commit record.

Every check matched every offset in all four captures.

Also observed:

- Blocking snapshots (`execute-snapshot`, `"type": "blocking"`): Debezium 3.2
  commits offsets with `snapshot: "BLOCKING"` and a `lsn_commit`; Debezium 2.7
  commits `snapshot: true` with no `lsn_commit` at all. Both are refused with
  `snapshot_in_progress`, and the capture asserts at least one in each
  directory.

- Offsets of the first change of a transaction often have `lsn == lsn_commit`,
  because the change record starts where the previous commit record ended.
  That is the PG 17 case of debezium/dbz#76, seen here on PG 16. The base
  rule's strict `<` handles it.
- During an incremental snapshot, Debezium 3.2 also writes
  `snapshot: "INCREMENTAL"`, `last_snapshot_record` and `snapshot_completed`.
  The incremental-snapshot check runs first, so those offsets get their own
  code.

# Resume by change key (#331)

## Summary

A CDC restart can drop records. It also has an ack path that can confirm the
slot past records not yet acked. Both happen because the connector treats a
change's LSN as its identity in stream order, and it is not one:

- **Not monotonic.** A transaction that began before another and committed after
  it delivers lower change LSNs than ones already delivered.
- **Not unique.** A multi-row insert (COPY, `heap_multi_insert`) is one WAL
  record. Every row decoded from it carries the same LSN.

The fix gives every change a **change key**: the transaction's commit LSN, plus
the change's ordinal within the transaction, counted from its `BeginMessage`.
Every CDC position carries the key (position format version 2). A restart skips
exactly the changes whose key is at or below the checkpoint's. The flush report
goes past acked records only when the last emitted key equals the last acked key.
It never decreases. The `walFlushed > walWritten` error, a normal state under
interleaving, is gone. A position written by v0.14.2 has no key; it resumes with
a rule that loses nothing but can repeat records once (see Decision).

## Problem

Measured on PG 17.5:

1. **Interleaving loses records on restart.** T1 inserts at `…868`. T2 inserts at
   `…958` and commits. T1 commits. The stream delivers T2's row, then T1's. The
   engine checkpoints T2's record and the process stops before T1's is acked.
   Postgres re-sends T1, because its commit is past the start point. The old
   guard (`WALStart <= StartLSN`, skip) then drops it.
2. **Interleaving kills the subscription.** After T2's record is acked while T1's
   is in flight, `walFlushed > walWritten`. The next standby status update
   returned `walWrite (…) should be >= walFlush (…)`.
3. **COPY loses records on restart.** COPY 5 rows: every row has
   `lsn=0/42E05B8`, and the transaction commits at `0/42E0818`. Ack only row 1
   and restart from it. The old guard, and the first revision of this fix
   (which keyed on `(commit LSN, change LSN)`), both drop rows 2–5.
4. **COPY confirms past unacked rows.** With row 1 acked and rows 2–5 in flight,
   `walFlushed == walWritten`, because the rows share an LSN. The gate opened and
   reported the keepalive WAL end, so `confirmed_flush_lsn` moved past the
   commit. After a crash the server would not re-send the transaction
   (invariant 1).

v0.14.2 has problems 1–3. Problem 4 is in `main`'s pre-B2 inline echo and in
#332's `reportedPositions`, both of which test `walFlushed == walWritten`. This
change replaces that test with the key comparison.

## Constraints

- Invariant 3 (at-least-once): no resume rule may drop an unacked change.
- Invariant 1: never confirm WAL past a record that has not been acked.
- Invariant 2: a position format change needs a versioned migration that reads
  every v0.14.2 position, plus an upgrade test.
- Main carries the unreleased DBZ-3 position format (version 1). The version
  numbers must not collide.

## Decision

- **Change key** (`internal.ChangeKey`): `(commit LSN, seq)`. The subscription
  sets the commit LSN from each `BeginMessage.FinalLSN`. It increments `seq` on
  every Insert, Update, Delete and Truncate, before deciding whether to skip, so
  a re-sent transaction gets identical keys. Postgres decodes transactions at
  commit, in commit order, and re-sends them identically, so keys are unique and
  strictly increasing in stream order.
- **Position format version 2.** CDC positions carry `tx_commit_lsn` and
  `tx_seq`. `ToSDKPosition` stamps `version: 2`. Version history: 0 is v0.14.2;
  1 is `main`'s DBZ-3 format, never written by v0.14.x; 2 is this change.
  Version 2 has never shipped, so it is defined here with both fields. Readers
  rely on the fields being present, not on the number. A position that lacks
  either field is read as legacy. On this branch, version 2 also carries the
  DBZ-3 fields. The v0.14.x hotfix writes version 2 without them, which reads
  as "behave as v0.14" for those fields. A DBZ-3 version 1 position has no
  key, so it gets the legacy resume rule.
- **The schema-drift marker (B1) carries the key too.** Its checkpoint resumes
  exactly: the boundary DML at the marker's key is skipped, as before (B1's D5
  disclosure).
- **Resume point** (`internal.ResumePoint.Delivered`):
  - A change whose commit LSN is unknown is never skipped.
  - **Exact** (the position has a key): skip if the change's key is at or below
    the checkpoint's. There is no loss and no duplicate.
  - **Legacy** (a v0.14.2 position: a change LSN only): skip only transactions
    whose commit LSN is below the checkpointed change LSN. Every such
    transaction was delivered ahead of the checkpointed record's transaction,
    so it was fully acked, and nothing is lost. **What may repeat:** up to
    everything that committed while the checkpointed record's transaction was
    open, plus that transaction's acked prefix. That is bounded by how long
    that transaction was open, not by a small count. A long-running
    transaction can mean many repeated records. This happens once, on the
    first restart after the upgrade. The first record delivered after it
    writes a version 2 position. **Sinks or processors that are not
    idempotent will see these duplicates.**
- **Flush gate** (`reportedPositions`, with `Subscription.allAcked`):
  - The baseline report is the last acked record's change LSN. That is safe
    even when a record in flight has a lower or equal LSN: that record's
    transaction commits after it, so the server re-sends it.
  - The report goes further, to the keepalive WAL end, only when the last
    emitted record's key equals the last acked one. Keys are unique and acks
    are FIFO, so that means everything emitted is acked. LSN equality cannot
    prove that (problem 4).
  - The report never decreases.
- **Streamed transactions are refused.** pgoutput's `streaming` option sends
  in-progress transactions in chunks, which breaks "whole transactions at
  commit, in commit order". The connector does not enable it. If such
  messages ever arrive, the subscription fails with an explicit error rather
  than mis-keying records.
- **Messages that are not changes are never skipped on resume:** Relation,
  Type, Origin, Begin and Commit. The old guard skipped any message with a
  non-zero `WALStart <= StartLSN`, which in principle included those. On
  PG 17.5 Relation messages arrive with `WALStart` 0 (measured), so it did not
  skip them there.

## Alternatives considered

- **Key on (commit LSN, change LSN).** This was the first revision of this
  fix. It is wrong for COPY, where rows share a change LSN (problem 3).
- **Resume by commit LSN alone** (skip whole transactions up to the
  checkpoint's commit). That loses the rest of a transaction checkpointed
  part-way through.
- **Drop the client-side guard and rely on Postgres's start point.** Every
  restart would then re-deliver whole transactions. Rejected, because the
  exact rule is cheap.
- **Gate on emitted and acked counts.** Equivalent while acks are exactly
  once and FIFO, but a duplicate ack would skew it permanently. The key
  comparison is idempotent.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Crash with a later-committing, lower-LSN record in flight | The restart delivers it: its key is above the checkpoint | 3 |
| Crash part-way through a COPY | The remaining rows are delivered: same LSN, higher seq | 3 |
| COPY rows in flight after the first is acked | The gate stays closed and the flush stays below the commit | 1 |
| Ack of a higher-LSN record while a lower one is in flight | The subscription keeps running. The report stays at the safe high-water mark | 1, 2 |
| Resume from a v0.14.2 position | Legacy rule: no loss. Duplicates up to everything committed while the checkpointed transaction was open, once | 2, 3 |
| Commit LSN unknown for a change (defensive) | Delivered, never skipped | 3 |
| Streamed-transaction messages | The subscription fails with an explicit error | 3 |
| Rollback to v0.14.2 with version 2 positions | The old binary ignores `version`, `tx_commit_lsn` and `tx_seq` and resumes with the old guard. That **re-exposes #331**: record loss with interleaved transactions or COPY on restart, and the subscription kill when acks of interleaved records arrive. It does not crash, but it is not safe. | 1–3 |
| A position from a newer format (version 1 or higher) | Read, not rejected. Without the key, the legacy rule applies | 2 |

## Upgrade and rollback

- **Upgrade from v0.14.2.** No step needed. The golden v0.14.2 positions in
  `source/position/testdata/` decode (`Test_ParseV0142GoldenPositions`).
  `TestInterleavedTx_UpgradeFromV0142Position` resumes from a v0.14.2-layout
  position with no loss. Expect the one-time duplicates described above.
  `Test_ParseGoldenPositions` also decodes a DBZ-3 version 1 position and a
  hotfix version 2 position. The kill-harness case
  `TestInterleavedTx_KillBeforeLaterCommittedRecordAck` covers the crash.
- **Rollback.** Pin v0.14.2. Positions stay readable. Rollback re-exposes the
  loss and the subscription kill described in the failure-mode table, so treat
  it as a temporary measure.

## Observability

Skipped re-sent changes are logged at trace level with their commit LSN, seq
and change LSN. No new metrics.

## Related

- ConduitIO/conduit-connector-postgres#331.
- The v0.14.x hotfix (`release/v0.14.x`), which writes the same version 2.
- `docs/design-documents/20261007-dbz3-b2-heartbeats.md`: the flush gate this
  change corrects, moving it from LSN equality to change keys.

# Resume by transaction commit LSN (#331)

## Summary

A CDC restart can drop records from a transaction that began before the
checkpointed record and committed after it. The fix records the transaction's
commit LSN in every CDC position (position format version 2) and decides what a
restart skips by comparing (commit LSN, change LSN) pairs, not change LSNs. A
position written by v0.14.2, which has no commit LSN, resumes with a rule that
loses nothing and may repeat a bounded set of records once. The
`walFlushed > walWritten` status-update error, which the same interleaving
triggers, is removed. In its place the flush report goes through one gated,
non-decreasing function.

## Problem

pgoutput sends each change with `XLogData.WALStart` set to that change's own LSN.
Transactions arrive in commit order. So when T1 inserts at `0/…868`, T2 inserts
at `0/…958` and commits, and then T1 commits, the stream delivers T2's row at
`…958` and then T1's at `…868`.

1. **Lost records.** The engine checkpoints T2's position (`last_lsn: …958`)
   and the process stops before T1's record is acked. On restart, Postgres
   re-sends T1, because its commit is past the start point. The subscription
   guard then drops it (`WALStart <= StartLSN`, skip). Reproduced on v0.14.2: T1's
   row is never delivered again.
2. **Killed subscription.** After T2's record is acked while T1's is in flight,
   `walFlushed (…958) > walWritten (…868)`. The next standby status update
   returned `walWrite (…) should be >= walFlush (…)`, and the subscription died.

## Constraints

- Invariant 3 (at-least-once): no resume rule may drop an unacked change.
- Invariant 2: a position format change needs a versioned migration that reads
  every v0.14.2 position, plus an upgrade test.
- Main carries the unreleased DBZ-3 position format (version 1). The version
  numbers must not collide when a v0.14.x pipeline later upgrades to it.

## Decision

- **Position format version 2.** Every CDC position now carries
  `tx_commit_lsn`: the `BeginMessage.FinalLSN` of the record's transaction.
  `ToSDKPosition` stamps `version: 2`. Version 1 is reserved for main's DBZ-3
  format, which v0.14.x never writes. Readers rely on whether `tx_commit_lsn`
  is present, not on the version number.
- **Resume point** (`internal.ResumePoint`). The subscription tracks the
  current transaction's commit LSN from its `BeginMessage` and skips a
  re-sent change only if `Delivered(commit, change)`:
  - **Exact** (version 2 position): skip if the (commit, change) pair is at or
    below the checkpoint's. Commit LSNs increase in stream order and change
    LSNs increase within a transaction, so this skips exactly what was
    delivered. There are no duplicates and no loss.
  - **Legacy** (v0.14.2 position, no commit LSN): skip only transactions whose
    commit LSN is below the checkpointed change LSN. Every such transaction
    was delivered ahead of the checkpointed record's transaction, so it was
    fully acked. Nothing is lost. Records that may repeat: transactions that
    committed while the checkpointed record's transaction was open, and that
    transaction's own acked prefix. This happens once, on the first restart
    after the upgrade. The first record delivered after it writes a version 2
    position.
- **Flush report** (`reportedPositions`). The report is the last acked
  record's LSN, extended to the keepalive WAL end only while
  `walFlushed == walWritten`, and never lower than what was already reported.
  Reporting an acked record's change LSN is safe even when an unacked record
  has a lower LSN: that record's transaction commits after it, so Postgres
  re-sends it. The `walFlushed > walWritten` error is gone, because that is
  a normal state under interleaving.

## Alternatives considered

- **Resume by commit LSN alone** (skip whole transactions up to the
  checkpoint's commit). That loses the rest of a transaction checkpointed
  part-way through. The pair is needed.
- **Drop the client-side guard and rely on Postgres's start point.** No
  format change, and no loss. But every restart then re-delivers whole
  transactions, not just the first one after an upgrade. Rejected, because the
  exact rule is cheap.
- **Buffer each transaction and checkpoint only at commit.** It changes
  batching and memory for large transactions. Out of scale for a hotfix.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Crash with a later-committing, lower-LSN record in flight | The restart delivers it: its (commit, change) pair is above the checkpoint | 3 |
| Crash mid-transaction | The rest of the transaction is delivered. The acked prefix is skipped (exact) or may repeat once (legacy) | 3 |
| Ack of a higher-LSN record while a lower one is in flight | The subscription keeps running. The report stays at the safe high-water mark | 1, 2 |
| Resume from a v0.14.2 position | Legacy rule: no loss, bounded duplicates once | 2, 3 |
| Rollback to v0.14.2 with a version 2 position | The old binary ignores `version` and `tx_commit_lsn` and resumes with the old rule, so the original bug comes back. No crash. | — |
| A position from a newer format (version 1 or higher) | Read, not rejected. Unknown fields are ignored. Without `tx_commit_lsn`, the legacy rule applies | 2 |

## Upgrade and rollback

- **Upgrade from v0.14.2.** No step needed. The golden v0.14.2 positions in
  `source/position/testdata/` are decoded by `Test_ParseV0142GoldenPositions`.
  `TestInterleavedTx_UpgradeFromV0142Position` resumes from a v0.14.2-layout
  position and asserts no loss.
- **Rollback.** Pin v0.14.2. Positions stay readable. Interleaved transactions
  are exposed to #331 again.

## Observability

Skipped re-sent changes are logged at trace level with their commit LSN and
change LSN. No new metrics.

## Related

- ConduitIO/conduit-connector-postgres#331.
- Forward-port to main, on top of the DBZ-3 position format (its version 1).

# DBZ-3 B2 — heartbeats (cut) and the gated slot advance

This note settles the parent design's Heartbeats section
(`docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`). It was written
against `main` at `0a59c82` (B1 merged) and Postgres 17.5, the image `make test`
uses.

## Summary

- **The heartbeat writer is cut** (DeVaris, 2026-10-08). The problem it was meant
  to solve, an idle publication pinning the slot while unrelated WAL grows, does
  not reproduce with this connector (Finding H1). Shipping a connector-managed
  table, new privileges, and four config parameters for a signal nobody has asked
  for is speculative. Revisit if Kafka Connect migration users need parity with
  Debezium heartbeats. The design that was pinned is kept below, under "If
  heartbeats come back", so that work does not have to be redone.
- **What B2 ships is the gate.** The flush position reported to Postgres comes
  from one function, `reportedPositions`. It reports beyond `walFlushed` (to the
  server WAL end carried by keepalives) **only when `walFlushed == walWritten`**,
  that is when no emitted record is unacked. It never reports a lower flush
  position than it reported before. Before B2 the same gate existed implicitly,
  as an inline branch with no test. It is now explicit, documented at the
  enforcement site, and covered by 7.4, 7.5, and a kill-harness case.

## Context: what the code already does (measured, not assumed)

### Finding H1: the idle-publication slot advance already works on `main`

The parent doc says the 10-second standby timer "only re-reports the last known
positions" and so cannot fix slot bloat on an idle publication. That is not what
the code does. `sendStandbyStatusUpdate` has a second branch: when
`walFlushed == walWritten && walFlushed < serverWALEnd` it sends
`WALWritePosition: serverWALEnd` and leaves the flush field zero, and
`pglogrepl.SendStandbyStatusUpdate` copies a zero flush position from the write
position. So when nothing is outstanding, the connector already confirms the
server's WAL end from the last keepalive. Postgres sends those keepalives whenever
the walsender has caught up past what the client has confirmed (walsender.c,
`WalSndWaitForWal`), and its WAL end advances over WAL the publication filters
out.

Measured on `main` with no heartbeat (throwaway test, `StatusTimeout = 1s`, idle
published table, 200-row inserts into an unpublished table once a second):

| Unrelated WAL source | `confirmed_flush_lsn` over 30 s |
| --- | --- |
| Another table, same database | tracked `pg_current_wal_lsn()` within one tick for the whole run |
| A table in a second database | same |

`restart_lsn` also moved during the same-database run, at the next
`xl_running_xacts` record, which is a Postgres-side cadence.

### Finding H2: change LSNs are not monotonic in stream order (pre-existing)

pgoutput sends each change with `XLogData.WALStart` set to that change's own LSN,
not its transaction's commit LSN. Transactions arrive in commit order, so a
transaction that started earlier and committed later delivers changes with
*lower* LSNs than ones already delivered. On restart, the resume guard
(`xld.WALStart <= s.StartLSN`, skip) then drops changes of a transaction that
began before the checkpointed LSN but committed after it, and the
`walFlushed > walWritten` guard can kill the subscription. That is a record lost
on restart. It is filed as ConduitIO/conduit-connector-postgres#331 and fixed
separately (v0.14.x hotfix and forward-port). B2 does not touch it.

What matters for B2: the gate stays sound under non-monotonic LSNs. Acks are FIFO
and every change has a distinct LSN. So `walFlushed == walWritten` means the last
emitted record was acked, which means every earlier one was too.

## Decision

### 1. The gate: one function decides what is reported

`sendStandbyStatusUpdate` delegates to
`reportedPositions(walWritten, walFlushed, serverWALEnd, lastReportedFlush)`:

```
flush := walFlushed
if walFlushed == walWritten {           // Invariant 1: nothing emitted is unacked
    flush = max(flush, serverWALEnd)
}
flush = max(flush, lastReportedFlush)   // never report a lower flush than before
write := max(walWritten, flush)
```

- **The gate.** Advancing past `walFlushed` is allowed only when every emitted
  record has been acked. Otherwise Postgres could prune WAL holding a record the
  engine has not durably written, and a crash would lose it (invariants 1–3).
- **Why a reported LSN stays safe.** Logical replication delivers transactions
  in commit order. When the gate is open at the moment keepalive WAL end E is
  processed, every transaction that committed before E has already been
  delivered, and all its records emitted and acked. Every transaction delivered
  later commits after E, and Postgres re-sends any transaction whose commit is
  past `confirmed_flush_lsn`. So reporting E, then or later, cannot cause
  Postgres to skip anything not yet acked.
- **Why monotonic.** On `main` the connector could report E, then report a lower
  `walFlushed` once a new record was in flight. Newer Postgres versions ignore a
  backwards confirmation and older ones may apply it. Either way it is noise, and
  it would make "the reported flush position never decreases" untestable.
- The pre-existing `walFlushed > walWritten` check is unchanged here; #331
  addresses it.

### 2. The heartbeat writer is not built

Reasons, in order:

1. H1: there is no idle-publication slot bloat to fix with this connector on
   Postgres 17. The keepalive echo covers it, through the same gate.
2. YAGNI. The remaining value is an end-to-end delivery signal and
   Debezium-style configuration. Neither has a user asking for it. Both would
   cost a connector-managed table in the user's database, extra privileges
   (`CREATE`, table and publication ownership), four config parameters, a
   runbook, and a naming collision to manage when Debezium shares the
   publication.
3. Nothing in the gate depends on it. If heartbeats come back, they add a
   second input to `reportedPositions` under the same condition.

Revisit when: Kafka Connect migration users need parity with Debezium
heartbeats, or a deployment is found where keepalives do not carry an advancing
WAL end (for example, a proxy or managed service that alters them).

## Alternatives considered

- **Build the heartbeat as originally decided** (parent doc, decision 3). It was
  implemented, then cut at review for the reasons in Decision 2.
- **`pg_logical_emit_message` instead of a table.** No table, no publication
  membership, no Debezium collision. But it needs the `messages` plugin option
  (Postgres 14+). Moot with the writer cut. Recorded for whoever revisits.
- **Leave the echo branch as it was.** It was correct but untested, with the
  safety condition buried in an inline boolean. Making it explicit is cheap, and
  the monotonic report closes the backwards-confirmation gap.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Keepalive WAL end arrives past a record that is emitted but unacked | Gate closed. The reported flush stays at `max(walFlushed, previously reported)`, below the unacked record's commit. | 1, 2, 3 |
| SIGKILL with a record in flight and the WAL end past it | The slot never confirmed past the record. The restart resumes from the last checkpoint and Postgres re-sends the record's transaction. No gap. | 1, 3 |
| Monotonic high-water mark held while records are in flight | Safe (see Decision 1). Worst case, the slot advances later than it could. | 2 |
| Records with interleaved LSNs (H2) | The gate stays sound (FIFO acks, distinct LSNs). The resume-guard loss is #331. | 1–3 |
| Teardown | The final standby update goes through the same function. B1's FM10 (`confirmed_flush_lsn >= marker LSN`) still holds. | 7 |

## Upgrade / rollback

There is no format, config, or schema change. Upgrading changes one observable
thing: the connector never confirms a lower flush position than it confirmed
earlier in the same run. Rolling back restores the old reporting, which was safe
but not monotonic.

## Acceptance criteria

- 7.4: idle publication plus unrelated WAL means `confirmed_flush_lsn` advances
  (`TestFlushGate_IdleSlotAdvance`). It passed on `main` before this change too
  (H1). It is the regression test for that path.
- 7.5: with a record emitted and not acked while the walsender moves past it,
  `confirmed_flush_lsn` stays below the record's commit, and it advances once the
  record is acked (deterministic and concurrent integration tests). A model test
  drives random emit, ack, keepalive, and WAL sequences through
  `reportedPositions`. All of them fail with the gate removed.
- Kill harness (`TestB2_KillWithWALEndPastUnackedRecord`): SIGKILL with a record
  in flight and the WAL end past it, then resume. The record is delivered on the
  second run, with no gap.
- 7.12 (heartbeat staleness runbook) and 7.17 (Debezium heartbeat collision
  note) are moot with the writer cut.

## If heartbeats come back

The pinned design, implemented and then cut. The code is in the history of
#332's branch, at commit `70000e2`.

- Table `"<schema>"."<table>"` (default `public._conduit_heartbeat`), columns
  `slot_name text PRIMARY KEY, beat bigint, beat_at timestamptz`, one row per
  slot, upserted every `logrepl.heartbeat.interval` (default `30s`). The writes
  start only once CDC streams, and each has a timeout of `min(interval, 10s)`.
- Setup on `Open`: `CREATE TABLE IF NOT EXISTS`, then
  `ALTER PUBLICATION ADD TABLE` if `pg_publication_tables` does not list it.
  Fails `Open` with `postgres.heartbeat.setup_failed`. That also covers the
  v0.14.x upgrade path.
- The handler recognizes the relation by namespace and name and records the
  change LSN. It skips drift detection and schema history, and returns 0, so
  `walWritten` never moves. The LSN feeds `reportedPositions` under the same gate.
- Validate identifiers to 63 bytes: Postgres truncates longer names silently,
  the handler would never match them, and heartbeat rows would flow as records.
- Reserve the table while enabled: exclude it from `tables: "*"`, and reject it
  in `tables`.
- Debezium: never share the heartbeat table, and give each tool its own
  publication, or each receives the other's heartbeat rows as data.
- Write failures: Warn with `postgres.heartbeat.write_failed`, no retry. Staleness
  after 3 intervals: Warn with `postgres.heartbeat.stale`, saying which side
  failed.

## Related

- `docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`: Heartbeats,
  Decisions 3 and 4.
- `docs/design-documents/20260829-dbz3-b1-schema-drift-escape-hatch.md`: D2, D4,
  FM10.
- `docs/design-documents/20260821-dbz3-b0-kill-harness.md`: the harness the kill
  case runs on.
- ConduitIO/conduit-connector-postgres#331: resume guard and interleaved
  transactions.

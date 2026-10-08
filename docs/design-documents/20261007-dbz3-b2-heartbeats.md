# DBZ-3 B2 — heartbeats and the gated slot advance

This note pins the parts of the parent design's Heartbeats section
(`docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`, "Heartbeats" and
"Upgrade / rollback") that it left open, and records two facts about the current
code that change what the feature is for. It was written against `main` at
`0a59c82` (B1 merged) and Postgres 17.5 (the image `make test` uses).

## Summary

- A connector-managed heartbeat table, `public._conduit_heartbeat` by default,
  opt-in (`logrepl.heartbeat.enabled=false` by default). Once CDC streaming is
  active the connector upserts one row per replication slot every
  `logrepl.heartbeat.interval` (default `30s`). The table is added to the
  connector's publication, so each write comes back through the replication
  stream as an ordinary row change.
- Heartbeat changes never become records. The handler recognizes the heartbeat
  relation by name, records the change's LSN as the *heartbeat-observed LSN*, and
  returns before schema-drift detection, schema history, or record emission.
- The flush position reported to Postgres comes from one function,
  `reportedPositions`. It reports beyond `walFlushed` (to the server WAL end
  carried by keepalives, or to the heartbeat-observed LSN) **only when
  `walFlushed == walWritten`**, that is when no emitted record is unacked. It
  also never reports a lower flush position than it reported before.
- A heartbeat write that fails is logged with a stable code and skipped: no
  retry, no backlog. It cannot touch `walFlushed`, `walWritten`, or any emitted
  position, so the worst it can do is leave the reported flush position where it
  already was.

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

So the problem the parent doc built heartbeats for, an idle publication pinning
the slot while unrelated WAL grows, does not reproduce with this connector on
Postgres 17. The keepalive echo already covers it, gated on the same condition
the parent doc derived for heartbeats.

What heartbeats still give, and why this note keeps them as decided:

1. **An end-to-end delivery signal.** A keepalive proves the walsender is alive.
   A heartbeat change arriving proves the whole chain works: the connector can
   write, the publication still contains the table, pgoutput decodes and sends
   it, the connector receives it. The two staleness numbers the observability
   section wants (time since the last successful write, time since the last
   heartbeat was observed) only exist with heartbeats.
2. **An advance that does not depend on keepalive timing.** The echo needs a
   keepalive whose WAL end is past `walFlushed`. Postgres decides when to send
   those. The heartbeat-observed LSN is a second source the connector controls.
3. **Parity with what operators coming from Debezium expect to configure.**

None of these is slot-bloat prevention on a stock Postgres 17. That is the
honest scope, and it is the main open question for sign-off (see the end).

### Finding H2: change LSNs are not monotonic in stream order (pre-existing)

pgoutput sends each change with `XLogData.WALStart` set to that change's own LSN,
not its transaction's commit LSN. Transactions arrive in commit order, so a
transaction that started earlier and committed later delivers changes with
*lower* LSNs than ones already delivered. Measured: T1 inserts at `0/4897868`
and stays open, T2 inserts at `0/4897958` and commits, then T1 commits. The
handler sees `Insert @ 0/4897958`, then `Insert @ 0/4897868`.

Two pre-existing consequences, independent of heartbeats and out of scope here
(filed as ConduitIO/conduit-connector-postgres#331):

- `Subscription.walWritten` can move backwards, and the
  `walFlushed > walWritten` guard in `sendStandbyStatusUpdate` can then kill the
  subscription.
- On restart the resume guard (`xld.WALStart <= s.StartLSN`, skip) drops changes
  of a transaction that began before the checkpointed LSN but committed after it.
  Postgres re-sends that transaction, because its commit is past
  `confirmed_flush_lsn`, and the connector then discards the rows. That is a
  record lost on restart (invariant 3). Reproduced: ack only the second-delivered
  record (the T2 row), restart from its position, and T1's row is never
  delivered again.

What matters for B2: the gate stays sound under non-monotonic LSNs. Acks are FIFO
and every change has a distinct LSN. So `walFlushed == walWritten` means the last
emitted record was acked, which means every earlier one was too. The gate does not
rely on LSN order. The monotonic high-water mark below does not either: a flush
position that was safe to report stays safe (see Decision 5).

## Decision

### 1. Table, schema, and write

```sql
CREATE TABLE IF NOT EXISTS "<schema>"."<table>" (
    slot_name text PRIMARY KEY,
    beat      bigint      NOT NULL,
    beat_at   timestamptz NOT NULL
);

INSERT INTO "<schema>"."<table>" AS t (slot_name, beat, beat_at)
VALUES ($1, 1, now())
ON CONFLICT (slot_name) DO UPDATE SET beat = t.beat + 1, beat_at = now();
```

- Defaults: schema `public`, table `_conduit_heartbeat`
  (`logrepl.heartbeat.schema`, `logrepl.heartbeat.table`).
- One row per replication slot, so several connectors can share the table
  without overwriting each other's counters. Each connector whose publication
  contains the table also *sees* the other connectors' beats. That is harmless:
  any heartbeat-table change observed while the gate is open is a safe flush
  position, whoever wrote it. Write staleness is tracked per connector, so a
  connector whose own writes fail still shows it.
- The default replica identity (the primary key) is enough. The connector only
  needs the change's LSN, not its values.
- Both identifiers are quoted. The configured names are used as-is,
  case-sensitive.

### 2. Cadence, configuration, and how to turn it off

| Parameter | Default | Meaning |
| --- | --- | --- |
| `logrepl.heartbeat.enabled` | `false` | Opt-in. When `false` the connector behaves as before this change, apart from Decision 5's monotonic flush. |
| `logrepl.heartbeat.interval` | `30s` | Time between writes. Must be positive. |
| `logrepl.heartbeat.schema` | `public` | Schema of the heartbeat table. |
| `logrepl.heartbeat.table` | `_conduit_heartbeat` | Name of the heartbeat table. |

- The writer starts when the CDC subscriber starts and stops at teardown or when
  the subscription ends. Nothing is written during the snapshot phase, so a
  heartbeat can never touch the snapshot transaction.
- One write happens immediately at start, then one per interval. Each write has
  a timeout of `min(interval, 10s)`, so a hung write cannot pile up behind the
  next tick.
- While enabled, the heartbeat table is reserved. `tables: "*"` excludes it, and
  listing it in `tables` is a configuration error
  (`postgres.heartbeat.table_conflict`).
- To turn it off, set `logrepl.heartbeat.enabled=false`. The connector stops
  writing and stops treating the table as special. The table and its publication
  membership stay. Remove them if nothing else uses them:
  `ALTER PUBLICATION <pub> DROP TABLE <schema>.<table>`, then optionally
  `DROP TABLE`. Leaving the table in place means that if other connectors keep
  writing to it, the disabled connector receives those writes as ordinary
  records. The README says so.

### 3. Required privileges

On `Open`, with heartbeats enabled, the connector runs setup in this order. It
fails fast, with code `postgres.heartbeat.setup_failed` and the failing step
named, if any step fails. The operator opted in, so carrying on without a
heartbeat would hide the misconfiguration.

1. `CREATE TABLE IF NOT EXISTS`: needs `CREATE` on the schema. A DBA can create
   the table beforehand instead. `IF NOT EXISTS` then needs no `CREATE`.
2. Publication membership. If `pg_publication_tables` does not list the table for
   the connector's publication, the connector runs
   `ALTER PUBLICATION <pub> ADD TABLE <schema>.<table>`. That requires owning both
   the publication and the table. A `FOR ALL TABLES` publication already lists
   it, so nothing is altered.
3. Each write needs `INSERT`, `UPDATE`, and `SELECT` on the table (the
   `ON CONFLICT ... SET beat = t.beat + 1` reads the existing row).

The error message names the privilege to grant, or says to pre-create the table
and add it to the publication by hand.

### 4. Recognizing heartbeat changes in the handler

- On a `RelationMessage` whose namespace and name match the configured table, the
  handler stores the relation ID and returns. That relation never reaches
  `handleRelation`: no drift detection, no schema history entry, so it never
  lands in the position, and no Avro schema. Altering the heartbeat table can
  therefore never halt the pipeline under B1's policy.
- On an Insert, Update, or Delete for that relation ID, the handler records the
  change's LSN as the heartbeat-observed LSN, notes the time, and returns `0`
  (nothing written). The change never reaches `addToBatch`, never stages or
  consumes a B1 drift marker, and never advances `walWritten`.
- Recognition is active only while heartbeats are enabled.

### 5. The gate: one function decides what is reported

`sendStandbyStatusUpdate` delegates to `reportedPositions(walWritten, walFlushed,
serverWALEnd, heartbeatLSN, lastReportedFlush)`:

```
flush := walFlushed
if walFlushed == walWritten {                  // Invariant 1: nothing emitted is unacked
    flush = max(flush, serverWALEnd, heartbeatLSN)
}
flush = max(flush, lastReportedFlush)          // never report a lower flush than before
write := max(walWritten, flush)
```

- **The gate.** Advancing past `walFlushed` is allowed only when every emitted
  record has been acked. Otherwise Postgres could prune WAL holding a record the
  engine has not durably written, and a crash would lose it (invariants 1–3).
  This is the parent doc's corrected gate. It now covers the keepalive echo as
  well as heartbeats, through the same line.
- **Why the reported LSN is safe once reported.** Logical replication delivers
  transactions in commit order. When the gate is open at the moment heartbeat H
  (or keepalive WAL end E) is processed, every transaction that committed before
  it has already been delivered, and all its records emitted and acked. Every
  transaction delivered later commits after H or E, and Postgres re-sends a
  transaction whenever its commit LSN is past `confirmed_flush_lsn`. So reporting
  H or E cannot cause Postgres to skip anything not yet acked, at that moment or
  any later one.
- **Why monotonic.** On `main` the connector can report E, then report a lower
  `walFlushed` once a new record is in flight. Newer Postgres versions ignore a
  backwards confirmation and older ones may apply it. Either way it is noise, and
  it would make "the reported flush position never decreases" untestable. Given
  the previous bullet, re-reporting the old high-water mark while the gate is
  closed is safe.
- The pre-existing `walFlushed > walWritten` error check is unchanged (see H2).

### 6. Behavior when a heartbeat write fails

- Logged at Warn level with `postgres.heartbeat.write_failed`, the error, and a
  consecutive-failure count. The write is not retried and not queued. The next
  tick tries again.
- The connector does not stall: the writer runs on its own goroutine with its
  own pooled connection, separate from the replication connection and the
  handler.
- Position safety: a failed write only means no new heartbeat-observed LSN, so
  the reported flush stays at most where it was. Emitted positions never carry
  heartbeat state.
- Staleness: each tick compares the time since the last observed heartbeat with
  `3 × interval`. When it is exceeded the connector logs one Warn,
  `postgres.heartbeat.stale`, per stale episode, including whether the writes
  themselves are succeeding, so an operator can tell connector→DB failure from
  DB→connector failure. Both timestamps are kept on the iterator for B3's
  `inspect --json`.

### 7. Interaction with the B1 schema-drift halt

- The heartbeat relation is never drift-checked (Decision 4), so heartbeats
  cannot cause a halt or a marker.
- While a drift marker is pending, B1's D4 skip makes every skipped DML return
  its LSN, so `walWritten > walFlushed` and the gate is closed. Heartbeats cannot
  advance the slot past a skipped record.
- After the marker is acked, with nothing emitted after it, the gate is open, and
  a later heartbeat (or keepalive) can move the reported flush past the marker
  LSN. That matches `main`'s keepalive echo today and loses nothing: D4 emitted
  nothing after the marker, and the boundary DML is the disclosed D5 drop. FM10's
  assertion (`confirmed_flush_lsn >= marker LSN`) still holds.

### 8. Upgrade from a slot created by v0.14.x

- The position format is unchanged. Heartbeat state is in memory only, so a
  v0.14 position resumes exactly as before.
- The slot is not touched. On the first `Open` with heartbeats enabled, the
  table is created if missing and added to the existing publication (Decision 3,
  step 2). There is no migration step and no pause.
- With heartbeats disabled (the default), the only change an upgraded pipeline
  sees is Decision 5's monotonic flush report.

### 9. Rollback

- To an older connector version: the position is byte-compatible, so the old
  version resumes. The heartbeat table stays in the publication. Nothing writes to
  it any more unless another connector shares it, and if one does, the old
  connector emits those writes as records. The rollback instruction is to run
  `ALTER PUBLICATION <pub> DROP TABLE <schema>.<table>` and drop the table if it
  is unused. Without that, an old connector configured with `tables: "*"` would
  also snapshot the table.
- Within this version: `logrepl.heartbeat.enabled=false` (Decision 2).

### 10. Coexisting with a Debezium heartbeat

Debezium's Postgres connector heartbeats through `heartbeat.action.query`, usually
against a table such as `debezium_heartbeat`. If both connectors read the same
database during a migration:

- Never point `logrepl.heartbeat.table` at Debezium's heartbeat table, or the
  other way round. Each would count the other's writes as its own liveness, and
  the staleness signal would lie.
- If both use the same publication, or Debezium uses a `FOR ALL TABLES`
  publication, each side receives the other's heartbeat changes as data:
  Conduit emits `debezium_heartbeat` rows as records, and Debezium publishes
  `_conduit_heartbeat` changes to a topic. Use separate publications, exclude the
  other side's table (`table.exclude.list` in Debezium; leave it out of `tables`
  in Conduit), or both.

The README carries this note. Per decision 3 of the parent doc, full collision
handling belongs to the Kafka Connect migration compatibility report.

## Alternatives considered

- **Heartbeat replaces the keepalive echo.** The echo is already gated correctly,
  and it covers the idle-publication case without a table (H1). Removing it would
  make slot advancement depend on an opt-in feature that defaults off: a
  regression for every pipeline that does not enable heartbeats. Rejected.
  Instead both sources go through the same gate.
- **`pg_logical_emit_message` instead of a table.** No table, no publication
  membership, and no collision with Debezium. But pgoutput forwards logical
  messages only when the `messages` plugin option is set, which exists from
  Postgres 14 on. The connector does not set it today, so this would change the
  plugin arguments of every replication session and drop support for older
  servers. The table approach works on every version with logical replication,
  and it is the recorded decision (parent doc, decision 3). Rejected for B2.
  Worth revisiting if the plugin arguments change for other reasons.
- **Ship no heartbeat, only harden the echo.** H1 makes this a real option: make
  the gate explicit, add the monotonic report, and add the 7.4/7.5 tests against
  the echo. It loses the end-to-end delivery signal (point 1 under H1) and the
  configuration Debezium users expect. The code is arranged so this remains
  possible: the gate and its tests stand on their own if the heartbeat writer is
  dropped. **Left to sign-off.**
- **Gate on "heartbeat XLogData processed".** That is the parent doc's earlier,
  wrong draft. It could report a heartbeat LSN past an unacked record. The 7.5
  tests are built to fail exactly that way when the gate is removed.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Heartbeat observed while a record is emitted but unacked | Gate closed. The reported flush stays at `max(walFlushed, previously reported)`, below the unacked record's commit. | 1, 2, 3 |
| SIGKILL after a heartbeat write, before the in-flight record's ack | The slot never confirmed past the record (it was outstanding the whole time). The restart resumes from the last checkpoint, and Postgres re-sends the record's transaction. No gap. | 1, 3 |
| Heartbeat write fails (privileges revoked, table dropped, lock timeout) | Warn with `postgres.heartbeat.write_failed`; no retry or backlog. The flush position cannot advance from heartbeats. The keepalive echo still applies. | 1 |
| Writes succeed but nothing arrives through the stream (table removed from the publication) | `postgres.heartbeat.stale` Warn naming the DB→connector side. No position effect. | none |
| Heartbeat table altered or recreated | The new `RelationMessage` refreshes the stored relation ID. It is never drift-checked, so no halt. | 6 |
| Setup fails at `Open` | `Open` fails with `postgres.heartbeat.setup_failed` and the failing step. The pipeline does not start half-configured. | none |
| Teardown | The writer is cancelled and awaited before the subscription teardown returns. The final standby update goes through the same gate. | 7 |
| Records with interleaved LSNs (H2) | The gate stays sound (FIFO acks, distinct LSNs). The pre-existing guard and resume-guard issues are unchanged and tracked separately. | 1–3 |

## Observability

- Logs: `postgres.heartbeat.write_failed` (each failure),
  `postgres.heartbeat.stale` (once per episode), and setup at Info level.
- State held for B3: last successful write time, last observed heartbeat time,
  last heartbeat-observed LSN, consecutive write failures.
- Runbook: `docs/operations/heartbeat-staleness.md`.

## Performance

B2 adds work in two places on the data path. Each data change passes through one
early check in `Handle`: a string compare when heartbeats are off, plus a type
switch and an ID compare when they are on. Each status update goes through
`reportedPositions` (a few comparisons, every 10 s).
`BenchmarkHandleHeartbeatCheck` puts the check at 1.28 ns per message with
heartbeats off and 1.66 ns with them on (6 runs each, spread under 1%). The
end-to-end cost of a CDC record in the same benchmark file is about 5 µs, so the
check is below 0.05% of it.

The end-to-end benchmark (`BenchmarkCDCThroughput`, 20,000-row insert then
read and ack through the CDC iterator, 10 iterations per run, 6 alternating
rounds) cannot resolve a 10% change on this machine. Its A/A floor (main
against main) is about 2x, because runs land at either about 100k or about 200k
records/s, independent of the code under test. The medians were: main 104k and
199k (the A/A pair), branch with heartbeats off 106k, branch with heartbeats on
104k. No regression is visible, and none can be ruled out below the floor from
this run. benchi was not used. Following parent decision 4, this is a
connector-local measurement.

## Acceptance criteria

- 7.4: idle publication plus unrelated WAL means `confirmed_flush_lsn` advances,
  with heartbeats on, to at least the heartbeat-observed LSN, which must be
  non-zero. Because of H1, a sibling case shows the same advance with heartbeats
  off. That case is the regression test for the keepalive echo.
- 7.5: with a record emitted and not acked while heartbeats keep arriving,
  `confirmed_flush_lsn` stays below the record's commit, and it advances once the
  record is acked. A model test drives random emit, ack, heartbeat, and keepalive
  sequences through `reportedPositions`. Both fail with the gate removed.
- Kill harness: SIGKILL with heartbeats observed and a record unacked, then
  resume. The record is delivered on the second run, with no gap.
- README note on the Debezium collision (7.17). Runbook for heartbeat staleness
  (7.12, heartbeat part).

## Open questions for sign-off

1. **Keep the heartbeat, given H1?** It is opt-in and default off. Its value is
   the end-to-end delivery signal and Debezium-style configuration, not slot
   advancement on stock Postgres 17. The alternative is to keep only the gate,
   the monotonic report, and the tests, and drop the writer and table.
2. **H2 is a data-loss bug on main** (the restart resume guard with interleaved
   transactions). It is not caused or worsened by B2, but it sits on the same
   path. Does it preempt B4's release?

## Related

- `docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`: Heartbeats,
  Upgrade / rollback, Decisions 3 and 4.
- `docs/design-documents/20260829-dbz3-b1-schema-drift-escape-hatch.md`: D2, D4,
  FM10.
- `docs/design-documents/20260821-dbz3-b0-kill-harness.md`: the harness the kill
  case runs on.

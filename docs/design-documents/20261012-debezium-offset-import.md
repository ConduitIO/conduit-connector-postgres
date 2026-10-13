# Resume from a Debezium offset (offset import)

## Summary

A team moving from Debezium Postgres on Kafka Connect to this connector should
not have to re-snapshot. This design lets the connector start from Debezium's
last committed offset, with no loss and with a stated bound on duplicates.

- The connector takes Debezium's raw offset in a new setting,
  `logrepl.import.debeziumOffset`. It applies the setting only when the
  pipeline has no position yet. `conduit migrate kafka-connect` reads the offset
  from Kafka Connect and writes the setting. It also gives a dry run that
  prints the computed resume point.
- **No new position format.** The import becomes a resume point that the
  connector already understands: skip every transaction whose commit LSN is
  below Debezium's `lsn_commit`. One extra in-memory rule skips the part of
  the next transaction that Debezium already delivered. The first acked record
  writes an ordinary version 2 position, and from then on the import plays no
  part.
- **Two slot strategies.** _Fresh_ (the recommended default): create a Conduit
  slot while Debezium is still running, wait until Debezium's committed offset
  passes the new slot's start, then cut over. This works with every Debezium
  decoding plugin and leaves Debezium's slot alone, so rollback is a restart.
  _Reuse_: take over Debezium's own `pgoutput` slot. This needs no second slot,
  but rollback is harder.
- Offsets taken during a Debezium snapshot (initial, blocking or incremental)
  are refused with a coded error. So are offsets that cannot be proven safe.
  The import never guesses.
- **Duplicate bound:** at most the rows that share one change LSN, which is one
  row for an ordinary statement and at most one multi-insert WAL record
  (≤ 1000 rows) for COPY. In some cases the bound is wider, and each case is
  stated below. Nothing is lost relative to what Debezium had committed,
  provided Debezium was stopped gracefully.

This is Tier 1: it decides what a resumed pipeline skips (invariants 1–3).

## Decisions (DeVaris, 2026-10-12)

DeVaris resolved the open questions on 2026-10-12, taking each
recommendation. This section was added after review began. The design text
below is kept as written; where a decision changes it, a block marked
**Amended 2026-10-12** follows the affected passage. If the two disagree, this
section, the Review fixes section and the amendment blocks govern.

| # | Decision | Effect on this design |
| --- | --- | --- |
| Q1 | **Ship the partial rule in v0.21.** | D4's in-memory partial rule is in scope for v0.21. |
| Q2 | **No override for `postgres.import.slot_ahead`.** | D5 check 5 is a hard refusal. Remedies are re-snapshot, dual-run, or redoing the cutover. |
| Q3 | **The CLI may write a logical marker message only behind an explicit opt-in flag.** | `--cutover` never writes to the source database by default. Without the flag, the runbook tells the user to write to a captured table; S5 corrects what that write must be. With `--emit-cutover-marker`, the CLI writes a non-transactional message, `pg_logical_emit_message(false, 'conduit.migrate.cutover', …)`. Debezium treats a non-transactional message as a commit and advances `lsn_commit` to the message's end (S1). Caveats: Debezium sees the message only if pgoutput's `messages` option is on and the prefix passes `message.prefix.include.list`/`message.prefix.exclude.list`. The offset that carries it is committed only through the message's own record or a heartbeat. That record lands in Debezium's `<topic.prefix>.message` topic, where downstream consumers see it. Where the marker does not move `lsn_commit` past C before the timeout, the CLI fails with `migrate.kafka_connect.cutover_marker_unsupported`. |
| Q4 | **v0.21 ships the fresh-slot strategy only.** Reusing Debezium's slot moves to v0.22, gated per Debezium version on a passing rollback test. | The CLI flag is `--slot-mode`, and its only value in v0.21 is `new` (the default). It replaces `--slot-strategy fresh\|reuse` in D3 and D10. In v0.21, pointing `logrepl.slotName` at Debezium's slot is refused with `postgres.import.slot_reuse_unsupported`. The reuse-only parts are v0.22: D3 "Reuse", the reuse column of D9 (redesigned per S7), the `autocleanup_on_reused_slot` code, and the reuse test cases. The n-based skip in D4 is dropped outright (B2). |
| Q5 | **Approved. The reviewer recommends deferral. Awaiting DeVaris.** | DeVaris approved the key. The Tier 1 review recommends deferring it, for two reasons: the first set this doc defined was wrong after a heartbeat or END record (S2), and a public metadata key is a contract. The Observability amendment designs both options. Until DeVaris decides, the implementation ships option A (an INFO log of the duplicate key range) and does not commit to the key. |
| Q6 | **The setting name is `logrepl.import.debeziumOffset`.** | No change: the design already uses it. |
| — | **`pgregory.net/rapid` is approved as a test-only dependency.** | It is used for the property tests (see the Tests amendment). |

With Q4 and B2, the **v0.21 duplicate bound**, when the partial rule is
armed, is the changes of T at Debezium's last change LSN L:

- 1 for an ordinary row;
- at most one multi-insert WAL record (≤ 1000 rows) for COPY;
- one record per relation for a multi-table TRUNCATE.

The corrected cases are in S2.

## Review fixes (Tier 1 design review, 2026-10-12)

A fresh-context Tier 1 review of this PR found three data-loss blockers
(B1–B3) and ten more issues (S1–S10, plus nits). This section was added after
review began. Each fix below governs over the text it amends, and an
**Amended 2026-10-12** pointer marks every affected passage.

### B1. Fresh mode on unpatched Postgres

D3 argues that a new slot decodes in full every transaction committing after
C. That holds only on Postgres builds with commit 2f3304ce, "Fix possibility
of logical decoding partial transaction changes", from 2024-07-11.

On older builds, a new slot can restore a serialized snapshot written by
another slot, such as Debezium's. Creating it then jumps to CONSISTENT while
still searching for its start point. The slot's `restart_lsn` and
`confirmed_flush_lsn` can then sit in the middle of a transaction. A long
transaction opened before the slot was created, and committed after Lc, is
decoded without its head. Those changes are lost.

The fix is in these releases, checked against the release notes and the
branch history rather than the release cadence:

| Major | First fixed release | Evidence |
| --- | --- | --- |
| 14 | 14.13 (2024-08-08) | Release notes: "Prevent incorrect initialization of logical replication slots". Absent from 14.12. Backpatch `f7d3caf9`. |
| 15 | 15.8 (2024-08-08) | Same note. Absent from 15.7. Backpatch `aee8c2b9`. |
| 16 | 16.4 (2024-08-08) | Same note. Absent from 16.3. `2f3304ce` is the REL_16_STABLE commit, and the REL_16_4 tag contains it. |
| 17 | 17.0 | `068674f4` on REL_17_STABLE, 2024-07-11, before 17.0 (2024-09-26) |
| 18 | all | — |

- **Preflight.** Both the CLI, before it creates the slot, and the connector,
  at `Open`, refuse fresh mode when `server_version_num` is below the fixed
  release for its major. The code is `postgres.import.server_unpatched`, and
  the fix is to upgrade to the listed minor or later. Majors below 14 are
  refused with `postgres.import.server_unsupported`.
- **Harness.** Images are pinned by digest at or above the fixed minors, and
  PG 18 joins the matrix with the Debezium versions that support it. One
  unpatched image (16.3) runs only the refusal test.
- **New cut point.** A transaction opened before Conduit's slot is created,
  committed after Lc, with Debezium stopped after that. The test asserts that
  its head is delivered.

### B2. Drop the `lsn_events_processed` branch

Debezium counts one event per relation for a multi-table TRUNCATE. pgoutput
sends that TRUNCATE as one message. Conduit's change key counts it once. So
skipping "the first n changes at L" can skip the second table's truncate:
that is a loss.

- The n-based skip is removed from D4. It is not deferred to v0.22. Bringing
  it back needs per-relation counting and its own design.
- The partial rule is L-only: skip T's changes with change LSN < L. A TRUNCATE
  at L is always delivered, at worst as a duplicate.
- `lsn_events_processed` is still parsed and shown in the dry run, and it is
  never used to skip anything.
- **New cut point.** A multi-table TRUNCATE (`TRUNCATE a, b`), with an offset
  that lands on the first relation's event. The test asserts that both tables'
  truncates reach the union. The harness sets `skipped.operations=none` (S6).
  The default `t` would hide truncates.

### B3. `publication_mismatch` must not say "add the tables"

pgoutput decides publication membership through the historic catalog of the
WAL it is decoding. If a table is added to the publication after the slot's
start, its earlier WAL is skipped silently, and nothing raises an error.

The fix text is now: _"Repair the publication, then redo the cutover with a
new fresh slot created after the repair (`conduit migrate kafka-connect
--cutover` drops and recreates its own slot). Do not add tables to a
publication that an existing slot will decode from."_

The CLI orders the steps as: create or repair the publication, verify that it
covers the pipeline's tables, then create the slot. In reuse mode (v0.22) the
mismatch is a refusal with no remediation path except a fresh cutover.

### S1. Non-transactional logical messages

For a non-transactional `MESSAGE`, Debezium calls `commitMessage`. That sets
`lsn_commit` to the message's LSN, which Postgres reports as the message's
**end**. Non-transactional messages are output when their WAL record is
decoded, not at a commit. So the stream is the WAL-ordered merge of commit
records and non-transactional message records.

**The base rule stays safe.**

- A transaction U with `U.commit < Lc = msg.end` has its commit record before
  the message record, because records do not overlap. U was output before the
  message and processed by Debezium.
- A transaction committing after the message has `U.commit ≥ msg.end = Lc`,
  equality included (debezium/dbz#76), so it is not skipped.
- A transaction open across the message is output at its commit, after the
  message, and is delivered in full.

**Harness.** A cut point where the last committed offset is a
non-transactional message, written with `--emit-cutover-marker`, sitting
between a transaction's first change and its commit. The test asserts zero
loss and that the open transaction is delivered in full.

### S2. The duplicate model after a heartbeat or END record

When the offset comes from a heartbeat or a transaction-metadata END record
(M = COMMIT), T is fully covered and the next transaction was never seen by
Debezium. The old table merged this into "xid mismatch → T's prefix", which
overstated the duplicates and would have tagged a transaction that is not a
duplicate. The corrected table, which replaces the one in Duplicate bound:

| Case | Duplicates |
| --- | --- |
| M = COMMIT (heartbeat, END record, non-transactional message per S1) | 0. T is fully covered by the base rule, and the partial rule is not armed. |
| M = BEGIN | 0 |
| M is DML, the first transaction ≥ Lc has xid X (partial rule armed) | T's changes at LSN L (see the v0.21 bound above) |
| M is DML, the first transaction ≥ Lc has another xid | 0. T had no changes on this slot: it touched only tables outside the pipeline, or PG 15+ skipped it as empty. The first transaction ≥ Lc was never processed by Debezium. |
| M = MESSAGE (transactional) or M absent | T's prefix that Debezium delivered, when T is identifiable by X; the partial rule is not armed |
| Debezium stopped ungracefully | as before |

### S3. Wrong-database offsets

The setting gains three fields:

- `database`: Debezium's `database.dbname`.
- `systemIdentifier`: the CLI captures it with `IDENTIFY_SYSTEM` on the
  replication connection, which needs no extra privilege.
- `debeziumSlotName`: renamed from `slotName`, for clarity. The old name is
  accepted, since no build has shipped with it.

At `Open`, the connector refuses with `postgres.import.database_mismatch`
unless all of these hold:

- `database` equals `current_database()`;
- `IDENTIFY_SYSTEM` returns the same `systemIdentifier`;
- Debezium's slot exists in `pg_replication_slots` with
  `database = current_database()`.

In fresh mode Debezium's slot is kept through cutover for rollback, so
requiring it costs nothing.

### S4. Debezium's decoding plugin

The setting gains `plugin` (Debezium's `plugin.name`; the CLI writes
`decoderbufs` when the config leaves it unset). The partial rule assumes each
change carries its own LSN.

- `pgoutput` and `decoderbufs` are accepted.
- `wal2json` and `wal2json_streaming` are refused with
  `postgres.import.plugin_unsupported`. They stamp one LSN for the whole
  transaction, and Debezium dropped them in 2.0.
- An absent or unknown plugin is refused with the same code.

### S5. Fix texts and check order

- **Snapshot completed, no streaming offset.** The record of a single write
  carries the `lsn_commit` of the commit **before** it. So a single write is
  not enough. The fix text is now: _"Debezium needs a committed transaction
  followed by a later record or heartbeat. Enable `heartbeat.interval.ms`, or
  write two separate transactions to a captured table. Wait for an offset
  flush (`offset.flush.interval.ms`), then retry."_
- **The Q3 runbook step has the same correction.** "Write one row" becomes
  "write two separate transactions to a captured table, or rely on
  heartbeats".
- **`offset_unsupported`.** The fix text drops the misleading "Debezium
  ≥ 1.9" pointer. It now reads: _"This offset has no `lsn_commit`. Run the
  connector on a Debezium version that writes it (every 2.x and 3.x release;
  the fixture set lists the 1.x builds that do), let it commit an offset after
  at least one transaction, then retry."_
- **D5 check order.**
  1. Parse.
  2. Incremental snapshot keys (`incremental_snapshot_in_progress`).
  3. `snapshot`, `last_snapshot_record`, `snapshot_completed`
     (`snapshot_in_progress`).
  4. `lsn_commit` present (`offset_unsupported`).
  5. Plugin (S4).
  6. Server version (B1).
  7. Database identity (S3).
  8. The slot checks, ending with F ≤ Lc.
  9. Publication coverage.
  10. The table subset.

  A running incremental snapshot always gets its own code, even when the offset
  also lacks `lsn_commit`.

### S6. "Delivered" means "processed under Debezium's configuration"

Goal 2 is restated. Every change in the pipeline's tables is either
**processed by Debezium under its configuration**, meaning covered by O, or
delivered by Conduit. Changes that Debezium's configuration dropped were never
in Debezium's output, and the import does not recover them.

The CLI warns about these settings in the compatibility report, with
`migrate.kafka_connect.lossy_source_config`:

- `errors.tolerance=all`;
- Filter SMTs;
- non-empty `skipped.operations` (including the 2.x default `t`);
- `column.exclude.list`.

The harness sets `skipped.operations=none`.

### S7. Reuse rollback is an open v0.22 design item

The write-back offset in D9 used only Conduit's last position. It must also
use F, the slot's `confirmed_flush_lsn` at Conduit's stop. Debezium streams
from max(its offset, F), and its locator looks for the stored LSNs in a stream
that starts at F. This is recorded as an open design item for the v0.22 reuse
work. It is not specified here.

### S8. What the D6 cap retains, and when cutover is done

While the cap holds, `confirmed_flush_lsn ≤ Lc`, so the slot pins two things:

- **WAL:** from `restart_lsn` (≤ Lc) onward. The amount is roughly the WAL
  rate × the time from cutover to the first ack.
- **`catalog_xmin`:** vacuum cannot remove system-catalog rows newer than it,
  so catalog bloat grows with DDL and temp-table churn.

Both end at the first ack. That ack happens only after Conduit has durably
persisted the position: `Source.Ack` in `conduit` `pkg/connector/source.go`
defers the plugin ack until the persister has flushed, per
`20260723-source-ack-persist-ordering-fix.md`.

If nothing captured arrives, the cap holds with no limit. With
`max_slot_wal_keep_size` set, the slot can then be invalidated: the running
subscription fails, and the next start refuses with `slot_invalidated`.

- `--cutover` starts the pipeline by default (S11) and waits for the import-complete signal before it reports
  success. The CLI sees it as the pipeline's source position becoming
  non-empty through the Conduit API (the INFO log line is the same event). It
  then tells the user to remove the setting.
- The runbook gains a scenario: "slot held at Lc after import". The symptom is
  the WARN line and growing `pg_replication_slots` lag. Diagnosis: no captured
  traffic since cutover. Remedy: write to a captured table. Escalation: the
  `max_slot_wal_keep_size` → `slot_invalidated` path, whose only remedy is a
  re-snapshot.

### S9. Crash-safe CLI re-runs and slot creation

- **Journal.** `--cutover` writes a journal next to the generated pipeline
  (`<pipeline>.cutover.json`). It records:
  - the slot name, C, and the publication;
  - the saved Debezium config;
  - the last completed step.

  Each step is written before it starts and marked done after.
- **Re-runs after a kill:**
  - If the journal names a slot that exists, is `pgoutput` and is inactive, the
    re-run reuses it and continues.
  - If that slot's `confirmed_flush_lsn` is past the recorded C, something
    consumed it. The re-run refuses with `migrate.kafka_connect.slot_advanced`;
    the fix is `--reset`, which drops the slot and starts over.
  - A slot with Conduit's name and no journal is refused with
    `migrate.kafka_connect.slot_unowned`.
- **Slot creation.** `CREATE_REPLICATION_SLOT` waits for every transaction
  running at creation to finish. Prepared transactions block it until they
  are resolved. `--slot-create-timeout` (default 5m) bounds the wait. On
  timeout, the CLI cancels, drops any partial slot, and fails with
  `migrate.kafka_connect.slot_create_blocked`. The diagnostic lists the oldest
  `pg_stat_activity` transactions (pid, `xact_start`, query) and
  `pg_prepared_xacts`.
- **New cut point.** Debezium's graceful stop runs past
  `task.shutdown.graceful.timeout.ms`, so the task is cancelled without its
  final offset commit. Expected: a `slot_ahead` refusal, or an import with no
  loss. Never loss.

### S11. `--cutover` starts the pipeline (coordinator, 2026-10-12)

`--cutover` **starts the imported pipeline by default**. It then waits for the
import-complete signal: the first ack, which lifts the D6 cap (S8). Only then
does it report success. Starting is the default for two reasons:

- From Debezium's stop until the pipeline starts, both slots hold WAL.
- An operator who forgets to start the pipeline leaves the cap in place with
  no limit.

`--no-start` writes the pipeline and stops after the import preflight. It is
for operators who deploy the pipeline themselves. The CLI then prints that the
pipeline must be started promptly, and why.

**If the start fails** (preflight refusal at `Open`, a plugin error, or a
timeout waiting for the first ack, set by `--start-timeout`, default 10m):

- the pipeline config stays written;
- Debezium stays stopped (the CLI never restarts it on its own);
- the journal records the step;
- the CLI prints `conduit migrate kafka-connect rollback --pipeline <name>`
  as the way back, and a re-run of `--cutover` as the way forward (S9);
- it exits with `migrate.kafka_connect.start_failed`. The connector's own
  code, when there is one, is passed through as the cause.

A first-ack timeout on an idle database is the same failure. The message adds
the S5 step: write two transactions to a captured table, then re-run.

### S10. CDC mode

With an import set, `cdcMode` must be `logrepl`. `auto` is rejected with
`postgres.import.cdc_mode_conflict`, because `auto` may fall back to long
polling, which would ignore the slot and the resume point. The CLI writes
`logrepl`.

### Nits

- **D5 check 6, fresh mode.** The check is now that Conduit's publication
  (`logrepl.publicationName`) covers the **pipeline's** tables, and that those
  are a subset of Debezium's resolved tables.
- **Tables.** The CLI resolves Debezium's `table.include.list` and
  `table.exclude.list` regexes (and `schema.*`) against the catalog to produce
  `tables`, and shows the resolved list in the dry run.
- **KIP-618 exactly-once.** The CLI reads both the worker's global offsets
  topic and the connector's own `offsets.storage.topic`. When both hold an
  offset, the per-connector one wins, as it does in Kafka Connect.
- **Slot creation in import mode.** Import mode does not go through
  `CreateSubscription`'s create-if-missing path at all. It uses a variant that
  requires an existing slot and never issues `CREATE_REPLICATION_SLOT`.

## Context

### What exists (inventory, 2026-10-12)

- Neither this repo nor `ConduitIO/conduit` contains offset-import code.
  `conduit migrate kafka-connect` has not been built yet: it is the v0.21 lane
  ConduitIO/conduit#3003. This lane is ConduitIO/conduit#3004, under the
  tracking issue #3002. Nothing in `conduitio-labs` touches `connect-offsets`.
- `conduit-kafka-connect-wrapper` runs Debezium jars inside Conduit. It could
  load Debezium's offsets natively, but it is not this connector (see
  Alternatives).
- ConduitIO/conduit-connector-sdk#378 (a position-only checkpoint) is open.
  If it existed, it would let the import persist itself at `Open`. This design
  does not depend on it.
- This connector on `main` (v0.15.0 candidate) already has what the import
  builds on:
  - the change key `(tx_commit_lsn, tx_seq)` and position format version 2
    (`20261008-interleaved-tx-resume.md`);
  - `ResumePoint.Delivered` and `ChangeKey.Predecessor` (#338);
  - the flush gate `reportedPositions` (`20261007-dbz3-b2-heartbeats.md`);
  - deciding drift on delivery and the #339 boundary handling
    (`20261008-drift-decision-on-delivery.md`,
    `20261009-drift-approval-delivers-boundary-row.md`).

### The Debezium offset

Kafka Connect stores source offsets as `partition → offset` maps.

- **Distributed mode** keeps them in the compacted topic `connect-offsets`.
  That is the default name; the worker's `offset.storage.topic` can rename it,
  and since KIP-618 a connector can set its own `offsets.storage.topic`. The
  record key is the JSON array `["<connector name>", {"server": "<topic.prefix>"}]`
  (`database.server.name` before Debezium 2.0). The value is the JSON offset
  map. The latest record for a key wins, and a null value is a tombstone.
- **REST** (Kafka 3.5+, KIP-875): `GET /connectors/{name}/offsets` returns
  `{"offsets":[{"partition":{...},"offset":{...}}]}`. The same KIP adds
  `PUT /connectors/{name}/stop` and `PATCH .../offsets`.
- **Standalone mode** and Debezium Server write a Java-serialized
  `HashMap<ByteBuffer,ByteBuffer>` file (`FileOffsetBackingStore`). Out of
  scope for v1. Users export the offset through the standalone worker's REST
  API instead (see Non-goals).

The Debezium Postgres offset fields are listed below. They were read from
`PostgresOffsetContext`, `SourceInfo`, `PostgresStreamingChangeEventSource` and
`WalPositionLocator` on Debezium `main`. "Where" says which versions write the
field, and is to be confirmed by the fixture capture in Tests.

| Key | Meaning | Where |
| --- | --- | --- |
| `lsn` | LSN of the last processed message. For a change, its WAL start. For BEGIN, the transaction's first LSN. For COMMIT, the **end** of the commit record. | all |
| `lsn_commit` | `lsn` of the last processed COMMIT, the end of that commit record. Updated only when a COMMIT is processed. This is what Debezium flushes to the slot. | 1.x (late) → 3.x |
| `lsn_proc` | last LSN all of whose events were processed. Debezium uses it as its streaming start, and as `lsn_commit` when that key is absent. | 1.x (late) → 3.x |
| `txId` | xid of the message's transaction (32-bit, carried as a long) | all |
| `ts_usec` | commit time | all |
| `messageType` | `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE`/`MESSAGE`/`BEGIN`/`COMMIT`/… | 1.x (late) → 3.x |
| `lsn_events_processed` | how many events at `lsn` in this transaction were processed, including the one this offset belongs to; reset at COMMIT | recent 3.x only |
| `snapshot` | present while a snapshot runs: boolean `true` in older versions, a snapshot-type string in newer ones (the fixtures pin which) | all |
| `last_snapshot_record`, `snapshot_completed` | snapshot finished; Debezium treats either as "snapshot done" | all / 3.x |
| `xmin` | slot xmin, only with `xmin.fetch.interval.ms` | optional |
| `incremental_snapshot_*`, `transaction_*` | incremental-snapshot state; transaction-metadata state | when enabled |

Two facts drive the mapping:

1. **`lsn_commit` is the end of a commit record, not its start.** For a COMMIT
   message, the walsender stamps the XLogData header with `txn->end_lsn`, and
   Debezium records that header value. This connector keys transactions by
   `BeginMessage.FinalLSN`, the **start** of the commit record. Debezium itself
   documents that on PG 17 an `end_lsn` can equal the next transaction's first
   change LSN (debezium/dbz#76). So the two LSN domains must never be compared
   with `<=`.
2. **Debezium flushes `lsn_commit` to the slot** in `commitOffset`. It does so
   after Kafka Connect has committed offsets, using the offset of the last
   record Kafka acknowledged. After a graceful stop, the slot's
   `confirmed_flush_lsn` (F below) is therefore at or below the committed
   `lsn_commit`. After an ungraceful stop it can be above.

### Notation

- **O** is Debezium's committed offset. **Lc** is its `lsn_commit`, **L** its
  `lsn`, **X** its `txId`, **M** its `messageType` and **n** its
  `lsn_events_processed`.
- **P** is the last transaction whose COMMIT Debezium processed, so
  `Lc = P.end_lsn`.
- **T** is the transaction of O's record, when O belongs to a change.
- **F** is the slot's `confirmed_flush_lsn`, and **C** is a fresh slot's
  consistent point.
- A transaction's commit LSN means `FinalLSN`, the start of its commit record,
  as in this connector's change key.

## Goals

1. Start a pipeline from O without a snapshot.
2. **No loss:** every change in the pipeline's tables is delivered either by
   Debezium (it is covered by O) or by Conduit.
3. **Bounded duplicates,** stated per case and checked by the harness against
   the dry run's prediction.
4. **Refuse rather than guess:** every unsafe input fails with a stable code
   and a suggested fix.
5. **Rollback to Debezium** is a documented, tested procedure.
6. **No position format change** and no change to the exact resume rule.

## Non-goals

- Offsets from standalone-mode files or Debezium Server (Java-serialized).
  Users export them through REST. A v0.22 follow-up could add these if anyone
  asks.
- Debezium **output** compatibility, meaning the same envelope and topics. That
  is v0.22 ("Debezium-compatible output mode"). This design only decides where
  the stream resumes.
- MySQL and other connectors (v0.22).
- Importing tables Debezium did not capture. Those need their own snapshot, in
  a separate pipeline.
- Debezium reading from a standby (PG 16+ logical decoding on standby). The
  import refuses when `pg_is_in_recovery()` is true.

## Constraints

- Invariant 1: the slot never moves past an unacked record. The import must
  not loosen the flush gate.
- Invariant 2: no position format change. The import produces no persisted
  state of its own.
- Invariant 3: every rule that skips a change has a proof that Debezium
  delivered it.
- A logical slot has one consumer at a time. A second `START_REPLICATION`
  fails with SQLSTATE 55006 (`replication slot … is active`).
- A slot is bound to one output plugin. Debezium's default `plugin.name` is
  `decoderbufs`, and this connector only speaks `pgoutput`.
- pgoutput reads publications through the historic catalog snapshot of the WAL
  it is decoding. A publication created after a slot's `restart_lsn` can fail
  with `publication "…" does not exist` when that WAL is decoded. So a reused
  slot must use Debezium's publication, and a fresh slot must be created after
  its publication.
- The import is applied only to an **empty** position. It never moves an
  existing position, forward or back.

## Decision

### D1. Where the work lives

| Piece | Repo | Why |
| --- | --- | --- |
| `source/dbzoffset`: parse and validate a Debezium offset, compute the resume point, run the preflight checks against Postgres | this repo, exported package | Postgres semantics belong to the connector. `conduit` already imports this connector as a built-in, so the CLI dry run calls the **same code** the connector runs at `Open`. |
| Import mode in `logrepl` (resume point, flush ceiling, preflight at `Open`) | this repo | data path |
| Reading Kafka Connect (topic, REST, JSON file), the cutover orchestration, `--dry-run`, the rollback export | `conduit`, `migrate kafka-connect` | Kafka Connect knowledge belongs to the CLI. It uses franz-go, which is already a dependency, so nothing new is added. |

`conduit` currently embeds connector v0.14.3. The import needs the v0.15.x bump
that lane #3004 already plans.

### D2. The connector setting

`logrepl.import.debeziumOffset` is a JSON string. It is empty by default. The
CLI writes it, and people do not normally write it by hand:

```json
{
  "format": "debezium-postgres/v1",
  "connector": "inventory-connector",
  "partition": {"server": "dbserver1"},
  "slotName": "debezium",
  "publicationName": "dbz_publication",
  "tables": ["public.orders", "public.customers"],
  "offset": {"lsn_proc": 24023128, "lsn_commit": 24023064, "lsn": 24023128,
             "txId": 555, "ts_usec": 1760000000000000, "messageType": "INSERT"}
}
```

`offset` is Debezium's map, unchanged. LSNs are parsed as `json.Number` into
`uint64`, never through `float64`. `slotName`, `publicationName` and `tables`
are Debezium's settings. They tell the connector whether it is reusing
Debezium's slot (`logrepl.slotName == slotName`) and let it check the table
set.

How the connector treats the setting:

- **Position empty:** apply the import. Validation runs first. With the import
  set, `snapshotMode` must be `never`, or validation fails with
  `postgres.import.snapshot_mode_conflict`, so the "no snapshot" intent is
  explicit. When reusing Debezium's slot, `logrepl.autoCleanup` must be
  `false`, or validation fails with `postgres.import.autocleanup_on_reused_slot`,
  because deleting the pipeline would drop Debezium's slot and publication and
  end any chance of rollback.
- **Position present:** ignore the setting and log one warning per start:
  `import ignored: pipeline has a position; remove logrepl.import.debeziumOffset`.

### D3. Slot strategy

**Fresh (default).** Creating a new slot with the same plugin is how Conduit
replication normally starts. Steps, run by `conduit migrate kafka-connect
--cutover`:

1. While Debezium runs, create the Conduit publication, then the Conduit
   `pgoutput` slot. Record its consistent point C.
2. Wait until Debezium's **committed** offset has `Lc ≥ C`, polling REST or the
   topic. On an idle database, Debezium does not process a commit, so Lc does
   not move. The CLI can prompt one (see Open questions, Q3). Otherwise the
   runbook says to write one row to any captured table.
3. Stop Debezium gracefully (`PUT /connectors/{name}/stop` on Kafka 3.5+, or
   delete the connector on older workers; the CLI saves the connector config
   for rollback). Wait until Debezium's slot is `active = false`.
4. Read the final O and check `Lc ≥ C` again.
5. Start the Conduit pipeline with the import.

Why it is gap-free: a new slot decodes in full every transaction that commits
at or after C. Every transaction that started before its snapshot reached
CONSISTENT has finished by C, which is how slot creation works. Conduit must
deliver T and everything after it, and `T.commit ≥ P.end_lsn = Lc ≥ C`. So the
fresh slot has all of it, whatever plugin Debezium used.

**Reuse (opt-in, `--slot-strategy=reuse`).** Stop Debezium, then point
`logrepl.slotName` and `logrepl.publicationName` at Debezium's slot and
publication. Requires `plugin = 'pgoutput'`. It saves a second slot and a
second WAL retention during the rollback window. The cost is that rollback
needs an offset written back to Debezium (D9).

> **Amended 2026-10-12 (Q4).** v0.21 ships only the fresh strategy, selected with
> `--slot-mode new`, which is the default and the only value. The reuse strategy
> above is v0.22. It will ship per Debezium version, and only for versions where
> the reuse rollback test passes. Until then, an import whose `logrepl.slotName`
> equals the offset's `slotName` fails with `postgres.import.slot_reuse_unsupported`.
>
> **Amended 2026-10-12 (B1).** The gap-free argument above needs a patched
> server: 14.13, 15.8, 16.4, 17.0 or later. Below those, a new slot can start
> mid-transaction. Fresh mode is refused there; see B1.

**Both consumers at once.** With reuse, Postgres refuses the second consumer
(55006). The preflight also checks `active = false` and, through REST, that the
connector is `STOPPED` or absent. `PAUSED` is not enough, because a paused task
can still hold the replication connection. With fresh, two consumers on two
slots is the overlap window. Conduit's slot is not read until cutover, so
nothing is delivered twice during it.

### D4. Mapping O to a resume point

The connector starts the subscription at the slot's position, as it does for a
fresh start (start LSN 0 → `restart_lsn`, and the server decodes from F). It
then replaces the subscription's resume point with the import's.

**Base rule:** skip every transaction whose commit LSN is **strictly below**
Lc. This is the existing exact rule with the key `(Lc − 1, MaxUint64)`, the
same construction as `ChangeKey.Predecessor`. `ResumePoint.Delivered` already
evaluates it. No new reading logic is needed.

**Partial rule (in memory only):** used when M is a DML type (`INSERT`,
`UPDATE`, `DELETE`, `TRUNCATE`) and X is present. For the **first**
transaction streamed with commit LSN ≥ Lc, and only if its `BeginMessage.Xid`
equals X:

- skip its changes whose change LSN is below L;
- if n is present **and** the slot is Debezium's own (reuse, so the same
  publication and the same row filters), also skip the first n changes at
  LSN L;
- deliver everything else.

> **Amended 2026-10-12 (Q1, B2, S2, S4).** The partial rule ships in v0.21
> (Q1), L-only. The second bullet, skipping the first n changes at L, is
> **removed** (B2: a multi-table TRUNCATE counts once per relation in Debezium
> but is one message here). The rule is armed only for `pgoutput` and
> `decoderbufs` offsets (S4). The duplicate cases are corrected in S2.

If the first such transaction's xid is not X, the partial rule is dropped,
with a warning, and that transaction is delivered in full. This can happen
when T touched only tables outside the pipeline. Dropping the rule only adds
duplicates; it never skips anything.

`ResumePoint` gets an optional `Partial{Xid, BeforeLSN, SkipAtLSN}` field,
used only by the subscription goroutine. Changes the partial rule skips still
advance the change key (seq is counted before the skip decision), so the
records Conduit delivers from T carry their real keys. The first ack persists
a normal version 2 position `(T.commit, seq)`, which the exact rule resumes
from. The partial rule exists only until that ack.

The other values of M:

- **M = COMMIT** (a heartbeat or a transaction-metadata END record): P is T,
  so Lc covers T and the base rule skips it.
- **M = BEGIN:** nothing of T was delivered, so T is delivered in full.
- **M = MESSAGE:** Postgres reports a logical message's **end** LSN. To keep
  the reasoning simple, the partial rule is not used and T is delivered in
  full.
- **M absent** (older 1.x): base rule only.

**Offsets without `lsn_commit`** (offsets older than late 1.x, or offsets with
only `lsn`) are refused with `postgres.import.offset_unsupported`. Fix: run
Debezium 1.9 or later until it commits an offset, then retry. The base rule
could use `lsn` with the legacy bound, but no supported Debezium writes such
offsets any more, and an untested rule is not worth having.

### D5. Preflight (the connector at `Open` and the CLI dry run share it)

All checks must pass before replication starts. Each failure has a stable code
(see Error codes).

1. O parses, and Lc is present and non-zero.
2. O is not a snapshot offset. The `snapshot` key is present (boolean or
   string), or `last_snapshot_record`/`snapshot_completed` is true with no
   `lsn_commit`. Also refused: any `incremental_snapshot_*` key that shows a
   snapshot in progress. See D7.
3. Lc ≤ `pg_current_wal_lsn()`, so the offset does not belong to another
   server or to a restored backup. `pg_is_in_recovery()` is false.
4. The slot exists (import mode never creates one: a slot created now would
   start at the current WAL and miss the gap), is on this database, has `plugin = 'pgoutput'`, is not
   `active`, and is not temporary. Its `wal_status` is not `lost` (PG 13+) and
   it has no `invalidation_reason` (PG 17).
5. **F ≤ Lc.** Postgres re-sends exactly the transactions whose commit is at or
   past F, and Conduit needs everything with commit ≥ Lc. If F > Lc, the
   changes in (Lc, F] are not re-sent and O does not cover them. Under reuse
   that means Debezium flushed past its committed offset (an ungraceful stop).
   Under fresh it means the slot was created after O. Either way the import
   refuses (`postgres.import.slot_ahead`). There is no override in v0.21 (Q2).

   > **Amended 2026-10-12 (Q2).** Decided: no override, in any version. The refusal is
   > final for that offset.
6. The publication exists and covers every table in `tables`
   (`pg_publication_tables`).
7. The pipeline's tables are a subset of Debezium's `tables`. A table Debezium
   did not capture has no history in O and needs a snapshot. The CLI checks
   this against the Kafka Connect config, and the connector checks it again
   against `tables`.

> **Amended 2026-10-12 (B3, S3, S4, S5, S10, nits).** The check order, the
> new checks (plugin, server version, database identity), the publication
> check in fresh mode, and the slot path that never creates a slot are in the
> Review fixes section. With an import set, `cdcMode` must be `logrepl`.

### D6. The flush gate in import mode

The gate is unchanged except for one addition. **Until the first ack of the
run, the reported flush position is capped at Lc.**

Why this is needed: at start, nothing has been emitted or acked, so the gate
is open and a keepalive could move `confirmed_flush_lsn` to the walsender's
`sentPtr`. That is safe for data, because `sentPtr` is below the commit of any
transaction not yet received (the reasoned assumption in
`20261008-interleaved-tx-resume.md`). But it can carry F above Lc. If the
process is then killed before the first ack, the position is still empty, the
import is re-applied, and check 5 refuses: a crash would turn into a stuck
pipeline. The cap keeps F ≤ Lc until a version 2 position exists, so
re-applying is always allowed and gives the same result.

The cap only ever lowers what is reported, so invariant 1 holds trivially. The
cost: on a database with no captured traffic, the slot stays at Lc until the
first record is acked. That is how Debezium behaves without heartbeats (B2 cut
heartbeats). The warning `import pending first ack; slot held at <Lc>` repeats
every minute, and the runbook covers it.

`reportedPositions` gets a `ceiling LSN` argument, zero meaning none. While
`acked` is unknown, the flush position is `min(flush, ceiling)`. Every earlier
report in this process was capped the same way, so the never-decrease rule
still holds: `reportedFlush` starts at zero in each process, and check 5
guarantees F ≤ Lc at start. A report below F is harmless, because Postgres
never moves `confirmed_flush_lsn` backwards.

> **Amended 2026-10-12 (S8).** What the cap retains (WAL and
> `catalog_xmin`), the `max_slot_wal_keep_size` path, and the citation for
> ack-after-persist are in S8.

### D7. Snapshot state is refused

| Offset shape | Code | Suggested fix |
| --- | --- | --- |
| `snapshot` present, not completed (initial or blocking snapshot running) | `postgres.import.snapshot_in_progress` | Let the Debezium snapshot finish and Debezium commit a streaming offset, then retry. |
| Snapshot completed but no streaming offset yet (`last_snapshot_record`/`snapshot_completed` true, no `lsn_commit`) | `postgres.import.snapshot_in_progress` | Write one row to a captured table so Debezium commits a streaming offset, then retry. |
| `incremental_snapshot_*` shows a snapshot in progress | `postgres.import.incremental_snapshot_in_progress` | Let it finish, or stop it with Debezium's `stop-snapshot` signal, then retry. |

Why snapshot-final offsets are refused: the boundary between Debezium's
snapshot and its stream is Debezium-internal. How a transaction that committed
around the snapshot's LSN was handled depends on the Debezium version and the
snapshot mode. The streaming offset that follows one ordinary write removes the
question.

> **Amended 2026-10-12 (S5).** The fix text for "Snapshot completed but no
> streaming offset yet" is replaced. One write is not enough; see S5.

### D8. Interaction with #339 and drift handling

- **No schema history is imported.** Debezium Postgres keeps none: it relies on
  Relation messages. An imported start therefore looks like a fresh start. The
  first delivered change of each table decides `driftInitial`, so no halt.
- **Replayed Relation messages do not decide drift** (#335). Changes skipped by
  the base or partial rule never reach the handler, so only shapes of
  delivered changes are decided. A DDL inside the delivered window is decided
  `driftInProcess` against the shape cached from the replay, under the
  configured policy, exactly as on any restart.
- **A halt before the first ack** emits the marker with the key one below the
  deciding change (#338). Its ack persists a version 2 position, and the
  approving restart resumes from it, not from the import. If the marker is the
  first record and its ack is the first ack, the D6 cap ends there, and the
  #338 rule that the gate stays closed after a marker still holds.
- **Drift across the cutover window is not detectable.** A DDL applied between
  Debezium's last change and Conduit's first is seen as the initial shape.
  This is the same as any fresh start, and it is documented.

### D9. Rollback (back to Debezium)

| When | Fresh slot | Reuse |
| --- | --- | --- |
| Before Conduit's first ack | Delete the pipeline (`autoCleanup` drops Conduit's slot and publication). Restart the Debezium connector, which resumes from O on its untouched slot. No loss, no duplicates. | Stop Conduit and restart Debezium. The D6 cap kept F ≤ Lc, so Debezium resumes exactly from O. |
| After Conduit has run | Restart Debezium. It resumes from O on its own slot and re-delivers everything since cutover into Kafka: no loss, and duplicates equal to Conduit's whole run. Then drop Conduit's slot. | `conduit migrate kafka-connect rollback` computes a Debezium offset from Conduit's last position and writes it with `PATCH /connectors/{name}/offsets` (Kafka 3.5+). |
| Window | As long as Debezium's slot is kept. It holds WAL the whole time, so set `max_slot_wal_keep_size` and drop the slot after a soak period. | Unbounded, but only supported where the harness proves the written-back offset (Q4). |

The reuse write-back offset, for Conduit's last persisted position
`(Tc, s)` with change LSN Lx:

```text
lsn_commit = Tc − 1   # forces Debezium to treat T' as unfinished
lsn = lsn_proc = Lx
messageType = <op of Lx>
txId = <xid of T'>
```

This depends on `WalPositionLocator`, which has changed in every Debezium
minor version, including the PG 17 false-match fix and the
`lsn_events_processed` skip. So it ships only for Debezium versions where the
rollback test is green. Elsewhere `rollback` refuses
(`migrate.kafka_connect.rollback_unsupported`) and points to the fresh-slot
strategy.

> **Amended 2026-10-12 (Q4, S7).** v0.21 supports only the Fresh slot column of the table above.
> The reuse write-back shown above is incomplete: it must also use F. It is an
> open v0.22 design item (S7).
>
> `conduit migrate kafka-connect rollback` in v0.21 restarts the Debezium
> connector, using the config the CLI saved at cutover, and drops Conduit's
> slot. The reuse column and the offset write-back are v0.22, per Debezium
> version.

### D10. How the user does it

The import uses both a CLI flag and a connector setting, each for its own job.
The CLI reads Kafka Connect and orchestrates the cutover. The connector setting
is what makes the import repeatable across a crash. A flag alone would leave
nothing for the connector to read when it restarts before its first ack.

```sh
# Plan: translate the connector config and show the computed resume point.
conduit migrate kafka-connect --config dbz-orders.json \
  --import-offset --connect-url http://connect:8083 --dry-run [--peek] --json

# Cut over: fresh slot, wait for Lc ≥ C, stop Debezium, import, write the pipeline.
conduit migrate kafka-connect --config dbz-orders.json \
  --import-offset --cutover --stop-connector --connect-url http://connect:8083

# Without the REST API, read the topic instead (read_committed, all partitions, to the high watermark).
  --offsets-from topic --bootstrap-servers kafka:9092 [--offsets-topic connect-offsets]
# Or read a saved REST export.
  --offsets-from file --offsets-file offsets.json

# Go back.
conduit migrate kafka-connect rollback --pipeline orders --connect-url ...
```

> **Amended 2026-10-12 (Q3, Q4).** The v0.21 flags are
> `--cutover [--stop-connector] [--slot-mode new] [--emit-cutover-marker]`.
> `--slot-mode` replaces `--slot-strategy`, and `new` is its only v0.21 value.
> `--emit-cutover-marker` is the opt-in from Q3, and writes
> `pg_logical_emit_message(false, …)`: without it, the CLI never writes to the
> source database. `--cutover` also gains `--slot-create-timeout` and `--reset`
> (S9). It starts the pipeline by default and waits for the import-complete
> signal (S8, S11); `--no-start` opts out. If Debezium's committed offset is not past C
> by the timeout, the CLI stops and prints the runbook step "write one row to
> a captured table" (corrected by S5 to two transactions, or heartbeats). It
> does not stop Debezium. In the dry-run JSON,
> `slot.strategy` becomes `slot.mode` with the value `"new"`.

`--stop-connector` is opt-in. Without it, the CLI checks that the connector is
stopped and fails with a fix if it is not.

The `--dry-run --json` output is shaped like this:

```json
{
  "offset": {"source": "rest", "connector": "inventory-connector",
             "partition": {"server": "dbserver1"},
             "lsn_commit": "0/16E8F58", "lsn": "0/16E8F98", "txId": 555,
             "messageType": "INSERT", "lsn_events_processed": null,
             "shape": "debezium>=1.9"},
  "slot": {"name": "debezium", "strategy": "reuse", "plugin": "pgoutput",
           "active": false, "confirmed_flush_lsn": "0/16E8F58",
           "restart_lsn": "0/16E1A20", "wal_status": "reserved",
           "retained_wal_bytes": 512000},
  "resume": {"skip_commits_below": "0/16E8F58",
             "partial": {"xid": 555, "skip_changes_below_lsn": "0/16E8F98",
                         "skip_at_lsn": 0},
             "duplicate_bound": "rows sharing LSN 0/16E8F98 in xid 555 (<= 1000)",
             "peek": {"duplicates_exact": 1, "replay_transactions": 3}},
  "checks": [{"name": "slot_not_ahead", "ok": true}],
  "errors": []
}
```

`--peek` opens the replication stream on the inactive slot and runs the same
`ResumePoint` over it without emitting anything. It sends **no** standby status
update, so the slot does not move. It stops at T's commit or after a timeout,
and reports the exact number of duplicates and the replay size. The harness
asserts the observed duplicates against this number.

### D11. Error codes

Connector codes, raised at validation or at `Open`. Each error says the config
path and how to fix it, following the `postgres.schema_drift.*` style.

| Code | Cause | Fix |
| --- | --- | --- |
| `postgres.import.offset_invalid` | unparseable, missing `lsn_commit` and `lsn`, an LSN that does not fit, or a wrong `format` | Re-export the offset with `migrate kafka-connect --dry-run`. |
| `postgres.import.offset_unsupported` | no `lsn_commit` (an offset older than late 1.x) | Run Debezium ≥ 1.9 until it commits an offset. |
| `postgres.import.snapshot_in_progress` | see D7 | see D7 |
| `postgres.import.incremental_snapshot_in_progress` | see D7 | see D7 |
| `postgres.import.snapshot_mode_conflict` | the import is set with `snapshotMode: initial` | Set `snapshotMode: never`. |
| `postgres.import.autocleanup_on_reused_slot` | reuse with `logrepl.autoCleanup: true` | Set it to `false`. |
| `postgres.import.slot_missing` | the slot does not exist (for example Debezium ran with `slot.drop.on.stop=true`) | If Debezium can run again, use `--slot-strategy fresh`. Otherwise re-snapshot or dual-run (runbook). |
| `postgres.import.slot_plugin` | the slot is not `pgoutput` | Use `--slot-strategy fresh`. |
| `postgres.import.slot_active` | the slot is in use | Stop Debezium (`PUT /connectors/{name}/stop`); pause is not enough. |
| `postgres.import.slot_invalidated` | `wal_status = lost` or `invalidation_reason` is set | The WAL is gone. Re-snapshot. |
| `postgres.import.slot_ahead` | F > Lc | See D5 check 5. Fresh: redo the cutover. Reuse: re-snapshot or dual-run. |
| `postgres.import.slot_database` | the slot is on another database | Fix `url`. |
| `postgres.import.offset_from_future` | Lc > `pg_current_wal_lsn()`, or the server is a standby | Wrong server, or one restored from backup. |
| `postgres.import.publication_mismatch` | the publication is missing or does not cover the tables | Add the tables to the publication, or fix `logrepl.publicationName`. |
| `postgres.import.tables_not_captured` | the pipeline has tables Debezium did not capture | Remove them and add them in a separate pipeline with a snapshot. |

> **Amended 2026-10-12 (Q3, Q4).** In the table above, the fix "use
> `--slot-strategy fresh`" now reads "use `--slot-mode new`", and
> `autocleanup_on_reused_slot` is v0.22. New in v0.21:
> `postgres.import.slot_reuse_unsupported` (the slot is Debezium's; fix: use
> `--slot-mode new`, and reuse arrives in v0.22) and
> `migrate.kafka_connect.cutover_marker_unsupported` (`--emit-cutover-marker`
> on a PG or Debezium combination where the marker does not advance
> `lsn_commit`; fix: write two transactions to a captured table).
>
> **Amended 2026-10-12 (review fixes).**
>
> - The `publication_mismatch` fix text is replaced (B3): repair the
>   publication, then redo the cutover with a new fresh slot. Never add tables
>   to a publication that an existing slot will decode from.
> - The `offset_unsupported` and post-snapshot fix texts are replaced (S5).
> - New connector codes: `server_unpatched` and `server_unsupported` (B1),
>   `database_mismatch` (S3), `plugin_unsupported` (S4) and
>   `cdc_mode_conflict` (S10).
> - New CLI codes: `lossy_source_config` (a warning, S6), and `slot_advanced`,
>   `slot_unowned` and `slot_create_blocked` (S9).

CLI codes (`migrate.kafka_connect.*`, mapped to exit codes per the
deterministic exit code ADR):

- `offset_not_found` – no offset for this connector.
- `ambiguous_partition` – more than one partition.
- `connector_running` – the connector is RUNNING or PAUSED.
- `offsets_unreachable` – neither REST nor the topic could be read.
- `cutover_timeout` – Lc did not reach C in time.
- `rollback_unsupported` – D9.

The CLI also passes the connector's codes through as they are.

## Correctness

> **Amended 2026-10-12 (S6, B2, S1).** Goal 2 is restated in S6. Changes
> that Debezium's configuration dropped count as processed by Debezium. In case
> 2 below, the clause about "the first n changes at L" is removed (B2). S1
> extends case 1 to non-transactional messages.

### No loss

Kafka Connect commits a source offset only after it has acknowledged the
record the offset belongs to and every earlier record from the task. Before
KAFKA-12226 it did this by flushing the producer before each commit; since
then it tracks acknowledged records in order. Assume also that Debezium was
stopped gracefully. Let c be a change in a pipeline table, in transaction U.

1. **U.commit < Lc.** Commit records do not overlap, so
   `U.commit < Lc = P.end_lsn` gives `U.commit ≤ P.commit`. U is P or a
   transaction before it in commit order, which is stream order. Debezium
   emitted a record for each of U's changes in a captured table before any
   record of T, or before the record that carries O. With in-order commits,
   all of them were acknowledged. **Skipped, delivered by Debezium.**
2. **U = T, xid = X, the partial rule applies, LSN(c) < L.** Within one
   transaction, pgoutput sends changes in WAL order, so c was emitted before
   O's record. **Skipped, delivered by Debezium.** The same holds for the first
   n changes at L under reuse: Debezium counted them on the same stream.
3. **Otherwise** U.commit ≥ Lc ≥ F (check 5), so Postgres re-sends U. Its WAL
   is retained, because a slot keeps what it needs to decode every transaction
   committing after `confirmed_flush_lsn`. Under fresh, `U.commit ≥ Lc ≥ C`
   (D3). Conduit delivers c under its existing invariants. **Delivered by
   Conduit.**
4. **Kill at any point.** Before the first ack, the position is empty. O and
   the setting are unchanged, F ≤ Lc still holds (D6), and the preflight and
   the rule give the same result. After the first ack, the version 2 position
   rules.

Assumptions, each turned into a harness assertion:

- (a) `lsn_commit` is the commit's end LSN.
- (b) `lsn` for a change is that change's WAL start.
- (c) The committed offset covers only acknowledged records, in order (both
  Kafka Connect commit paths above).
- (d) The table set matches.

### Duplicate bound

Conduit re-delivers a change that Debezium also delivered only in these cases:

| Case | Duplicates |
| --- | --- |
| Partial rule, reuse, n present | 0 |
| Partial rule otherwise | the changes of T at LSN L that Debezium delivered: 1 for an ordinary row, ≤ one `heap_multi_insert` record (≤ 1000 rows) for COPY or a multi-row INSERT |
| M = BEGIN or COMMIT | 0 |
| M = MESSAGE, M absent, or xid mismatch | T's prefix that Debezium delivered (bounded by T's size) |
| Debezium stopped ungracefully | the records Debezium produced after O, up to one `offset.flush.interval.ms` of traffic. They are in Kafka but not in O. In this case check 5 usually refuses. |

Every case is exercised by the harness. With `--peek`, the dry run predicts the
exact count, and the harness asserts equality, not just the bound.

> **Amended 2026-10-12 (S2).** The table above is replaced by the corrected
> table in S2.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Debezium still running (reuse) | Preflight `slot_active`. If Debezium is mid-reconnect, Postgres rejects the second consumer (55006). | 3 |
| Debezium stopped ungracefully, F > Lc | `slot_ahead` refusal | 3 |
| Debezium's slot dropped on stop | `slot_missing`; never auto-create a slot in import mode (that would start at the current WAL and lose the gap) | 3 |
| Fresh slot created after O (C > Lc) | `slot_ahead` refusal; the CLI also refuses before stopping Debezium | 3 |
| Slot invalidated (`max_slot_wal_keep_size`) | `slot_invalidated` | 3 |
| Conduit killed before the first ack | Re-import gives the same resume point; the cap keeps F ≤ Lc | 1, 2, 3 |
| Conduit killed after the first ack | Version 2 exact resume | 2, 3 |
| Pipeline reset months later with the setting still present | F ≫ Lc, so `slot_ahead`; the operator removes the setting | 3 |
| Idle database after import | Slot held at Lc until the first ack (D6); warning and runbook | — |
| Snapshot-state offset | Coded refusal (D7) | 3 |
| First transaction ≥ Lc is not X | Partial rule dropped, T delivered in full, warning | 3 |
| Table set mismatch | `tables_not_captured` / `publication_mismatch` | 3 |
| Offset from another server | `offset_from_future`, or an inconsistent slot leads to `slot_ahead`. An offset from another server that is **below** this server's WAL is not detectable from the offset alone. The CLI cross-checks the slot name and database from the Kafka Connect config. | 3 |
| Large LSN in JSON | Parsed as `uint64` through `json.Number`; values that do not fit fail | — |
| Aborted EOS offset writes in `connect-offsets` | Read with `read_committed` | 3 |
| Downgrade to a build without import before the first ack | An older build ignores the unknown setting or rejects it at validation. If it starts, it resumes from F with the legacy rule: duplicates of (F, Lc], no loss, because the slot is present | 3 |

## Upgrade and rollback (of the connector)

- There is no format change. The setting is new and optional. Positions
  written after an import are ordinary version 2 positions.
- Downgrading after the first ack works the same as for any version 2
  position (see the interleaved-tx doc).
- Debezium rollback: D9.

## Observability

- INFO at `Open`: the parsed O, the strategy, F, `restart_lsn`, the computed
  rule, and the duplicate bound.
- INFO at the first ack: `import complete at <key>; remove logrepl.import.debeziumOffset`.
- WARN every minute while the D6 cap holds the slot.
- WARN when the partial rule is dropped.
- Record metadata `postgres.import.possibleDuplicate=true` on the records in
  the duplicate set. With the partial rule, that is T's changes at L. Without
  it, all of T's changes. Sinks that are not idempotent can filter or dedupe
  on it (Q5).
- The connector has no metrics of its own today. Lane #3013 owns connector
  metrics, and an `import_pending` gauge is requested there.

> **Amended 2026-10-12 (Q5: approved, the reviewer recommends deferral,
> awaiting DeVaris).** The design has two options. Implementation slice 2
> ships option A until DeVaris decides.
>
> **Option A (defer the key; the reviewer's recommendation).** Nothing is
> added to records. At `Open`, an INFO line states the duplicate set in
> change-key terms. When the partial rule is armed, that is "xid X, the first
> transaction with commit LSN ≥ Lc, changes at LSN L". When T is delivered in
> full, it is "xid X, every change". When the set is empty (S2), the line says
> so. When the first record of the set is acked, a second INFO line gives the
> actual key range, `(T.commit, firstSeq)`–`(T.commit, lastSeq)`, for operators
> deduplicating downstream. It is not a public contract.
>
> **Option B (ship the key).** The key is `postgres.import.possibleDuplicate`.
> The name follows the connector's pattern `postgres.<area>.<flag>`
> (`postgres.snapshot.resumed`, `postgres.schema.drift`), with a
> lowerCamelCase flag for a two-word name, as OpenCDC does (`opencdc.readAt`).
>
> - The value is `"true"`, and the key is absent otherwise, like
>   `postgres.snapshot.resumed`.
> - It is set only on the S2 duplicate set: T's changes at L when the partial
>   rule is armed; all of T when M is a transactional MESSAGE or absent and T
>   is identified by X; nothing when the set is empty (M = COMMIT or BEGIN,
>   xid mismatch).
> - If the set cannot be bounded (X absent and the partial rule not armed),
>   every change of the first transaction ≥ Lc is tagged, so the contract
>   "absent ⇒ not a cross-system duplicate" still holds.
> - It is never set after the first ack.
> - The constant is `logrepl.MetadataImportPossibleDuplicate`. It is documented
>   in the README metadata section and on conduit.io. The harness checks that
>   Conduit ∩ Debezium is a subset of the tagged records, and that no record
>   is tagged in the S2 zero-duplicate cases.

## Tests

### Unit and property

- `FuzzParseDebeziumOffset` with a seed corpus of the fixtures captured from
  every supported Debezium version (`source/dbzoffset/testdata/<version>/*.json`),
  covering streaming, snapshot, snapshot-completed and incremental-snapshot
  offsets.
- A reference model for `ResumePoint` with `Partial`. Random streams with
  interleaved transactions, COPY runs that share an LSN, PG 17 `end_lsn`
  collisions and random O cut points are checked against a model of "what
  Debezium delivered", with loss = 0 and duplicates ≤ the bound. The
  connector has no property framework yet. Adding `pgregory.net/rapid` as a
  test-only dependency is requested here: it is what CLAUDE.md names, and the
  invariants are about sequences.
- `reportedPositions` with a ceiling: the table cases, plus never-decrease.
- Preflight against a real Postgres for each check.

> **Amended 2026-10-12 (rapid approved).** `pgregory.net/rapid` is approved as a
> test-only dependency. It appears only in `_test.go` files, never in the
> connector binary. The reason: the properties here are about generated
> sequences (interleaved transactions, LSNs shared by COPY rows, random cut
> points), and rapid shrinks a failing sequence to a minimal one. Native Go
> fuzzing cannot shrink structured sequences. CLAUDE.md also names
> rapid/gopter for this kind of test. The implementation PR repeats this
> justification.

### Integration harness: real Debezium (`test/debezium`)

- **Compose:** Postgres 14, 15, 16 and 17 (`quay.io/debezium/postgres:<v>`,
  which also has decoderbufs); Kafka in KRaft mode; and Kafka Connect with the
  Debezium Postgres connector (`quay.io/debezium/connect`) in versions 2.7
  and the latest 3.x, plus 1.9 on PG 14 for the topic-only path (pre-KIP-875).
- **Matrix:**
  - Pull requests: PG 17 × Debezium 3.x × pgoutput × {fresh, reuse}.
  - Nightly: the full PG × Debezium × {pgoutput: fresh and reuse;
    decoderbufs: fresh} matrix.
  - This shares infrastructure with the migration harness lane (#3010) and
    hands it the Postgres case.
- **Workload:** a generator with a ledger (it reuses `test/chaos/ledger.go`).
  Every row has a unique id and version. The mix:
  - single-row transactions;
  - one large transaction (100k rows);
  - COPY batches;
  - interleaved long-open transactions (T1 begins, many commit, then T1
    commits);
  - updates that change the primary key, and deletes;
  - an `ALTER TABLE ADD COLUMN` mid-stream (drift under `halt` and `evolve`).
- **Cut points** (each one a test):
  1. graceful stop at quiescence;
  2. graceful stop while Debezium is emitting the large transaction (offset
     mid-T, partial rule);
  3. stop during the long-open interleaved T1;
  4. stop during a COPY (repeated with a randomized delay until an offset
     lands at a shared LSN; the test asserts that it did);
  5. offset written by a heartbeat or transaction-metadata END record
     (M = COMMIT);
  6. Kafka Connect worker `kill -9`, which expects either a successful import
     with no loss or a `slot_ahead` refusal, and never loss.
- **Diff:** expected = the ledger. Observed = Debezium's topics (read to the
  end) ∪ Conduit's destination (file destination, JSON Lines). Identity is
  `(table, pk, version, op)`. The test asserts:
  - loss is 0;
  - Conduit ∩ Debezium equals the `--peek` prediction;
  - there are no duplicates inside Conduit's own output beyond the
    connector's at-least-once window.
- **Kill tests** (the `test/chaos` process-kill harness, SIGKILL): kill after
  the preflight but before streaming; after the first change is emitted but
  before its ack (re-import, then assert F ≤ Lc after the kill); after the
  first ack; at `chaospoint.StandbyStatusUpdate` during the cap. Each is
  followed by recovery and the same diff.
- **Rollback tests:** fresh, before and after Conduit has run. Reuse write-back
  on every Debezium version in the matrix. The versions where this is green
  are the supported list for `rollback` under reuse.
- **Refusal tests:** one per error code, each asserting the code and that the
  slot did not move.

> **Amended 2026-10-12 (Q4).** For v0.21 the matrix is fresh slot only.
>
> - **Pull requests:** PG 17 × Debezium 3.x × pgoutput.
> - **Nightly:** PG 14–17 × Debezium {2.7, 3.x} × {pgoutput, decoderbufs},
>   plus Debezium 1.9 on PG 14 for the topic-only path.
> - **Moves to v0.22:** reuse cases, the reuse rollback tests, and the
>   n-based zero-duplicate case.
> - **Added:**
>   - a refusal test for `slot_reuse_unsupported`;
>   - a test that `--cutover` without `--emit-cutover-marker` makes no write
>     to the source database (checked through `pg_stat_database`
>     `xact_commit` and the WAL position);
>   - the Q3 harness check of `--emit-cutover-marker` for each PG and
>     Debezium version;
>   - a `postgres.import.possibleDuplicate` assertion: every record in
>     Conduit ∩ Debezium carries the key.
> - **Fresh-slot rollback tests** (before and after Conduit has run) stay in
>   v0.21.
>
> **Amended 2026-10-12 (review fixes).**
>
> - **Images:** pinned by digest at or above 14.13, 15.8, 16.4 and 17.0, with
>   PG 18 added (B1). An unpatched 16.3 image runs only the `server_unpatched`
>   refusal test.
> - **Debezium:** `skipped.operations=none` (S6).
> - **New cut points:**
>   - a transaction opened before slot creation and committed after Lc (B1);
>   - a multi-table TRUNCATE (B2);
>   - a non-transactional message as the last offset (S1);
>   - a heartbeat or END record as the last offset, asserting 0 duplicates
>     (S2);
>   - Debezium's graceful stop exceeding `task.shutdown.graceful.timeout.ms`
>     (S9);
>   - a CLI kill at every journal step, then a re-run (S9);
>   - a slot creation blocked by a prepared transaction (S9);
>   - an offset from another database or cluster (S3).
> - **Removed:** every assertion about the n-based skip (B2).

## Alternatives considered

1. **Re-snapshot (today's path).** Start a new pipeline with
   `snapshotMode: initial`. It needs no new code, but it takes hours of load on
   large tables and duplicates every row, which is exactly what this lane
   exists to avoid. It remains the fallback for `slot_missing`,
   `slot_invalidated` and `slot_ahead`.
2. **Dual-run cutover with no offset.** Create Conduit's slot, run Conduit
   alongside Debezium with `snapshotMode: never`, then stop Debezium. It needs
   no parsing and no new connector code, only a runbook. There is no loss if
   Conduit's slot exists before Debezium stops. But the duplicates are
   everything committed between C and Debezium's stop, minutes to hours of
   traffic, and two writers feed the sinks at once. It is documented as the
   fallback for unsupported Debezium versions, and rejected as the default
   because its duplicates are unbounded by design.
3. **Snapshot-from-LSN / peek-computed exact position.** Run
   `pg_logical_slot_peek_binary_changes` before starting, find T's commit LSN
   and the seq of L, and write an exact version 2 position, with no runtime
   rule. Conduit has no API to set a connector's position, so it would still
   need a setting. The SQL peek materializes T in full (a tuplestore that
   spills to disk), and it would run again at every start before the first
   ack. The partial rule gets the same result in one pass. Peeking is kept
   only for the dry run, done over the stream with no feedback.
4. **Read Debezium's output topics** to find the last delivered event
   (`source.lsn`, `source.txId`) instead of `connect-offsets`. That is more
   precise, since it includes records produced after the last offset commit.
   But it needs Debezium's converters and Schema Registry, every table topic,
   and an order across partitions. It is rejected for v1. If ungraceful stops
   turn out to matter, it could make the `slot_ahead` case safe.
5. **Keep Debezium through `conduit-kafka-connect-wrapper`.** The wrapper
   loads Debezium's offsets natively, so there is no mapping risk. But it does
   not move the user to this connector, its Postgres coverage is unproven
   (`20260722-debezium-compete-roadmap.md`), and it keeps the JVM. It is the
   right path for databases without a native connector. It is not this one.
6. **Base rule only** (no partial rule). This is less code on the data path,
   and the bound becomes "the prefix of T that Debezium delivered". Stopping
   Debezium during a bulk load would then duplicate up to the whole load. The
   partial rule is about 30 lines and is checked by the property model, so it
   is included. Q1 asks whether to ship it in v0.21.

## Implementation slices

Each slice is its own PR. All are Tier 1 except slice 5.

1. `source/dbzoffset`: the parser, the fixtures, fuzzing, and the
   resume-point computation. No data-path wiring yet.
2. Import mode: the setting, validation, preflight at `Open`, `Resume`
   override with `Partial`, the flush ceiling, and the error codes. Unit and
   property tests, plus chaos kill cases.
3. `test/debezium` harness and CI (PR and nightly matrices).
4. `conduit`: `migrate kafka-connect --import-offset` (REST, topic, file),
   `--dry-run`/`--peek`, `--cutover`, `rollback`. This depends on #3003's
   command skeleton.
5. Docs: the runbook `docs/operations/postgres-debezium-offset-import.md` in
   `conduit` (symptom → diagnosis → fix for every code, plus the slot-held
   warning and the overlap WAL growth), the conduit.io migration page,
   `llms.txt`, and the changelog.

## Open questions for DeVaris

> **Resolved 2026-10-12, except Q5.** See Decisions. Q5 was approved, but
> the reviewer recommends deferral, so it awaits DeVaris. The questions are
> kept below as they were asked.

- **Q1.** Ship the partial rule in v0.21, or only the base rule? The
  recommendation is to ship it (see Alternative 6).
- **Q2.** Should `slot_ahead` have an override? The recommendation is no for
  v0.21. The case means O does not cover what the slot discarded. Overriding
  would rely on Debezium having produced records it never committed, and that
  is not provable.
- **Q3.** May the CLI write to the source database during `--cutover`, to move
  an idle Debezium past C? `pg_logical_emit_message(true, …)` writes no table
  rows, but whether it advances `lsn_commit` depends on the PG version (PG 15+
  skips empty transactions) and on Debezium's `messages` option. The
  recommendation is to try it in the harness, and to fall back to the runbook
  ("write one row") where it does not work.
- **Q4.** Reuse rollback: ship the offset write-back only for the Debezium
  versions where the harness is green, or drop reuse from v0.21 and ship fresh
  only? The recommendation is to ship both, with fresh as the default and
  write-back gated per version.
- **Q5.** Add the `postgres.import.possibleDuplicate` metadata key? It would
  be a new public metadata contract.
- **Q6.** Setting name: `logrepl.import.debeziumOffset` (proposed, matching
  the `logrepl.*` prefix) or `cdc.importOffset` (the name in the lane brief)?

> **Amended 2026-10-12 (Q4, Q5, review fixes).**
>
> - Slice 2 includes option A of Q5 (the INFO duplicate-range log), not the
>   metadata key, until DeVaris decides Q5. It also includes the B1, S3, S4
>   and S10 preflights and the `slot_reuse_unsupported` refusal.
> - Slice 4 ships `--slot-mode new`, `--emit-cutover-marker`, the cutover
>   journal (S9), and starting the pipeline by default (S11).
> - Reuse (D3 "Reuse", the D9 write-back per S7) is a separate v0.22 slice,
>   gated per Debezium version on its rollback test.

## Related

- ConduitIO/conduit#3004 (this lane), #3002 (v0.21 tracking), #3003
  (migrate/kc CLI), #3010 (migration harness), #3013 (connector
  observability).
- ConduitIO/conduit-connector-sdk#378 (position-only checkpoint). If it
  lands, the D6 cap could be replaced by persisting the import at `Open`.
- `20261008-interleaved-tx-resume.md`, `20261007-dbz3-b2-heartbeats.md`,
  `20261008-drift-decision-on-delivery.md`,
  `20261009-drift-approval-delivers-boundary-row.md`,
  `20260724-dbz3-postgres-cdc-parity.md`.
- Debezium sources read for the offset semantics: `PostgresOffsetContext`,
  `PostgresStreamingChangeEventSource` (`processReplicationMessages`,
  `commitOffset`) and `WalPositionLocator` (debezium/debezium `main`,
  2026-10-12).

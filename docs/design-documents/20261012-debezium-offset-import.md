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
  decoding plugin and leaves Debezium's slot alone, so rollback is a restart
  (in v0.21, a manual one, checked by `--check-rollback`).
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
section, the Scope cut section, the Review fixes sections and the amendment blocks
govern.

| # | Decision | Effect on this design |
| --- | --- | --- |
| Q1 | **Ship the partial rule in v0.21.** | D4's in-memory partial rule is in scope for v0.21. |
| Q2 | **No override for `postgres.import.slot_ahead`.** | D5 check 5 is a hard refusal. Remedies are re-snapshot, dual-run, or redoing the cutover. |
| Q3 | **The CLI may write a logical marker message only behind an explicit opt-in flag.** | `--cutover` never writes to the source database by default. Without the flag, the runbook tells the user to write to a captured table; S5 corrects what that write must be. With `--emit-cutover-marker`, the CLI writes a non-transactional message, `pg_logical_emit_message(false, 'conduit.migrate.cutover', …)`. Debezium treats a non-transactional message as a commit and advances `lsn_commit` to the message's end (S1). Caveats: Debezium sees the message only if pgoutput's `messages` option is on and the prefix passes `message.prefix.include.list`/`message.prefix.exclude.list`. The offset that carries it is committed only through the message's own record or a heartbeat. That record lands in Debezium's `<topic.prefix>.message` topic, where downstream consumers see it. Where the marker does not move `lsn_commit` past C before the timeout, the CLI fails with `migrate.kafka_connect.cutover_marker_unsupported`. |
| Q4 | **v0.21 ships the fresh-slot strategy only.** Reusing Debezium's slot moves to v0.22, gated per Debezium version on a passing rollback test. | The CLI flag stays `--slot-strategy fresh\|reuse`, as in D3 and D10 and in the migrate design (#3026). `fresh` is the default and the only strategy v0.21 executes. `--slot-strategy reuse` is accepted by the parser and refused in v0.21 with `postgres.import.slot_reuse_unsupported`. In v0.21, pointing `logrepl.slotName` at Debezium's slot is refused with `postgres.import.slot_reuse_unsupported`. The reuse-only parts are v0.22: D3 "Reuse", the reuse column of D9 (redesigned per S7), the `autocleanup_on_reused_slot` code, and the reuse test cases. The n-based skip in D4 is dropped outright (B2). |
| Q5 | **Defer the `possibleDuplicate` metadata key** (DeVaris, 2026-10-12, after the review recommended deferral). | v0.21 ships logging only: a count at INFO, and the records' keys at DEBUG (see Observability). No public metadata key is added. The key is kept as a future-work note. |
| Q6 | **The setting name is `logrepl.import.debeziumOffset`.** | No change: the design already uses it. |
| — | **`pgregory.net/rapid` is approved as a test-only dependency.** | It is used for the property tests (see the Tests amendment). |

With Q4 and B2, the **v0.21 duplicate bound**, when the partial rule is
armed, is the changes of T at Debezium's last change LSN L:

- 1 for an ordinary row;
- at most one multi-insert WAL record (≤ 1000 rows) for COPY;
- one record per relation for a multi-table TRUNCATE.

The corrected cases are in S2.

## Scope cut and final decisions (round-7 completion, DeVaris, 2026-10-12)

This section governs every other part of this document.

**Automated `--reset` and `rollback` are cut from v0.21.** Rounds 4 to 7 of
the Tier 1 review kept finding loss paths in the commands that restart
Debezium and drop Conduit's slot. Round 7 found two more: a re-run after the
restart had no specified proof state, and `--reset` at step 7 with Debezium
reporting RUNNING had a wrong target and no way out (Kafka 3.5's
`stopConnector` writes empty task configs before STOPPED, so a resume restarts
the tasks and flushes past any target taken earlier). DeVaris cut them
rather than add an eighth round of machinery that had never run against a
real Kafka Connect.

- **Removed from v0.21:** the `--reset` flag and the `rollback` subcommand;
  their journal steps; the CLI-driven Debezium restart, the attach proof's
  restart-to-drop flow, the closing path for pending operations,
  `reset_target`; the saved-config resubmission and `--debezium-secret`; the
  flags `--reset`, `--reset-timeout` and `--rollback-timeout`; and the codes
  `reset_pending`, `reset_unsafe`, `rollback_pending` and
  `rollback_slot_lost`.
- **Kept:** everything the forward path needs. Preflight, the import, the
  cutover and its journal, `--without-import`, the lock (S15), the Debezium
  config refusals (S5 item 9), and F_stop and R_stop, which the cutover
  journals at step 8 for S13's `identity_ahead` warning and for
  `--check-rollback`.
- **Added:** a manual reset and rollback runbook with exact commands, and a
  read-only `conduit migrate kafka-connect --check-rollback` that runs the
  same safety checks and prints a verdict (S12). It never drops a slot,
  never stops or starts anything, and never deletes a pipeline.
- **Deferred to v0.22:** automated reset and rollback, to be built against a
  real Kafka Connect and Debezium harness. ConduitIO/conduit#3044 tracks it,
  with the round-4 to round-7 findings listed as its requirements.

**Decided:**

- **`logrepl.requireExistingSlot` is tri-state** (S13): unset refuses to
  create a slot or publication for a non-empty position; `true` always
  refuses; an explicit `false` allows it, with a WARN and the
  `slot_recreated` metric. It ships with the behaviour-change release note in
  S13.
- **The equal-position proof is accepted** (S15): a new walsender whose
  first sighting shows the same `confirmed_flush_lsn` as the last one under
  the old walsender is the same slot. `--check-rollback` and the runbook use
  it.

## Review history (Tier 1 design review, 2026-10-12)

Seven review rounds ran on this PR. Every fix was made in place, and passages
are tagged with the pass that last changed them, **(2nd pass)** to **(7th
pass)**. Nothing here has merged, so no superseded row is left for an
implementer to copy. Findings about the automated `--reset` and rollback are
no longer in the design: the scope cut removed those flows, and #3044 carries
the findings as v0.22 requirements.

| Round | Found | Where the fix lives now |
| --- | --- | --- |
| 1 | Fresh mode on unpatched Postgres (B1), the n-based TRUNCATE skip (B2), `publication_mismatch` fix text (B3), and S1–S10 | B1–B3 and S1–S10 below |
| 2 | Loss paths in re-runs and the first rollback; eight should-fixes | S2, S5, S8, S9, S11, S13 |
| 3 | `slot.drop.on.stop`, flush settings, the `creating_slot` and `starting` steps, saved-config handling | S5 item 9, S9 |
| 4 | Debezium resumes at max(requested, F_d), not Lc; `connector_and_driver`; `--without-import` preconditions; `autoCleanup` and `LifecycleOnDeleted`; slot adoption | S5 item 9, S9, S13, S16; reset and rollback parts are #3044 requirements |
| 5 | Step 7 handling, Key zero on the fallback, the lock, provisioned pipelines, the marker | S9, S13, S14, S15, S16; reset and rollback parts are #3044 requirements |
| 6 | The attach proof across walsender pids, the lock under poolers, `--pipelines-path`, `postgres.logrepl.*` codes | S13, S15 (now read-only, for `--check-rollback`), S16 |
| 7 | Lock hardening, startup options that move the slot, the equality proof, #3033 pins, default-refuse; two blockers in automated reset and rollback | S5 item 9, S13, S15; the blockers are resolved by the scope cut |

**Decisions recorded (DeVaris, 2026-10-12).**

- An orphaned walsender at `creating_slot`: the CLI refuses with
  `slot_create_blocked` and prints the pid; it never terminates a backend.
  The pid and `backend_start` are journalled and printed (S9).
- `resume: slotOnly`, approved in the 4th pass, is replaced by
  `logrepl.requireExistingSlot`, now decided as tri-state (S13).
- The residual window after a Debezium restart was acceptable with a runbook
  warning only if no cheap proof existed. The attach proof is that proof; it
  now runs inside `--check-rollback --watch`, and the runbook keeps the
  warning (S12, S15).
- The scope cut and the two decisions above.

## Review fixes (sections B1 to S16)

The sections below are the fixes themselves, edited in place through all seven
rounds. Where they and the earlier Decision text disagree, they govern; the
Scope cut section governs them.

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
| 18 | all | master commit `bb19b70081` (2024-07-11), before the REL_18 branch |

- **Preflight.** Both the CLI, before it creates the slot, and the connector,
  at `Open`, refuse fresh mode when `server_version_num` is below the fixed
  release for its major. The code is `postgres.import.server_unpatched`, and
  the fix is to upgrade to the listed minor or later. Majors below 14 are
  refused with `postgres.import.server_unsupported`. **(2nd pass)** That is a
  policy choice, not a correctness one: 13.16 carries the fix too. Postgres 13
  is past end of life (November 2025) and outside the test matrix, so it is
  not supported.
- **Harness.** Images are pinned by digest at or above the fixed minors, and
  PG 18 joins the matrix with the Debezium versions that support it. One
  unpatched image (16.3) runs only the refusal test.
- **New cut point (3rd pass, reworded).** A transaction straddles slot
  creation:
  1. It is opened, and writes rows, before the CLI issues
     `CREATE_REPLICATION_SLOT`.
  2. Creation blocks until it ends. The test asserts the block (S9's
     timeout diagnostic names the pid).
  3. It commits while creation is blocked, so on a patched server it commits
     **before** C.
  4. Debezium delivers it, Lc passes C, and Debezium is stopped.

  The diff shows zero loss, and Conduit's slot does not re-send any part of
  it. An unpatched image is refused with `server_unpatched` before any slot
  exists. The variant that matters on unpatched servers, a transaction
  committing after C with its head before C, runs only on the refusal path.

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

**Telling it apart from the offset alone (2nd pass).** For a
non-transactional message, `commitMessage` sets `lsn_commit` to the record's
own `lsn`. So the offset has `messageType = MESSAGE` and `lsn == lsn_commit`.
A transactional message keeps the earlier commit's `lsn_commit`. **(3rd
pass)** The rule is: `MESSAGE` with `lsn == lsn_commit` is non-transactional
and commit-class, so the partial rule is not armed and there are 0
duplicates. Every other `MESSAGE` is transactional (see the S2 table).
**(4th pass)** When Conduit itself receives messages (the
Debezium-compatible output mode, conduit#3033), the import checks a
non-transactional message against Lc inclusively, and the partial rule
covers transactional messages in T. Both are in S14.

**Harness.** A cut point where the last committed offset is a
non-transactional message, written with `--emit-cutover-marker`, sitting
between a transaction's first change and its commit. The test asserts zero
loss and that the open transaction is delivered in full.

### S2. The duplicate model (rewritten, 2nd pass)

Two corrections from the reviews:

- A heartbeat or END record written **after a commit** covers T in full.
- The partial rule must not be disarmed by an earlier transaction that
  Debezium never saw. Such a transaction Q exists when Debezium's publication
  drops rows the pipeline's publication keeps: a row filter (`prqual`), a
  column list, or `publish` flags that leave out an operation. Q commits
  after P and before T, so Debezium never processed it and Conduit must
  deliver it. If Q disarmed the rule, T's prefix would be duplicated while
  the log claimed the set was empty.

**Partial rule, as corrected.**

- The rule is armed when M is a DML type and X is present.
- It applies to the transaction whose `BeginMessage.Xid == X` and whose commit
  LSN is ≥ Lc. When `ts_usec` is present, the transaction's `CommitTime` must
  also equal `ts_usec`. Debezium takes `ts_usec` from the same BEGIN message.
- **(3rd pass, corrected reasoning.)** What makes xid reuse harmless is the
  LSN bound, not `ts_usec`. A different transaction that reuses X can only
  start after T has ended, so all of its changes are written after L. The
  rule skips only changes below L, so it skips nothing of that transaction.
  The `ts_usec` match keeps the duplicate accounting and the logs pointed at
  the right transaction. It does not carry safety.
- **(3rd pass)** X is normalised to `uint32`. Older Debezium read the
  decoderbufs xid as a signed int, so a negative `txId` is taken as
  `txId + 2^32`. Values outside [−2^31, 2^32) fail with `offset_invalid`.
- It stays armed until that transaction has been streamed. Transactions with
  another xid are delivered in full and do **not** disarm it.
- It is disarmed when the process restarts from a version 2 position. After a
  crash, T is delivered in full: more duplicates, never loss.
- A mid-transaction heartbeat carries the messageType of the last change it
  saw. It is therefore a DML case, not a COMMIT case.

| Case | How to recognise it from the offset | Duplicates |
| --- | --- | --- |
| Commit-class: a heartbeat after a commit, an END record, or a non-transactional message | M = COMMIT, or M = MESSAGE with `lsn == lsn_commit` | 0. T is fully covered by the base rule, and the partial rule is not armed. |
| BEGIN | M = BEGIN | 0 |
| DML: a change record, or a heartbeat emitted mid-transaction | M ∈ INSERT/UPDATE/DELETE/TRUNCATE | T's changes at LSN L that Debezium delivered: 1 for an ordinary row, ≤ 1000 for one multi-insert record, one per relation for a multi-table TRUNCATE. **(4th pass)** Plus at most one transactional message of T whose end LSN is L, when Conduit receives messages (S14). Transactions with another xid that arrive first are not duplicates: Debezium never saw them. |
| DML, and T never arrives on this slot | the same offset; T touched only tables outside the pipeline, or PG 15+ skipped it as empty | 0 |
| Transactional MESSAGE, or M absent | M = MESSAGE with `lsn != lsn_commit`, or no messageType | T's prefix that Debezium delivered. T is identified by X (and `ts_usec`) for logging; nothing is skipped. |
| Debezium stopped ungracefully | — | **(4th pass)** In fresh mode Debezium's slot passing Lc is `identity_ahead`, a warning, and the changes in (Lc, F_stop] are duplicates. `slot_ahead` applies only to Conduit's slot. Otherwise, the records produced after O, up to one `offset.flush.interval.ms` of traffic. |

**`lossy_source_config` inspection (2nd pass).** The CLI also inspects
Debezium's publication: its `publish` flags (`pg_publication.pubinsert`,
`pubupdate`, `pubdelete`, `pubtruncate`), row filters
(`pg_publication_rel.prqual`, PG 15+) and column lists (`prattrs`, PG 15+).
Any difference from what the pipeline captures is reported as lossy, because
Debezium's output never contained those rows or operations.

### S3. Wrong-database offsets

The setting gains three fields:

- `database`: Debezium's `database.dbname`.
- `systemIdentifier`: the CLI captures it with `IDENTIFY_SYSTEM` on the
  replication connection, which needs no extra privilege.
- `debeziumSlotName`: renamed from `slotName`. Only the new name is accepted,
  since no build has shipped with either.

At `Open`, the connector refuses with `postgres.import.database_mismatch`
unless all of these hold:

- `database` equals `current_database()`;
- `IDENTIFY_SYSTEM` returns the same `systemIdentifier`;
- Debezium's slot exists in `pg_replication_slots` with
  `database = current_database()`.

In fresh mode Debezium's slot is kept through cutover for rollback, so
requiring it costs nothing.

**(2nd pass)** The `systemIdentifier` comparison alone cannot fail in
practice. The CLI and the connector use the same connection string, and
clones and promoted standbys share the identifier. The real proof that the
offset belongs to this server is the slot check in S13.

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
  followed by a later record. Write two separate transactions to a captured
  table, or set `heartbeat.action.query` to an insert or update on a captured
  table. Wait for an offset flush (`offset.flush.interval.ms`), then retry."_
  (D7 is edited in place.)
- **The Q3 runbook step, before the stop,** has the same correction. Moving
  Debezium past C needs two separate transactions to a captured table, or a
  `heartbeat.action.query` that writes to a captured table.
- **Why not plain heartbeats (4th pass).** `heartbeat.interval.ms` alone emits
  heartbeat records that carry the current offset. Their `lsn_commit` moves
  only when Debezium processes a commit. On an idle database there is none,
  and on PG 15+ the walsender's keepalives and skipped empty transactions do
  not produce one either. So every idle-database fix text in this design
  (`cutover_timeout`, D7, and the runbook's idle-database case, S12) names
  `heartbeat.action.query` writing to a captured table, two transactions on a
  captured table, or `--emit-cutover-marker` where the flow accepts it.
- **After the stop (2nd pass),** waiting for the import to complete needs one
  committed write to a **pipeline** table, and no re-run (S11). Conduit needs
  one acked record, not a Debezium offset.
- **`offset_unsupported`.** The fix text drops the misleading "Debezium
  ≥ 1.9" pointer. It now reads: _"This offset has no `lsn_commit`. Run the
  connector on a Debezium version that writes it (every 2.x and 3.x release;
  the fixture set lists the 1.x builds that do), let it commit an offset after
  at least one transaction, then retry."_
- **D5 check order (3rd pass, edited in place).** The CLI runs every
  offset-independent check **before** `PUT /connectors/{name}/stop`, against
  the latest committed offset, and before it creates any slot where marked.
  After the stop, only Lc ≥ C and the S13 identity re-check can still fail.
  The connector at `Open` runs the full list again.
  1. Parse (including the `txId` normalisation).
  2. Incremental snapshot keys (`incremental_snapshot_in_progress`).
  3. `snapshot`, `last_snapshot_record`, `snapshot_completed`
     (`snapshot_in_progress`).
  4. `lsn_commit` present (`offset_unsupported`).
  5. **Before slot creation:** `pg_is_in_recovery()` is false and Lc ≤
     `pg_current_wal_lsn()` (`offset_from_future`). This also closes the
     standby case: a PG 17 `sync_replication_slots` copy of Debezium's slot
     on a standby.
  6. Plugin (S4).
  7. Server version (B1), before slot creation.
  8. Database identity (S3).
  9. **Debezium's settings (3rd pass):**
     - `slot.drop.on.stop=true` is refused with
       `migrate.kafka_connect.slot_drop_on_stop`. Stopping would destroy the
       slot that S13 and the manual rollback (S12) depend on.
     - `lsn.flush.mode=manual` or `flush.lsn.source=false` is refused with
       `migrate.kafka_connect.lsn_flush_mode_unsupported`. With either, Debezium
       never moves its slot, so S13 cannot prove anything.
     - **(4th pass)** `lsn.flush.mode=connector_and_driver` is refused with
       the same code. The driver flushes keepalive LSNs, which moves Debezium's
       slot past Lc with no record behind it. A restarted Debezium then
       resumes at that F, not at Lc, so going back to Debezium would skip
       (Lc, F]. Refusing it before the stop removes that cause. The F_stop
       checks in S12 stay, because a recreated slot or an ungraceful stop can
       still leave Debezium's slot past Lc.
     - **(7th pass) Slot-moving startup options.** Debezium 3.4 added
       `offset.mismatch.strategy`. With `trust_offset` or `trust_greater_lsn`
       it calls `pg_replication_slot_advance` on every `startStreaming`,
       including after a `reconnect()` (`PostgresReplicationConnection`
       L738–739). With `trust_slot` it moves the offset forward to the slot.
       Debezium 2.7's `slot.seek.to.known.offset.on.start=true` also advances
       the slot at start. Positions:
       - `trust_slot` is refused: on a restart with F_d > Lc it silently moves
         Debezium past (Lc, F_d], the exact gap S12 exists to prevent.
       - `trust_offset` and `trust_greater_lsn` are refused: they move
         Debezium's slot at every start and reconnect with no flush behind
         it, which breaks the premise of S13 and of the continuity checks
         (S15) that the slot moves only on a flushed, committed offset.
       - `slot.seek.to.known.offset.on.start=true` is refused for the same
         reason.
       - Unset (the behaviour before 3.4) and `slot.seek.to.known.offset.on.start=false`
         are accepted.

       The code is `migrate.kafka_connect.slot_advance_on_start_unsupported`,
       with the fix "remove `offset.mismatch.strategy` (or set
       `slot.seek.to.known.offset.on.start=false`) for the cutover window; it
       can be set back after the cutover, or after Debezium has taken over
       again".
     - Masked secrets in the connector config are checked here too (S9).
     - **(4th pass)** These checks run against the live config before the
       stop, and again against the config saved at step 6, so a config
       edited between preflight and the stop is caught before the stop.
       `--check-rollback` runs them again before the operator restarts
       Debezium: against the live config, or the saved config on a worker
       older than Kafka 3.5 (S12, R6).
  10. Publication coverage, including Debezium's `publish` flags, row filters
      and column lists (S2).
  11. The table subset, and that no pipeline with the target name exists
      unless this cutover's journal wrote it (S9). **(4th pass)** A pipeline
      with the target name and no journal is refused with `pipeline_exists`
      whether or not it has a position. The CLI never overwrites it.
  12. The slot checks on Conduit's slot, ending with F ≤ Lc (connector only;
      the CLI's equivalent is S9's re-run table).

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
- `column.exclude.list`;
- **(2nd pass)** Debezium's publication `publish` flags, row filters and
  column lists, where they differ from the pipeline's (S2).

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

- `--cutover` starts the pipeline by default (S11). It reports success only
  after a **durable** import-complete signal. **(2nd pass)** The pipeline's
  source position, as reported by the Conduit API, does not qualify:
  `Source.Ack` sets `Instance.State` before the persister has flushed
  (`pkg/connector/source.go`, around line 592). The CLI accepts either of
  two signals:
  - the connector's own `import complete` event, which the connector emits
    from inside its `Ack` (Conduit calls `Ack` only after the position is
    durable);
  - Conduit's slot reaching `confirmed_flush_lsn > Lc`, read from
    `pg_replication_slots`. Under the D6 cap that can happen only after a
    durable ack.

  The second works without any new Conduit API, and is the one v0.21
  implements. The CLI then tells the user to remove the setting.
- **The retention bound (2nd pass).** From the stop until the first durable
  ack, both slots retain WAL from their `restart_lsn`: Debezium's at or below
  Lc, Conduit's at or below C. Both also hold `catalog_xmin`. The bound is
  the WAL generated over (time from stop to first ack) plus the C-to-stop
  overlap. Under `max_slot_wal_keep_size`, Debezium's stopped slot is the one
  more likely to be invalidated first. That only removes the rollback path
  (S12); it does not cause loss for Conduit.
- The runbook gains a scenario: "slot held at Lc after import".
  - **Symptom:** the WARN line, and growing lag in `pg_replication_slots`.
  - **Diagnosis:** no captured traffic since the cutover.
  - **Remedy:** commit one write to a pipeline table. No re-run is needed.
  - **Escalation:** the `max_slot_wal_keep_size` path. If Conduit's slot
    becomes invalidated, the next start refuses with `slot_invalidated`, and
    the only remedy is a re-snapshot. If Debezium's slot is invalidated,
    rollback is no longer possible (S12).

### S9. Crash-safe cutover and re-runs (scope cut: no `--reset`)

**The journal is state, so it obeys invariant 5.** It is written to a
temporary file, fsynced, and renamed over `<pipeline>.cutover.json` in
`--journal-dir`, and the directory is fsynced after the rename so the rename
itself survives a crash. The file mode is 0600. It records:

- **(Final review)** the connection details `--check-rollback` needs: the
  Postgres connection string without its password, the Kafka Connect URL, the
  Conduit API address, the pipeline's id (S16), and the server's timeline
  from `IDENTIFY_SYSTEM`;
- Conduit's slot name, C, and the publication;
- S13's Debezium-slot proof;
- the final O, and **(4th pass)** F_stop and R_stop, the `confirmed_flush_lsn`
  and `restart_lsn` of Debezium's slot read after the stop. They feed S13's
  `identity_ahead` warning and `--check-rollback` (S12);
- the saved Debezium config;
- the current step.

**The saved Debezium config (3rd pass; scope cut).**

- It is written **before** `PUT /connectors/{name}/stop` (or the delete, on
  workers before Kafka 3.5), and redacted in all CLI output, logs and
  `--json`.
- **(Final review) Secrets in the journal.** On a Kafka 3.5+ worker the
  connector is only stopped, Kafka Connect keeps its config, and the journal
  stores a **redacted** copy (every secret-typed value replaced by
  `<redacted>`), enough for R6's comparison and for diagnosis. On an older
  worker the journal must store the **complete** config, credentials
  included, because the runbook re-creates the connector from it. The
  journal is then a secret at rest: mode 0600, the security notes in the
  runbook say so, and the runbook archives it with the same care after the
  rollback.
- The CLI never resubmits it. It is the record the manual runbook (S12) uses,
  and `--check-rollback` compares the live config with it.
- On a Kafka 3.5+ worker the connector is stopped, and Kafka Connect keeps its
  config, so the runbook resumes it in place. On an older worker the
  connector is deleted, and the runbook re-creates it from this file. So on
  that path only, the file must be complete: `GET /connectors/{name}/config`
  returns config-provider placeholders (`${file:/path:key}`, `${env:VAR}`,
  `${secrets:ns/name:key}`) as written, and those are fine, but some
  platforms return **literal masks** (`[hidden]`, or a value made only of `*`
  characters). With literal masks on the delete path, the CLI refuses before
  the stop with `migrate.kafka_connect.config_masked`. The fix text: _"Kafka
  Connect returned masked values, so the saved config could not re-create
  this connector. Use a Kafka 3.5+ worker, where the connector is stopped
  rather than deleted, or replace the masked values with config-provider
  references in the connector's config first."_
- **Re-validation (4th pass).** The S5 item 9 checks run again against the
  saved config at step 6, before the stop.

**Steps,** in order:

| Step | Name | Journalled |
| --- | --- | --- |
| 1 | `preflight` | the offset-independent checks of S5 have passed |
| 2 | `publication` | the publication is created or verified |
| 3 | `creating_slot` | the slot **name**, and the creating backend's pid and `backend_start`, written before `CREATE_REPLICATION_SLOT` |
| 4 | `slot_created` | C |
| 5 | `waiting` | Debezium's committed Lc ≥ C, and S13's proof |
| 6 | `config_saved` | the saved Debezium config, re-validated (S5 item 9) |
| 7 | `stopping` | before the stop call |
| 8 | `stopped` | the final O, re-checked; F_stop and R_stop |
| 9 | `pipeline_written` | — |
| 10 | `starting` | before the start call |
| 11 | `started` | — |
| 12 | `complete` | the durable signal (S8) was seen |

**Finishing step 7.** `stopping` is journalled before the stop call, so a kill
or a timeout can leave the journal at step 7 whatever happened to Debezium. A
re-run of `--cutover` finishes the observation first:

- **Debezium is STOPPED or deleted:** read the committed O, and F_stop and
  R_stop from Debezium's slot, and journal step 8; then apply the step 8
  rules, including S13's `identity_ahead` warning. If Debezium's slot is
  missing, the re-run stops with `identity_mismatch`.
- **Debezium is RUNNING:** the stop did not take effect, or is still pending
  (Kafka Connect's stop is asynchronous). The re-run issues the stop call
  again and waits for STOPPED.

`--check-rollback` performs the same observation read-only when it finds the
journal at step 7 and Debezium stopped, and reports, without journalling,
when it finds Debezium RUNNING there (S12).

**Re-run rules (step-aware).** Let F_c be the `confirmed_flush_lsn` of
Conduit's slot. On a re-run of `--cutover` after a kill:

| Journal step | Required | Otherwise |
| --- | --- | --- |
| 3 `creating_slot` | Debezium is still running here. See "Adopting a slot at `creating_slot`" below. | — |
| 4–7 (Debezium running or stopping) | F_c = C, then finish step 7 as above | `slot_advanced` |
| **(Scope cut)** 8 and later, Debezium RUNNING again | Someone restarted Debezium, most likely a manual rollback (S12). Two writers must never run, so the re-run changes nothing. | `connector_running` |
| 8 and later, empty pipeline position | F_c ≤ Lc. This covers `stopped`, `pipeline_written`, `starting` and `started`; the D6 cap holds it. | `slot_advanced` |
| **(4th pass)** 8 and later, non-empty position, pipeline running | The first ack has happened. The re-run waits, under `--start-timeout`, for F_c > Lc (the S8 signal), then records `complete`. This is the rule S11's false negative relies on. | `import_pending`, pipeline left running |
| **(5th pass)** 8 and later, non-empty position, pipeline stopped | The first ack has happened, so the import is done: the setting is ignored from here on (D2). If F_c > Lc, the re-run records `complete`. Otherwise it starts the pipeline and continues as in the row above. | `start_failed` |
| **(6th pass)** 8 and later, pipeline degraded (for example `postgres.logrepl.slot_missing`, `postgres.logrepl.slot_invalidated`) | Conduit's slot is gone or unusable, so the cutover cannot go forward. The re-run stops the pipeline and refuses with the connector's code. The fix text points to the manual rollback (S12, "Degraded pipeline"), checked with `--check-rollback`; if Debezium's slot is gone too, re-snapshot. | the connector's code |
| 12 `complete` | done; nothing to do | — |

**(Final review) A manual rollback in progress.** While
`<pipeline>.rollback-watch.json` exists in `--journal-dir`, `--cutover`
refuses with `migrate.kafka_connect.rollback_in_progress` and changes
nothing. Fix text: _"A manual rollback was started for this pipeline (watch
log `<path>`). Finish it with the runbook, or end it with
`--check-rollback --abandon --reason <text>` (Debezium must be stopped),
then re-run."_ An abandoned log (`<pipeline>.rollback-watch.abandoned.json`)
does not block; the re-run reads its R7 snapshot ("Going forward after a
failed restart", S12):

| Re-run after `--abandon` | Required | Otherwise |
| --- | --- | --- |
| Conduit's slot missing | — | `postgres.logrepl.slot_missing`: nothing to go forward from; re-snapshot |
| pipeline exists with a position | its topology is intact (the journalled connectors and processors are present), and the current F_c = the snapshot's F_c; then start it | `pipeline_exists` (topology changed) or `slot_advanced` |
| pipeline exists with an empty position, was never written, or was deleted before it ran (F_c = C ≤ Lc) | F_c ≤ Lc (the normal rule); then write and start the import pipeline | `slot_advanced` |
| pipeline deleted after it ran (F_c > Lc), snapshot present | current F_c = the snapshot's F_c; then write the pipeline without the import (`requireExistingSlot: true`) and start it | `slot_advanced` |
| pipeline deleted after it ran, no snapshot | — | `slot_advanced` |

A pipeline with the target name that this cutover's journal did not write is
refused at preflight with `migrate.kafka_connect.pipeline_exists`, whatever
its position. **(4th pass)** An empty position does not make it safe to
overwrite: the CLI cannot tell what it belongs to.

**Pipelines the CLI writes** are specified in S16: how they are provisioned,
`logrepl.autoCleanup: false`, `logrepl.requireExistingSlot: true`, and the
delete-then-drop order the runbook follows.

**`slot_advanced` fix text (scope cut):**

- _Before the stop:_ "Conduit's slot was consumed by something other than this
  cutover. Debezium is still running, so nothing is lost. Start over with the
  manual reset before the stop (S12: delete the written pipeline, if any, and
  drop Conduit's slot and publication while Debezium runs), then run
  `--cutover` again."
- _After the stop:_ "Conduit's slot moved past Debezium's offset with no
  import recorded. Follow the manual rollback (S12), checked with
  `--check-rollback`: Debezium's slot still holds everything from O."

**The journal's slot is active.**

- If the running pipeline holds it (the pid in `pg_stat_replication` is the
  Conduit instance's), the re-run attaches and waits.
- Otherwise it refuses with `migrate.kafka_connect.slot_in_use`, showing the
  holder's pid, `application_name` and `client_addr`. The `creating_slot` case
  is below.

**Adopting a slot at `creating_slot` (4th pass; orphan handling decided in
the 5th).** Debezium is still running at this step, so no outcome here can
lose data. The CLI opens its replication connection with `application_name`
set to `conduit-migrate:<pipeline>`, so its own sessions are recognisable.
Before it sends `CREATE_REPLICATION_SLOT`, it journals that connection's
backend pid and `backend_start` (`pg_backend_pid()` and `pg_stat_activity`)
with the step. On a re-run:

- **No slot with the journalled name:** create it.
- **A slot that is inactive** is adopted only when all of these hold: `plugin
  = 'pgoutput'`, `two_phase = false`, `temporary = false`, `database =
  current_database()`, `wal_status` is not `lost`, there is no
  `invalidation_reason`, and `confirmed_flush_lsn` is not null. C is then its
  `confirmed_flush_lsn`: a slot that was never consumed still sits at its
  consistent point. If `wal_status` is `lost`, the slot is dropped and
  created again. Any other mismatch is refused with `slot_unowned`, because
  the slot is not one this CLI created.
- **A slot that is active, with `confirmed_flush_lsn` null,** is still being
  created. Usually this is an orphaned walsender: the killed CLI's backend is
  still blocked in `CREATE_REPLICATION_SLOT`, waiting for running
  transactions, because the server has not noticed the client is gone.
  Postgres drops such a slot by itself when that backend exits, since it is
  not yet persistent. **Decided (DeVaris, 2026-10-12, 5th pass): the CLI never
  terminates it.** It refuses with `migrate.kafka_connect.slot_create_blocked`
  and prints the holder's pid, `backend_start` and `application_name`, the
  journalled pid and `backend_start` (equal when it is the CLI's own orphan),
  the oldest `pg_stat_activity` transactions and `pg_prepared_xacts`. The fix
  text: _"End the blocking transactions, or terminate the creating backend
  with `SELECT pg_terminate_backend(<pid>)` once you have checked it is the
  one listed, then re-run."_
- **A slot that is active with a non-null `confirmed_flush_lsn`** is in use by
  someone: `slot_in_use`.

**A slot with Conduit's name but no journal** is refused with
`migrate.kafka_connect.slot_unowned`. Fix text: _"No cutover journal was found
for this slot. If this run is on an ephemeral runner, restore the journal from
the earlier run's `--journal-dir` and re-run. If the journal is lost, drop the
slot by hand only when **all** of these hold:_

- _no pipeline uses it, or that pipeline has no position;_
- _its `confirmed_flush_lsn` is at or below Debezium's committed `lsn_commit`;_
- _Debezium's slot exists, is not lost, and Debezium is running and
  streaming._

_Use a persistent `--journal-dir` for cutovers run from CI."_

**Slot creation.** `CREATE_REPLICATION_SLOT` waits for every transaction
running at creation to finish, and prepared transactions block it until they
are resolved.

- `--slot-create-timeout` (default 5m) bounds the wait.
- On timeout, the CLI cancels, drops any partial slot, and fails with
  `migrate.kafka_connect.slot_create_blocked`.
- The diagnostic lists the oldest `pg_stat_activity` transactions (pid,
  `xact_start`, query) and `pg_prepared_xacts`.
- **(3rd pass)** The slot is created as
  `CREATE_REPLICATION_SLOT <name> LOGICAL pgoutput`, without `TWO_PHASE`.
  The connector never requests streaming. Both are verified in the connector
  code:
  - `startReplication` sends only `proto_version '1'` and
    `publication_names`, and protocol 1 cannot stream;
  - `CreateSubscription` passes no two-phase option;
  - streamed-transaction messages are refused (`subscription.go`).

**Cut points.**

- Debezium's graceful stop runs past `task.shutdown.graceful.timeout.ms`, so
  the task is cancelled without its final offset commit. Expected: S13's
  warning and an import with no loss. Never loss.
- The CLI is killed at every journal step, including `creating_slot` and
  `starting`, then re-run.
- The CLI is killed, or times out, at `stopping`: once with the stop landed
  (the re-run journals step 8, with F_stop > Lc when the Kafka Connect worker
  was killed after a slot flush), once with the stop still pending (the
  re-run re-issues it).
- **(Scope cut)** Debezium restarted by hand after step 8: a `--cutover`
  re-run refuses with `connector_running` and changes nothing.
- **(4th pass, changed in the 5th)** `creating_slot` adoption: an orphaned
  walsender held in `CREATE_REPLICATION_SLOT` by a prepared transaction is
  **not** terminated; the re-run refuses with `slot_create_blocked` and prints
  the pid and `backend_start`, which match the journalled ones. A slot of the
  right name with `two_phase = true` or another plugin is refused with
  `slot_unowned`.
- **(5th pass, changed in the 6th and 7th)** Two CLI processes on the same
  slot: the second refuses with `operation_in_progress`, directly and through
  a transaction-mode pgbouncer. Killing the first CLI's replication session
  makes it abort with `lock_unavailable` before its next mutating step.
- **(4th pass)** A pipeline with the target name, an empty position and no
  journal is refused with `pipeline_exists` and left unchanged.
- **(3rd pass) The round-3 loss sequence:**
  1. Debezium runs with `slot.drop.on.stop=true`. The test asserts the
     pre-stop refusal.
  2. A forced variant flips the setting after preflight but before step 6.
     The test asserts the refusal at step 6, against the saved config.
  3. A forced variant flips it after step 6. The test then asserts the
     post-stop `identity_mismatch`, that `--check-rollback` reports the
     Debezium slot as lost or moved (both before and after a manual Debezium
     restart that recreates the slot), and that the S13 fallback
     (`--without-import`) delivers with zero loss.
- **(4th pass)** `lsn.flush.mode=connector_and_driver` is refused before the
  stop.
- The manual runbook and `--check-rollback` have their own cases (S12).

### S11. `--cutover` starts the pipeline (rewritten, 2nd pass)

`--cutover` **starts the imported pipeline by default**. It reports success
only after the durable import-complete signal (S8). Starting is the default
for two reasons:

- From Debezium's stop until the pipeline starts, both slots hold WAL.
- An operator who forgets to start the pipeline leaves the cap in place with
  no limit.

`--no-start` writes the pipeline and stops after the import preflight. It is
for operators who deploy the pipeline themselves. The CLI then prints that the
pipeline must be started promptly, and why.

Timeouts:

- `--cutover-wait-timeout` (default 30m) bounds step 4, waiting for
  Debezium's Lc to reach C.
- `--start-timeout` (default 10m) bounds waiting for the durable signal.

**If the start fails** (a refusal at `Open`, or a plugin error):

- The CLI stops the pipeline, so it is not left retrying.
- The pipeline config stays written, and Debezium stays stopped. The CLI
  never restarts Debezium on its own.
- The journal records the step.
- The CLI prints the manual rollback (S12) as the way back, with
  `conduit migrate kafka-connect --check-rollback --pipeline <name>` as its
  first step. **(5th pass)** When F_stop > Lc, rollback is not safe until
  Conduit has durably delivered past F_stop (S12), and a pipeline that does
  not start never will. The text then names `--cutover --without-import` as
  the way out: it starts on Conduit's slot with no skip rule, so it gets past
  F_stop, after which the check can pass.
- The way forward is to fix the cause, then re-run `--cutover`. **(5th pass,
  corrected)** If the failure came at `Open`, the position is still empty and
  S9's empty-position row applies. If the pipeline acked records before it
  failed, the position is not empty, the import is done, and S9's
  non-empty-position rows apply: the re-run starts the pipeline without
  re-applying the import.
- It exits with `migrate.kafka_connect.start_failed`, with the connector's
  own code as the cause when there is one.

**If the durable signal does not arrive by `--start-timeout`** (an idle
database), the pipeline is healthy and is left **running**. Stopping it would
not release anything, because the cap holds either way. The CLI exits with
`migrate.kafka_connect.import_pending`, and the fix text reads: _"Commit one
write to any pipeline table. The pipeline completes the import by itself. A
re-run of `--cutover` is not needed; it would only resume waiting."_ A
manual rollback, if chosen instead, stops the pipeline first (S12).

**A false negative (3rd pass).** The signal is "F_c > Lc". After the first
durable ack, F_c can stay at or below Lc for a while. The gate reports the
acked change's LSN, which can be below Lc when transactions interleave, until
everything emitted is acked and a keepalive arrives. In that window
`import_pending` can be spurious. The fix text says so: _"The import may
already be complete. A re-run of `--cutover` re-checks it and records
`complete`."_ This is safe, because nothing acts on a false negative except
the user waiting longer.

### S12. Going back to Debezium: runbook and `--check-rollback`

v0.21 has no command that restarts Debezium or drops Conduit's slot. Going
back is a short manual procedure of numbered operator steps (ConduitIO/conduit#3029,
doc `docs/design-documents/20261012-migrate-debezium-postgres-offset-import.md`,
section "Runbook: manual reset and rollback", shipped as part of
`docs/operations/postgres-debezium-offset-import.md`). Before every step
that changes state, the operator runs a read-only check and needs a specific
verdict:

```sh
conduit migrate kafka-connect --check-rollback --pipeline <name> \
  --journal-dir <dir> [--pg-url <url>] [--connect-url <url>] \
  [--conduit-url <url>] [--watch] [--watch-timeout 30m] [--json]
conduit migrate kafka-connect --check-rollback --pipeline <name> \
  --journal-dir <dir> --abandon --reason <text> [--json]
```

**Runbook scope.** v0.21 ships the check and the numbered steps, not a
script. Step-by-step automation of the procedure is v0.22 (#3044).

Automated reset and rollback are deferred to v0.22 (#3044), to be built
against a real Kafka Connect and Debezium harness. The conditions below are
the round-4 to round-7 analysis and the final review; #3044 lists them as its
requirements.

**What `--check-rollback` never does.** It never drops or creates a slot
(it takes no lock), never stops, starts or resumes a connector or a pipeline,
never deletes a pipeline, never writes the cutover journal and never writes
to the database. Its only write is its own watch log (below). Its Kafka
Connect calls are `GET`s plus `PUT /connector-plugins/{class}/config/validate`,
which validates without changing anything. Postgres access is `SELECT` on
`pg_replication_slots`, `pg_stat_activity` and `pg_current_wal_lsn()`.

**Where it connects.** The journal records, at `preflight`, the Postgres
connection string without its password, the Kafka Connect URL, the Conduit API
address and the server's timeline (from `IDENTIFY_SYSTEM`), and, at
`pipeline_written`, the pipeline's **id** (a UUID for an API-provisioned
pipeline, the file's `id` otherwise; S16). #3029 names the journal fields the
runbook reads. `--pg-url`, `--connect-url` and `--conduit-url` override them.
The Postgres password the check uses comes from the standard libpq sources
(`PGPASSFILE`, or `PGPASSWORD`); do not put it in `--pg-url`, where it shows
in process listings. The check never writes it anywhere. (Separately, on a
worker older than Kafka 3.5 the journal's saved Debezium config holds
Debezium's own credentials; see S9.)

**Privileges.** The check's Postgres role needs `pg_read_all_stats` (or
`pg_monitor`) so that `pg_stat_activity` shows other roles' sessions. Without
it, `backend_type` and `backend_start` read as NULL for Debezium's walsender.
The check fails closed on a NULL in either column
(`rollback_check_privileges`), and requires `backend_type = 'walsender'` for
every `active` sighting, so a SQL session consuming the slot (for example
`pg_logical_slot_get_changes`) can never become an accepted sighting: that is
`rollback_check_continuity_unproven`.

**Why the conditions are what they are.**

- A restarted Debezium resumes at max(its offset, F_d), where F_d is its
  slot's `confirmed_flush_lsn`, not at Lc. When F_stop > Lc it skips
  (Lc, F_stop], so those changes must already be in Conduit's durable output,
  and that must hold at the drop, not only before the restart.
- A Debezium slot recreated after the stop starts at the WAL of its creation
  and skips everything before it. It must be detected before Debezium is
  restarted, and continuity must then be followed until Conduit's slot is
  dropped.
- Conduit's slot is the only other copy of the WAL since C. It is dropped
  last, after the pipeline is deleted.

**States the check reads, and how it maps them.** Any state not listed here
fails closed with `rollback_check_state_unmapped`.

| Source | Reported | Treated as |
| --- | --- | --- |
| Kafka Connect `GET /connectors/{name}/status` | `STOPPED` | stopped |
| Kafka Connect | 404, on a worker older than Kafka 3.5 where the cutover deleted the connector | stopped |
| Kafka Connect | `RUNNING` (connector and every task) | running |
| Kafka Connect | `UNASSIGNED` (connector or a task) | `waiting` (`rollback_check_connector_state`) |
| Kafka Connect | `PAUSED`, `FAILED` or `RESTARTING` (connector or a task), or any other value | `unsafe` (`rollback_check_connector_state`), with the task's trace; a paused task can still hold its replication connection |
| Conduit `GET /v1/pipelines/{id}` | `STATUS_STOPPED` with `stoppedReason` `STOPPED_REASON_USER` | stopped |
| Conduit | 404 | absent |
| Conduit | `STATUS_RUNNING` | running |
| Conduit | `STATUS_RECOVERING`, `STATUS_DEGRADED`, or `STATUS_STOPPED` with `STOPPED_REASON_SYSTEM` or `STOPPED_REASON_UNSPECIFIED` | `unsafe` (`rollback_check_pipeline_state`): the lifecycle may restart these, so they are not stopped for R7 |
| Conduit | unreachable, or any other answer | `unsafe` (`rollback_check_conduit_unreachable`) |

**The checks.** Lc, C, F_stop and R_stop come from the journal, or from the
watch log when the check observed them itself (step 7, below). B =
max(final Lc, F_stop).

| # | Check | Fails with |
| --- | --- | --- |
| R1 | The journal exists. At step 6 or earlier, Debezium was never stopped. At step 7 with Debezium stopped and no stopped sighting logged yet, the check reads the committed O (from the journalled offsets source; see "Committed offset at step 7") and F_stop and R_stop from Debezium's slot, and requires F_stop ≤ the Lc it just read (see R4). The watch records them, with the stopped sighting, in its log, and never records them again; a one-shot check reports them without recording. At step 7 with Debezium running and no stopped sighting logged, the stop may be pending. | `rollback_check_no_journal`; `rollback_check_stop_incomplete` |
| R2 | No `--cutover` is running: neither lock slot (S15) exists. | `rollback_check_cutover_running` |
| R3 | Debezium's slot exists on `current_database()`, with `wal_status` not `lost` and no `invalidation_reason`. | `rollback_check_slot_lost` |
| R4 | Before Debezium is restarted: its slot has `confirmed_flush_lsn` ≤ B and `restart_lsn` ≤ B. A recreated or consumed slot has `confirmed_flush_lsn` past B; detection rests on that column, and `restart_lsn` alone proves nothing. **At step 7** B comes from the F_stop observed by the check itself, which would make R4 circular, so there the check requires F_stop ≤ Lc instead. A slot recreated after the stop starts strictly above the WAL position at its creation, which is past Lc, so it cannot pass; an ungraceful stop that flushed past Lc is refused too, and the operator goes forward. | `rollback_check_slot_moved` |
| R5 | If F_stop > final Lc: Conduit's durable progress P ≥ F_stop. P is F_c, Conduit's slot's `confirmed_flush_lsn`; when Conduit's slot is missing (degraded), P is Kc, the commit LSN of the pipeline's persisted position (the checkpoint's LSN when it is keyless), taken from the R7 snapshot in the watch log once the pipeline is deleted. With Conduit's slot missing and no R7 snapshot, R5 fails closed. **Checked in every row that can lead to `ready_to_drop`, after the restart as well as before it.** | `rollback_check_conduit_behind` |
| R6 | Before the restart only: Debezium's config passes S5 item 9 and `PUT /connector-plugins/{class}/config/validate`. On a Kafka 3.5+ worker this is the live config of the stopped connector; on an older worker, where the connector was deleted, it is the **saved** config from the journal. It is not run once Debezium is running: the validation's behaviour against a running connector holding its slot is unverified (pre-merge gate). | `rollback_check_config_unsafe`, with the S5 code as the cause |
| R7 | Never two writers: Conduit's pipeline is stopped (`STATUS_STOPPED`, `STOPPED_REASON_USER`) or absent before Debezium runs. When the check first sees the pipeline stopped, it records the R7 snapshot in the watch log: pipeline id, position K, Kc and F_c. | `rollback_check_two_writers` |
| R8 | After the restart, continuity holds (S15): the watch log covers the restart, the first `active` sighting is ≤ B, every sighting is a `walsender`, and every change of walsender passed the equality rule or the same-walsender rule. | `rollback_check_continuity_unproven` |
| R9 | Before the drop: Debezium's slot `confirmed_flush_lsn` ≥ F_c (skipped when Conduit's slot is missing), and the pipeline is **absent**. While it still exists (stopped), the verdict is `ready_to_delete_pipeline`; the slot drop is never offered before the delete (S16). | `rollback_check_not_caught_up` (a `waiting` verdict, not a failure) |
| R10 | Once the watch log shows Debezium running after the stop, Debezium must not be stopped again. A restart followed by a new stop ends the attempt: going back cannot be resumed from there. | `rollback_check_restart_interrupted` |

All codes are `migrate.kafka_connect.*`.

**Why R5 compares these values (domain note).** Both F_stop and F_c are slot
positions, `confirmed_flush_lsn`. For any logical slot, Postgres skips, on
restart, exactly the transactions whose commit record **starts** below that
position, so each value says "every transaction with commit start below this
was processed by this consumer". F_stop is Debezium's, a commit end it
flushed; F_c is Conduit's, an acked change LSN or a keepalive end that the
flush gate reports only once everything below it is acked. So F_c ≥ F_stop
means every transaction the restarted Debezium will skip was durably
delivered by Conduit. Kc is a commit start (`FinalLSN`): acks are FIFO, so
every transaction with commit start below Kc was acked, and the checkpoint's
own transaction, at Kc ≥ F_stop, is not skipped by Debezium. A keyless
checkpoint's LSN m bounds the same way (#3033: every transaction committing
before m was delivered before the message).

**Verdicts.** The check works out the state from the journal, the watch
log, Kafka Connect, Conduit and Postgres. Each row lists the checks it runs;
any failed check makes the verdict `unsafe` with that check's code, and any
combination not in the table is `unsafe` (`rollback_check_state_unmapped`).

| Journal | Debezium | Pipeline | Checks | Verdict |
| --- | --- | --- | --- | --- |
| step ≤ 6 | running | absent (it is written only at step 9) | R1–R3 | `ready_to_drop`: drop Conduit's slot and publication (reset before the stop) |
| step ≤ 6 | stopped or 404 | any | — | `unsafe` (`state_unmapped`): the cutover never stopped Debezium, so someone else did; restart it deliberately and check again |
| step 7, no stopped sighting logged | running | absent | R1 | `unsafe` (`stop_incomplete`): finish the stop |
| step 7, no stopped sighting logged | stopped | absent | R1–R4 (at-step-7 form), R5, R6 | `ready_to_restart_debezium`; the watch records F_stop, R_stop and the stopped sighting when it starts |
| step 7, stopped sighting logged | running | absent | R1–R5, R7–R10 | as "step ≥ 8, running": Debezium was restarted, not left mid-stop |
| step 7 or ≥ 8, restart logged | stopped | any | R10 | `unsafe` (`restart_interrupted`): go forward (below) |
| step ≥ 8, no restart logged | stopped | running | R1–R6 | `ready_to_stop_conduit` |
| step ≥ 8, no restart logged | stopped | stopped or absent | R1–R7 | `ready_to_restart_debezium`: start `--watch`, then restart Debezium |
| step ≥ 8 | running | stopped | R1–R5, R7–R10 | `waiting` until Debezium's slot passes F_c, then `ready_to_delete_pipeline` |
| step ≥ 8 | running | absent | R1–R5, R7–R10 | `waiting` until R9 holds, then `ready_to_drop` |
| step ≥ 8 | running | running | R7 | `unsafe` (`two_writers`) |
| any | `UNASSIGNED` | any | — | `waiting` (`connector_state`) |
| any | `PAUSED`, `FAILED`, `RESTARTING` or unknown | any | — | `unsafe` (`connector_state`) |
| any | any | `RECOVERING`, `DEGRADED`, or stopped by the system | — | `unsafe` (`pipeline_state`) |
| any | any | Conduit unreachable | — | `unsafe` (`conduit_unreachable`); inside `--watch`, `waiting` (transient) |

Inside `--watch`, every transient finding in this table (`connector_state`,
`pipeline_state`, `conduit_unreachable`, `privileges`, `offset_unreadable`)
is `waiting` and the watch keeps polling; terminal findings stick (above).

"Step ≥ 8, pipeline absent" also covers a cutover killed between `stopped`
and `pipeline_written`: Conduit's slot then still sits at C, so R5 fails
whenever F_stop > Lc, and R9 needs Debezium's slot only at C. In the
degraded path R5 uses Kc from the R7 snapshot, both before and after the
restart.

Exit codes follow the deterministic exit-code ADR: 0 for a `ready_*` verdict,
and distinct non-zero codes for `waiting` and `unsafe`. `--json` prints the
verdict, the names it was computed for (Conduit's slot, its publication,
Debezium's slot, the connector, the pipeline id), every check with its
observed values (LSNs, pids, `backend_type`, `backend_start`, timeline), and
the next runbook step. The runbook reads names only from that output and
from the journal. If the server's timeline differs from the journalled one,
the output says a failover happened since the cutover.

**Terminal and transient findings (verification round 2).** A finding is
**terminal** when it shows that going back is no longer safe, whatever
happens next: `continuity_unproven`, `slot_moved`, `slot_lost`,
`two_writers`, `restart_interrupted`, and `conduit_behind` once Debezium has
been restarted. The watch records a terminal finding in its log, and from
then on every check and every later watch returns that `unsafe` verdict
until `--abandon`. Every other finding is **transient**: an unreachable
Conduit or Kafka Connect API (`conduit_unreachable`, or the Kafka Connect
call timing out), a `FAILED`, `RESTARTING`, `UNASSIGNED` or `PAUSED` task
(`connector_state`), `pipeline_state`, `privileges`, `offset_unreadable` and
`not_caught_up`. Inside `--watch` a transient finding is `waiting`, with the
code as the reason, and the watch keeps polling; a one-shot check reports it
as `unsafe` ("not now") without recording it.

**`--watch`.** Continuity after the restart needs an observer, so the
operator starts `--check-rollback --watch` while Debezium is still stopped,
then restarts Debezium. The watch runs through the pipeline delete and exits
only at `ready_to_drop`, a terminal `unsafe`, or its timeout. The watch:

- polls at least once a second, reading `pg_replication_slots` and
  `pg_stat_activity` twice per poll and requiring agreement (S15). From the
  moment Kafka Connect first reports Debezium `RUNNING` it polls every 50 to
  100 ms, until it has seen Debezium's **second** walsender (the reconnect
  that follows the WAL position search, S15) or for 60 s, then goes back to
  once a second. The burst is there because a Debezium offset flush landing
  between the last sighting of the search connection and the first sighting
  of the reconnect would make the equality rule fail. With the burst the
  unobserved window is at most about 100 ms against a 60 s default
  `offset.flush.interval.ms`, so a false `continuity_unproven` on a normal
  restart is rare (on the order of 0.2% of restarts at the default interval,
  more with a shorter one). It is always on the safe side: the operator goes
  forward. Whether Debezium flushes at attach (pre-merge gate item 4) would
  change this rate;
- records in its watch log, `<pipeline>.rollback-watch.json` in
  `--journal-dir`: the stopped sighting and the values observed at step 7, the
  R7 snapshot, every change of `active`, `active_pid`, `backend_start`,
  `query_start` or `confirmed_flush_lsn`, every Kafka Connect or Conduit
  state change, a heartbeat entry at least every 10 s, every terminal
  finding, and **its final verdict**. Polls that change nothing are not
  written, so the log grows with events, not time;
- writes the log by temporary file, fsync, rename, then fsync of the
  directory, with mode 0600;
- is the log's **only writer**: it holds an exclusive `flock` on
  `<pipeline>.rollback-watch.lock` for its whole run, and a second `--watch`
  refuses with `operation_in_progress`. **A one-shot check never writes the
  log**; it only reads it;
- exits with `ready_to_drop`, a terminal `unsafe`, or `waiting` after
  `--watch-timeout` (default 30m). Progress goes to stderr; with `--json`,
  stdout carries **exactly one** JSON document, the final verdict, for the
  watch and for a one-shot check alike.

**The CLI owns the final verdict.** The drop is never gated on a file the
shell wrote. A one-shot check reads the verdict the watch recorded in its log
**and** the current state, and returns `ready_to_drop` only when both say so:
the watch's recorded verdict is `ready_to_drop`, no terminal finding is
recorded, and a fresh sighting now passes the continuity rules against the
log's last entry, with R5 and R9 re-checked. Before printing `ready_to_drop`
it also checks that the Conduit slot and publication it names equal the
journal's `.slot.name` and `.publication.name`, and that neither equals
Debezium's slot or publication; a mismatch is `state_unmapped`. The verdict
prints both names so the operator can compare them before the drop.

**Resuming the watch is specified.** A watch that is killed, or that times
out, leaves its log, including any terminal finding, which stays in force. A
new `--watch` first re-checks R8 over the **whole** log (every recorded
sighting, in order), then judges its own first sighting as a change of
walsender against the log's last entry (S15: the same-walsender rule, else
the equality rule), whether or not Debezium reconnected in between. A gap in
observation is treated exactly like an inactive gap. A restart with no watch
log at all cannot be proven, and the verdict is `unsafe`
(`rollback_check_continuity_unproven`, terminal). While the watch log exists,
`--cutover` refuses with `migrate.kafka_connect.rollback_in_progress` (S9).

**Timeouts.** Every HTTP call (Kafka Connect, Conduit) has a 10 s connect
timeout and a 30 s request timeout; Postgres connections use
`connect_timeout=10` and `statement_timeout=10s`. A timeout is the transient
finding for that system.

**Committed offset at step 7.** At step 7 the check reads Debezium's
committed offset from the source the cutover used, which the journal
records (`offsets.source`: REST, or the topic with its bootstrap servers and
topic name; the same `--offsets-from`, `--bootstrap-servers` and
`--offsets-topic` flags override it). On a worker older than Kafka 3.5 there
is no REST offsets endpoint, so the topic is the only source; if it is not
journalled or not readable, the check fails closed with the transient
`rollback_check_offset_unreadable`, and nothing at step 7 is judged safe.

**When F_stop > Lc.** These fail closed through R4's step-7 form and R5, and
the operator goes forward. Known causes:

- Debezium 3.1 or earlier with a newer pgjdbc whose `automaticFlush` flushes
  received LSNs to the slot on its own (the pgjdbc version is probed:
  pre-merge gate item 14);
- a drain that hit `task.shutdown.graceful.timeout.ms`, so the final offset
  commit did not happen after the last slot flush;
- an offset write that failed after the slot flush.

**Going forward after a failed restart (verification round 2).** After the
restart, going back to Debezium can fail with a terminal finding:
`slot_moved`, `continuity_unproven`, `conduit_behind` with Debezium running,
or `restart_interrupted`. That attempt cannot be resumed, because nothing
proves Debezium's slot still holds every change since its offset.
`--abandon` ends it. A later attempt starts from scratch and must pass R4
again, which a Debezium that has streamed usually fails, since its slot has
moved past B. So in practice the way on is forward, onto Conduit's slot,
which still holds everything since F_c:

1. **Stop Debezium again** (`PUT /connectors/{name}/stop`, or `DELETE` on an
   older worker) and wait for `STOPPED` or 404. This also ends the second
   writer.
2. **`conduit migrate kafka-connect --check-rollback --abandon --reason
   "<text>" [--json]`.** It is read-only apart from the watch log. It
   requires Debezium exactly `STOPPED`, or 404 on a worker older than Kafka
   3.5; any other state, `UNASSIGNED` included, is refused with
   `connector_running`. It takes the log's lock and appends the reason, the
   current F_c and both slots' retained WAL
   (`pg_current_wal_lsn() - restart_lsn`). Then it archives the log, with its
   recorded verdict and findings, as
   `<pipeline>.rollback-watch.abandoned.json` (fsync, rename, directory
   fsync). That clears `rollback_in_progress`.
3. **Re-run `--cutover`.** The journal is at step 8 or later, so S9's
   abandon table applies:
   - the pipeline exists with a position: the re-run first verifies its
     topology (the source and destination connectors and every processor the
     cutover wrote are present, by the journalled ids), then, if the current
     F_c equals the snapshot's F_c, starts it, and it resumes from its own
     position on its own slot; otherwise `slot_advanced`;
   - the pipeline exists with an empty position, or was never written, or
     was deleted before it ran (F_c = C ≤ Lc): the import pipeline is written
     and started as usual;
   - the pipeline was deleted after it ran (F_c > Lc): if the current F_c
     **equals** the snapshot's F_c, the re-run writes the pipeline without the
     import (as `--without-import`, with `logrepl.requireExistingSlot:
     true`) and starts it; it streams from F_c, the duplicates are what
     Conduit delivered after its last slot flush, and there is no loss. If F_c
     differs from the snapshot, or there is no snapshot, `slot_advanced`;
   - Conduit's slot is missing: there is nothing to go forward from. The
     re-run refuses with `postgres.logrepl.slot_missing`; what is retained is
     Conduit's output up to K and Debezium's output in Kafka, and the operator
     re-snapshots;
   - after `slot_advanced` there is no loss-free way forward from the slots
     either: the operator re-snapshots (a new pipeline with
     `snapshotMode: initial`), then drops both slots by hand.
4. **WAL retention.** From the failed restart until the forward pipeline
   runs, **both** slots retain WAL: Debezium's from its `restart_lsn`,
   Conduit's from its own. `--abandon` prints both amounts and says so. Once
   Conduit is running again and the operator gives up on Debezium, Debezium's
   slot is dropped by hand; until then, watch `max_slot_wal_keep_size`.

**Fix texts.** Placeholders are shown as `<…>`.

- `rollback_check_no_journal`: _"No cutover journal for `<pipeline>` in
  `<dir>`. Point `--journal-dir` at the directory the cutover used. If the
  journal is lost, the check cannot judge F_stop or C: do not drop Conduit's
  slot until you have followed the `slot_unowned` conditions by hand."_
- `rollback_check_stop_incomplete`: _"The cutover was stopping Debezium when
  it ended, and Kafka Connect still reports it running: the stop may be
  pending. Finish it (`PUT /connectors/<name>/stop` on Kafka 3.5+, or
  `DELETE /connectors/<name>` on an older worker, then wait for STOPPED or
  404), then run the check again. Do not resume the connector to cancel the
  stop."_ Round 7 showed that a resume after a pending stop restarts the
  tasks and can flush past any target taken earlier.
- `rollback_check_cutover_running`: _"A `--cutover` holds the lock for this
  slot. Let it finish or stop it before going back."_
- `rollback_check_slot_lost`: _"Debezium's slot no longer has the WAL since
  the cutover. Conduit's pipeline is the only complete consumer: keep it
  running. Returning to Debezium now requires a Debezium re-snapshot."_
- `rollback_check_slot_moved`: _"Debezium's slot is past its committed offset
  and the stop position, so it was recreated or consumed (or, at step 7,
  flushed past its offset), and restarting Debezium would skip changes. Keep
  Conduit's slot and go forward: 'Going forward after a failed restart',
  steps 1–3."_
- `rollback_check_conduit_behind`: _"Debezium's slot is ahead of its
  committed offset. Conduit must deliver up to `<F_stop>` before Debezium
  can take over. If Debezium is stopped: keep the pipeline running and
  commit one write to a pipeline table, then check again; if the pipeline
  never started or cannot start, run `--cutover --without-import` first. If
  Debezium is already running again: go forward ('Going forward after a
  failed restart')."_
- `rollback_check_config_unsafe`: the S5 code's own fix text.
- `rollback_check_two_writers`: _"Debezium and the Conduit pipeline are both
  running. Stop one of them now."_
- `rollback_check_continuity_unproven`: _"Debezium's slot could not be shown
  to be the same slot since the restart (last proven `<lsn>` under pid
  `<pid>`; then `<lsn>` under pid `<pid>`, `backend_type` `<type>`). Keep
  Conduit's slot. This attempt cannot be resumed: end it with `--abandon`
  and go forward ('Going forward after a failed restart')."_
- `rollback_check_not_caught_up`: _"Debezium is streaming but its slot is
  at `<lsn>`, below Conduit's `<F_c>`. Wait; on an idle database, commit two
  separate transactions to a table Debezium captures. The watch reports
  `ready_to_delete_pipeline` or `ready_to_drop` when it passes."_
- `rollback_check_connector_state`: _"Kafka Connect reports `<state>` for
  `<connector>` (task `<n>`: `<trace>`). `UNASSIGNED`: wait. `PAUSED`: a
  paused task can still hold its replication connection; stop it or resume
  it deliberately. `FAILED` or `RESTARTING`: fix the cause and let it run;
  the watch judges the new walsender against its log."_
- `rollback_check_privileges`: _"`pg_stat_activity` hides other roles'
  sessions from this role, so the walsender cannot be identified. Grant
  `pg_read_all_stats` (or `pg_monitor`) to `<role>`, or run the check as a
  role that has it."_
- `rollback_check_pipeline_state`: _"The Conduit pipeline is `<status>`
  (`<stoppedReason>`). The engine may restart it, so it does not count as
  stopped. Stop it through the API (`POST /v1/pipelines/<id>/stop`) and wait
  for `STATUS_STOPPED` with `STOPPED_REASON_USER`."_
- `rollback_check_conduit_unreachable`: _"The Conduit API at `<url>` did not
  answer. The check cannot rule out a running pipeline, so it stops here."_
- `rollback_check_restart_interrupted`: _"Debezium was restarted during this
  rollback and is stopped again. The restart cannot be resumed. Go forward
  ('Going forward after a failed restart')."_
- `rollback_check_offset_unreadable`: _"The check could not read
  Debezium's committed offset from `<source>`. On a worker older than Kafka
  3.5 pass `--offsets-from topic --bootstrap-servers <servers>
  [--offsets-topic <topic>]`. Nothing at step 7 is judged safe until it
  can."_
- `rollback_check_state_unmapped`: _"The combination `<journal step>`,
  `<connector state>`, `<pipeline state>` is not one the runbook covers.
  Nothing was judged safe. Bring the systems to a listed state, or go
  forward."_

**Duplicates and loss.** Debezium re-delivers into Kafka everything since
max(Lc, F_stop), so everything Conduit delivered since the cutover is
duplicated there. No change in the pipeline's tables is lost when every gate
passed: each one is in Conduit's output up to K (which covers
(Lc, F_stop] by R5, checked again at the drop), or retained by Debezium's
slot, which R3, R4 and R8 showed to be the original. Debezium may capture
tables outside the pipeline's subset (D5 check 7). When F_stop > Lc their
changes in (Lc, F_stop] are in neither output; that is Debezium's own
behaviour after an ungraceful stop, and the check prints those tables and the
range as a warning.

**Tests.** Real Debezium harness (`test/debezium`), each case following the
runbook and asserting the verdict, then zero loss after the manual steps:

- a rollback after Conduit has run, and one before the pipeline ever started;
- F_stop > Lc with Conduit behind (`conduit_behind`), then caught up;
- **the final-review loss path:** F_c < F_stop, the operator stops the
  pipeline and restarts Debezium anyway; R8 and R9 would pass, and the
  verdict must still be `unsafe` (`conduit_behind`), never
  `ready_to_drop`;
- Debezium's slot recreated after the stop (`slot_moved`), and invalidated
  (`slot_lost`);
- the normal restart with an `lsn_proc` offset (WAL search, then
  `reconnect()` under a new pid): accepted on every Debezium version in the
  matrix, on a busy and an idle database;
- a SQL session consuming Debezium's slot while the watch runs:
  `continuity_unproven`;
- a role without `pg_read_all_stats`: `privileges`;
- a flush between two polls across a reconnect (forced with
  `offset.flush.interval.ms=100`): `continuity_unproven`;
- the watch killed after the restart and resumed: judged from its log;
- the journal left at `stopping`: with the stop pending (`stop_incomplete`),
  with the stop landed (step 7 observed, then the step ≥ 8 path), and
  restarted after a logged stopped sighting (not `stop_incomplete`);
- a cutover killed between `stopped` and `pipeline_written`;
- a pre-3.5 worker where the connector was deleted: treated as stopped, R6
  on the saved config;
- `UNASSIGNED`, `PAUSED` and `FAILED` connector states;
- both running: `two_writers`; a `--cutover` holding the lock:
  `cutover_running`; `--cutover` while a watch log exists:
  `rollback_in_progress`;
- Conduit's slot missing (degraded pipeline), with a keyed and a keyless
  checkpoint, R5 checked again after the restart;
- an idle database during the watch: `waiting`, then the ready verdicts after
  two writes to a captured table;
- **(final verification)** the normal path on a busy database with a
  file-provisioned pipeline: the watch runs through the delete and its
  engine restart (well over `offset.flush.interval.ms`) and ends at
  `ready_to_drop`; and a one-shot check after a watch gap accepts the same
  walsender (same `active_pid` and `backend_start`);
- **(final verification)** going forward after each failed post-restart
  verdict (`slot_moved`, `continuity_unproven`, `conduit_behind` with
  Debezium running, `restart_interrupted`): stop Debezium, `--abandon`,
  re-run `--cutover`, for each row of S9's abandon table, including the
  pipeline already deleted; zero loss, and `slot_advanced` when the slot
  moved since the snapshot;
- **(final verification)** a Debezium slot recreated between the stop and a
  step-7 observation: refused by R4's at-step-7 form (F_stop > Lc);
- **(final verification)** every unmapped or non-stopped state:
  step ≤ 6 with Debezium stopped or deleted; a pipeline `RECOVERING`,
  `DEGRADED` or stopped by the system; Conduit unreachable; Kafka Connect
  `RESTARTING`;
- **(verification round 2)** the burst polling from Debezium `RUNNING`
  catches the search connection and the reconnect on every Debezium
  version in the matrix; a terminal finding recorded by one watch is still
  `unsafe` after the watch is restarted; a resumed watch re-checks R8 over
  the whole log; a `startStreaming` retry on the same connection (same pid
  and `backend_start`, new `query_start`) is judged by the equality rule;
  Kafka Connect or Conduit unavailable, and a `FAILED` task, keep the watch
  in `waiting`; a one-shot check never writes the watch log; `--json` prints
  one document; `--abandon` refuses `UNASSIGNED`; the abandon table's rows,
  including a pipeline deleted before it ran, a changed topology, and
  Conduit's slot missing; the drop gate refuses when the printed slot name
  differs from the journal's or equals Debezium's;
- `--check-rollback` makes no write to any system: `pg_stat_database`
  `xact_commit` and the WAL position on the source, the Kafka Connect
  connector state and the Conduit pipeline list are unchanged across every
  case.

### S13. Proving the offset belongs to this server (rewritten, 3rd and 4th passes)

Lc comes from Kafka Connect. The slot that proves it is on Postgres. Linking
the two is what makes the import safe:

- **During step 5 (`waiting`),** the CLI requires Debezium's slot on **this**
  server (`pg_replication_slots`, `slot_name = debeziumSlotName`,
  `database = current_database()`) to reach `confirmed_flush_lsn ≥ C`. That is
  in addition to the committed Lc reaching C. It records the observed value in
  the journal and in the setting (`identity.debeziumSlotFlushAtWait`). An
  offset from another server, a clone or a promoted standby cannot move this
  slot. This is why S5 refuses the settings that stop Debezium moving its slot
  (`lsn.flush.mode=manual`, `flush.lsn.source=false`) before the stop.
- **After the stop,** it re-checks Debezium's slot, and journals F_stop and
  R_stop (S9):
  - It must still exist, which `slot.drop.on.stop=true` would break; S5
    refuses that before the stop, and again against the saved config.
  - Its F must be ≥ C. This **lower bound is hard**: a failure is
    `migrate.kafka_connect.identity_mismatch`.
  - The **upper bound**, F ≤ Lc, is a **heuristic** for the import. Conduit
    reads its own slot from C, so a Debezium slot past Lc cannot cause loss
    in the import. **(4th pass)** With `connector_and_driver` refused (S5), it
    means Debezium flushed past its committed offset in an ungraceful stop.
    Changes in (Lc, F] may be in Kafka **and** in Conduit's output, so they
    are duplicates. The CLI warns with `migrate.kafka_connect.identity_ahead`
    and records the window. It does not refuse the import. It does change the
    way back: going back to Debezium needs Conduit's durable progress ≥ F_stop
    first (S12, check R5), because a restarted Debezium would skip
    (Lc, F_stop].
- **At `Open`,** the connector repeats the hard checks against the setting's
  `identity` block before it streams: Debezium's slot exists here, and its F
  is ≥ C. They fail with `postgres.import.database_mismatch`.

**The documented fallback after a post-stop `identity_mismatch` (3rd pass,
preconditions and mode added in the 4th pass).** Conduit's slot is intact,
holds everything from C, and F_c = C. Debezium was running across C, as
observed during step 5. So the safe way forward is to start Conduit's
pipeline **without** the import:

```sh
conduit migrate kafka-connect --cutover --without-import
```

**Preconditions.** The CLI checks all of these and refuses otherwise,
changing nothing:

| Precondition | Refusal |
| --- | --- |
| Debezium is STOPPED or deleted (the journal is at step 8 or later) | `connector_running` |
| Conduit's slot exists on `current_database()` | `postgres.logrepl.slot_missing` |
| its `wal_status` is not `lost` and it has no `invalidation_reason` | `postgres.logrepl.slot_invalidated` |
| its plugin is `pgoutput` | `postgres.logrepl.slot_plugin` |
| F_c ≤ final Lc | `slot_advanced` |
| the pipeline's position is empty | `pipeline_exists` (the pipeline has already taken over; there is nothing to fall back to) |

In the `identity_mismatch` path F_c is C: the CLI fails before it writes the
import pipeline, and an import pipeline that did start would have run the
same identity checks at `Open` and refused.

**Require-existing-slot mode (`logrepl.requireExistingSlot`, decided:
tri-state; supersedes the 4th-pass `slotOnly`).** A
pipeline with no import setting goes through `CreateSubscription`, which
creates the slot when it is missing (`subscription.go`, `CreateSubscription`:
`CreateReplicationSlot`, with the duplicate-object error tolerated), and
`NewCDCIterator`, which creates the publication the same way (`cdc.go`). If
Conduit's slot were dropped between the check and the start, the pipeline
would silently create a new slot at the current WAL and miss everything since
C. The same gap exists today for **any** pipeline with a position: if its
slot is dropped, the next start creates a new one and resumes from the
current WAL.

The 4th pass closed this for `--without-import` with an import variant,
`"resume": "slotOnly"`. The round-5 review recommended a plain setting
instead, and DeVaris decided it, tri-state, at the round-7 completion:

- **`logrepl.requireExistingSlot`** (unset, `true` or `false`; the table below).
  When it refuses, `Open` fails closed instead of creating anything. A missing
  slot fails with `postgres.logrepl.slot_missing`, and a missing publication
  with `postgres.logrepl.publication_missing`. The slot must also be `pgoutput`,
  on this database, and not lost or invalidated (`postgres.logrepl.slot_plugin`,
  `postgres.logrepl.slot_invalidated`). It changes nothing else: not the resume
  point, not the flush gate.
- **(6th pass) One code family for non-import slot preconditions.** The
  `postgres.logrepl.*` codes are used for every slot precondition outside the
  import, in the connector and in the CLI (the `--without-import` table
  above). The `postgres.import.slot_*` codes stay for the import path only.
  Fix texts:

  | Code | Fix |
  | --- | --- |
  | `postgres.logrepl.slot_missing` | "The replication slot `<name>` does not exist, and `logrepl.requireExistingSlot` forbids creating it: a new slot would start at the current WAL and skip every change since the old one. If the slot was dropped by mistake, the changes since the pipeline's position are not recoverable from Postgres: re-snapshot (a new pipeline with `snapshotMode: initial`), or, during a migration, go back to Debezium with the manual rollback while `conduit migrate kafka-connect --check-rollback` passes. To start fresh on purpose, set `logrepl.requireExistingSlot: false` for one start, then set it back." This is the steady-state case: a pipeline whose slot was dropped after the migration finished. |
  | `postgres.logrepl.slot_invalidated` | "The slot `<name>` lost its WAL (`wal_status = lost` or `invalidation_reason` is set, often `max_slot_wal_keep_size`). The changes it held are gone: re-snapshot, and size `max_slot_wal_keep_size` for the longest expected stall." |
  | `postgres.logrepl.slot_plugin` | "The slot `<name>` uses plugin `<plugin>`; this connector reads `pgoutput` only. Point `logrepl.slotName` at a `pgoutput` slot. A slot cannot change its plugin." |
  | `postgres.logrepl.publication_missing` | "The publication `<name>` does not exist, and `logrepl.requireExistingSlot` forbids creating it: a publication created after the slot's start makes pgoutput skip the earlier WAL of its tables. Recreate the slot and publication together (publication first), then re-snapshot, or restore the publication only if you know no WAL since the slot's start touched its tables." |

- **(6th pass) What it catches.** It catches a **missing** slot. It does not
  catch a slot that was dropped and recreated under the same name before the
  pipeline starts: that slot exists, so `Open` accepts it, and the pipeline
  resumes from the new slot's start. Detecting that needs the slot's identity,
  which Postgres does not expose; the import path is protected by its own
  checks (D5 check 5, F ≤ Lc), and a steady-state pipeline is not.
- **Default-refuse (decided, DeVaris, 2026-10-12).** A position means a slot
  existed, and creating a new one is always a silent gap. So
  `logrepl.requireExistingSlot` is tri-state:

  | Value | Behaviour at `Open` |
  | --- | --- |
  | unset (the default) | Refuse to create a slot or publication when the position is non-empty, snapshot positions included; create them when the position is empty, as today. Fails with `postgres.logrepl.slot_missing` |
  | `true` | Always refuse to create, whatever the position (what the CLI writes) |
  | `false` (explicit) | Always allow, as today. Creating a slot for a non-empty position logs a WARN with `postgres.logrepl.slot_recreated` and increments a `slot_recreated` counter, so the gap is visible. The counter needs the connector metrics from #3013; until they land, the WARN is the signal |

  This closes the steady-state gap for every pipeline, not only CLI-written
  ones. It is a **behaviour change** for existing users, so it ships with:
  - **A release note** (marked "Behaviour change"): _"The Postgres source no
    longer creates a replication slot for a pipeline that already has a
    position. A missing slot used to be recreated at the current WAL,
    silently skipping every change since the old one. It now fails with
    `postgres.logrepl.slot_missing`. Set `logrepl.requireExistingSlot: false`
    to keep the old behaviour; the connector then logs
    `postgres.logrepl.slot_recreated`."_
  - **Runbook entries** (`docs/operations/`), each symptom → diagnosis →
    remediation:
    - _slot dropped by mistake:_ the changes since the position are gone from
      Postgres; re-snapshot, or set `false` for one start if the gap is
      acceptable;
    - _database restored from backup:_ slots are not in base backups taken
      with most tools; re-snapshot;
    - _`pg_upgrade` from a version below 17:_ logical slots are not carried
      over before PG 17; recreate the slot before writes resume and set
      `false` for one start, or re-snapshot;
    - _failover to a standby:_ unless the slot was synchronised (PG 17
      `sync_replication_slots`, or a platform equivalent), it is missing on
      the new primary; re-snapshot, or `false` for one start if the gap is
      acceptable;
    - _RDS blue/green switchover:_ logical slots do not move to the green
      environment; plan the switchover as a re-snapshot, or `false` for one
      start.
- **The CLI writes it, `true`, on every pipeline it creates**: the import, and
  the `--without-import` fallback. After the import completes, the setting
  keeps the pipeline from silently recreating a dropped slot.
- **`--without-import`** writes the pipeline with no import setting and
  `logrepl.requireExistingSlot: true`. Compared with `slotOnly` it needs no
  Lc, no D6 cap and no F ≤ Lc check in the connector. With no skip rule, a
  keepalive that moves Conduit's slot before the first ack is harmless: the
  gate only reports positions that are data-safe, and nothing re-applied at
  the next start depends on F. The CLI's own preconditions above (including
  F_c ≤ Lc) still run before it writes the pipeline.
- **B2 (5th pass): the resume point on this path.** With an empty position,
  the connector builds `ResumePoint{Key: zero, LSN: start, MessageLSN: 0}`
  with no `Partial`, where `start` is the slot's position as on any fresh
  start. Nothing is treated as delivered: neither a change nor a
  non-transactional message (#3033's keyless rule would otherwise derive
  `MessageLSN` from `LSN`, but there is no checkpoint here, so nothing was
  acked). S14's `MessageLSN = Lc` applies only to a real import.
- **Upgrade and rollback.** A connector build without the setting either
  rejects it at validation or ignores it; if it ignores it, it falls back to
  today's create-if-missing behaviour. Pipelines `migrate` writes require the
  build that ships the import, which has the setting.
- #3033 (signed off at `53b767d1`) specifies the same non-import resume
  point (Key zero): `MessageLSN = 0`, no `Partial`.

**Duplicate bound.** The connector streams from C. Duplicates are every
change Debezium produced into Kafka with commit at or after C: the records up
to the last one Debezium produced before it stopped, not only up to Lc. After
an ungraceful stop that includes records produced after O. There is no loss.
This is the dual-run guarantee (Alternative 2), bounded by the short
C-to-stop window. Going back to Debezium is **not** the fix there:
`--check-rollback` reports Debezium's slot as lost or moved (S12).

### S14. Logical decoding messages on the import path (4th pass)

Conduit receives logical decoding messages only when it requests pgoutput's
`messages` option, which the Debezium-compatible output mode does
(ConduitIO/conduit#3033, _Logical decoding messages: positions and change
keys_). The default output does not request it, and then nothing here
applies. #3033 owns the message positions and change keys. This section
states how the import's resume point treats them, so the duplicate
prediction is exact.

**Non-transactional messages are checked against Lc, inclusive.** The base
rule is the key (Lc − 1, MaxUint64), and #3033's known-key rule for a
non-transactional message at m would read `m ≤ Lc − 1`. That would re-deliver
a message at exactly Lc. It is the message Debezium's last offset came from:
for a non-transactional message, Debezium's `lsn_commit` is that message's
LSN as the walsender reports it, which is the same m Conduit sees (S1). So
the import's resume point keeps Lc itself for messages: #3033 (head
`53b767d1`) sets `ResumePoint.MessageLSN = Lc` for an imported resume point,
with the rule **delivered iff m ≤ Lc**. With it:

- a non-transactional message at m < Lc was processed by Debezium before P's
  commit or at it, and is skipped;
- the message at m = Lc is O's own record, and is skipped. This keeps the
  commit-class row of the S2 table at 0 duplicates;
- a message at m > Lc was not covered by O, and is delivered.

**Transactional messages in T: the partial rule applies.** A transactional
message is a change of T: #3033 counts it in `Seq` and gives it the normal
change check. Its change LSN is the LSN the walsender reports for it, which
is the message record's **end** (S1). The partial rule treats it like any
other change of T: it is skipped when that LSN is below L, and delivered
otherwise. This is safe. A message whose end is below L was written, and
therefore emitted, before O's change, so Debezium processed it (under
Debezium's own message filters, S6). It is also bounded: a WAL record's end
is the start of the next record, so only the message written immediately
before O's change can end at L. That message, if there is one, is the one
extra duplicate.

The other choice, never skipping transactional messages, is equally safe but
re-delivers every message in T's prefix, a set with no fixed bound. Applying
one rule to every change of T keeps the bound to a single message and keeps
the resume code free of a message special case.

**Seq counting is unchanged by the partial rule.** #3033 counts a
transactional message in `Seq` like any other change of its transaction, and
the import agrees: D4 counts `seq` before the skip decision, for messages as
for rows, so a message the partial rule skips still consumes its ordinal and
the records Conduit delivers from T carry their real keys. The partial rule
itself compares change LSNs with L, never `Seq`, so it gives the same result
under either numbering.

**Where Lc lives.** The import never writes a position (Constraints: no
persisted state of its own). Lc lives in the connector setting,
`logrepl.import.debeziumOffset`, as `offset.lsn_commit`. At every start with
an empty position, including a restart after a crash before the first durable
ack, the connector re-reads it from the setting and builds the in-memory
`ResumePoint`: `Key = (Lc − 1, MaxUint64)`, `LSN = Lc`, `MessageLSN = Lc`,
plus `Partial` when armed. So `LastLSN` is not where Lc lives: there is no
imported `Position` to hold it, and once a position exists the setting is
ignored (D2). These values apply **only to a real import**; the
`requireExistingSlot` path uses Key zero, `MessageLSN = 0` and no `Partial`
(S13, B2).

**The first position (5th pass, corrected to #3033 `53b767d1`).** The first
durable ack writes an ordinary version 2 position in the **effective**
numbering. #3033 (head `53b767d1`; the model was introduced at `e9d0f414`)
replaced the earlier `tx_seq_msgs` flag, which is gone, with a separate
field. When the `messages` option is effective (the normal case for a
migrated pipeline: `migrate` enables Debezium-compatible output by default for
Debezium Postgres pipelines with a Kafka destination, #3026, and the option is
on from PG 14), the position carries `tx_commit_lsn` and **`tx_seq_m`**, with
`tx_seq` omitted. With the option off it carries `tx_seq` as today. If the
first acked record is a non-transactional message, the first position is
**keyless**: `LastLSN = m`, and neither `tx_seq` nor `tx_seq_m`. The import's
own resume point has no stored numbering, so #3033's numbering comparison
does not apply to it, and there is nothing to mismatch.

**The duplicate set** for the DML case is therefore T's changes at LSN L,
plus at most one transactional message of T whose end LSN is L. `--peek` runs
the same `ResumePoint` over the stream, so its prediction includes that
message. The harness asserts equality with it (Tests, 4th pass).

### S15. Mutual exclusion and slot continuity

**Mutual exclusion (6th pass: a temporary replication slot).** The 5th pass
used a session advisory lock. The round-6 review showed it is not exclusive
behind a transaction-mode pooler: a probe through pgbouncer saw a stale lock
and re-entrant grants, and nothing noticed when the server session went away.
Two fixes were evaluated:

- **Advisory lock, hardened:** require a direct or session-mode connection,
  detect a pooler (`pg_backend_pid()` differs across two transactions), check
  `pg_locks` for the lock under `pg_backend_pid()` before every mutating step,
  and send a keepalive query. It works, but it relies on detecting the pooler,
  and a misdetection silently loses exclusion.
- **A temporary replication slot (chosen).** Every mutating command
  (`--cutover` and `--cutover --without-import`; v0.21 has no others) opens
  **one** replication session (`replication=database`) and, on it, runs
  `CREATE_REPLICATION_SLOT <lock name> TEMPORARY PHYSICAL`, without
  `RESERVE_WAL`, for each lock. **(Final review)** The same session holds
  both lock slots and is the one that runs `CREATE_REPLICATION_SLOT` for
  Conduit's logical slot, so a single walsender carries the whole command;
  the review's probe confirmed that on PG 17, and PG 14–18 are on the
  pre-merge verification list. **(7th pass)** The
  lock name is `conduit_migrate_lock_` plus the first 32 hex characters of
  the SHA-256 of the protected slot's name: 53 bytes in all. A name built
  from the slot name itself could pass 63 bytes, which Postgres silently
  truncates, so two long slot names could collide on one lock.

Why the slot wins:

- Slot names are unique on the server, so a second command's create fails
  with "already exists". The command then refuses with
  `migrate.kafka_connect.operation_in_progress` and prints the holder from
  `pg_replication_slots.active_pid` joined to `pg_stat_activity`.
- A temporary slot is dropped by Postgres when its session ends, so a killed
  CLI leaves nothing behind, and a dropped session is visible: the slot is
  gone.
- Replication connections cannot be multiplexed by a transaction pooler. A
  pooler that passes replication through gives each client its own server
  session, so the lock and the session live and die together.
- A physical slot without `RESERVE_WAL` has no `restart_lsn` and no `xmin`, so
  it retains no WAL and holds back no vacuum. Unlike a logical slot, creating
  it does not wait for running transactions.
- The cost is one `max_replication_slots` entry and one `max_wal_senders`
  connection per lock for the command's lifetime.

**(7th pass) Two locks, in a fixed order.** Two pipelines can cut over from
one Debezium connector, and both would stop and restart it. So every command
takes two locks: one keyed on Conduit's slot and one keyed on Debezium's
slot, each named as above. It takes them in a fixed order (by lock name,
ascending), so two commands cannot each hold one and wait for the other; the
second create simply fails with "already exists" and the command refuses.

**(7th pass) Capacity, errors and poolers.**

- **Headroom preflight (formula fixed in the final review).** Before taking
  the locks, the command requires free walsenders ≥ 2 (the lock session,
  plus one for a Debezium reconnect while its old walsender is still
  exiting) and free replication slots ≥ 2 lock slots + Conduit's slot (or
  just the 2 lock slots when Conduit's slot already exists). The Conduit
  pipeline's own walsender connects later, after Debezium has stopped and
  released its connection. Otherwise it refuses with `lock_unavailable`,
  naming the setting and the counts.
- **Every create error fails closed.** "Already exists" (SQLSTATE 42710) is
  `operation_in_progress`. Every other error, including
  `max_replication_slots` or `max_wal_senders` exhausted (53400), a missing
  `REPLICATION` privilege (42501), an unsupported command on a managed
  platform, or a connection failure, is `lock_unavailable` with the Postgres
  error attached. No mutating step runs without both locks.
- **Poolers.** Replication connections through pgbouncer need pgbouncer
  1.23 or later. The command detects a pooler on the replication connection
  (`SHOW VERSION` answered by pgbouncer, or `pg_backend_pid()` changing
  between two calls) and refuses with `lock_unavailable` below 1.23 or when
  the pooler is not in session mode for that connection. A direct connection
  is the recommendation.
- **Network partitions.** With default settings, Postgres keeps a dead
  client's session, and so the lock slot, for about two hours. The lock
  connection therefore sets server-side keepalives in its startup options:
  `options=-c tcp_keepalives_idle=5 -c tcp_keepalives_interval=2 -c
  tcp_keepalives_count=2 -c tcp_user_timeout=10000`. The round-7 review's
  probe saw the slot dropped within 20 s of a partition with these settings.
- **A stale holder.** The `operation_in_progress` fix text names the holder
  (`active_pid`, `application_name`, `client_addr`, `backend_start`) and
  says: _"If that process is gone (for example a CLI on a host that lost its
  network), end its session with `SELECT pg_terminate_backend(<pid>)`. The
  lock slot is temporary and disappears with it. Then re-run."_

Before **every** mutating step (slot creation, a stop or start call, a
pipeline write or delete, a drop), the command checks that both lock slots
still exist with `active_pid` equal to its own walsenders' pids (read once
with `pg_backend_pid()` on the replication connection, which accepts SQL in
`replication=database` mode). If not, it aborts with `lock_unavailable`
before the step, and a re-run resumes from the journal. An idle replication
connection is also kept busy with a `SELECT 1` every 10 s.

`--check-rollback` takes no lock. It reports a held lock as
`rollback_check_cutover_running` (S12), so the manual procedure never runs
beside a `--cutover`.

**Slot continuity (the attach proof of the 5th to 7th passes, now read-only;
the equality proof is decided).** `--check-rollback --watch` (S12) uses these
rules to show that the Debezium slot streaming after a manual restart is the
slot that was checked before it. Debezium reconnects under a new walsender
pid on every normal restart: with an `lsn_proc` offset its streaming source
runs a WAL position search and then calls `reconnect()`
(`PostgresStreamingChangeEventSource`, main L202–216, 2.7 L162–176;
`PostgresReplicationConnection.reconnect`, main L1186), and task rebalances
and network blips change the pid too. So the rules follow the slot's
position across pids:

- Each poll reads `pg_replication_slots` (`active`, `active_pid`,
  `confirmed_flush_lsn`) and `pg_stat_activity` (the pid's `backend_start`)
  **twice**, and requires the two reads to agree; the views share no
  snapshot, and a disagreement is treated as a change of walsender.
  `pg_current_wal_lsn()` is recorded for diagnosis only; no rule depends on
  it.
- **Every `active` sighting** must show `backend_type = 'walsender'` for the
  holder, with non-NULL `backend_type` and `backend_start`; a NULL is
  `rollback_check_privileges`, a non-walsender holder (a SQL session
  consuming the slot) is `rollback_check_continuity_unproven` (S12,
  "Privileges").
- **The first sighting** with `active = true` must show
  `confirmed_flush_lsn` ≤ B = max(final Lc, F_stop).
- **A change of walsender** (a new `active_pid` or `backend_start`, an
  inactive gap, or a gap in observation such as a restarted watch) is
  accepted only when the new walsender's first sighting shows
  `confirmed_flush_lsn` **equal** to F_last, the last value seen before the
  change.
- **Why equality is a proof (decided, DeVaris, 2026-10-12).** A recreated
  slot starts at its consistent point, which is **strictly above** the WAL
  insert position at its creation: creating a logical slot writes its own
  `xl_running_xacts` record and starts after it. The round-7 review's probe
  measured `confirmed_flush_lsn` = insert position + `0x38` on PG 17 and
  PG 18. The insert position at creation is at or above F_last, and a slot's
  `confirmed_flush_lsn` never moves backwards. So a recreated slot can never
  show F_last, on a busy or an idle database.
- **The same-walsender rule (final review; corrected in verification
  round 2).** The identity of a sighting is the tuple (`active_pid`,
  `backend_start`, `query_start`), with both timestamps compared at their
  full microsecond precision. Across a gap in observation (a restarted
  watch, or a one-shot check after the watch), a sighting with the **same**
  tuple as the last one, and `confirmed_flush_lsn` ≥ F_last, is continuity
  whatever the value. Proof: `query_start` of a walsender is the start of its
  current replication command, so an unchanged tuple means the same
  `START_REPLICATION` command is still running in the same backend. A
  walsender holds the slot for the whole of that command, and a slot that is
  held cannot be dropped, so the slot it holds now is the slot it held then.
  An earlier version of this rule claimed Debezium opens a new connection for
  every stream; that is false (`startStreaming` can retry on the same
  connection). A retry issues a new `START_REPLICATION`, which changes
  `query_start`, so it is judged by the equality rule instead. That a retry
  always changes `query_start` is pre-merge gate item 12.
- **Anything else** (a first sighting above B, or a new walsender whose
  first sighting differs from F_last) is
  `rollback_check_continuity_unproven`.
- `offset.flush.interval.ms` matters for liveness only. Debezium moves its
  slot only after a Kafka Connect offset commit, so a lower interval only
  raises the chance that a flush lands inside a change of walsender, which
  fails the check: the safe side. `catalog_xmin` and `restart_lsn` are not
  used; both move with ordinary progress.

**Residual risk, kept in the runbook (DeVaris decision, 2026-10-12).** No
rule can exclude another actor dropping Debezium's slot between the last
pre-restart check and Debezium's attach; the first-sighting rule catches it
afterwards. The runbook says not to run other slot tooling against these two
slots while going back.

**`--emit-cutover-marker`** is a `--cutover` flag only (Q3). Every pipeline
the CLI writes excludes its prefix from the pipeline's messages (S16), so a
pipeline never emits a marker, and a committed Lc at the marker's end proves
only that Debezium processed everything before it, not which slot it read
from. The runbook's idle-database step uses ordinary writes to a captured
table instead.

### S16. Pipelines the CLI writes (5th pass)

**How the CLI deploys a pipeline.** Conduit has two kinds of pipelines, and
the API treats them differently. Pipelines provisioned through the API can be
created, updated, started, stopped and deleted through it. Pipelines
provisioned from a file in `pipelines.path` can be started and stopped
through the API, but an update or a delete is refused with
`orchestrator.immutable_provisioned_by_config`
(`CodeImmutableProvisionedByConfig`, `pkg/orchestrator/pipelines.go`), and the
file is read only at engine start (or by the `--dev` watcher). The CLI
supports both:

- **API (the default).** The CLI creates, updates, starts and stops the
  pipeline through the Conduit API. It never deletes one in v0.21; the manual
  runbook deletes it with the API (S12), which removes its engine state.
- **File (`--pipelines-path DIR`).** The CLI writes `DIR/<pipeline>.yaml`
  (temporary file, fsync, rename) and starts and stops the pipeline through
  the API. An update or a delete is a file change that the engine picks up
  only when it reloads. The CLI makes the change (an update, for
  `--without-import`), prints what the engine needs, and waits, under the
  step's timeout, until the API shows the new config. The wait is
  journalled, so a re-run resumes it.
  - **(6th pass) A delete needs an engine restart** (the runbook's delete,
    S12). The `--dev` watcher
    applies new and changed files, but never deletes: on a removed file it
    leaves the pipelines running (`pkg/conduit/dev/apply.go`,
    `handleDeleted`). Only the provisioning pass at engine start removes
    pipelines whose file is gone (`deleteOldPipelines`), with their engine
    state. A delete is complete only when the API no longer lists the
    pipeline.
  - **(6th pass) The status in the file.** The file is written with `status:
    stopped` and stays so until the `starting` step. At `starting`, the CLI
    rewrites it with `status: running` and also calls the API start, so the
    pipeline runs now and after any later engine restart. `--no-start` leaves
    the file at `stopped`. An engine restart before `starting` leaves the
    pipeline stopped: the engine's init turns a persisted running status into
    system-stopped, and provisioning applies the file's `stopped` as
    user-stopped, so nothing but the CLI starts it.
  - **Stopping a file-provisioned pipeline** (the stop after `start_failed`,
    and the runbook's stop, S12) first sets `status: stopped` in its file,
    then calls stop. Otherwise an engine restart would start it again from
    the file.

**Settings on every pipeline the CLI writes.**

- `logrepl.requireExistingSlot: true` (S13): a dropped slot fails `Open`
  instead of being recreated at the current WAL.
- `logrepl.autoCleanup: false`. With the connector default, `true`, a
  pipeline delete calls `LifecycleOnDeleted`, which drops the slot and the
  publication (`source.go`).
- `conduit.migrate.cutover` in `debezium.message.prefix.exclude.list` (or
  removed from an include list the CLI translated), so the pipeline never
  emits the `--emit-cutover-marker` message. #3033's filtered-message rule
  then applies: the message does not advance `emitted`. This is a stated
  difference from Debezium's own output, which carries the marker on
  `<topic.prefix>.message`.

**`autoCleanup` and `LastActiveConfig`.** The engine runs
`LifecycleOnDeleted` with the source's `LastActiveConfig`
(`pkg/connector/source.go`, `OnDelete`), which is the config the source last
**started** with, not the current one. A pipeline that never started has
none, and its delete triggers nothing. So `autoCleanup: false` protects the
slot only once the pipeline has started with it, and a hand edit to `true`
followed by a start would make a later delete drop the slot. The runbook's
order makes this harmless: the delete comes only after `--check-rollback`
returns `ready_to_delete_pipeline`, which already includes every condition for
the drop except the pipeline's absence, so whether the delete drops the slot
or the operator does, it happens at a point where dropping is correct.

**Pipeline id (final review).** The Conduit API addresses pipelines,
connectors and processors by id, not by name. An API-provisioned pipeline
gets a UUID at creation (`PipelineOrchestrator.Create`); a file-provisioned
one uses the file's `id`. The cutover journals the pipeline id at
`pipeline_written`, and the runbook addresses the pipeline by it. The ids of
its connectors and processors are re-read from the API
(`GET /v1/pipelines/{id}`, `connectorIds` and `processorIds`) at delete
time, because the API, not the journal, is the source of truth for what is
attached; the journal's copy is for diagnosis only.

**The order at the end of the manual reset or rollback (S12, final review).**
`--check-rollback` returns `ready_to_delete_pipeline` while the stopped
pipeline still exists, and `ready_to_drop` only once it is gone, so the
delete always comes first:

1. delete the pipeline. The API refuses to delete a pipeline that still has
   connectors or processors attached
   (`orchestrator.pipeline_has_connectors_attached`,
   `orchestrator.pipeline_has_processors_attached`), and a connector with
   processors (`orchestrator.connector_has_processors_attached`). So the
   order is: connector-level processors, pipeline-level processors, the
   connectors, then the pipeline. Deleting the source connector runs
   `OnDelete`, which calls `LifecycleOnDeleted` with `LastActiveConfig`;
   with `logrepl.autoCleanup: false` that is inert. For a file-provisioned
   pipeline, remove the file and restart Conduit, which removes the pipeline
   and its engine state;
2. run `--check-rollback` again: `ready_to_drop`;
3. drop Conduit's slot (`pg_drop_replication_slot`, tolerating "does not
   exist");
4. drop the publication;
5. archive the journal and the watch log.

Deleting first means no engine restart can start the pipeline against a
dropped slot, and with `requireExistingSlot` it would fail closed anyway.

**Neither the import nor `requireExistingSlot` ever creates the
publication.** The CLI creates or repairs it before the slot (B3), and the
connector only verifies it (`postgres.import.publication_mismatch`,
`postgres.logrepl.publication_missing`). Creating it later would be wrong: a
publication created after the slot's start makes pgoutput skip the earlier
WAL of its tables, and a PG 18 probe in the round-5 review lost a row that
way.

**After `complete`.** `logrepl.autoCleanup: false` stays on the pipeline
for good. The completion message and the runbook say so: _"The import is
complete. Remove `logrepl.import.debeziumOffset`. This pipeline keeps its slot
and publication when deleted (`logrepl.autoCleanup: false`). To have a delete
clean them up, set `logrepl.autoCleanup: true` and restart the pipeline once,
since the delete uses the config the pipeline last started with. Otherwise
drop the slot and publication by hand when you retire it."_ The runbook adds
that `logrepl.requireExistingSlot: true` should stay.

### S10. CDC mode

With an import set, `cdcMode` must be `logrepl`. `auto` is rejected with
`postgres.import.cdc_mode_conflict`, and the CLI writes `logrepl`.
**(Corrected in the 7th pass)** On `main`, `auto` does not fall back to long
polling: it falls through to logical replication (`source.go`, the `cdcMode`
switch, L96–99, with a TODO to add the check). The refusal stays because
`auto` documents a fallback, and an import must not depend on a mode whose
meaning may change; requiring `logrepl` states the intent.

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
  **(5th pass)** It never creates the publication either (S16), and the same
  holds under `logrepl.requireExistingSlot` (S13).
- **(2nd pass)** The setting uses `debeziumSlotName` everywhere. The D2 JSON
  now carries `database`, `systemIdentifier`, `plugin` and `identity`. The
  dry run no longer has `"strategy": "reuse"` or `skip_at_lsn`, and
  `Partial.SkipAtLSN` is gone. D2 and D10 are edited in place.

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
| Reading Kafka Connect (topic, REST, JSON file), the cutover orchestration, `--dry-run`, `--check-rollback` | `conduit`, `migrate kafka-connect` | Kafka Connect knowledge belongs to the CLI. It uses franz-go, which is already a dependency, so nothing new is added. |

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
  "database": "inventory",
  "systemIdentifier": "7412345678901234567",
  "plugin": "pgoutput",
  "debeziumSlotName": "debezium",
  "publicationName": "dbz_publication",
  "tables": ["public.orders", "public.customers"],
  "identity": {"conduitSlotConsistentPoint": "0/16E0000",
               "debeziumSlotFlushAtWait": "0/16E2000"},
  "offset": {"lsn_proc": 24023128, "lsn_commit": 24023064, "lsn": 24023128,
             "txId": 555, "ts_usec": 1760000000000000, "messageType": "INSERT"}
}
```

`offset` is Debezium's map, unchanged. LSNs are parsed as `json.Number` into
`uint64`, never through `float64`. These fields are Debezium's settings:

- `database` is Debezium's `database.dbname`.
- `plugin` is `plugin.name`; the CLI writes `decoderbufs` when it is unset.
- `debeziumSlotName` and `publicationName` name Debezium's slot and
  publication.
- `tables` is Debezium's capture list, with its regexes resolved against the
  catalog.

`systemIdentifier` comes from `IDENTIFY_SYSTEM`. `identity` is S13's proof.
**(5th pass)** The 4th pass added an optional `resume` field
(`"slotOnly"`). It is withdrawn in favour of the separate setting
`logrepl.requireExistingSlot` (S13), decided tri-state.
`logrepl.slotName == debeziumSlotName` means reuse, which v0.21 refuses with
`postgres.import.slot_reuse_unsupported`.

How the connector treats the setting:

- **Position empty:** apply the import. Validation runs first:
  - `snapshotMode` must be `never`, or validation fails with
    `postgres.import.snapshot_mode_conflict`, so the "no snapshot" intent is
    explicit.
  - `cdcMode` must be `logrepl`, or it fails with
    `postgres.import.cdc_mode_conflict`.
  - In v0.22's reuse mode, `logrepl.autoCleanup` will also have to be
    `false`.
- **Position present:** ignore the setting and log one warning per start:
  `import ignored: pipeline has a position; remove logrepl.import.debeziumOffset`.

### D3. Slot strategy

**Fresh (default).** Creating a new slot with the same plugin is how Conduit
replication normally starts. Steps, run by `conduit migrate kafka-connect
--cutover`:

1. While Debezium runs, create the Conduit publication, then the Conduit
   `pgoutput` slot. Record its consistent point C.
2. Wait until Debezium's **committed** offset has `Lc ≥ C`, polling REST or the
   topic. Also require Debezium's slot on this server to reach C (S13). On an
   idle database, Debezium does not process a commit, so Lc does not move.
   With the opt-in `--emit-cutover-marker` the CLI prompts one (Q3).
   Otherwise the runbook says to write two separate transactions to a
   captured table, or to set a `heartbeat.action.query` that writes to a
   captured table (S5). Every offset-independent
   preflight has already passed at this point (S5).
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
> `--slot-strategy fresh`, the default. `--slot-strategy reuse` is refused in
> v0.21 with `postgres.import.slot_reuse_unsupported`. The reuse strategy
> above is v0.22. It will ship per Debezium version, and only for versions where
> the reuse rollback test passes. Until then, an import whose `logrepl.slotName`
> equals the setting's `debeziumSlotName` fails with `postgres.import.slot_reuse_unsupported`.
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
`UPDATE`, `DELETE`, `TRUNCATE`), X is present, and the plugin is `pgoutput`
or `decoderbufs` (S4). It applies to the transaction streamed with commit LSN
≥ Lc whose `BeginMessage.Xid` equals X. When `ts_usec` is present, that
transaction's `CommitTime` must also equal `ts_usec`. The rule stays armed
until that transaction has been streamed; transactions with another xid do not
disarm it (S2). For that transaction:

- skip its changes whose change LSN is below L;
- deliver everything else, including every change at L. A multi-table
  TRUNCATE at L is always delivered (B2).

> **Amended 2026-10-12 (Q1, B2, S2, S4).** The partial rule ships in v0.21
> (Q1), L-only. The second bullet, skipping the first n changes at L, is
> **removed** (B2: a multi-table TRUNCATE counts once per relation in Debezium
> but is one message here). The rule is armed only for `pgoutput` and
> `decoderbufs` offsets (S4). The duplicate cases are corrected in S2.

If T never arrives, because it touched only tables outside the pipeline, the
rule never fires. That only means nothing is skipped.

`ResumePoint` gets an optional `Partial{Xid, CommitTime, BeforeLSN}` field,
used only by the subscription goroutine. Changes the partial rule skips still
advance the change key (seq is counted before the skip decision), so the
records Conduit delivers from T carry their real keys. The first ack persists
a normal version 2 position, which the exact rule resumes from. After a
restart from a version 2 position the partial rule is gone, so a T not yet
streamed is delivered in full: more duplicates, never loss.

The other values of M:

- **M = COMMIT** (a heartbeat after a commit, or a transaction-metadata END
  record), **or M = MESSAGE with `lsn == lsn_commit`** (a non-transactional
  message, S1): Lc covers T, and the base rule skips it.
- **M = BEGIN:** nothing of T was delivered, so T is delivered in full.
- **M = MESSAGE with `lsn > lsn_commit`** (a transactional message): Postgres
  reports a message's **end** LSN, so the partial rule is not used and T is
  delivered in full.
- **M absent** (older 1.x): base rule only.

**Offsets without `lsn_commit`** (offsets older than late 1.x, or offsets with
only `lsn`) are refused with `postgres.import.offset_unsupported`. The fix
text is in D11. The base rule
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
   start at the current WAL and miss the gap), is on this database, has `plugin
   = 'pgoutput'`, is not `active`, and is not temporary. Its `wal_status` is not
   `lost` (PG 13+) and it has no `invalidation_reason` (PG 17).
5. **F ≤ Lc.** Postgres re-sends exactly the transactions whose commit is at or
   past F, and Conduit needs everything with commit ≥ Lc. If F > Lc, the
   changes in (Lc, F] are not re-sent and O does not cover them. Under reuse
   that means Debezium flushed past its committed offset (an ungraceful stop).
   Under fresh it means the slot was created after O. Either way the import
   refuses (`postgres.import.slot_ahead`). There is no override in v0.21 (Q2).

   > **Amended 2026-10-12 (Q2).** Decided: no override, in any version. The
   > refusal is final for that offset.
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
guarantees F ≤ Lc at start.

**(2nd pass, corrected.)** Earlier text said a report below F is harmless
because Postgres never moves `confirmed_flush_lsn` backwards. That is false on
older minors. The fix "Don't retreat slot's confirmed_flush LSN" landed in
14.19, 15.14, 16.10 and 17.6 (`e68459489c`, `9d1a623596`, `c0f51fde53`,
`7318f241d2`, 2025-05-19, backpatched through 13). **(3rd pass)** It is also
in 13.22 (`e323d9df00`) and 18.0 (master `ad5eaf390c`). Before those, a lower
report can move it back. That makes Postgres re-send more (duplicates), but
loses nothing, and it can make the S9 re-run check see F_c lower than
expected, which is the safe direction. To keep the cap from causing even that,
import mode also floors every report at F0, the slot's `confirmed_flush_lsn`
when the subscription started: `flush = max(min(flush, Lc), F0)`. F0 ≤ Lc by
check 5.

> **Amended 2026-10-12 (S8).** What the cap retains (WAL and
> `catalog_xmin`), the `max_slot_wal_keep_size` path, and the citation for
> ack-after-persist are in S8.

### D7. Snapshot state is refused

| Offset shape | Code | Suggested fix |
| --- | --- | --- |
| `snapshot` present, not completed (initial or blocking snapshot running) | `postgres.import.snapshot_in_progress` | Let the Debezium snapshot finish and Debezium commit a streaming offset, then retry. |
| Snapshot completed but no streaming offset yet (`last_snapshot_record`/`snapshot_completed` true, no `lsn_commit`) | `postgres.import.snapshot_in_progress` | Debezium needs a committed transaction followed by a later record or heartbeat. Enable `heartbeat.interval.ms`, or write two separate transactions to a captured table. Wait for an offset flush (`offset.flush.interval.ms`), then retry. |
| `incremental_snapshot_*` shows a snapshot in progress | `postgres.import.incremental_snapshot_in_progress` | Let it finish, or stop it with Debezium's `stop-snapshot` signal, then retry. |

Why snapshot-final offsets are refused: the boundary between Debezium's
snapshot and its stream is Debezium-internal. How a transaction that committed
around the snapshot's LSN was handled depends on the Debezium version and the
snapshot mode. A streaming offset with an `lsn_commit` removes the question.

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

**v0.21 (fresh strategy, scope cut):** a manual procedure gated by the
read-only `--check-rollback` (S12). In order:

1. `--check-rollback`: Debezium's slot is valid and has not moved past
   max(final Lc, F_stop); Conduit's durable progress ≥ F_stop when
   F_stop > Lc; the live Debezium config passes S5 item 9 and Kafka Connect's
   validation.
2. Stop the Conduit pipeline (setting `status: stopped` first for a
   file-provisioned pipeline, S16).
3. Start `--check-rollback --watch`, then resume (or re-create) Debezium by
   hand; the watch follows slot continuity (S15).
4. Wait for `ready_to_delete_pipeline`: Debezium's slot `confirmed_flush_lsn`
   ≥ Conduit's slot's, and Conduit's durable progress still ≥ F_stop when
   F_stop > Lc.
5. Delete the pipeline, check again for `ready_to_drop`, and only then drop
   Conduit's slot and publication (S16).

Automated reset and rollback are v0.22 (#3044).

There is no loss for the pipeline's tables when every gate passed (S12). The
duplicates are everything Conduit delivered since the cutover, which Debezium
re-delivers into Kafka. The window lasts as long as Debezium's slot stays valid.
It holds WAL **and `catalog_xmin`** for the whole soak: vacuum cannot remove
catalog rows newer than it. So size `max_slot_wal_keep_size` for the soak
period, watch catalog bloat, and drop Debezium's slot when the soak ends.

**v0.22 (reuse strategy, open design item S7):** rollback writes a Debezium
offset back with `PATCH /connectors/{name}/offsets` (Kafka 3.5+). The offset
must be derived from **both** F, the slot's `confirmed_flush_lsn` at
Conduit's stop, and Conduit's last persisted position, because Debezium
streams from max(its offset, F). It ships per Debezium version, and only where
the rollback test passes. No write-back formula is specified in this document.

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

# Without the REST API, read the topic instead
# (read_committed, all partitions, to the high watermark).
  --offsets-from topic --bootstrap-servers kafka:9092 [--offsets-topic connect-offsets]
# Or read a saved REST export.
  --offsets-from file --offsets-file offsets.json

# Go back (v0.21: the manual runbook, gated by this read-only check; S12).
conduit migrate kafka-connect --check-rollback --pipeline orders \
  --journal-dir ... --connect-url ... [--watch]
```

> **Amended 2026-10-12 (Q3, Q4).** The v0.21 flags are
> `--cutover [--stop-connector] [--slot-strategy fresh] [--emit-cutover-marker]`.
> `fresh` is the default and the only strategy v0.21 executes;
> `--slot-strategy reuse` is refused with `postgres.import.slot_reuse_unsupported`.
> `--emit-cutover-marker` is the opt-in from Q3, and writes
> `pg_logical_emit_message(false, …)`: without it, the CLI never writes to the
> source database.
>
> `--cutover` also gains (2nd pass):
>
> - `--slot-create-timeout` and `--journal-dir` (S9);
> - `--cutover-wait-timeout`, default 30m, which bounds waiting for Lc ≥ C;
> - `--start-timeout` (S11);
> - `--without-import`, the fallback in S13;
> - `--pipelines-path DIR`, which selects file provisioning (S16).
>
> **(Scope cut)** The read-only `--check-rollback [--watch]
> [--watch-timeout 30m] [--pg-url] [--connect-url] [--conduit-url] [--json]`
> replaces the `rollback` subcommand (S12), with
> `--check-rollback --abandon --reason <text> [--json]` to end a failed
> attempt. `--reset`,
> `--reset-timeout`, `--rollback-timeout` and `--debezium-secret` are removed
> from v0.21.
>
> It starts the pipeline by default and waits for the durable import-complete
> signal (S8, S11); `--no-start` opts out.
>
> If Debezium's committed offset is not past C by `--cutover-wait-timeout`,
> the CLI exits with `cutover_timeout`. Debezium is still running at that
> point, and the CLI prints the fix: two separate transactions to a captured
> table, or a `heartbeat.action.query` that writes to a captured table (S5).
> In the dry-run JSON, `slot.strategy` is always
> `"fresh"` in v0.21.

`--stop-connector` is opt-in. Without it, the CLI checks that the connector is
stopped and fails with a fix if it is not.

The `--dry-run --json` output is shaped like this:

```json
{
  "offset": {"source": "rest", "connector": "inventory-connector",
             "partition": {"server": "dbserver1"},
             "lsn_commit": "0/16E8F58", "lsn": "0/16E8F98", "txId": 555,
             "messageType": "INSERT", "lsn_events_processed": null},
  "slot": {"name": "conduitslot", "strategy": "fresh", "plugin": "pgoutput",
           "active": false, "consistent_point": "0/16E0000",
           "confirmed_flush_lsn": "0/16E0000",
           "restart_lsn": "0/16DF000", "wal_status": "reserved",
           "retained_wal_bytes": 512000},
  "debezium_slot": {"name": "debezium", "database": "inventory",
                    "confirmed_flush_lsn": "0/16E8F58", "wal_status": "reserved"},
  "resume": {"skip_commits_below": "0/16E8F58",
             "partial": {"xid": 555, "commit_time_usec": 1760000000000000,
                         "skip_changes_below_lsn": "0/16E8F98"},
             "duplicate_bound": "xid 555 at LSN 0/16E8F98 (+1 tx message)",
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
| `postgres.import.offset_unsupported` | no `lsn_commit` (an offset older than late 1.x) | This offset has no `lsn_commit`. Run the connector on a Debezium version that writes it (every 2.x and 3.x release; the fixture set lists the 1.x builds that do), let it commit an offset after at least one transaction, then retry. |
| `postgres.import.snapshot_in_progress` | see D7 | see D7 |
| `postgres.import.incremental_snapshot_in_progress` | see D7 | see D7 |
| `postgres.import.snapshot_mode_conflict` | the import is set with `snapshotMode: initial` | Set `snapshotMode: never`. |
| `postgres.import.cdc_mode_conflict` | the import is set with `cdcMode` other than `logrepl` | Set `cdcMode: logrepl`. |
| `postgres.import.slot_reuse_unsupported` | `logrepl.slotName` equals `debeziumSlotName`, or the CLI was given `--slot-strategy reuse` | v0.21 supports only `--slot-strategy fresh`. Reuse arrives in v0.22. This is the single code for every reuse request. |
| `postgres.import.slot_missing` | Conduit's slot does not exist | Go back to Debezium with the manual runbook, gated by `conduit migrate kafka-connect --check-rollback` (S12), then redo the cutover. Import mode never creates a slot. |
| `postgres.import.slot_plugin` | Conduit's slot is not `pgoutput` | Go back to Debezium with the manual runbook (S12), then redo the cutover. |
| `postgres.import.slot_active` | the slot is in use | Stop the other consumer. The message shows its pid and `application_name`. |
| `postgres.import.slot_invalidated` | `wal_status = lost` or `invalidation_reason` is set | The WAL is gone. Re-snapshot. |
| `postgres.import.slot_ahead` | F > Lc on Conduit's slot | Something consumed Conduit's slot past Debezium's offset. Go back with the manual runbook (S12): Debezium's slot still holds everything from O. Then cut over again. If Debezium's slot is lost, re-snapshot. |
| `postgres.import.slot_database` | the slot is on another database | Fix `url`. |
| `postgres.import.offset_from_future` | Lc > `pg_current_wal_lsn()`, or the server is a standby | Wrong server, or one restored from backup. |
| `postgres.import.database_mismatch` | `database`, `systemIdentifier` or the S13 slot proof does not match this server | Point `url` at the database Debezium read from; re-export the offset. |
| `postgres.import.plugin_unsupported` | `plugin` absent, `wal2json`, `wal2json_streaming`, or unknown | Not importable. Re-snapshot or dual-run. |
| `postgres.import.server_unpatched` | below 14.13, 15.8, 16.4 or 17.0 (B1) | Upgrade to the listed minor or later. |
| `postgres.import.server_unsupported` | Postgres 13 or earlier (a policy choice, B1) | Upgrade to 14.13 or later. |
| `postgres.import.publication_mismatch` | the publication is missing or does not cover the pipeline's tables | Repair the publication, then redo the cutover with a new fresh slot created after the repair (the manual reset, S12, then `--cutover`). Never add tables to a publication that an existing slot will decode from: pgoutput reads the historic catalog and silently skips their earlier WAL. |
| `postgres.import.tables_not_captured` | the pipeline has tables Debezium did not capture | Remove them and add them in a separate pipeline with a snapshot. |

> **Amended 2026-10-12 (2nd pass).** The table above is edited in place.
> `autocleanup_on_reused_slot` is removed, and will come back with v0.22
> reuse.

CLI codes (`migrate.kafka_connect.*`, mapped to exit codes per the
deterministic exit code ADR):

- `offset_not_found` – no offset for this connector.
- `ambiguous_partition` – more than one partition.
- `connector_running` – the connector is RUNNING or PAUSED: before
  `--cutover` without `--stop-connector`, for a `--cutover` re-run that finds
  Debezium running again after step 8 (S9), for `--without-import`, and for
  `--check-rollback --abandon` (S12).
- `offsets_unreachable` – neither REST nor the topic could be read.
- `cutover_timeout` – Lc (or Debezium's slot, S13) did not reach C within
  `--cutover-wait-timeout`.
- `cutover_marker_unsupported` – Q3.
- `identity_mismatch` – S13.
- `lossy_source_config` – a warning (S2, S6).
- `slot_advanced`, `slot_in_use`, `slot_unowned`, `slot_create_blocked`,
  `pipeline_exists`, `config_masked` – S9.
- `slot_advance_on_start_unsupported` – S5 item 9 (7th pass), before the
  stop and again at step 6.
- `slot_drop_on_stop`, `lsn_flush_mode_unsupported` – S5, before the stop and
  again at step 6. `lsn_flush_mode_unsupported`
  covers `manual`, `connector_and_driver` and `flush.lsn.source=false`
  (4th pass). `conduit kc lint` reports the same configurations as
  `kc.lint.postgres_slot_drop_on_stop` and `kc.lint.postgres_lsn_flush_mode`;
  `--cutover` emits these `migrate.kafka_connect.*` codes (#3026).
- `identity_ahead` – a warning (S13).
- `start_failed`, `import_pending` – S11.
- `operation_in_progress`, `lock_unavailable` – S15 (5th to 7th passes).
- `rollback_in_progress` – `--cutover` while an un-abandoned rollback watch
  log exists (S9).
- `--check-rollback` (S12): `rollback_check_no_journal`,
  `rollback_check_stop_incomplete`, `rollback_check_cutover_running`,
  `rollback_check_slot_lost`, `rollback_check_slot_moved`,
  `rollback_check_conduit_behind`, `rollback_check_config_unsafe`,
  `rollback_check_two_writers`, `rollback_check_continuity_unproven`,
  `rollback_check_connector_state`, `rollback_check_privileges`,
  `rollback_check_pipeline_state`, `rollback_check_conduit_unreachable`,
  `rollback_check_restart_interrupted`, `rollback_check_state_unmapped`,
  `rollback_check_offset_unreadable`, and
  `rollback_check_not_caught_up` (a `waiting` verdict). `--abandon` (S12)
  ends a failed attempt. Verdicts:
  `ready_to_stop_conduit`, `ready_to_restart_debezium`,
  `ready_to_delete_pipeline`, `ready_to_drop`, `waiting`, `unsafe`.

The scope cut removed `reset_unsafe`, `reset_pending`, `rollback_slot_lost`
and `rollback_pending`; they return, if at all, with #3044.

Connector codes added in the 5th pass, for `logrepl.requireExistingSlot`
(S13): `postgres.logrepl.slot_missing`, `postgres.logrepl.slot_plugin`,
`postgres.logrepl.slot_invalidated`, `postgres.logrepl.publication_missing`.
The 7th-pass default-refuse proposal adds the warning
`postgres.logrepl.slot_recreated` (decided tri-state, S13).

`rollback_unsupported` is not in v0.21; it returns with v0.22 reuse.

The CLI also passes the connector's codes through as they are.

## Correctness

> **Amended 2026-10-12 (S6, B2, S1).** Goal 2 is restated in S6. Changes
> that Debezium's configuration dropped count as processed by Debezium. Case 2
> is edited in place. S1 extends case 1 to non-transactional messages.
> **(2nd pass)** Case 3 needs F ≤ Lc on the slot Conduit reads. S13 is what
> ties Lc to this server.

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
2. **U = T (xid = X, and commit time = `ts_usec` when present), the partial
   rule applies, LSN(c) < L.** Within one transaction, pgoutput (and
   decoderbufs) send changes in WAL order, so c was emitted before O's record.
   **Skipped, delivered by Debezium.** Changes at L itself are never skipped
   (B2).
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

Conduit re-delivers a change that Debezium also delivered only in the cases
of the S2 table, which is the authoritative version and is repeated here so
there is one copy to implement against:

| Case (offset) | Duplicates |
| --- | --- |
| M = COMMIT, or M = MESSAGE with `lsn == lsn_commit` | 0 |
| M = BEGIN | 0 |
| M is DML (including a mid-transaction heartbeat), and T arrives | T's changes at LSN L: 1 for an ordinary row, ≤ 1000 for one multi-insert record, one per relation for a multi-table TRUNCATE; plus at most one transactional message of T ending at L, when Conduit receives messages (S14) |
| M is DML, and T never arrives on this slot | 0 |
| M = MESSAGE with `lsn > lsn_commit`, or M absent | T's prefix that Debezium delivered |
| A crash after the first ack, before T is streamed | T's prefix (the partial rule is not restored, S2) |
| Debezium stopped ungracefully | the records produced after O, up to one `offset.flush.interval.ms` of traffic. **(4th pass)** Debezium's slot past Lc is the `identity_ahead` warning, not a refusal; `slot_ahead` applies only to Conduit's slot |

Every case is exercised by the harness. With `--peek`, the dry run predicts the
exact count, and the harness asserts equality, not just the bound.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Debezium still running at import (fresh) | `--cutover` refuses `connector_running` without `--stop-connector`. At `Open`, Debezium's slot must be inactive and satisfy S13. | 3 |
| Debezium stopped ungracefully, Debezium's F > Lc | **(4th pass)** `identity_ahead` warning; (Lc, F_stop] are duplicates. Going back needs Conduit's durable progress ≥ F_stop first (`rollback_check_conduit_behind`, S12). `slot_ahead` is only for Conduit's slot. | 3 |
| Debezium's slot dropped on stop (`slot.drop.on.stop=true`) | Refused before the stop (`slot_drop_on_stop`, S5). If the setting changes after preflight, the CLI hits `identity_mismatch` at the post-stop re-check first, before the connector's `database_mismatch`. The fix is the S13 fallback (`--without-import`); `--check-rollback` reports the slot lost, so going back is not offered. | 3 |
| CLI killed mid-cutover, then re-run | The step-aware rules of S9, including `creating_slot`, `stopping` and `starting`. | 3, 5 |
| Going back with Debezium's slot gone | `rollback_check_slot_lost`; the runbook keeps Conduit's slot (S12) | 3 |
| Debezium restarted by hand, not yet streaming | `--check-rollback` reports `waiting`; the runbook keeps both slots (S12) | 3 |
| Debezium's `lsn.flush.mode=manual` / `connector_and_driver`, or `flush.lsn.source=false` | Refused before the stop (`lsn_flush_mode_unsupported`), and again against the saved config at step 6; `--check-rollback` checks the live config before a manual restart (S12) | 3 |
| Literal masks (`[hidden]`, `****`) in Debezium's config | Refused before the stop (`config_masked`) on workers older than Kafka 3.5, where the connector is deleted and the saved config must re-create it. Config-provider placeholders are saved as they are (S9) | — |
| **(4th pass)** Debezium's slot recreated after the stop | `--check-rollback` sees `confirmed_flush_lsn` above max(Lc, F_stop) and reports `rollback_check_slot_moved`; the runbook keeps Conduit's slot (S12) | 3 |
| **(4th pass)** A pipeline delete drops Conduit's slot through `LifecycleOnDeleted` | Cannot happen: the CLI writes `logrepl.autoCleanup: false`, and in the runbook the operator drops the slot by hand, last, after `ready_to_drop` (S12, S16) | 3 |
| **(4th pass, decided in the 5th)** CLI killed while `CREATE_REPLICATION_SLOT` is blocked | `slot_create_blocked` with the holder's pid and `backend_start` and the journalled ones; the CLI never terminates a backend (S9). Debezium is still running. | — |
| **(4th pass)** `--without-import` after Conduit's slot was dropped | Refused by the CLI (`postgres.logrepl.slot_missing`, 6th pass); if the slot disappears after the check, `logrepl.requireExistingSlot` fails `Open` with `postgres.logrepl.slot_missing` instead of creating one (S13) | 3 |
| **(5th pass)** CLI killed or timed out at `stopping`, then going back | `--check-rollback` reports `rollback_check_stop_incomplete` while Kafka Connect shows RUNNING; once STOPPED it observes F_stop itself, and F_stop > Lc leads to `conduit_behind` (S12) | 3 |
| **(5th pass)** Debezium's slot recreated during the manual restart | The continuity rules fail: `rollback_check_continuity_unproven`, the runbook keeps both slots (S12, S15) | 3 |
| **(5th pass, 6th)** Two CLI processes on one slot, including through a pooler | The temporary lock slot makes the second refuse with `operation_in_progress`; a lost lock session aborts the first with `lock_unavailable` before its next mutating step (S15) | 3, 5 |
| **(6th pass)** Debezium reconnects under a new walsender pid during the `--check-rollback --watch` | Accepted when the new walsender's first sighting shows the same `confirmed_flush_lsn` as the last one under the old; otherwise `continuity_unproven` (S15) | 3 |
| **(5th pass, narrowed in the 6th)** A positioned pipeline whose slot is missing | `logrepl.requireExistingSlot` fails `Open` with `postgres.logrepl.slot_missing` instead of silently starting from the current WAL (S13). A slot dropped and recreated under the same name before the start is **not** caught (S13) | 3 |
| **(5th pass)** Engine restart starts a file-provisioned pipeline during rollback | Prevented by the runbook: the operator sets `status: stopped` in the file before stopping the pipeline, and `--check-rollback` reports `two_writers` if both run (S12, S16) | 1, 3 |
| Offset read on a standby (PG 17 synced slots) | Refused before slot creation (`offset_from_future`, S5 step 5) | 3 |
| Going back with Debezium's slot invalidated | `rollback_check_slot_lost`, nothing changed (S12) | 3 |
| Going back before Debezium catches up | `waiting` (`rollback_check_not_caught_up`); the runbook keeps both slots (S12) | 3 |
| Fresh slot created after O (C > Lc) | `slot_ahead` refusal; the CLI also refuses before stopping Debezium | 3 |
| Slot invalidated (`max_slot_wal_keep_size`) | `slot_invalidated` | 3 |
| Conduit killed before the first ack | Re-import gives the same resume point; the cap keeps F ≤ Lc | 1, 2, 3 |
| Conduit killed after the first ack | Version 2 exact resume | 2, 3 |
| Pipeline reset months later with the setting still present | F ≫ Lc, so `slot_ahead`; the operator removes the setting | 3 |
| Idle database after import | Slot held at Lc until the first durable ack (D6). `--cutover` exits with `import_pending` and leaves the pipeline running (S11); runbook (S8). | — |
| Snapshot-state offset | Coded refusal (D7) | 3 |
| A transaction ≥ Lc other than X arrives first | Delivered in full. The rule stays armed for X (S2). | 3 |
| Table set mismatch | `tables_not_captured` / `publication_mismatch` | 3 |
| Offset from another server, a clone or a promoted standby | S13: Debezium's slot on this server must have reached C during the wait, and after the stop must still exist with F ≥ C. Otherwise `identity_mismatch` / `database_mismatch`. F > Lc after the stop is only `identity_ahead` (4th pass). | 3 |
| Large LSN in JSON | Parsed as `uint64` through `json.Number`; values that do not fit fail | — |
| Aborted EOS offset writes in `connect-offsets` | Read with `read_committed` | 3 |
| Downgrade to a build without import before the first ack | An older build ignores the unknown setting or rejects it at validation. If it starts, it resumes from F with the legacy rule: duplicates of (F, Lc], no loss, because the slot is present | 3 |

## Upgrade and rollback (of the connector)

- There is no format change. The setting is new and optional. Positions
  written after an import are ordinary version 2 positions.
- Downgrading after the first ack works the same as for any version 2
  position (see the interleaved-tx doc). **(5th pass)** With the `messages`
  option on, that position carries `tx_seq_m` and no `tx_seq` (#3033,
  `53b767d1`). A build that predates the field reads it as a legacy position
  (`position.go`: a position with `TxCommitLSN` but no `TxSeq` is legacy) and
  resumes by the legacy rule: duplicates, never a skip.
- **(5th pass)** `logrepl.requireExistingSlot` is a new optional setting
  (S13). An older build rejects or ignores it; ignoring it means today's
  create-if-missing behaviour.
- Debezium rollback: D9.

## Observability

- INFO at `Open`: the parsed O, the strategy, F, `restart_lsn`, the computed
  rule, and the duplicate bound.
- INFO from inside the first `Ack` (which runs only after the position is
  durable): `import complete at <key>; remove logrepl.import.debeziumOffset`.
- WARN every minute while the D6 cap holds the slot.
- The duplicate set is logged: a count at INFO, keys at DEBUG (see the Q5
  block below). No metadata key is added in v0.21.
- The connector has no metrics of its own today. Lane #3013 owns connector
  metrics, and an `import_pending` gauge is requested there.

> **Amended 2026-10-12 (Q5 decided: defer the key, 3rd pass).** DeVaris
> deferred the metadata key on 2026-10-12. v0.21 ships logging only:
>
> - **At `Open`,** INFO states the expected set from the S2 table: "xid X,
>   changes at LSN L", "xid X, all changes (T in full)", or "empty".
> - **When the set closes,** INFO gives the **count** and the set's change-key
>   range `(T.commit, firstSeq)`–`(T.commit, lastSeq)`. The set closes when
>   T's last change at L has been emitted, or T's commit is reached, or the
>   case is one where the set is empty.
>   - For the bounded set, each record's `(collection, key)` is logged at
>     **DEBUG** only, because keys can be personal data.
>   - For the "T in full" cases, INFO gives the xid, T's commit LSN and the
>     count, and says plainly that sinks cannot deduplicate this set by key.
>     Consumers that must not see duplicates should compare against
>     Debezium's topic for that xid.
>   - None of this is a public contract.
>
> **Future work (not in v0.21): a metadata key.** If users need record-level
> marking, a later release can add `postgres.import.possibleDuplicate`
> (`"true"`, absent otherwise, following `postgres.snapshot.resumed` and
> OpenCDC's lowerCamelCase). It would be set on exactly the S2 set, or on the
> first transaction ≥ Lc when the set cannot be bounded. That is a public
> contract and needs its own decision.

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
     with no loss (with the `identity_ahead` warning when Debezium's slot
     passed Lc; 4th pass), or a refusal, and never loss.
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
- **Rollback tests:** fresh, before and after Conduit has run, following the
  manual runbook gated by `--check-rollback` (S12, scope cut). Reuse
  write-back is v0.22.
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
>   - a duplicate-set assertion against the Q5 logging (see below).
> - **Fresh-slot rollback tests** (before and after Conduit has run) stay in
>   v0.21.
>
> **Amended 2026-10-12 (review fixes).**
>
> - **Images:** pinned by digest at or above 14.13, 15.8, 16.4 and 17.0, with
>   PG 18 added (B1). An unpatched 16.3 image runs only the `server_unpatched`
>   refusal test. To exercise the D6 floor, the matrix includes both a minor
>   below the confirmed-flush retreat fix (for example 16.9) and one at or
>   above it (16.10).
> - **Debezium:** `skipped.operations=none` (S6).
> - **New cut points:**
>   - a transaction opened before slot creation and committed after Lc (B1);
>   - a multi-table TRUNCATE (B2);
>   - a non-transactional message as the last offset (S1);
>   - a heartbeat or END record as the last offset, asserting 0 duplicates
>     (S2);
>   - Debezium's graceful stop exceeding `task.shutdown.graceful.timeout.ms`
>     (S9);
>   - a CLI kill at every journal step, then a re-run, asserting the S9
>     table (S9);
>   - **(scope cut)** the manual rollback cases of S12, each asserting the
>     `--check-rollback` verdict and zero loss after the runbook's steps;
>   - a pipeline-side row filter or `publish` flag difference, so that a
>     transaction Q that Debezium never saw arrives before T. The test
>     asserts zero loss, and that T's prefix is still skipped (S2);
>   - an idle database after the stop: `import_pending`, then one write to a
>     pipeline table completes the import without a re-run (S11);
>   - a slot creation blocked by a prepared transaction (S9);
>   - an offset from another database or cluster (S3).
>   - **(4th pass)** the adoption and `pipeline_exists` cases listed under
>     S9's cut points;
>   - **(7th pass) Debezium startup options:** on Debezium 3.4+ each
>     `offset.mismatch.strategy` value (`trust_offset`, `trust_greater_lsn`,
>     `trust_slot`) is refused before the stop with
>     `slot_advance_on_start_unsupported`, and the unset case passes; on
>     Debezium 2.7, `slot.seek.to.known.offset.on.start=true` is refused and
>     `false` passes. A forced variant enables each one after preflight and
>     asserts the refusal at step 6 against the saved config. Debezium 3.4+
>     joins the nightly matrix.
>   - **(7th pass) Locks:** a network partition of the CLI's host drops both
>     lock slots within 30 s with the keepalive options; a stale holder is
>     named in the `operation_in_progress` text; two pipelines cutting over
>     from one Debezium connector are serialised by the Debezium-slot lock; a
>     slot name over 63 bytes gets a 53-byte lock name; exhausted
>     `max_wal_senders` and `max_replication_slots` fail the headroom
>     preflight with `lock_unavailable`; pgbouncer below 1.23, and in
>     transaction mode, is refused.
>   - **(7th pass) Managed platforms, before sign-off:** Amazon Aurora
>     PostgreSQL, Amazon RDS for PostgreSQL, Google Cloud SQL and Azure Database
>     for PostgreSQL must each be verified to accept `CREATE_REPLICATION_SLOT …
>     TEMPORARY PHYSICAL` on a replication connection with the keepalive
>     options, and to drop it on disconnect. A platform that does not is
>     unsupported for `--cutover` until a different lock is designed, and the
>     CLI refuses there with `lock_unavailable`.
>   - **(4th pass)** `--without-import`: zero loss and the S13 duplicate
>     bound; refused when Conduit's slot is missing, and with
>     `logrepl.requireExistingSlot: true` the connector never creates a slot
>     or a publication (5th pass);
>   - **(5th pass)** `logrepl.requireExistingSlot`: a pipeline with a
>     position whose slot was dropped fails `Open` with
>     `postgres.logrepl.slot_missing`; with an empty position the resume
>     point is Key zero, `MessageLSN = 0`, no `Partial` (B2), checked with a
>     non-transactional message at the slot's start;
>   - **(4th pass, S14, Debezium-compatible output mode on)** the last offset
>     is a non-transactional message at m = Lc: 0 duplicates, the message is
>     not re-delivered; a transactional message written immediately before
>     O's change: the duplicate set is T's changes at L plus that message,
>     and equals the `--peek` prediction; a transactional message earlier in
>     T's prefix is skipped.
> - **Removed:** every assertion about the n-based skip (B2).
> - **Q5 (deferred key, 3rd pass):** the test asserts that the logged set
>   covers Conduit ∩ Debezium. The harness runs at DEBUG, and every
>   cross-system duplicate must appear in the logged `(collection, key)`
>   list, or under the logged xid for the "T in full" cases. The INFO count
>   must equal |Conduit ∩ Debezium|. The INFO line must say "empty" exactly
>   in the zero-duplicate cases. There is no metadata assertion.

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

## Pre-merge gate for the implementation PRs (final review)

The final review of the scoped design listed what must be verified before any
implementation PR for this design merges. Each item becomes a harness or
probe result linked from that PR; an unverified item blocks the merge of the
slice it belongs to.

The final review's list, items 1–10, followed by the items the author lane
had that it does not cover (11–14):

1. Aurora, RDS, Cloud SQL and Azure accept `CREATE_REPLICATION_SLOT …
   TEMPORARY PHYSICAL` on a replication connection with the `-c tcp_*`
   startup options, and drop it on disconnect (S15).
2. On PG 14–18, one session can hold both locks and also create the slot.
   Only PG 17 has been probed (S15).
3. Debezium `config/validate` while the slot is active: does it error, and
   does it open a JDBC connection only? (R6, S12)
4. Does Debezium send a status update carrying its offset LSN when it
   attaches? If it does, normal restarts give a false
   `continuity_unproven` (R8, S15).
5. Debezium 3.4+: the effective default of `offset.mismatch.strategy`. If it
   is not "unset", the refusal fires on every cutover (S5 item 9).
6. Kafka 3.5+: `PUT /resume` on a STOPPED connector resumes from the stored
   offsets, and a STOPPED status in `/status` means the final offset commit
   has happened (S9, S12).
7. The check role has `pg_read_all_stats` on each managed platform (S12,
   "Privileges").
8. Fixture capture of `lsn_commit` and `lsn` semantics for each Debezium
   version, as this design already plans (Tests).
9. How the CLI picks the pipeline id, and the delete sequence (connectors and
   processors first) (S16).
10. The `0x38` strictly-above result on PG 14–16 (S15).
11. The runbook, executed end to end in the `test/debezium` harness for every
    case in S12's Tests list, on Kafka 3.5+ and on an older worker.
12. Debezium's WAL position search followed by `reconnect()` under a new
    walsender, per Debezium version, which the equality rule is built for
    (S15).
13. Workers older than Kafka 3.5: after the stop-equivalent `DELETE`,
    `/status` returns 404 only once the task has stopped and committed its
    final offset (S9, S12).
14. The pgjdbc version Debezium ships, per Debezium version, and whether its
    `automaticFlush` flushes received LSNs to the slot on its own (Debezium
    3.1 and earlier with a newer pgjdbc), which makes F_stop > Lc and fails
    every rollback closed (S12, "When F_stop > Lc").

## Implementation slices

Each slice is its own PR. All are Tier 1 except slice 5.

1. `source/dbzoffset`: the parser, the fixtures, fuzzing, and the
   resume-point computation. No data-path wiring yet.
2. Import mode: the setting, validation, preflight at `Open`, `Resume`
   override with `Partial`, the flush ceiling, and the error codes. Unit and
   property tests, plus chaos kill cases.
3. `test/debezium` harness and CI (PR and nightly matrices).
4. `conduit`: `migrate kafka-connect --import-offset` (REST, topic, file),
   `--dry-run`/`--peek`, `--cutover`, `--check-rollback`. This depends on #3003's
   command skeleton.
5. Docs: the runbook `docs/operations/postgres-debezium-offset-import.md` in
   `conduit` (symptom → diagnosis → fix for every code, the slot-held
   warning, the overlap WAL growth, and the manual reset and rollback drafted
   in #3029), the behaviour-change release note for
   `logrepl.requireExistingSlot` (S13), the conduit.io migration page,
   `llms.txt`, and the changelog.

## Open questions for DeVaris

> **Resolved 2026-10-12.** See Decisions. Q5: the key is deferred. The
> questions are kept below as they were asked.

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
> - Slice 2 includes the Q5 logging (a count at INFO, keys at DEBUG), the B1,
>   S3, S4 and S10 preflights, and the `slot_reuse_unsupported` refusal. The
>   metadata key is **not** in any slice; it is future work (Q5 deferred).
> - Slice 4 ships:
>   - `--slot-strategy fresh` (with `reuse` refused) and `--emit-cutover-marker`;
>   - the cutover journal (S9), and the read-only `--check-rollback` with the
>     manual runbook (S12; automated reset and rollback are #3044, v0.22);
>   - the identity proof and the `--without-import` fallback (S13);
>   - starting the pipeline by default (S11).
> - Reuse (D3 "Reuse", the D9 write-back per S7) is a separate v0.22 slice,
>   gated per Debezium version on its rollback test.

## Related

- ConduitIO/conduit#3004 (this lane), #3002 (v0.21 tracking), #3003
  (migrate/kc CLI), #3010 (migration harness), #3013 (connector
  observability), #3044 (v0.22: automated reset and rollback).
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

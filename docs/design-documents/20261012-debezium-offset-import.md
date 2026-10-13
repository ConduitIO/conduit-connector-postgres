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
| Q4 | **v0.21 ships the fresh-slot strategy only.** Reusing Debezium's slot moves to v0.22, gated per Debezium version on a passing rollback test. | The CLI flag stays `--slot-strategy fresh\|reuse`, as in D3 and D10 and in the migrate design (#3026). `fresh` is the default and the only strategy v0.21 executes. `--slot-strategy reuse` is accepted by the parser and refused in v0.21 with `postgres.import.slot_reuse_unsupported`. In v0.21, pointing `logrepl.slotName` at Debezium's slot is refused with `postgres.import.slot_reuse_unsupported`. The reuse-only parts are v0.22: D3 "Reuse", the reuse column of D9 (redesigned per S7), the `autocleanup_on_reused_slot` code, and the reuse test cases. The n-based skip in D4 is dropped outright (B2). |
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

**Second pass (delta review, 2026-10-12).** The delta review confirmed B1–B3
as resolved. It found two new loss paths that the first amendments
introduced: S9 re-runs, and rollback. It also raised eight should-fixes and a
set of nits. Because nothing here has merged, these are fixed **in place**:

- S2, S5, S8, S9 and S11 are rewritten.
- S12 (rollback) and S13 (offset identity) are new.
- D2's JSON, D4's partial rule, D6, D7, D9, D10's JSON and flags, the D11
  code table, the Duplicate bound table and the Failure modes rows are edited
  directly.

No stale row is left for an implementer to copy. The second-pass items are
tagged **(2nd pass)**.

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
- **New cut point (2nd pass, restated).** A transaction straddles slot
  creation:
  1. It is opened, and writes rows, before the CLI issues
     `CREATE_REPLICATION_SLOT`.
  2. Creation blocks until it ends. The test asserts the block (S9's
     timeout diagnostic names the pid).
  3. It commits after Lc.
  4. Debezium is stopped after that.

  The diff shows zero loss. On a patched server, the slot is created after
  the transaction ends, Debezium delivers it, and Lc covers it. On an
  unpatched image, the same case is refused with `server_unpatched` before
  any slot exists.

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
A transactional message keeps the earlier commit's `lsn_commit`, which is
below its `lsn`. The connector classifies `MESSAGE` with `lsn == lsn_commit`
as commit-class: the partial rule is not armed, and there are 0 duplicates.
It classifies `MESSAGE` with `lsn > lsn_commit` as a transactional message
(see the S2 table).

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
  Requiring it makes xid wraparound irrelevant.
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
| DML: a change record, or a heartbeat emitted mid-transaction | M ∈ INSERT/UPDATE/DELETE/TRUNCATE | T's changes at LSN L that Debezium delivered: 1 for an ordinary row, ≤ 1000 for one multi-insert record, one per relation for a multi-table TRUNCATE. Transactions with another xid that arrive first are not duplicates: Debezium never saw them. |
| DML, and T never arrives on this slot | the same offset; T touched only tables outside the pipeline, or PG 15+ skipped it as empty | 0 |
| Transactional MESSAGE, or M absent | M = MESSAGE with `lsn > lsn_commit`, or no messageType | T's prefix that Debezium delivered. T is identified by X (and `ts_usec`) for logging; nothing is skipped. |
| Debezium stopped ungracefully | — | refused by S13's identity re-check or by `slot_ahead`. Otherwise, the records produced after O, up to one `offset.flush.interval.ms` of traffic. |

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
  followed by a later record or heartbeat. Enable `heartbeat.interval.ms`, or
  write two separate transactions to a captured table. Wait for an offset
  flush (`offset.flush.interval.ms`), then retry."_ (D7 is edited in place.)
- **The Q3 runbook step, before the stop,** has the same correction. Moving
  Debezium past C needs two separate transactions to a captured table, or
  heartbeats.
- **After the stop (2nd pass),** waiting for the import to complete needs one
  committed write to a **pipeline** table, and no re-run (S11). Conduit needs
  one acked record, not a Debezium offset.
- **`offset_unsupported`.** The fix text drops the misleading "Debezium
  ≥ 1.9" pointer. It now reads: _"This offset has no `lsn_commit`. Run the
  connector on a Debezium version that writes it (every 2.x and 3.x release;
  the fixture set lists the 1.x builds that do), let it commit an offset after
  at least one transaction, then retry."_
- **D5 check order.** Steps 2–10 are offset-independent, except step 8's
  last check. **(2nd pass)** The CLI runs every offset-independent check
  **before** `PUT /connectors/{name}/stop`, against the latest committed
  offset: blocking report entries, plugin, server version, database identity,
  publication, tables, and `cdcMode`/`snapshotMode`. After the stop, only two
  checks can still fail: Lc ≥ C, and the S13 identity re-check. The
  connector at `Open` runs the full list again.
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

### S9. Crash-safe cutover, re-runs and `--reset` (rewritten, 2nd pass)

**The journal is state, so it obeys invariant 5.** It is written to a
temporary file, fsynced, and renamed over `<pipeline>.cutover.json`. The
directory can be moved with `--journal-dir`. It records:

- Conduit's slot name, C, and the publication;
- S13's Debezium-slot identity proof;
- the final O;
- the saved Debezium config;
- the current step.

The saved Debezium config is written **before** the CLI calls
`PUT /connectors/{name}/stop` (or the delete, on old workers). A crash
between the two can therefore never lose the only copy.

**Steps,** in order:

| Step | Name |
| --- | --- |
| 1 | `preflight` (offset-independent checks, S5) |
| 2 | `publication` |
| 3 | `slot_created` (C recorded) |
| 4 | `waiting` (Debezium's committed Lc ≥ C and S13's slot proof) |
| 5 | `config_saved` |
| 6 | `stopping` |
| 7 | `stopped` (final O read and re-checked) |
| 8 | `pipeline_written` |
| 9 | `started` |
| 10 | `complete` |

**Re-run rules (step-aware).** Let F_c be Conduit's slot's
`confirmed_flush_lsn`. On a re-run after a kill, it must hold:

| Journal step | Pipeline | Allowed state of F_c | Otherwise |
| --- | --- | --- | --- |
| up to and including 6 (Debezium may still be running) | not written, or written with an empty position and not running | F_c = C. Nothing has consumed the slot. | `migrate.kafka_connect.slot_advanced` |
| 7 or 8 | written, never started | F_c = C | `slot_advanced` |
| 9 (started), empty position | any | F_c ≤ Lc (the D6 cap) | `slot_advanced` |
| any step, pipeline has a non-empty position | — | any F_c. The pipeline is the authority, and the re-run only resumes waiting for the durable signal (S8). | — |

**The journal's slot is active.**

- If the pipeline is running (per the Conduit API) and holds the slot (its
  pid in `pg_stat_replication` belongs to the Conduit instance's replication
  connection), the re-run attaches and waits.
- Otherwise it refuses with `migrate.kafka_connect.slot_in_use`. The message
  shows the holder's pid, `application_name` and `client_addr`, and says to
  stop that consumer before re-running.

**`--reset` (start over).**

- It **refuses** with `migrate.kafka_connect.reset_unsafe` while the pipeline
  exists with a non-empty position, or is running. Dropping the slot then
  would lose everything from the pipeline's position K up to the new slot's
  C2. Past that point the only ways out are rollback (S12) or letting the
  pipeline continue.
- Otherwise it:
  1. deletes the written pipeline, if any;
  2. drops Conduit's slot;
  3. restarts Debezium from the journal's saved config, if the journal says
     it was stopped or deleted, and waits until it is RUNNING;
  4. deletes the journal.

  The user then runs `--cutover` again from step 1.

**A slot with Conduit's name but no journal** is refused with
`migrate.kafka_connect.slot_unowned`. Fix text: _"No cutover journal was found
for this slot. If this run is on an ephemeral runner, restore the journal from
the earlier run's `--journal-dir` and re-run. If the journal is lost and the
pipeline does not exist or has no position, drop the slot after confirming
nothing consumes it (`SELECT pg_drop_replication_slot('<name>')`), restart
Debezium if you stopped it, and run `--cutover` again. Use a persistent
`--journal-dir` for cutovers run from CI."_

**Slot creation.** `CREATE_REPLICATION_SLOT` waits for every transaction
running at creation to finish, and prepared transactions block it until they
are resolved.

- `--slot-create-timeout` (default 5m) bounds the wait.
- On timeout, the CLI cancels, drops any partial slot, and fails with
  `migrate.kafka_connect.slot_create_blocked`.
- The diagnostic lists the oldest `pg_stat_activity` transactions (pid,
  `xact_start`, query) and `pg_prepared_xacts`.

**Cut points.**

- Debezium's graceful stop runs past `task.shutdown.graceful.timeout.ms`, so
  the task is cancelled without its final offset commit. Expected: a
  `slot_ahead` refusal, or an S13 re-check failure, or an import with no
  loss. Never loss.
- The CLI is killed at every journal step, then re-run.
- `--reset` is attempted with a running pipeline and with a positioned one
  (refused), and with an empty one (Debezium restarted).

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
- The CLI prints `conduit migrate kafka-connect rollback --pipeline <name>` as
  the way back. The way forward is to fix the cause, then re-run `--cutover`,
  which S9 allows: the pipeline still has an empty position.
- It exits with `migrate.kafka_connect.start_failed`, with the connector's
  own code as the cause when there is one.

**If the durable signal does not arrive by `--start-timeout`** (an idle
database), the pipeline is healthy and is left **running**. Stopping it would
not release anything, because the cap holds either way. The CLI exits with
`migrate.kafka_connect.import_pending`, and the fix text reads: _"Commit one
write to any pipeline table. The pipeline completes the import by itself. A
re-run of `--cutover` is not needed; it would only resume waiting. Do not use
`--reset`."_ Rollback, if chosen instead, stops the pipeline first (S12).

### S12. Rollback (fresh strategy, v0.21) (new, 2nd pass)

Rollback must never drop the only slot that still holds WAL, and must never
let two writers run at once. Debezium's stopped slot is the one most likely to
have been invalidated under `max_slot_wal_keep_size`.
`conduit migrate kafka-connect rollback --pipeline <name>` does this:

1. **Check Debezium's slot.** It must exist on `current_database()`, with
   `wal_status` not `lost` and no `invalidation_reason` (PG 17+). Otherwise
   rollback refuses with `migrate.kafka_connect.rollback_slot_lost`. Fix text:
   _"Debezium's slot no longer has the WAL since the cutover. Conduit's
   pipeline is the only complete consumer: keep it running. Returning to
   Debezium now requires a Debezium re-snapshot."_ Nothing is changed.
2. **Stop the Conduit pipeline** gracefully, and wait until it is stopped.
   Record K, its last persisted position, and Kc, its commit LSN.
3. **Restart Debezium** from the journal's saved config: resume a STOPPED
   connector, or re-create a deleted one. Wait until the connector and its
   task are RUNNING. Debezium resumes from O on its own slot.
4. **Wait** until Debezium's committed Lc (REST or topic) is greater than Kc.
   That means Debezium has processed the commit of K's transaction, or a
   later one. The wait is bounded by `--rollback-timeout` (default 30m). On
   timeout, the CLI exits with `migrate.kafka_connect.rollback_pending` and
   leaves **both** slots in place. The fix text says to commit two separate
   transactions to a captured table, and re-run `rollback`, which resumes at
   this step.
5. Only then **drop Conduit's slot and publication** and delete the
   pipeline.

The journal records each step, written and renamed as in S9. Duplicates: in
Kafka, Debezium re-delivers everything since the cutover. Loss: none. Every
change is either in Conduit's output up to K, or still retained by Debezium's
slot, whose validity step 1 checked.

Rollback before the pipeline has ever started is the same procedure. Step 2
is then a no-op, and step 4 is skipped, because K is empty.

### S13. Proving the offset belongs to this server (new, 2nd pass)

Lc comes from Kafka Connect. The slot that proves it is on Postgres. Linking
the two is what makes the import safe:

- **During step 4,** the CLI requires Debezium's slot on **this** server
  (`pg_replication_slots`, `slot_name = debeziumSlotName`,
  `database = current_database()`) to reach `confirmed_flush_lsn ≥ C`. That
  is in addition to the committed Lc reaching C. It records the observed
  value in the journal and in the setting (`identity.debeziumSlotFlushAtWait`).
  An offset from another server, a clone or a promoted standby cannot move
  this slot.
- **After the stop,** it re-checks Debezium's slot: its F must equal the final
  Lc, or lie in [C, Lc]. F above Lc means Debezium flushed past its committed
  offset, an ungraceful stop; it is refused with `slot_ahead` semantics, as
  `migrate.kafka_connect.identity_mismatch`. F below C contradicts the step 4
  proof, and is refused the same way.
- **At `Open`,** the connector repeats both checks against the setting's
  `identity` block before it streams. They fail with
  `postgres.import.database_mismatch`.

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
   captured table, or to enable heartbeats (S5). Every offset-independent
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
guarantees F ≤ Lc at start.

**(2nd pass, corrected.)** Earlier text said a report below F is harmless
because Postgres never moves `confirmed_flush_lsn` backwards. That is false on
older minors. The fix "Don't retreat slot's confirmed_flush LSN" landed in
14.19, 15.14, 16.10 and 17.6 (`e68459489c`, `9d1a623596`, `c0f51fde53`,
`7318f241d2`, 2025-05-19, backpatched through 13). Before those, a lower
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

**v0.21 (fresh strategy):** the procedure is S12, edited in place here. In
order:

1. Refuse if Debezium's slot is lost or invalidated.
2. Stop the Conduit pipeline.
3. Restart Debezium and wait until it is RUNNING.
4. Wait until Debezium's committed Lc passes Conduit's last persisted commit.
5. Only then drop Conduit's slot.

There is no loss. The duplicates are everything Conduit delivered since the
cutover, which Debezium re-delivers into Kafka. The window lasts as long as
Debezium's slot stays valid. It holds WAL the whole time, so size
`max_slot_wal_keep_size` for the soak period and drop Debezium's slot when the
soak ends.

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

# Without the REST API, read the topic instead (read_committed, all partitions, to the high watermark).
  --offsets-from topic --bootstrap-servers kafka:9092 [--offsets-topic connect-offsets]
# Or read a saved REST export.
  --offsets-from file --offsets-file offsets.json

# Go back.
conduit migrate kafka-connect rollback --pipeline orders --connect-url ...
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
> - `--slot-create-timeout`, `--reset` and `--journal-dir` (S9);
> - `--cutover-wait-timeout`, default 30m, which bounds waiting for Lc ≥ C;
> - `--start-timeout` (S11).
>
> It starts the pipeline by default and waits for the durable import-complete
> signal (S8, S11); `--no-start` opts out.
>
> If Debezium's committed offset is not past C by `--cutover-wait-timeout`,
> the CLI exits with `cutover_timeout`. Debezium is still running at that
> point, and the CLI prints the fix: two separate transactions to a captured
> table, or heartbeats. In the dry-run JSON, `slot.strategy` is always
> `"fresh"` in v0.21.

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
             "duplicate_bound": "changes of xid 555 at LSN 0/16E8F98 (<= 1000 rows, or one per relation for a TRUNCATE)",
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
| `postgres.import.slot_missing` | Conduit's slot does not exist | Redo the cutover with `conduit migrate kafka-connect --cutover --reset`, which is allowed only while the pipeline has no position (S9). Import mode never creates a slot. |
| `postgres.import.slot_plugin` | Conduit's slot is not `pgoutput` | Redo the cutover with `--reset` (S9). |
| `postgres.import.slot_active` | the slot is in use | Stop the other consumer. The message shows its pid and `application_name`. |
| `postgres.import.slot_invalidated` | `wal_status = lost` or `invalidation_reason` is set | The WAL is gone. Re-snapshot. |
| `postgres.import.slot_ahead` | F > Lc | See D5 check 5. Redo the cutover with `--reset` if the pipeline has no position, otherwise re-snapshot. |
| `postgres.import.slot_database` | the slot is on another database | Fix `url`. |
| `postgres.import.offset_from_future` | Lc > `pg_current_wal_lsn()`, or the server is a standby | Wrong server, or one restored from backup. |
| `postgres.import.database_mismatch` | `database`, `systemIdentifier` or the S13 slot proof does not match this server | Point `url` at the database Debezium read from; re-export the offset. |
| `postgres.import.plugin_unsupported` | `plugin` absent, `wal2json`, `wal2json_streaming`, or unknown | Not importable. Re-snapshot or dual-run. |
| `postgres.import.server_unpatched` | below 14.13, 15.8, 16.4 or 17.0 (B1) | Upgrade to the listed minor or later. |
| `postgres.import.server_unsupported` | Postgres 13 or earlier (a policy choice, B1) | Upgrade to 14.13 or later. |
| `postgres.import.publication_mismatch` | the publication is missing or does not cover the pipeline's tables | Repair the publication, then redo the cutover with a new fresh slot created after the repair (`--cutover --reset`, S9). Never add tables to a publication that an existing slot will decode from: pgoutput reads the historic catalog and silently skips their earlier WAL. |
| `postgres.import.tables_not_captured` | the pipeline has tables Debezium did not capture | Remove them and add them in a separate pipeline with a snapshot. |

> **Amended 2026-10-12 (2nd pass).** The table above is edited in place.
> `autocleanup_on_reused_slot` is removed, and will come back with v0.22
> reuse.

CLI codes (`migrate.kafka_connect.*`, mapped to exit codes per the
deterministic exit code ADR):

- `offset_not_found` – no offset for this connector.
- `ambiguous_partition` – more than one partition.
- `connector_running` – the connector is RUNNING or PAUSED.
- `offsets_unreachable` – neither REST nor the topic could be read.
- `cutover_timeout` – Lc (or Debezium's slot, S13) did not reach C within
  `--cutover-wait-timeout`.
- `cutover_marker_unsupported` – Q3.
- `identity_mismatch` – S13.
- `lossy_source_config` – a warning (S2, S6).
- `slot_advanced`, `slot_in_use`, `slot_unowned`, `slot_create_blocked`,
  `reset_unsafe` – S9.
- `start_failed`, `import_pending` – S11.
- `rollback_slot_lost`, `rollback_pending` – S12.

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
| M is DML (including a mid-transaction heartbeat), and T arrives | T's changes at LSN L: 1 for an ordinary row, ≤ 1000 for one multi-insert record, one per relation for a multi-table TRUNCATE |
| M is DML, and T never arrives on this slot | 0 |
| M = MESSAGE with `lsn > lsn_commit`, or M absent | T's prefix that Debezium delivered |
| A crash after the first ack, before T is streamed | T's prefix (the partial rule is not restored, S2) |
| Debezium stopped ungracefully | usually refused (S13, `slot_ahead`); otherwise up to one `offset.flush.interval.ms` of traffic |

Every case is exercised by the harness. With `--peek`, the dry run predicts the
exact count, and the harness asserts equality, not just the bound.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Debezium still running at import (fresh) | `--cutover` refuses `connector_running` without `--stop-connector`. At `Open`, Debezium's slot must be inactive and satisfy S13. | 3 |
| Debezium stopped ungracefully, F > Lc | `slot_ahead` refusal | 3 |
| Debezium's slot dropped on stop (`slot.drop.on.stop=true`) | Fresh mode does not need it to stream, but S13 needs it as proof, so `database_mismatch` is raised. Fix: set `slot.drop.on.stop=false` and redo the cutover. Import mode never creates a slot. | 3 |
| CLI killed mid-cutover, then re-run | The step-aware rules of S9. `--reset` refuses once the pipeline has a position. | 3, 5 |
| Rollback with Debezium's slot invalidated | `rollback_slot_lost`, nothing changed (S12) | 3 |
| Rollback before Debezium catches up | `rollback_pending`, both slots kept (S12) | 3 |
| Fresh slot created after O (C > Lc) | `slot_ahead` refusal; the CLI also refuses before stopping Debezium | 3 |
| Slot invalidated (`max_slot_wal_keep_size`) | `slot_invalidated` | 3 |
| Conduit killed before the first ack | Re-import gives the same resume point; the cap keeps F ≤ Lc | 1, 2, 3 |
| Conduit killed after the first ack | Version 2 exact resume | 2, 3 |
| Pipeline reset months later with the setting still present | F ≫ Lc, so `slot_ahead`; the operator removes the setting | 3 |
| Idle database after import | Slot held at Lc until the first durable ack (D6). `--cutover` exits with `import_pending` and leaves the pipeline running (S11); runbook (S8). | — |
| Snapshot-state offset | Coded refusal (D7) | 3 |
| A transaction ≥ Lc other than X arrives first | Delivered in full. The rule stays armed for X (S2). | 3 |
| Table set mismatch | `tables_not_captured` / `publication_mismatch` | 3 |
| Offset from another server, a clone or a promoted standby | S13: Debezium's slot on this server must have reached C during the wait, and must match Lc after the stop. Otherwise `identity_mismatch` / `database_mismatch`. | 3 |
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
- INFO from inside the first `Ack` (which runs only after the position is
  durable): `import complete at <key>; remove logrepl.import.debeziumOffset`.
- WARN every minute while the D6 cap holds the slot.
- The duplicate set: see the Q5 block below. No metadata key is committed to
  until DeVaris decides Q5.
- The connector has no metrics of its own today. Lane #3013 owns connector
  metrics, and an `import_pending` gauge is requested there.

> **Amended 2026-10-12 (Q5: approved, the reviewer recommends deferral,
> awaiting DeVaris).** The design has two options. Implementation slice 2
> ships option A until DeVaris decides.
>
> **Option A (defer the key; the reviewer's recommendation; 2nd pass).**
> Nothing is added to records.
>
> - **At `Open`,** an INFO line states the expected set from the S2 table: "xid
>   X, changes at LSN L", "xid X, all changes (T in full)", or "empty".
> - **When the set closes,** a second INFO line is emitted. The set closes
>   when T's last change at L has been emitted, or T's commit is reached, or
>   the case is one where the set is empty. The line is emitted at close, not
>   at the first ack.
>   - For the bounded set (changes at L: ≤ 1000 rows, or one per relation),
>     the line lists every record as `(collection, key)`, plus its change key
>     `(T.commit, seq)`.
>   - For the "T in full" cases, it gives the xid, T's commit LSN and the
>     record count. It says plainly that sinks cannot deduplicate this set by
>     key, and that consumers which must not see duplicates should compare
>     against Debezium's topic for that xid.
>   - It is not a public contract.
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
>   is identified by X; nothing when the set is empty (M = COMMIT, a
>   non-transactional MESSAGE, BEGIN, or T never arriving).
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
>   - a duplicate-set assertion, per the Q5 option in force (see below).
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
>     table, including `--reset` refusals (S9);
>   - rollback with Debezium's slot invalidated (refused), rollback before
>     Debezium catches up (`rollback_pending`, both slots kept), and rollback
>     after Conduit has run (no loss; Conduit's slot dropped only after
>     Lc > Kc) (S12);
>   - a pipeline-side row filter or `publish` flag difference, so that a
>     transaction Q that Debezium never saw arrives before T. The test
>     asserts zero loss, and that T's prefix is still skipped (S2);
>   - an idle database after the stop: `import_pending`, then one write to a
>     pipeline table completes the import without a re-run (S11);
>   - a slot creation blocked by a prepared transaction (S9);
>   - an offset from another database or cluster (S3).
> - **Removed:** every assertion about the n-based skip (B2).
> - **Q5, option A (2nd pass):** the `possibleDuplicate` assertion is
>   replaced. The test asserts that the logged set covers Conduit ∩ Debezium:
>   every cross-system duplicate appears in the close-time INFO line (by
>   `(collection, key)`, or by xid for the "T in full" cases). It also asserts
>   that the line says "empty" exactly in the zero-duplicate cases. The
>   metadata assertion returns only if DeVaris picks option B.

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
> - Slice 4 ships `--slot-strategy fresh` (with `reuse` refused), `--emit-cutover-marker`, the cutover
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

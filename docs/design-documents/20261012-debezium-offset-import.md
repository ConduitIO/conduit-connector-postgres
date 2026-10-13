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
| Q5 | **Defer the `possibleDuplicate` metadata key** (DeVaris, 2026-10-12, after the review recommended deferral). | v0.21 ships logging only: a count at INFO, and the records' keys at DEBUG (see Observability). No public metadata key is added. The key is kept as a future-work note. |
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

**Third pass (round-3 review, 2026-10-12).** The round-2 fixes held. But the
new `--reset` brought back "drop the only slot holding WAL". The sequence:

1. Debezium runs with `slot.drop.on.stop=true`, and its slot is dropped at
   the stop.
2. The post-stop identity check fails.
3. The fix said to use `--reset`, which dropped Conduit's slot.
4. Debezium restarted with a new slot at the current WAL, and resumed from O.

Everything between the stop and the restart was lost. Fixed in place:

- `--reset` (S9) checks Debezium's slot is valid, refuses on the slot
  position or completion rather than "the pipeline has a position", and drops
  Conduit's slot only after proof that Debezium is streaming.
- `slot.drop.on.stop=true` and the lsn-flush settings are refused before the
  stop (S5).
- After a post-stop identity failure, the fallback is to run without the
  import (S13).
- The journal gains the `creating_slot` and `starting` steps.
- Rollback's binding condition is now based on the two slots' positions
  (S12).
- The standby check runs before slot creation, and the saved Debezium config
  is handled securely.
- Nits. Q5 is decided: defer the key.

Third-pass items are tagged **(3rd pass)**.

**Fourth pass (round-4 review, 2026-10-12).** `--reset` and rollback still
assumed that a restarted Debezium resumes from Lc. Postgres starts it at
max(requested, F_d), where F_d is its slot's `confirmed_flush_lsn`. F_d > Lc
was an accepted state (`identity_ahead` was only a warning), so both flows
could drop the only copy of (Lc, F_d]. That happens two ways: the slot is
recreated at the current WAL after an `identity_mismatch`, or
`lsn.flush.mode=connector_and_driver` flushes keepalive LSNs past Lc. Fixed in
place:

- The journal records F_stop, Debezium's F after the stop (S9).
- `--reset` and rollback check Debezium's slot against
  max(final Lc, F_stop) before they restart Debezium, refuse `--reset` when
  F_stop > Lc, and make rollback wait for Conduit's durable F_c ≥ F_stop
  (S9, S12).
- `connector_and_driver` is refused before the stop, in all three PRs (S5).
- `--without-import` has preconditions, a require-existing-slot connector
  mode, and a corrected duplicate bound (S13).
- `--reset` refuses a non-empty pipeline position. The re-run table has a row
  for it (S9).
- `config_masked` refuses only literal masks, and `--debezium-secret` takes
  `key=env:VAR` (S9).
- The saved config is re-validated before every Debezium restart (S5, S9,
  S12).
- The CLI writes `logrepl.autoCleanup: false`. Found while checking connector
  `main` for this pass: deleting a pipeline whose source has run calls
  `LifecycleOnDeleted` (`conduit` `pkg/connector/source.go`), which drops the
  slot and publication when `logrepl.autoCleanup` is true, the default
  (`source.go` `LifecycleOnDeleted`, `source/config.go`). Old `--reset` step 3
  deleted the pipeline before Debezium was proven to be streaming, so it could
  drop Conduit's slot early.
- Slot adoption at `creating_slot` checks plugin, `two_phase`, `temporary` and
  database, and handles an orphaned walsender (S9).
- Idle-database fix texts name `heartbeat.action.query` (S5).
- Stale `slot_ahead` text and the D9 rollback test line are fixed.

Fourth-pass items are tagged **(4th pass)**.

**Fifth pass (round-5 review, 2026-10-12).** Fixed in place, tagged **(5th
pass)**:

- B1: every command finishes the `stopping` observation (O, F_stop, R_stop)
  before any Debezium restart; `--reset` and rollback never re-issue the stop,
  and rollback defines what happens without F_stop (S9, S12).
- B2: the `--without-import` path builds Key zero, `MessageLSN = 0` and no
  `Partial`; `MessageLSN = Lc` is for a real import only (S13, S14).
- An attach proof (first active sighting at or below the bound, then the same
  walsender pid until the drop) carries slot continuity from the restart to
  the drop (S15).
- A per-slot lock makes the commands mutually exclusive
  (`operation_in_progress`, S15). This pass used an advisory lock; the 6th
  replaced it with a temporary replication slot.
- How the CLI writes, stops and deletes API- and file-provisioned pipelines,
  the delete-then-drop order, `LastActiveConfig`, and the permanent
  `autoCleanup: false` (S16).
- The marker prefix is excluded from the pipeline's messages, and the
  marker's proof is stated correctly (S15).
- Rollback validates the resolved config with Kafka Connect, names
  `--without-import` as the way out of the F_stop deadlock, and qualifies
  "no loss" for tables outside the pipeline (S12).
- S14 follows #3033 (`tx_seq_m`, keyless first acks); pinned at the
  signed-off `53b767d1` in the 7th pass.

**Sixth pass (round-6 review, 2026-10-12).** Fixed in place, tagged **(6th
pass)**:

- B1: the attach proof follows `confirmed_flush_lsn` across walsender pids,
  because Debezium reconnects under a new pid on every normal restart; a
  re-run closes a pending operation once the operator has dropped Conduit's
  slot by hand, without the CLI dropping any slot (S15).
- The lock is a temporary physical replication slot, not an advisory lock,
  and is re-verified before every mutating step (S15).
- `--pipelines-path`: deletes need an engine restart, and the file's status
  is specified step by step (S16).
- Non-import slot preconditions use `postgres.logrepl.*` with fix texts, and
  the `requireExistingSlot` claim is narrowed to a missing slot (S13).
- `--reset` at `stopping` calls resume before treating Debezium as never
  stopped (S9).
- A re-run row for a degraded pipeline, rollback with Conduit's slot missing,
  and a pending-DeVaris proposal to refuse slot creation for any non-empty
  position.

**Seventh pass (round-7 review, partial, 2026-10-12).** The round-7 review
found two blockers in the automated `--reset` and rollback attach-proof
machinery (a re-run after the restart has no specified proof state; `--reset`
at step 7 with Debezium RUNNING). Whether automated `--reset` and rollback
stay in v0.21 or become a manual runbook is with DeVaris, so those items, and
the related should-fixes, are **not** addressed in this pass. Fixed in place,
tagged **(7th pass)**:

- The lock: server-side keepalives, a stale-holder fix text, a fixed-length
  hashed name, a second lock on Debezium's slot taken in a fixed order, a
  headroom preflight, fail-closed create errors, pgbouncer ≥ 1.23, and
  managed platforms in the test plan (S15).
- The attach proof reads the two views twice and treats a disagreement as a
  change; the WAL position is diagnostic only; equality is a proof because a
  recreated slot starts strictly above the insert position (S15).
- Debezium 3.4 `offset.mismatch.strategy` and 2.7
  `slot.seek.to.known.offset.on.start` are refused when they move the slot or
  the offset at start (S5 item 9).
- #3033 pinned at the signed-off `53b767d1`; the `slotOnly` branches are
  gone; Kc for a keyless checkpoint; S10 corrected for `cdcMode: auto`.
- Default-refuse for slot creation on a non-empty position, as a tri-state
  `requireExistingSlot`, **recommended, pending DeVaris** (S13).

**Decisions recorded (DeVaris, 2026-10-12, 5th pass).**

- The residual window after the restart: accepted with a runbook warning only
  if no cheap proof existed. One exists, so the attach proof is implemented
  and the runbook warning is kept (S15).
- An orphaned walsender at `creating_slot`: the CLI refuses with
  `slot_create_blocked` and prints the pid; it never terminates a backend.
  The pid and `backend_start` are journalled and printed (S9).
- `resume: slotOnly` was approved in the 4th pass. The round-5 review
  recommends replacing it with `logrepl.requireExistingSlot: true` on every
  pipeline the CLI creates. This pass adopts that, **pending DeVaris
  confirmation (supersedes the approved `slotOnly`)** (S13).

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
  (`cutover_timeout`, `reset_pending`, `rollback_pending`, D7) names
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
       slot that S13 and rollback depend on.
     - `lsn.flush.mode=manual` or `flush.lsn.source=false` is refused with
       `migrate.kafka_connect.lsn_flush_mode_unsupported`. With either, Debezium
       never moves its slot, so S13 cannot prove anything.
     - **(4th pass)** `lsn.flush.mode=connector_and_driver` is refused with
       the same code. The driver flushes keepalive LSNs, which moves Debezium's
       slot past Lc with no record behind it. A restarted Debezium then
       resumes at that F, not at Lc, so `--reset` and rollback would skip
       (Lc, F]. Refusing it before the stop removes that cause. The F_stop
       checks in S9 and S12 stay, because a recreated slot or an ungraceful
       stop can still leave Debezium's slot past Lc.
     - **(7th pass) Slot-moving startup options.** Debezium 3.4 added
       `offset.mismatch.strategy`. With `trust_offset` or `trust_greater_lsn`
       it calls `pg_replication_slot_advance` on every `startStreaming`,
       including after a `reconnect()` (`PostgresReplicationConnection`
       L738–739). With `trust_slot` it moves the offset forward to the slot.
       Debezium 2.7's `slot.seek.to.known.offset.on.start=true` also advances
       the slot at start. Positions:
       - `trust_slot` is refused: on a restart with F_d > Lc it silently moves
         Debezium past (Lc, F_d], the exact gap S9 and S12 exist to prevent.
       - `trust_offset` and `trust_greater_lsn` are refused: they move
         Debezium's slot at every start and reconnect with no flush behind
         it, which breaks the premise of S13 and of the attach proof (S15)
         that the slot moves only on a flushed, committed offset.
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
       stop, again against the config saved at step 6, and again before any
       Debezium restart in `--reset` or rollback (S9, S12). A config edited
       between preflight and the stop is caught at step 6, before the stop.
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

### S9. Crash-safe cutover, re-runs and `--reset` (rewritten, 3rd pass)

**The journal is state, so it obeys invariant 5.** It is written to a
temporary file, fsynced, and renamed over `<pipeline>.cutover.json` in
`--journal-dir`. The file mode is 0600. It records:

- Conduit's slot name, C, and the publication;
- S13's Debezium-slot proof;
- the final O, and **(4th pass)** F_stop and R_stop, the `confirmed_flush_lsn`
  and `restart_lsn` of Debezium's slot read after the stop;
- the saved Debezium config;
- the current step, and the steps of a running `--reset` or rollback.

**The saved Debezium config (3rd pass, 4th pass).**

- It is written **before** `PUT /connectors/{name}/stop` (or the delete, on
  old workers).
- It is redacted in all CLI output, logs and `--json`.
- **Masked values.** `GET /connectors/{name}/config` returns the config as
  submitted. Config-provider placeholders such as `${file:/path:key}`,
  `${env:VAR}` or `${secrets:ns/name:key}` come back as written. They are
  accepted and saved as they are, because Kafka Connect resolves them again
  when the config is resubmitted. Some platforms instead return a **literal
  mask**: `[hidden]`, or a value made only of `*` characters. The CLI detects
  those **before** the stop and refuses with
  `migrate.kafka_connect.config_masked` unless the user supplies each one as
  `--debezium-secret <key>=env:<VAR>`. The `env:` prefix keeps the syntax
  apart from Kafka Connect's own `${env:VAR}` provider. The journal stores
  only the reference. The value is resolved from the CLI's environment when
  Debezium is restarted, and a missing variable fails before anything is
  stopped (S12 step 1). **(5th pass)** The resolved value is resubmitted to
  Kafka Connect as plaintext in the connector config, and Kafka Connect then
  stores it in its config topic. To keep a provider reference, set the
  variable to the reference itself, for example
  `VAR='${vault:secret/dbz:password}'`; it is resubmitted as written. The
  CLI warns when it resubmits a value that is not a `${…}` reference.
- **Re-validation (4th pass).** The S5 item 9 checks (`slot.drop.on.stop`,
  the flush settings) run again against the saved config at step 6, and again
  immediately before any Debezium restart in `--reset` or rollback.

**Steps,** in order:

| Step | Name | Journalled |
| --- | --- | --- |
| 1 | `preflight` | the offset-independent checks of S5 have passed |
| 2 | `publication` | the publication is created or verified |
| 3 | `creating_slot` | the slot **name** only, written before `CREATE_REPLICATION_SLOT` |
| 4 | `slot_created` | C |
| 5 | `waiting` | Debezium's committed Lc ≥ C, and S13's proof |
| 6 | `config_saved` | the saved Debezium config, re-validated (S5 item 9) |
| 7 | `stopping` | before the stop call |
| 8 | `stopped` | the final O, re-checked; F_stop and R_stop (4th pass) |
| 9 | `pipeline_written` | — |
| 10 | `starting` | before the start call |
| 11 | `started` | — |
| 12 | `complete` | the durable signal (S8) was seen |

**Finishing step 7 (4th pass, tightened in the 5th).** `stopping` is
journalled before the stop call, so a kill or a timeout can leave the
journal at step 7 whatever happened to Debezium. Every command that finds
step 7 finishes the observation **before it does anything else, and before
any Debezium restart**:

- **Debezium is STOPPED or deleted:** read the committed O, and F_stop and
  R_stop from Debezium's slot, and journal step 8. `--cutover` then applies
  the step 8 rules, including S13's `identity_ahead` warning. `--reset` and
  rollback apply their own step 1 to the values just recorded (below and
  S12). If Debezium's slot is missing, F_stop does not exist: `--cutover`
  stops with `identity_mismatch`, `--reset` refuses with `reset_unsafe`, and
  rollback refuses with `rollback_slot_lost`.
- **Debezium is RUNNING:** the stop did not take effect. `--cutover` issues
  the stop call again. `--reset` and rollback **never** issue it.
  - **(6th pass)** Kafka Connect's stop is asynchronous, so RUNNING can mean
    a stop that is still pending. Before treating Debezium as never stopped,
    `--reset` calls `PUT /connectors/{name}/resume`, which overrides a pending
    stop, and waits for RUNNING again. It then proceeds as for journal steps 1
    to 6 (below), with the attach proof applied from the resume call and the
    bound `reset_target`: if the stop had already taken effect and the task
    restarts on the resume, its walsender changes, and S15's rules decide.
    Debezium flushes only committed offsets (S5 item 9), so a continuous slot
    is at or below `reset_target` at the first sighting.
  - Rollback refuses with `connector_running` and the fix "Debezium was never
    stopped; use `--reset`".

So F_stop is never missing when a restart happens: either step 8 was
journalled, or the observation above wrote it, or the command refused.

**Re-run rules (step-aware).** Let F_c be the `confirmed_flush_lsn` of
Conduit's slot. On a re-run of `--cutover` after a kill:

| Journal step | Required | Otherwise |
| --- | --- | --- |
| 3 `creating_slot` | Debezium is still running here. See "Adopting a slot at `creating_slot`" below. | — |
| 4–7 (Debezium running or stopping) | F_c = C, then finish step 7 as above | `slot_advanced` |
| 8 and later, empty pipeline position | F_c ≤ Lc. This covers `stopped`, `pipeline_written`, `starting` and `started`; the D6 cap holds it. | `slot_advanced` |
| **(4th pass)** 8 and later, non-empty position, pipeline running | The first ack has happened. The re-run waits, under `--start-timeout`, for F_c > Lc (the S8 signal), then records `complete`. This is the rule S11's false negative relies on. | `import_pending`, pipeline left running |
| **(5th pass)** 8 and later, non-empty position, pipeline stopped | The first ack has happened, so the import is done: the setting is ignored from here on (D2). If F_c > Lc, the re-run records `complete`. Otherwise it starts the pipeline and continues as in the row above. | `start_failed` |
| **(6th pass)** 8 and later, pipeline degraded (for example `postgres.logrepl.slot_missing`, `postgres.logrepl.slot_invalidated`) | Conduit's slot is gone or unusable, so the cutover cannot go forward. The re-run stops the pipeline and refuses with the connector's code. The fix text: run `rollback` (S12), which handles a missing Conduit slot; if Debezium's slot is gone too, re-snapshot. | the connector's code |
| 12 `complete` | done; nothing to do | — |

A journal that records a `--reset` or rollback in progress makes `--cutover`
refuse with `migrate.kafka_connect.operation_in_progress` (S15).

A pipeline with the target name that this cutover's journal did not write is
refused at preflight with `migrate.kafka_connect.pipeline_exists`, whatever
its position. **(4th pass)** An empty position does not make it safe to
overwrite: the CLI cannot tell what it belongs to.

**Pipelines the CLI writes** are specified in S16 (5th pass): how they are
provisioned, `logrepl.autoCleanup: false`, `logrepl.requireExistingSlot:
true`, and the order of deletes and drops.

**`slot_advanced` fix text:**

- _Before the stop:_ "Conduit's slot was consumed by something other than this
  cutover. Debezium is still running, so nothing is lost. Run `--reset`
  (below), then `--cutover` again."
- _After the stop:_ "Conduit's slot moved past Debezium's offset with no
  import recorded. Run `rollback`: Debezium's slot still holds everything
  from O."

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

**`--reset` (start over), rewritten in the 3rd, 4th and 5th passes.** It
never drops the only slot that holds WAL. It runs under the S15 lock, has its
own journal steps so a re-run resumes it, and runs step 1 on **every**
invocation, including a re-run after `reset_pending`.

1. **Check Debezium's slot.** First finish step 7 if the journal is there
   (above). The slot must exist on `current_database()`, with `wal_status`
   not `lost` and no `invalidation_reason`. Otherwise `--reset` refuses with
   `migrate.kafka_connect.reset_unsafe`, because Conduit's slot may be the
   only one holding WAL since C. The fix text names the fallback in S13.
   After the stop (journal step 8 or later), and until the reset has restarted
   Debezium itself, two more conditions apply:
   - **F_stop ≤ final Lc.** Otherwise refuse with `reset_unsafe`. A restarted
     Debezium resumes at max(Lc, F_d), so it would skip (Lc, F_stop], and
     those changes may never have reached Kafka. The fix text: _"Debezium's
     slot is ahead of its committed offset. Going back to Debezium would skip
     the changes between them. Complete the import (`--cutover`), or run
     `--cutover --without-import`."_
   - **Debezium's slot has `confirmed_flush_lsn` ≤ max(final Lc, F_stop) and
     `restart_lsn` ≤ final Lc.** A slot recreated after the stop starts with
     `confirmed_flush_lsn` at the WAL position of its creation, which is past
     the bound unless no WAL at all was written since the stop. A slot someone
     has consumed has moved past it too. Either way, refuse with
     `reset_unsafe` and the same fix text. **(5th pass, corrected)**
     Detection rests on `confirmed_flush_lsn`. `restart_lsn` is checked
     because it is cheap, but it proves nothing alone: a recreated slot can
     have `restart_lsn` equal to the old bound. The equal case (no WAL since
     the stop) loses nothing, because there is nothing between the bound and
     the new slot's start.
2. **Refuse** with `reset_unsafe` if F_c > Lc, if the pipeline's position is
   **not empty**, or if the journal step is `complete`: Conduit has taken
   over, and `rollback` is the way back. Refuse too while the pipeline is
   running; the user stops it first, or uses `rollback`. The position check is
   the one S11 relies on: once a pipeline has a position, only rollback can
   hand back to Debezium.
3. **Fix the wait target.** After the stop, the target is the journalled
   final Lc. Before the stop (journal steps 1 to 6, or step 7 with Debezium
   still RUNNING), it is the committed Lc the reset reads when it starts. That
   value is journalled as `reset_target`, so a re-run waits for the same
   target and does not chase a moving one.
4. **Restart Debezium** from the saved config, if it was stopped or deleted:
   resume a STOPPED connector, or re-create a deleted one. Immediately before
   the call, the CLI re-runs step 1, re-validates the saved config (S5 item
   9), resolves every `--debezium-secret`, and journals `reset_restarting`.
   From the call on, it runs the **attach proof** (S15). Then it waits for
   **proof that Debezium is streaming**:
   - the attach proof holds: its slot is `active`, held by the journalled pid;
   - **and** its committed Lc has moved past `reset_target`.

   RUNNING alone is not proof. The wait is bounded by `--reset-timeout`
   (default 30m). On timeout, or when the attach proof fails, both slots are
   kept and the CLI exits with `migrate.kafka_connect.reset_pending`. The fix
   text asks for two transactions on a captured table, a
   `heartbeat.action.query` that writes to a captured table, or a re-run with
   `--emit-cutover-marker`, and a re-run of `--reset`, which starts again at
   step 1. When the attach proof failed, the fix text is S15's instead.

   Before the stop (journal steps 1 to 6), Debezium was never stopped, so
   there is no restart and no attach proof: its slot must be `active`, and the
   committed Lc must pass `reset_target`. **(6th pass)** At step 7 with
   Debezium RUNNING, the attach proof does apply, from the resume call, with
   the bound `reset_target` ("Finishing step 7").
5. **Re-check, then delete and drop.** Re-run step 1's existence checks and
   the attach proof's pid check; re-check that the pipeline is stopped and its
   position is empty. Then, in this order (S16): delete the pipeline (its
   configuration and its engine state), drop Conduit's slot, drop the
   publication, and delete the journal.

**Why the step 1 bounds stop at the restart (4th pass).** Once the reset has
restarted Debezium, its slot moves past max(final Lc, F_stop) as it streams.
That is the progress step 4 waits for, so the bounds cannot be checked again
before step 5 without refusing every successful reset. Continuity from the
restart to the drop is carried by the attach proof (S15).

**`--emit-cutover-marker` with `--reset` and rollback** is specified in S15.

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
- `--reset` cases:
  - refused with a running pipeline;
  - refused with F_c > Lc;
  - refused with Debezium's slot missing or invalidated;
  - allowed with an empty pipeline, where Conduit's slot is dropped only after
    Debezium's slot is active and its Lc has moved.
  - **(4th pass)** refused with a non-empty pipeline position;
  - **(4th pass)** refused when F_stop > Lc (forced by killing the Kafka
    Connect worker after a slot flush);
  - **(4th pass)** refused when Debezium's slot was recreated after the stop
    (a manual Debezium restart with the slot dropped first), detected by
    `confirmed_flush_lsn` > max(Lc, F_stop);
  - **(4th pass)** a CLI kill at `reset_restarting`, then a re-run: step 1 runs
    again, and the reset completes without a second restart;
  - **(4th pass)** a kill after `reset_pending`, then a re-run: step 1 runs
    first;
  - **(4th pass)** the pipeline is deleted at step 5 and Conduit's slot is
    still present until the explicit drop (`logrepl.autoCleanup: false`).
- **(4th pass, changed in the 5th)** `creating_slot` adoption: an orphaned
  walsender held in `CREATE_REPLICATION_SLOT` by a prepared transaction is
  **not** terminated; the re-run refuses with `slot_create_blocked` and prints
  the pid and `backend_start`, which match the journalled ones. A slot of the
  right name with `two_phase = true` or another plugin is refused with
  `slot_unowned`.
- **(5th pass, B1)** The CLI is killed, or times out, at `stopping` after
  the stop landed, with Debezium's slot flushed past Lc (forced by killing
  the Kafka Connect worker after a slot flush). `--reset` then finishes the
  step 7 observation, journals F_stop > Lc, and refuses with `reset_unsafe`;
  it never re-issues the stop and never restarts Debezium. The same journal
  with Debezium RUNNING: `--reset` proceeds as before the stop. Rollback at
  `stopping` with Debezium RUNNING refuses with `connector_running`.
- **(5th pass, attach proof, S15)** `--reset` and rollback with the slot
  dropped by an external actor between the pre-restart check and Debezium's
  attach: the first `active` sighting shows `confirmed_flush_lsn` above the
  bound, the command exits `*_pending`, both slots are kept.
- **(6th pass, S15)** The normal restart with an `lsn_proc` offset (WAL
  search, then `reconnect()` under a new pid) is accepted on every Debezium
  version in the matrix, on a busy and on an idle database; a task restart
  with no flush in between is accepted; a flush landing between the last poll
  of the old walsender and the first of the new one (forced with
  `offset.flush.interval.ms=100`) ends in `*_pending`; a slot dropped and
  recreated during the wait ends in `*_pending`.
- **(6th pass, S15)** After `*_pending` and a manual drop of Conduit's slot, a
  re-run closes the operation: the pipeline is deleted, no slot is dropped by
  the CLI, the journal is closed, and `--cutover` no longer refuses.
- **(5th pass, changed in the 6th)** Two CLI processes on the same slot: the
  second refuses with `operation_in_progress`, directly and through a
  transaction-mode pgbouncer. Killing the first CLI's replication session
  makes it abort with `lock_unavailable` before its next mutating step.
  `--cutover` with a reset or rollback recorded in the journal refuses with
  `operation_in_progress`.
- **(6th pass)** `--reset` at `stopping` with Debezium reporting RUNNING while
  its stop is pending: the CLI calls resume first, and the attach proof runs
  with the bound `reset_target`.
- **(5th pass, S16)** For both provisioning modes: the pipeline is deleted
  before the slot is dropped; a pipeline started once with
  `logrepl.autoCleanup: true` (hand-edited) loses its slot at the delete,
  which is after every drop precondition was checked; a provisioned pipeline
  restarted by the engine during a rollback stays stopped (`status:
  stopped`).
- **(4th pass)** A pipeline with the target name, an empty position and no
  journal is refused with `pipeline_exists` and left unchanged.
- **(3rd pass) The round-3 loss sequence:**
  1. Debezium runs with `slot.drop.on.stop=true`. The test asserts the
     pre-stop refusal.
  2. A forced variant flips the setting after preflight but before step 6.
     **(4th pass)** The test asserts the refusal at step 6, against the saved
     config.
  3. A forced variant flips it after step 6. The test then asserts:
     - the post-stop `identity_mismatch`;
     - `--reset` refused with `reset_unsafe`, both before and after a manual
       Debezium restart that recreates the slot (4th pass);
     - the S13 fallback (`--without-import`) delivers with zero loss.
- **(4th pass)** `lsn.flush.mode=connector_and_driver` is refused before the
  stop.

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
  the way back. **(5th pass)** When F_stop > Lc, rollback cannot complete
  until Conduit has durably delivered past F_stop (S12), and a pipeline that
  does not start never will. The text then names `--cutover
  --without-import` as the way out: it starts on Conduit's slot with no skip
  rule, so it gets past F_stop, after which rollback can run.
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
re-run of `--cutover` is not needed; it would only resume waiting. Do not use
`--reset`."_ Rollback, if chosen instead, stops the pipeline first (S12).

**A false negative (3rd pass).** The signal is "F_c > Lc". After the first
durable ack, F_c can stay at or below Lc for a while. The gate reports the
acked change's LSN, which can be below Lc when transactions interleave, until
everything emitted is acked and a keepalive arrives. In that window
`import_pending` can be spurious. The fix text says so: _"The import may
already be complete. A re-run of `--cutover` re-checks it and records
`complete`."_ This is safe, because nothing acts on a false negative except
the user waiting longer.

### S12. Rollback (fresh strategy, v0.21) (rewritten, 3rd, 4th and 5th passes)

Rollback must never drop the only slot that still holds WAL, and must never
let two writers run at once. Debezium's stopped slot is the one most likely to
have been invalidated under `max_slot_wal_keep_size`. A restarted Debezium
resumes at max(its offset, F_d), not at Lc **(4th pass)**, so rollback also
has to cover the changes in (Lc, F_d] that Debezium will skip.
`conduit migrate kafka-connect rollback --pipeline <name>` runs under the S15
lock and does this:

1. **Check, before anything is changed.** This step runs on every
   invocation, including a resume after `rollback_pending`.
   - **(5th pass) F_stop must exist.** If the journal is at step 7, finish
     the observation first (S9, "Finishing step 7"). If Debezium is RUNNING
     there, it was never stopped: refuse with `connector_running` and the fix
     "use `--reset`". If Debezium's slot is missing, refuse with
     `rollback_slot_lost`. Rollback never re-issues the stop.
   - **Debezium's slot** must exist on `current_database()`, with
     `wal_status` not `lost` and no `invalidation_reason` (PG 17+).
     Otherwise rollback refuses with `migrate.kafka_connect.rollback_slot_lost`
     and changes nothing. Fix text: _"Debezium's slot no longer has the WAL
     since the cutover. Conduit's pipeline is the only complete consumer:
     keep it running. Returning to Debezium now requires a Debezium
     re-snapshot."_
   - **(4th pass) Until rollback has restarted Debezium itself:** the slot
     must have `confirmed_flush_lsn` ≤ max(final Lc, F_stop) and
     `restart_lsn` ≤ max(final Lc, F_stop). A slot recreated after the stop,
     or consumed by someone else, has `confirmed_flush_lsn` past the bound,
     and is refused with `rollback_slot_lost`: the slot that held the WAL
     since O is gone. As in S9, detection rests on `confirmed_flush_lsn`.
   - **(4th pass) Conduit's durable F_c ≥ F_stop, when F_stop > final Lc.**
     The restarted Debezium will skip (Lc, F_stop], so those changes must
     already be in Conduit's durable output. F_c moves only on durable acks
     (S8), so F_c ≥ F_stop proves it. Otherwise rollback exits with
     `rollback_pending` and changes nothing. Fix text: _"Debezium's slot is
     ahead of its committed offset. Conduit must deliver up to <F_stop>
     before Debezium can take over. Keep the pipeline running and commit one
     write to a pipeline table, then re-run rollback. If the pipeline never
     started, or cannot start (`start_failed`), run `conduit migrate
     kafka-connect --cutover --without-import` first: it streams from C with
     no skip rule, so it delivers past <F_stop>."_ When F_stop ≤ Lc,
     Debezium resumes at Lc and nothing is skipped, so this condition does not
     apply. That covers rollback before the pipeline ever started, where
     F_c = C.
   - **(6th pass) Conduit's slot missing** (a degraded pipeline, S9) before
     rollback has restarted Debezium: F_c cannot be read. The CLI uses Kc,
     the commit LSN of the pipeline's persisted position K, instead: acks are
     FIFO, so every commit below Kc was durably delivered. **(7th pass)** For
     a keyless checkpoint (a non-transactional message, #3033 L699), Kc is
     the checkpoint's LSN. The F_stop check
     becomes Kc ≥ F_stop (with an empty position, it fails whenever
     F_stop > Lc). Step 4 then waits only for the attach proof and for
     Debezium's committed Lc to move, since there is no Conduit slot left to
     protect, and step 5 skips the slot drop.
   - **(4th and 5th passes) The saved config,** before anything is stopped:
     it passes the S5 item 9 checks; every `--debezium-secret` resolves from
     the environment; the Kafka Connect REST API answers; and **(5th pass)**
     `PUT /connector-plugins/{class}/config/validate` accepts the resolved
     config with no errors. So the Conduit pipeline is never stopped by a
     rollback that cannot restart Debezium.
2. **Stop the Conduit pipeline** gracefully, and wait until it is stopped.
   **(5th pass)** For a pipeline provisioned from a file, the CLI first sets
   `status: stopped` in the pipeline file, so an engine restart does not start
   it again (S16). Record K, its last persisted position; Kc, its commit LSN;
   and F_c, its slot's `confirmed_flush_lsn` after the stop. F_c only grows,
   so step 1's F_c ≥ F_stop still holds.
3. **Restart Debezium** from the journal's saved config: resume a STOPPED
   connector, or re-create a deleted one. Immediately before the call, step 1
   is re-run and `rollback_restarting` is journalled. **(5th pass)** From the
   call on, the CLI runs the attach proof (S15). Wait until the connector and
   its task are RUNNING, and its slot is `active` under the journalled pid.
4. **Wait for the binding condition:** Debezium's slot's
   `confirmed_flush_lsn` ≥ F_c, with the attach proof holding throughout.
   - Both are slot positions, compared in the same domain. F_c is an acked
     change LSN or a keepalive WAL end. Debezium's value is a commit **end**
     LSN.
   - Once Debezium's slot is at or past F_c, Debezium has processed every
     commit whose start is below F_c, from max(Lc, F_stop) on. What lies in
     (Lc, F_stop] is in Conduit's output by step 1. Everything Conduit's slot
     still retains is then retained by Debezium's slot too, or already
     delivered by Debezium.
   - Debezium's committed Lc > Kc is shown as a progress (liveness) check,
     not used as the condition. Lc lives in Kafka and Kc is a commit start,
     so they are different domains.
   - The wait is bounded by `--rollback-timeout` (default 30m). On timeout,
     or when the attach proof fails, the CLI exits with
     `migrate.kafka_connect.rollback_pending`, and leaves **both** slots in
     place. The fix text asks for two separate transactions on a captured
     table, a `heartbeat.action.query` that writes to a captured table, or a
     re-run with `--emit-cutover-marker` (S15), and a re-run of `rollback`,
     which starts again at step 1.
5. **(5th pass, reordered) Re-check, then delete and drop.** Re-run step 1's
   existence and validity checks and the attach proof's pid check, and
   re-check that the pipeline is stopped and its position is K. Then, in this
   order (S16): delete the pipeline (its configuration and its engine state),
   drop Conduit's slot, drop the publication, and delete the journal.

**Why the slot bounds stop at the restart (4th pass).** After step 3 Debezium
streams, and its slot moves past max(final Lc, F_stop): step 4 waits for
exactly that. So from `rollback_restarting` on, the step 1 slot check is
existence and validity only, and continuity rests on the attach proof (S15).
This is a deliberate departure from re-checking the bounds immediately before
the drop, which would refuse every rollback that succeeds.

**Why `restart_lsn` is bounded by max(final Lc, F_stop), not final Lc (4th
pass).** A logical slot's `restart_lsn` is never above its
`confirmed_flush_lsn`. When F_stop > Lc, a slot that was never recreated can
have `restart_lsn` above Lc, and bounding it by Lc would refuse a rollback
that is safe. `--reset` uses the tighter bound, because it already requires
F_stop ≤ Lc.

The journal records each step, written and renamed as in S9. Duplicates: in
Kafka, Debezium re-delivers everything since max(Lc, F_stop). **Loss: none
for the pipeline's tables.** Every change in them is either in Conduit's
output up to K (which covers (Lc, F_stop] by step 1), or retained by
Debezium's slot, which step 1 checked again right before the drop.
**(5th pass)** Debezium may capture tables outside the pipeline's subset (D5
check 7 allows a subset). Their changes in (Lc, F_stop] are in neither
output: Conduit never captured them, and the restarted Debezium skips them.
That is Debezium's own behaviour after an ungraceful stop, not something the
rollback introduces, and it happens only when F_stop > Lc. The rollback
prints those tables and the LSN range as a warning.

Rollback before the pipeline has ever started is the same procedure. Step 2
is a no-op, F_c is C, and step 1 refuses it only when F_stop > Lc.

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
    way back: `--reset` then refuses, and rollback must first see Conduit's
    durable F_c ≥ F_stop (S9, S12), because a restarted Debezium would skip
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

**Require-existing-slot mode (5th pass: `logrepl.requireExistingSlot`,
pending DeVaris confirmation; supersedes the approved `slotOnly`).** A
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
`"resume": "slotOnly"`, which DeVaris approved. The round-5 review
recommends a plain setting instead, and this pass adopts it, **pending
DeVaris confirmation**:

- **`logrepl.requireExistingSlot`** (bool, default `false`). When `true`,
  `Open` fails closed instead of creating anything. A missing slot fails with
  `postgres.logrepl.slot_missing`, and a missing publication with
  `postgres.logrepl.publication_missing`. The slot must also be `pgoutput`,
  on this database, and not lost or invalidated (`postgres.logrepl.slot_plugin`,
  `postgres.logrepl.slot_invalidated`). It changes nothing else: not the
  resume point, not the flush gate.
- **(6th pass) One code family for non-import slot preconditions.** The
  `postgres.logrepl.*` codes are used for every slot precondition outside the
  import, in the connector and in the CLI (the `--without-import` table
  above). The `postgres.import.slot_*` codes stay for the import path only.
  Fix texts:

  | Code | Fix |
  | --- | --- |
  | `postgres.logrepl.slot_missing` | "The replication slot `<name>` does not exist, and `logrepl.requireExistingSlot` forbids creating it: a new slot would start at the current WAL and skip every change since the old one. If the slot was dropped by mistake, the changes since the pipeline's position are not recoverable from Postgres: re-snapshot (a new pipeline with `snapshotMode: initial`), or, during a migration, use `conduit migrate kafka-connect rollback` while Debezium's slot is valid. To start fresh on purpose, set `logrepl.requireExistingSlot: false` for one start, then set it back." This is the steady-state case: a pipeline whose slot was dropped after the migration finished. |
  | `postgres.logrepl.slot_invalidated` | "The slot `<name>` lost its WAL (`wal_status = lost` or `invalidation_reason` is set, often `max_slot_wal_keep_size`). The changes it held are gone: re-snapshot, and size `max_slot_wal_keep_size` for the longest expected stall." |
  | `postgres.logrepl.slot_plugin` | "The slot `<name>` uses plugin `<plugin>`; this connector reads `pgoutput` only. Point `logrepl.slotName` at a `pgoutput` slot. A slot cannot change its plugin." |
  | `postgres.logrepl.publication_missing` | "The publication `<name>` does not exist, and `logrepl.requireExistingSlot` forbids creating it: a publication created after the slot's start makes pgoutput skip the earlier WAL of its tables. Recreate the slot and publication together (publication first), then re-snapshot, or restore the publication only if you know no WAL since the slot's start touched its tables." |
- **(6th pass) What it catches.** It catches a **missing** slot. It does not
  catch a slot that was dropped and recreated under the same name before the
  pipeline starts: that slot exists, so `Open` accepts it, and the pipeline
  resumes from the new slot's start. Detecting that needs the slot's identity,
  which Postgres does not expose; the import path is protected by its own
  checks (D5 check 5, F ≤ Lc), and a steady-state pipeline is not.
- **Default-refuse (6th pass proposal, made concrete in the 7th:
  recommended, pending DeVaris).** A position means a slot existed, and
  creating a new one is always a silent gap. So `logrepl.requireExistingSlot`
  becomes tri-state:

  | Value | Behaviour at `Open` |
  | --- | --- |
  | unset (the default) | Refuse to create a slot or publication when the position is non-empty, snapshot positions included; create them when the position is empty, as today. Fails with `postgres.logrepl.slot_missing` |
  | `true` | Always refuse to create, whatever the position (what the CLI writes) |
  | `false` (explicit) | Always allow, as today. Creating a slot for a non-empty position logs a WARN with `postgres.logrepl.slot_recreated` and increments a `slot_recreated` counter, so the gap is visible |

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
- #3033 (signed off at `53b767d1`) specifies the same resume point for the
  `logrepl.requireExistingSlot` fallback: Key zero, `MessageLSN = 0`, no
  `Partial`.

**Duplicate bound.** The connector streams from C. Duplicates are every
change Debezium produced into Kafka with commit at or after C: the records up
to the last one Debezium produced before it stopped, not only up to Lc. After
an ungraceful stop that includes records produced after O. There is no loss.
This is the dual-run guarantee (Alternative 2), bounded by the short
C-to-stop window. `--reset` is **not** the fix there; step 1 of `--reset`
refuses it whenever Debezium's slot is gone or has been recreated.

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

### S15. Mutual exclusion, the attach proof and the marker (5th pass, rewritten in the 6th)

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
  (`--cutover`, `--cutover --without-import`, `--reset`, `rollback`) opens a
  replication connection and runs `CREATE_REPLICATION_SLOT
  <lock name> TEMPORARY PHYSICAL`, without `RESERVE_WAL`. **(7th pass)** The
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

- **Headroom preflight.** Before taking the locks, the command checks that
  `max_wal_senders` minus the walsenders in use leaves at least 2 free, and
  that `max_replication_slots` minus the slots in use leaves at least 1 free
  beyond the slot Conduit's pipeline will need (and beyond the lock slots
  themselves). Otherwise it refuses with `lock_unavailable`, naming the
  setting and the counts. The headroom keeps the locks from taking the last
  walsender a restarted Debezium or the pipeline needs.
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

A journal that records a `--reset` or rollback in progress (any of its steps
journalled, and the journal not closed) also makes `--cutover` refuse with
`operation_in_progress`, whether or not the lock is free. Re-running that
operation finishes it, or closes it through "Closing a pending operation"
below.

**The attach proof (5th pass, rewritten in the 6th).** The checks before a
restart (S9 step 1, S12 step 1) show that Debezium's slot is the original one
at that moment. The attach proof carries that to the drop. The 5th pass
required one walsender pid from the first sighting to the drop. The round-6
review showed that fails on every normal restart: with an `lsn_proc` offset,
Debezium's streaming source runs its WAL position search and then calls
`reconnect()`, which opens a new replication connection and so a new walsender
pid (`PostgresStreamingChangeEventSource`, main L202–216, 2.7 L162–176;
`PostgresReplicationConnection.reconnect`, main L1186). Task rebalances and
network blips change the pid too. The proof now follows the slot's position
across pids:

- From the restart call on, the CLI polls at least once a second and
  journals, for each poll, the slot's `active`, `active_pid`, the pid's
  `backend_start`, `confirmed_flush_lsn`, and `pg_current_wal_lsn()`.
  **(7th pass)** `pg_replication_slots` and `pg_stat_activity` are separate
  views read without a common snapshot, so each poll reads both twice and
  requires the two reads to agree on pid, `backend_start` and
  `confirmed_flush_lsn`; a disagreement is treated as a change of walsender
  and judged by the rules below. `pg_current_wal_lsn()` is **diagnostic
  only**: since the 7th pass the proof does not depend on it (see the
  equality bullet).
- **The first sighting** with `active = true` must show
  `confirmed_flush_lsn` ≤ the bound: max(final Lc, F_stop) for rollback, final
  Lc for `--reset` after the stop, `reset_target` for `--reset` before it.
- **A change of walsender** (a new `active_pid` or `backend_start`, or an
  inactive gap between sightings) is accepted only when the new walsender's
  first sighting shows `confirmed_flush_lsn` **equal** to F_last, the last
  value seen under the old one. (W_last, `pg_current_wal_lsn()` at the old
  walsender's last poll, is journalled for diagnosis.)
  - **Equality proves continuity (corrected in the 7th pass).** A recreated
    slot starts at its consistent point, which is **strictly above** the WAL
    insert position at its creation: creating a logical slot writes its own
    `xl_running_xacts` record and starts after it. The round-7 review's probe
    measured `confirmed_flush_lsn` = insert position + `0x38` on PG 17 and
    PG 18. The insert position at creation is at or above W_last, and so at
    or above F_last, and a slot's `confirmed_flush_lsn` never moves
    backwards. So a recreated slot can never show F_last, whether F_last is
    below W_last or equal to it (an idle database). The 6th pass's separate
    argument for the equal case is no longer needed.
- **Anything else** (a first sighting above the bound, or a new walsender
  whose first sighting differs from F_last) ends the command with
  `reset_pending` or `rollback_pending`, both slots kept.
- The check immediately before the drop is the same: the slot is active and
  its history since the last accepted walsender change satisfies the rules
  above.

`offset.flush.interval.ms` matters for liveness only. Debezium moves its slot
only after a Kafka Connect offset commit, so a sighting between two flushes
sees an unchanged `confirmed_flush_lsn`. A lower interval only raises the
chance that a flush lands between the last poll under the old walsender and
the first under the new one. That sends the command to `*_pending`, the safe
side; it never accepts a gap. `catalog_xmin` and `restart_lsn` are not used:
both move with ordinary progress, and a recreated slot can have `restart_lsn`
equal to the old bound.

When the proof fails, the CLI cannot tell a gap from progress, so it keeps
both slots and changes nothing else. The fix text: _"Debezium's slot could
not be proven continuous since the restart (last proven <lsn> under pid
<pid>; then <lsn> under pid <pid>). Conduit's slot is kept and still holds
everything since C. Check Debezium's task log since <time> for a replication
slot being created. If one was created, or you cannot tell, trigger a
Debezium incremental snapshot of the pipeline's tables first. Then drop
Conduit's slot by hand under the `slot_unowned` conditions, and re-run the
same command to close it."_ There is no override flag (DeVaris decision,
which the round-6 review agrees with).

**Closing a pending operation (6th pass).** The supported way out of
`reset_pending` or `rollback_pending` after a failed attach proof is not a
bypass: the CLI never drops a slot on the operator's word. When a `--reset`
or rollback whose journal is at `reset_restarting` / `rollback_restarting` or
later (so Debezium was restarted by the operation) is re-run and finds
**Conduit's slot gone** (the operator dropped it by hand after following the
fix text), it:

1. verifies the pipeline is stopped (for rollback, at position K; for
   `--reset`, with an empty position);
2. deletes the pipeline: its configuration and its engine state (S16). For a
   rollback this includes the pipeline written with
   `logrepl.requireExistingSlot: true`, which can never start again without
   its slot;
3. drops Conduit's publication, which nothing else reads once the slot is
   gone;
4. closes the journal (step `closed_after_manual_drop`, then deletes it), so
   `--cutover`'s `operation_in_progress` clears.

It drops no slot itself, and it reports what it found and did with exit code
0 and the note `closed after a manual slot drop`. If Conduit's slot still
exists, the re-run applies the normal rules instead.

**Decision recorded (DeVaris, 2026-10-12).** The 4th pass left a residual
window: another actor dropping Debezium's slot between the pre-restart check
and Debezium's attach. DeVaris accepted it with a runbook warning only if no
cheap proof existed. The round-5 review found one, so the attach proof above
is implemented, **and** the runbook keeps the warning: do not run other slot
tooling against these two slots during `--reset` or rollback.

**`--emit-cutover-marker` with `--reset` and rollback (4th pass, corrected in
the 5th).** It is accepted. After the restart, the CLI writes the same
non-transactional message as in Q3, `pg_logical_emit_message(false,
'conduit.migrate.cutover', …)`.

- It writes no table rows.
- **(5th pass, corrected)** Conduit's pipelines do receive it when the
  `messages` option is on, which it is under Debezium-compatible output
  (#3033). So every pipeline the CLI writes adds the prefix
  `conduit.migrate.cutover` to `debezium.message.prefix.exclude.list` (or
  removes it from an include list it translated), and the pipeline never
  emits a marker. #3033's filtered-message rule then applies: the message
  does not advance `emitted`. This is a stated difference from Debezium's
  output, which does carry the marker on `<topic.prefix>.message`.
- **(5th pass, corrected)** A committed Lc at the marker's end proves that
  Debezium, on this server, processed every commit before the marker. It does
  **not** prove that it did so from its original slot: a recreated slot sees
  the marker too. Slot continuity is the attach proof's job, and the marker
  only answers "is Debezium streaming".
- It costs one record on Debezium's `<topic.prefix>.message` topic, as at
  cutover.

Where the marker does not move Lc in time, the CLI exits with `reset_pending`
or `rollback_pending` (both slots kept), not `cutover_marker_unsupported`,
because Debezium is running and nothing is lost.

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

- **API (the default).** The CLI creates, updates, starts, stops and deletes
  the pipeline through the Conduit API. Delete removes its engine state.
- **File (`--pipelines-path DIR`).** The CLI writes `DIR/<pipeline>.yaml`
  (temporary file, fsync, rename) and starts and stops the pipeline through
  the API. An update or a delete is a file change that the engine picks up
  only when it reloads. The CLI makes the change, prints what the engine
  needs, and waits, under the step's timeout, until the API shows the new
  config or no pipeline. The wait is journalled, so a re-run resumes it.
  - **(6th pass) A delete needs an engine restart.** The `--dev` watcher
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
  - **Stopping a file-provisioned pipeline** (rollback step 2, and the stop
    after `start_failed`) first sets `status: stopped` in its file, then calls
    stop. Otherwise an engine restart would start it again from the file.

**Settings on every pipeline the CLI writes.**

- `logrepl.requireExistingSlot: true` (S13): a dropped slot fails `Open`
  instead of being recreated at the current WAL.
- `logrepl.autoCleanup: false`. With the connector default, `true`, a
  pipeline delete calls `LifecycleOnDeleted`, which drops the slot and the
  publication (`source.go`).
- `conduit.migrate.cutover` in the message exclude list (S15).

**`autoCleanup` and `LastActiveConfig`.** The engine runs
`LifecycleOnDeleted` with the source's `LastActiveConfig`
(`pkg/connector/source.go`, `OnDelete`), which is the config the source last
**started** with, not the current one. A pipeline that never started has
none, and its delete triggers nothing. So `autoCleanup: false` protects the
slot only once the pipeline has started with it, and a hand edit to `true`
followed by a start would make a later delete drop the slot. The step order
below makes this harmless: the delete runs only after every check that makes
the drop safe, so whether the delete drops the slot or the CLI does, it
happens at a point where dropping is already correct. The CLI's own drop
then tolerates "does not exist".

**The order at the end of `--reset` and rollback.** Immediately before:
re-check that the pipeline is stopped and its position is what the command
expects (empty for `--reset`, K for rollback). Then:

1. delete the pipeline: its configuration (API delete, or the file removed
   and the reload awaited) and its engine state;
2. drop Conduit's slot;
3. drop the publication;
4. delete the journal.

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
**(5th pass)** The 4th pass added an optional `resume` field
(`"slotOnly"`). It is withdrawn in favour of the separate setting
`logrepl.requireExistingSlot` (S13), pending DeVaris confirmation.
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

**v0.21 (fresh strategy):** the procedure is S12, edited in place here. In
order:

1. Refuse if Debezium's slot is lost or invalidated, or (until rollback
   restarts Debezium) has moved past max(final Lc, F_stop). Wait instead if
   F_stop > Lc and Conduit's durable F_c has not reached F_stop. Check the
   saved config, the secrets, Kafka Connect and the connector-plugin
   validation. F_stop is observed first if the journal is at `stopping`. This
   is re-checked on every resume and right before step 5.
2. Stop the Conduit pipeline (setting `status: stopped` first for a
   file-provisioned pipeline, S16).
3. Restart Debezium under the attach proof (S15), and wait until it is
   RUNNING with its slot active under the proven pid.
4. Wait until Debezium's slot's `confirmed_flush_lsn` is ≥ Conduit's slot's
   `confirmed_flush_lsn`.
5. Only then delete the pipeline, then drop Conduit's slot and publication
   (S16).

There is no loss. The duplicates are everything Conduit delivered since the
cutover, which Debezium re-delivers into Kafka. The window lasts as long as
Debezium's slot stays valid. It holds WAL **and `catalog_xmin`** for the whole
soak: vacuum cannot remove catalog rows newer than it. So size
`max_slot_wal_keep_size` for the soak period, watch catalog bloat, and drop
Debezium's slot when the soak ends.

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
> - `--slot-create-timeout`, `--reset`, `--reset-timeout`, `--journal-dir`
>   and `--debezium-secret` (S9);
> - `--cutover-wait-timeout`, default 30m, which bounds waiting for Lc ≥ C;
> - `--start-timeout` (S11);
> - `--without-import`, the fallback in S13.
>
> `rollback` takes `--rollback-timeout`, default 30m (S12).
>
> **(4th pass)** `--debezium-secret` takes `<key>=env:<VAR>`. **(5th pass)**
> The value is resubmitted to Kafka Connect as plaintext in the connector
> config. To keep a config-provider reference instead, put the reference
> itself in the variable (`VAR='${vault:path:key}'`): it is resubmitted as
> written and Kafka Connect resolves it. `--pipelines-path DIR` selects file
> provisioning (S16).
> `--emit-cutover-marker` is also accepted by `--reset` and `rollback`, to
> prove an idle Debezium is streaming (S9).
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
             "duplicate_bound": "changes of xid 555 at LSN 0/16E8F98 (<= 1000 rows, or one per relation for a TRUNCATE), plus at most one transactional message ending there",
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
| `postgres.import.slot_missing` | Conduit's slot does not exist | Redo the cutover with `conduit migrate kafka-connect --cutover --reset`. S9 allows that only when the cutover is not complete, the pipeline's position is empty, Debezium's slot is valid and has not moved past max(Lc, F_stop), F_stop ≤ Lc, and Conduit's slot (if any) is at or below Lc. Otherwise use `rollback`. Import mode never creates a slot. |
| `postgres.import.slot_plugin` | Conduit's slot is not `pgoutput` | Redo the cutover with `--reset`, under the same conditions (S9). Otherwise use `rollback`. |
| `postgres.import.slot_active` | the slot is in use | Stop the other consumer. The message shows its pid and `application_name`. |
| `postgres.import.slot_invalidated` | `wal_status = lost` or `invalidation_reason` is set | The WAL is gone. Re-snapshot. |
| `postgres.import.slot_ahead` | F > Lc on Conduit's slot | Something consumed Conduit's slot past Debezium's offset. `--reset` refuses here, because F_c > Lc (S9). Use `rollback`: Debezium's slot still holds everything from O. Then cut over again. If Debezium's slot is lost, re-snapshot. |
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
  `reset_unsafe`, `reset_pending`, `pipeline_exists`, `config_masked` – S9.
- `slot_advance_on_start_unsupported` – S5 item 9 (7th pass), before the
  stop and before any restart.
- `slot_drop_on_stop`, `lsn_flush_mode_unsupported` – S5, before the stop,
  again at step 6, and before any Debezium restart. `lsn_flush_mode_unsupported`
  covers `manual`, `connector_and_driver` and `flush.lsn.source=false`
  (4th pass). `conduit kc lint` reports the same configurations as
  `kc.lint.postgres_slot_drop_on_stop` and `kc.lint.postgres_lsn_flush_mode`;
  `--cutover` emits these `migrate.kafka_connect.*` codes (#3026).
- `identity_ahead` – a warning (S13).
- `start_failed`, `import_pending` – S11.
- `rollback_slot_lost`, `rollback_pending` – S12.
- `operation_in_progress`, `lock_unavailable` – S15 (5th and 6th passes).

Connector codes added in the 5th pass, for `logrepl.requireExistingSlot`
(S13): `postgres.logrepl.slot_missing`, `postgres.logrepl.slot_plugin`,
`postgres.logrepl.slot_invalidated`, `postgres.logrepl.publication_missing`.
The 7th-pass default-refuse proposal adds the warning
`postgres.logrepl.slot_recreated` (pending DeVaris).

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
| Debezium stopped ungracefully, Debezium's F > Lc | **(4th pass)** `identity_ahead` warning; (Lc, F_stop] are duplicates. `--reset` refuses with `reset_unsafe`, and rollback waits for Conduit's durable F_c ≥ F_stop (S9, S12). `slot_ahead` is only for Conduit's slot. | 3 |
| Debezium's slot dropped on stop (`slot.drop.on.stop=true`) | Refused before the stop (`slot_drop_on_stop`, S5). If the setting changes after preflight, the CLI hits `identity_mismatch` at the post-stop re-check first, before the connector's `database_mismatch`. The fix is the S13 fallback (`--without-import`), never `--reset`, which refuses. | 3 |
| CLI killed mid-cutover, then re-run | The step-aware rules of S9, including `creating_slot` and `starting`. `--reset` refuses when F_c > Lc, after `complete`, or when Debezium's slot is invalid. | 3, 5 |
| `--reset` with Debezium's slot gone | `reset_unsafe`; Conduit's slot is kept (S9) | 3 |
| Debezium restarted by `--reset`, not yet streaming | `reset_pending`; both slots kept (S9) | 3 |
| Debezium's `lsn.flush.mode=manual` / `connector_and_driver`, or `flush.lsn.source=false` | Refused before the stop (`lsn_flush_mode_unsupported`), and again against the saved config at step 6 and before any restart (4th pass) | 3 |
| Literal masks (`[hidden]`, `****`) in Debezium's config | Refused before the stop (`config_masked`), unless supplied as `--debezium-secret <key>=env:<VAR>`. Config-provider placeholders are saved as they are (4th pass) | — |
| **(4th pass)** Debezium's slot recreated after the stop (a manual restart after `identity_mismatch`) | `--reset` and rollback see `confirmed_flush_lsn` above max(Lc, F_stop) before they restart Debezium, and refuse (`reset_unsafe`, `rollback_slot_lost`). Conduit's slot is kept. | 3 |
| **(4th pass)** A pipeline delete drops Conduit's slot through `LifecycleOnDeleted` | Cannot happen: the CLI writes `logrepl.autoCleanup: false` and drops the slot itself, last (S9) | 3 |
| **(4th pass, decided in the 5th)** CLI killed while `CREATE_REPLICATION_SLOT` is blocked | `slot_create_blocked` with the holder's pid and `backend_start` and the journalled ones; the CLI never terminates a backend (S9). Debezium is still running. | — |
| **(4th pass)** `--without-import` after Conduit's slot was dropped | Refused by the CLI (`postgres.logrepl.slot_missing`, 6th pass); if the slot disappears after the check, `logrepl.requireExistingSlot` fails `Open` with `postgres.logrepl.slot_missing` instead of creating one (S13) | 3 |
| **(5th pass)** CLI killed or timed out at `stopping` with F_d > Lc, then `--reset` | The step 7 observation records F_stop > Lc, `reset_unsafe`; the stop is never re-issued and Debezium never restarted (S9) | 3 |
| **(5th pass)** Debezium's slot recreated or re-attached during a `--reset` or rollback wait | The attach proof fails: `*_pending`, both slots kept (S15) | 3 |
| **(5th pass, 6th)** Two CLI processes on one slot, including through a pooler | The temporary lock slot makes the second refuse with `operation_in_progress`; a lost lock session aborts the first with `lock_unavailable` before its next mutating step (S15) | 3, 5 |
| **(6th pass)** Debezium reconnects under a new walsender pid during a `--reset` or rollback wait | Accepted when the new walsender's first sighting shows the same `confirmed_flush_lsn` as the last one under the old; otherwise `*_pending` (S15) | 3 |
| **(5th pass, narrowed in the 6th)** A positioned pipeline whose slot is missing | `logrepl.requireExistingSlot` fails `Open` with `postgres.logrepl.slot_missing` instead of silently starting from the current WAL (S13). A slot dropped and recreated under the same name before the start is **not** caught (S13) | 3 |
| **(5th pass)** Engine restart starts a file-provisioned pipeline during rollback | Cannot happen: the CLI sets `status: stopped` in the file before stopping it (S16) | 1, 3 |
| Offset read on a standby (PG 17 synced slots) | Refused before slot creation (`offset_from_future`, S5 step 5) | 3 |
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
>     table, including `--reset` refusals (S9);
>   - rollback with Debezium's slot invalidated (refused), rollback before
>     Debezium catches up (`rollback_pending`, both slots kept), and rollback
>     after Conduit has run (no loss; Conduit's slot dropped only after
>     F_d ≥ F_c) (S12);
>   - a pipeline-side row filter or `publish` flag difference, so that a
>     transaction Q that Debezium never saw arrives before T. The test
>     asserts zero loss, and that T's prefix is still skipped (S2);
>   - an idle database after the stop: `import_pending`, then one write to a
>     pipeline table completes the import without a re-run (S11);
>   - a slot creation blocked by a prepared transaction (S9);
>   - an offset from another database or cluster (S3).
>   - **(4th pass)** the `--reset`, rollback, adoption and `pipeline_exists`
>     cases listed under S9's cut points, and for rollback: F_stop > Lc with
>     F_c < F_stop (`rollback_pending`, nothing changed), then F_c ≥ F_stop
>     (rollback completes, zero loss); a Debezium slot recreated after the
>     stop (`rollback_slot_lost`); a missing `--debezium-secret` variable
>     (refused before the pipeline is stopped);
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
>     PostgreSQL, Amazon RDS for PostgreSQL, Google Cloud SQL and Azure
>     Database for PostgreSQL must each be verified to accept
>     `CREATE_REPLICATION_SLOT … TEMPORARY PHYSICAL` on a replication
>     connection with the keepalive options, and to drop it on disconnect.
>     A platform that does not is unsupported for `--cutover`, `--reset` and
>     rollback until a different lock is designed, and the CLI refuses there
>     with `lock_unavailable`.
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
>   - the cutover journal and `--reset` (S9), and rollback (S12);
>   - the identity proof and the `--without-import` fallback (S13);
>   - starting the pipeline by default (S11).
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

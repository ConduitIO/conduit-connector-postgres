# DBZ-3: Postgres CDC to real parity — failure-mode design

## Summary

This is the gating design doc for DBZ-3 (v0.20 Workstream 7): hardening
`conduit-connector-postgres` (v0.14) from "log-based CDC that works" to Debezium-depth parity
on the two hardest, most underspecified problems — **incremental/resumable snapshot** and
**DDL/schema-evolution reconstruction from `pgoutput`** — plus heartbeats and their interaction
with the just-shipped engine-side ack-ordering fix. No DBZ-3 implementation PR opens until this
document has DeVaris's sign-off, per the v0.20 execution plan and the cross-repo Tier-1 rule in
`ConduitIO/conduit/CLAUDE.md`.

Grounding this document in the actual connector source (not the one-paragraph epic summary)
surfaced two things worth stating plainly up front:

1. **The existing "resumable" snapshot is not actually resumable in the correctness sense.** It
   resumes the table _cursor position_, but silently loses the transactional consistency
   guarantee (the exported snapshot) that made the original snapshot↔stream handoff gap-free.
   See [Finding 1](#finding-1-the-txsnapshotid-is-not-durable-across-a-restart).
2. **The connector currently cannot distinguish "column is NULL" from "unchanged TOASTed column
   value" on UPDATE.** Both decode through the same code path today, which is a live invariant-6
   violation independent of anything DBZ-3 adds — surfaced here because DDL/TOAST handling shares
   the same relation-decoding path this design doc has to touch anyway. See
   [Finding 2](#finding-2-toast-vs-null-is-never-disambiguated-a-live-invariant-6-gap).

Both findings are cited to file:line below. Neither is fixed by this document — this document
proposes how to fix them, as part of the same body of work DBZ-3 already has to do to the
snapshot and relation-decode paths.

## Problem

The Debezium-compete epic (`ConduitIO/conduit/docs/design-documents/20260722-debezium-compete-roadmap.md`)
scoped DBZ-3 to Postgres depth: incremental/resumable snapshot, DDL/schema evolution, heartbeats,
transaction-boundary metadata, and operator-facing observability. The epic states plainly that the
hard core — resumable snapshot and DDL reconstruction from `pgoutput` — is "currently one
paragraph," not enough to build against. This document is that missing specification.

### Current state, grounded in code

**Snapshot + streaming handoff** (`source/logrepl/combined.go:70-112`,
`source/logrepl/internal/subscription.go:62-141`): on first run, `CombinedIterator` initializes
the CDC iterator first, which calls `pglogrepl.CreateReplicationSlot` with
`SnapshotAction: "EXPORT_SNAPSHOT"` (`subscription.go:85-93`). This returns a `TXSnapshotID` — a
Postgres exported-snapshot identifier valid only for the lifetime of the transaction that created
it — plus the slot's `RestartLSN`. The snapshot iterator (`source/snapshot/iterator.go:56-82`,
`source/snapshot/fetch_worker.go:160-229`) then runs one `FetchWorker` per table, each opening a
`REPEATABLE READ, READ ONLY` transaction and pinning it to the exported snapshot via
`SET TRANSACTION SNAPSHOT '<TXSnapshotID>'` (`fetch_worker.go:407-422`). Because every table's
snapshot read is pinned to the _exact_ point-in-time view that existed when the replication slot
was created, and the CDC stream starts from that same slot's `RestartLSN`, the handoff is
gap-free and dup-free by construction — this is the standard Debezium initial-snapshot design and
it is correctly built, _for the case where the snapshot completes in one run_.

**Per-table resumability** (`fetch_worker.go:105-127`, `source/position/position.go:41-46`): the
position format already tracks `SnapshotPosition{LastRead, SnapshotEnd}` per table, and
`createCursor` (`fetch_worker.go:231-255`) opens `WHERE key > lastRead AND key <= snapshotEnd`, so
a restart resumes each table's cursor from its last-acked key rather than re-reading from row
zero. `updateSnapshotEnd` (`fetch_worker.go:257-270`) only (re)computes `snapshotEnd` when it is
still `0`, i.e. the originally captured upper bound is preserved across a resume, not recomputed —
so the _set of rows this snapshot covers_ does not silently grow. This part is sound.

#### Finding 1: the TXSnapshotID is not durable across a restart

This is the actual gap, and it undermines the gap-free/dup-free claim above the moment a snapshot
spans a restart:

- `CreateSubscription` (`subscription.go:85-104`) calls `pglogrepl.CreateReplicationSlot` on
  _every_ connector start, including a resume. On resume the slot already exists, so the call
  fails with Postgres error code `42710`; the duplicate-slot branch
  (`subscription.go:96-104`, guarded by `internal.IsPgDuplicateErr`) only logs a warning and
  continues — it does **not** re-derive a snapshot ID, because there is nothing to derive: an
  exported snapshot only exists for the duration of the transaction that exported it, and that
  transaction closed when the slot-creation connection was released. `result.SnapshotName` on
  this path is the zero value, `""`.
- `CombinedIterator.initSnapshotIterator` (`combined.go:207-213`) passes
  `c.cdcIterator.TXSnapshotID()` straight through — which is now `""` — into
  `snapshot.Config.TXSnapshotID`.
- `FetchWorker.withSnapshot` (`fetch_worker.go:407-422`) checks for an empty `TXSnapshotID` and,
  finding one, **logs a warning and proceeds without `SET TRANSACTION SNAPSHOT`** — silently
  falling back to whatever the fresh `REPEATABLE READ` transaction sees as of _now_, not as of
  slot creation.

The practical consequence: a snapshot that survives a crash and resumes is no longer reading a
consistent point-in-time view correlated with the slot's `RestartLSN`. It happens not to lose or
duplicate rows in the common append-only case (verified by tracing the update/delete paths below),
but it silently drops the one property — "this table read and that WAL starting point are the
same instant" — that the whole snapshot→CDC handoff design depends on, and does so with no signal
to the operator beyond a `Warn`-level log line. Concretely, once the resumed read is no longer
snapshot-pinned:

- An **UPDATE** landing on an unread key (`lastRead < key <= snapshotEnd`) between the crash and
  the resume is picked up by the resumed cursor read with its _new_ values (looks like an
  insert-shaped snapshot record), and is _also_ replayed later by CDC as an UPDATE once
  streaming starts (WAL for it was never pruned — nothing had confirmed past that LSN). Two
  records, same final state — survivable under at-least-once, but not what "resumable snapshot"
  is supposed to mean, and the "insert, then update to the same values" shape is confusing to a
  consumer expecting Debezium's do-no-redundant-work semantics.
- A **DELETE** on an unread key is invisible to the resumed cursor read (the row is gone), so no
  snapshot record is ever emitted for it, and CDC later emits the DELETE directly — this
  specific case is actually correct by accident, not by design.
- The severity axis that matters for the design below is not data loss (none demonstrated) but
  **loss of the explicit, auditable consistency guarantee**, which blocks ever supporting
  Debezium's actual incremental-snapshot feature (ad-hoc re-snapshot of a table while streaming
  is already running) — that feature requires exactly this kind of watermark reconciliation to
  be designed in, not bolted on.

**Schema / DDL** (`source/logrepl/handler.go:119-153`, `source/logrepl/internal/relationset.go:41-51`):
`pgoutput` never emits a DDL statement. What it emits is a new `RelationMessage` — sent again by
Postgres whenever a relation's shape changes relative to what the plugin last announced, always
before the next DML referencing that relation (`handler.go:126-128`). Today,
`RelationSet.Add` (`relationset.go:41-43`) unconditionally overwrites the cached relation by ID
with no diffing, no policy check, and no history. A dropped column, added column, type change, or
renamed column all take the identical code path: the new column list silently becomes truth for
every subsequent record, with no halt, no DLQ, no drift record, and (when `WithAvroSchema` is
off) no signal to the destination at all beyond payload keys quietly changing shape. This is
exactly the "no Conduit CDC connector auto-handles DDL" gap the epic names as the single largest
universal gap versus Debezium — verified in code, not assumed.

#### Finding 2: TOAST vs. NULL is never disambiguated (a live invariant-6 gap)

`pglogrepl.TupleDataColumn` (vendored at
`github.com/jackc/pglogrepl@v0.0.0-20240307033717-828fbfe908e9/message.go:361-422`) carries a
`DataType` byte distinguishing four wire cases: `'n'` (SQL NULL), `'u'` (**unchanged TOASTed
value** — Postgres didn't resend it because it didn't change and `REPLICA IDENTITY` is not
`FULL`), `'t'`/`'b'` (an actual text/binary value follows). `RelationSet.Values`
(`relationset.go:53-77`) iterates `row.Columns` and calls `rs.decodeValue(col, tuple.Data)` — it
reads `tuple.Data` directly and **never branches on `tuple.DataType`**. For both the null case and
the unchanged-TOAST case, `Data` is empty, so both decode through the identical path today. A
large `jsonb`/`text` column that didn't change on an UPDATE is therefore indistinguishable, in the
current code, from that same column being set to `NULL` — a silent data-corrupting coercion,
squarely inside invariant 6 ("Schema handling never silently mangles data... never silent coercion
or truncation"). This predates DBZ-3 and is not introduced by it, but DBZ-3 has to touch this exact
decode path for schema/DDL work regardless, so fixing it here — rather than filing it separately
and touching the same lines twice — is the efficient sequencing. It is called out explicitly as an
acceptance criterion below, not folded silently into "general hardening."

## Goals

- A snapshot that can crash mid-run and resume **without losing the point-in-time consistency
  guarantee**, not just without re-reading rows already read.
- DDL changes detected by diffing successive `RelationMessage`s per relation, with a configured
  policy (halt / DLQ / evolve) — never silent coercion, per invariant 6.
- Fix the TOAST/NULL conflation (Finding 2) as part of the same relation-decode work.
- Heartbeats that keep the replication slot's `confirmed_flush_lsn` advancing on low-traffic
  publications, without ever advancing it past a position the engine has not durably persisted
  (invariants 1, 2).
- Transaction-boundary metadata (begin/end + affected relations) — boundary markers, not
  cross-destination atomic commit, stated plainly per the epic's scope note.
- Operator-facing observability: snapshot progress, replication lag, heartbeat staleness,
  schema-drift decisions, on the pipeline API and consumed by `conduit pipeline inspect`.
- A connector position-format upgrade that old (v0.14) positions deserialize into cleanly, with an
  explicit version field going forward.

## Non-goals

- Composite or non-integer primary keys for snapshot chunking. Today's `supportedKeyTypes` is
  `smallint|integer|bigint` (`fetch_worker.go:41-45`) and this document does not widen it — it is
  a real, separate hardening item, out of scope here to keep this doc's surface bounded to what
  the epic scoped.
- Debezium's full ad-hoc **incremental snapshot** (re-snapshot a live table without stopping the
  stream). Fixing Finding 1 makes crash-resume of the _initial_ snapshot correct; it does not by
  itself build the signal-table/watermark machinery for re-snapshotting a table that is already
  streaming. That is named as a fast-follow in [Alternatives — incremental snapshot](#area-1-resumable-snapshot-consistency)
  and is explicitly the deepest item the v0.20 contingency plan may defer to v0.21.
- Cross-destination atomic commit. Transaction-boundary metadata is boundary information only.
- A universal, cross-dialect DDL/schema-evolution framework (that is DBZ-4, scoped after two
  connectors exist). This document defines Postgres's own `SchemaChangeAdapter`-shaped internals;
  DBZ-4 later decides what generalizes.
- Distributed snapshots, event-time watermarks, or any state-layer feature beyond the connector's
  own position — per `CLAUDE.md`'s state-layer discipline.

## Design constraints

- **Invariant 1 (no early ack).** The connector must not report a WAL position to Postgres
  (`confirmed_flush_lsn`, via `SendStandbyStatusUpdate`) that reflects data the engine has not
  durably persisted. Today this holds for data records: `Subscription.Ack`
  (`subscription.go:270-273`) is only invoked from `CDCIterator.Ack`
  (`source/logrepl/cdc.go:266-287`), which the SDK calls after the engine's own
  persist-before-ack fix (`045f283`, `pkg/connector/source.go` in Conduit core). Any heartbeat
  mechanism must preserve this — it may advance the reported WAL watermark using a
  heartbeat-generated LSN **only when no emitted data record is currently unacked**
  (`walFlushed == walWritten`, per the corrected gate in [Heartbeats](#heartbeats) below); it must
  never advance past an outstanding, unacked data record merely because a heartbeat message was
  itself observed or processed.
- **Invariant 2 (monotonic, crash-safe positions).** The connector's own position format
  (`source/position/position.go`) is versioned implicitly by field presence today; this doc adds
  an explicit `Version` field and defines the upgrade path (below).
- **Invariant 3 (at-least-once floor).** No design here may introduce a code path where a DDL
  event, a TOAST-unchanged column, or a crash mid-snapshot can silently drop a record rather than
  deliver or DLQ it.
- **Invariant 4 (per-partition ordering).** Postgres logical replication delivers one ordered
  stream per slot; this doc does not change that. Per-table snapshot ordering is by primary key
  (`ORDER BY %q` in `createCursor`, `fetch_worker.go:234-239`) and is preserved.
- **Invariant 5 (atomic state writes).** The connector's position write is the engine's
  responsibility (the connector returns a position on each record; the engine persists it). This
  doc does not add a connector-owned state store — heartbeat/DDL-history state, if any needs to
  persist across restarts, rides the existing position, not a new file or table the connector
  privately owns (see [DDL history](#area-2-ddl-reconstruction-from-pgoutput) below for the one exception, which
  is Postgres-owned, not connector-owned).
- **Invariant 6 (schema handling).** DDL drift and TOAST/NULL disambiguation must never silently
  coerce. Both get an explicit, configured, testable policy.
- **Invariant 7 (graceful shutdown).** No change here alters the connector's Teardown paths
  (`combined.go:145-165`, `subscription.go:301-316`); heartbeats and DDL-history writes must
  respect the same context-cancellation discipline already in place.
- **No clustering, no distributed snapshot machinery** — a single connector process, single
  replication slot, single set of `FetchWorker`s. Nothing here adds membership, leader election,
  or a second connector instance coordinating over the network.
- **Backward compatibility.** An existing v0.14 pipeline's serialized position
  (`{"type":...,"snapshots":{...},"last_lsn":"..."}`, no `Version` field) must deserialize under
  the new format with the same runtime behavior it has today.

## Area 1: Resumable snapshot consistency

### Alternatives considered

**A. Do nothing; document the gap.** Leave Finding 1 as a documented limitation ("resume
correctness assumes no concurrent DDL/updates in the resumed key range during the gap"). Rejected:
this doc exists specifically because the epic said the one-paragraph version wasn't enough to
build against, and "we know it's not really consistent" is not a design, it's an unresolved
invariant-1/3 question left for an incident to find. It also permanently blocks incremental
snapshot (which needs a real watermark reconciliation model anyway), so punting here doesn't even
buy simplicity later — it just moves the same problem downstream with less context.

**B. Re-export a fresh snapshot on every resume, restart every table's cursor from row 0.**
Simple, and trivially consistent (every resume is transactionally clean). Rejected: it throws away
the one thing that already works — per-table resumability from `lastRead` — turning every crash
during a large-table snapshot into a full re-scan. For a multi-hour initial snapshot on a large
table this reintroduces exactly the "re-snapshot from scratch" failure mode the plan explicitly
calls out as unacceptable (invariant 3's spirit: at-least-once should not mean "redo unbounded
work" as its enforcement mechanism).

**C. (Chosen) Debezium-style low/high-watermark reconciliation, adapted to this connector's
existing per-table cursor design.** Structure:

1. **Record the watermark LSN, not just a snapshot ID, at snapshot start.** When the CDC iterator
   creates the replication slot for the first time, capture both `TXSnapshotID` (for the _first_
   run's pinned read) and the slot's `RestartLSN` as the snapshot's **low watermark**, and persist
   both in the position (`position.Position` gains `SnapshotLowWatermarkLSN string`, alongside the
   existing `LastLSN`).
2. **On resume, do not attempt to re-acquire a `TXSnapshotID`.** It cannot be re-acquired; stop
   pretending otherwise. Instead, resume the per-table cursor read _without_ `SET TRANSACTION
   SNAPSHOT` (as today), but tag every snapshot record emitted after a resume with a
   `opencdc.Metadata["postgres.snapshot.resumed"] = "true"` marker and reconcile at the CDC
   boundary: **do not allow the CDC stream to prune WAL (advance `confirmed_flush_lsn`) past the
   low watermark until the snapshot has fully completed**, exactly as today (this already holds,
   because CDC doesn't start consuming until `useCDCIterator`, `combined.go:228-242`). This
   guarantees every UPDATE/DELETE that lands in the resumed-but-unpinned window is _also_ replayed
   by CDC afterward — so the resumed snapshot's loss of consistency degrades to "possible harmless
   duplicate," never "gap." This makes the existing accidental-correctness argument in Finding 1 an
   explicit, tested guarantee instead of an accident.
3. **Make the degradation observable, not silent.** Emit a structured warning metric/event
   (`postgres_snapshot_resumed_without_pin_total`) the first time a resume happens without a valid
   snapshot pin, and surface it on the operator API (see Observability) — so an operator watching
   a long resumed snapshot knows it's in "eventually consistent via replay" mode, not "same
   guarantee as a fresh start" mode. This is the honest fix for Finding 1: not eliminating the
   accidental-correctness path (rebuilding true snapshot isolation across a process restart would
   require Postgres session-level state Conduit cannot own without its own coordination layer,
   which is out of scope per the no-clustering constraint), but bounding it, testing it, and never
   letting it be silent.
4. **Incremental (ad-hoc re-snapshot) is deliberately deferred**, but this design's watermark
   field is the seam it would attach to: a future signal-table-driven re-snapshot of one table
   while CDC is running would define its own low/high watermark pair scoped to that table and
   reconcile against the live stream the same way. Not built here (non-goal), but the position
   format leaves room for it (`SnapshotLowWatermarkLSN` is per-snapshot-run, not global) rather
   than closing the door.

**Why C over B, restated plainly:** B is "simple and wrong for the workload" (large tables are
exactly where crash-resume matters most); C keeps the existing resumability property, makes its
actual (weaker, replay-based) guarantee explicit and tested instead of implicit and lucky, and
doesn't foreclose incremental snapshot later.

## Area 2: DDL reconstruction from `pgoutput`

### Alternatives considered

**A. Parse SQL DDL text directly.** Not available — `pgoutput` never carries DDL statements
(unlike MySQL binlog, which does carry the original `ALTER TABLE` text). Rejected outright; not a
real option for this plugin, stated in the epic and reconfirmed here for completeness of the
alternatives requirement.

**B. Track relation shape via `pg_catalog` polling (a background poller diffing
`information_schema.columns` on a timer).** Rejected: adds a second, independent source of schema
truth that can race with the replication stream (a poll could observe a DDL before or after the
corresponding `RelationMessage` arrives, with no ordering guarantee against in-flight WAL), and it
adds continuous query load against the source database purely for schema-drift detection — the
kind of write/read amplification this epic explicitly criticizes trigger-based CDC for. It also
doesn't compose with per-relation-ID history, since catalog polling sees current state only, not a
change history.

**C. (Chosen) Diff successive `RelationMessage`s per relation ID, keyed by column identity, not
position.** `RelationSet.Add` becomes `RelationSet.Update`, returning a diff instead of silently
overwriting:

1. **Compare against the previously cached `RelationMessage` for that `RelationID`** (already
   available — `relationSet.relations[r.RelationID]`, `relationset.go:29,41-43`) using column
   `Name` + `DataType` + `TypeModifier` triples as identity (matches how the avro schema is already
   extracted per-column, `schema/avro.go:66-79`). Columns present before and absent after: dropped.
   Absent before, present after: added. Same name, different `DataType`/`TypeModifier`: type
   changed. Name changed with matching position and type: treated conservatively as
   drop+add (a true rename cannot be distinguished from drop+add using `pgoutput`'s relation
   message alone — no rename-tracking column-OID information is exposed at this layer — state that
   limitation plainly rather than guess).
2. **Every relation ID also gets a durable local history: a small append-only log of
   `(RelationID, LSN-first-seen, column-set-hash)` tuples**, keyed by the connector's own
   position/state (not a separate file — see the invariant-5 note above: this rides in the
   position payload as `position.Position.SchemaHistory map[string]SchemaVersion`, versioned per
   table, so it survives restarts the same way snapshot/CDC position does, with no new storage
   backend). This is what makes the diff meaningful across a restart: without it, a fresh process
   would see the first post-restart `RelationMessage` as "no prior relation" and either wrongly
   treat every column as newly added, or (worse) silently accept whatever shape it first observes
   as ground truth with no continuity. Bounded size: one entry per distinct schema version per
   table actually observed, not per record — pruned to the last N versions (default 10,
   configurable) to keep the position payload bounded, consistent with invariant-2's "positions
   are monotonic and crash-safe," not "positions grow unboundedly."
3. **Configured drift policy — `logrepl.schemaDrift.policy`, one of `halt | dlq | evolve`
   (default `halt`)**, evaluated the moment a diff is detected, _before_ the first record referencing
   the new shape is emitted:
   - `halt`: the pipeline stops with a coded, actionable error naming the table, the specific
     columns added/dropped/retyped, and the LSN at which the change was observed. This is the safe
     default — never guess when unsure.
   - `dlq`: records using the new shape are routed to the connector's error-return path (Conduit's
     existing DLQ mechanism at the pipeline level) until an operator acknowledges the drift via the
     operator API (see below); inherits the existing "no queryable DLQ record content" gap named in
     `docs/design-documents/20260715-dlq-record-visibility.md` in Conduit core — this document does
     not build a Postgres-specific DLQ record store as a side effect, per that gap's explicit
     inheritance rule in the epic.
   - `evolve`: the new shape is accepted automatically, a schema-drift event is emitted on the
     operator API and (if `WithAvroSchema`) a new Avro schema is registered and attached going
     forward — but _never_ for a narrowing change (dropped column, incompatible type change) without
     `dlq` or `halt` semantics also applying to in-flight records referencing the dropped/changed
     column; `evolve` only auto-proceeds for backward-compatible widenings (new nullable column,
     compatible type widening per the existing Avro schema-compatibility rules already used for
     `WithAvroSchema`). This is the one place this design intentionally never fully automates —
     narrowing changes always require `halt` or `dlq`, regardless of configured policy, because a
     narrowing change is exactly the "unknown fields, type mismatches... silent coercion" case
     invariant 6 names directly.
4. **Fix Finding 2 in the same PR that touches `RelationSet.Values`.** Branch on
   `tuple.DataType`: `TupleDataTypeNull` → explicit `nil`; `TupleDataTypeToast` → omit the field
   from the payload entirely (matches Debezium's own behavior — an omitted field signals "unknown,
   apply as a partial update," not "cleared") and set
   `opencdc.Metadata["postgres.toast.unchanged.<column>"] = "true"` so a destination that does full
   upserts (this connector's own destination among them) can choose to preserve the prior value
   rather than null it out; `TupleDataTypeText`/`Binary` → existing decode path, unchanged. This is
   a regression-test-required fix per `CLAUDE.md`'s bug-fix rule, independent of DBZ-3's broader DDL
   scope — it gets its own acceptance criterion below rather than being folded into "general DDL
   work."

**Why C over B:** C is strictly event-driven off the same stream already being consumed for data
(no new query load, no separate race-prone source of truth), and it composes with the durable
per-table version history a real drift policy needs. B would have been simpler to describe but
wrong on both correctness (race) and cost (continuous catalog polling against the source database,
which this epic's own critique of trigger-based CDC argues against).

## Position carry-forward is an implementation requirement, not just a struct field

Adding `SnapshotLowWatermarkLSN` and `SchemaHistory` to `position.Position` (Areas 1 and 2 above)
is necessary but not sufficient. **`CDCHandler.buildPosition`
(`source/logrepl/handler.go:312-318`) currently constructs a brand-new `position.Position{}` for
every single CDC record**, populating only `Type` and `LastLSN`:

```go
func (*CDCHandler) buildPosition(lsn pglogrepl.LSN) opencdc.Position {
	return position.Position{
		Type:    position.TypeCDC,
		LastLSN: lsn.String(),
	}.ToSDKPosition()
}
```

If this doesn't change, `SnapshotLowWatermarkLSN` and `SchemaHistory` would be set once (on the
snapshot-side positions, via `snapshot.Iterator.buildRecord`, `iterator.go:149-165`, which does
carry `i.lastPosition` forward correctly) and then **silently dropped the instant the first CDC
record is emitted** after the snapshot→CDC handoff — every position from that point on would be a
fresh, field-sparse `Position{Type: TypeCDC, LastLSN: ...}` with no low watermark and no schema
history. A restart immediately after that point would see `Version == 0`-shaped data (per the
upgrade section below) even on a connector that has run past its first snapshot — silently
regressing to Finding-1/pre-DDL-tracking behavior on every single CDC-mode restart, not just on a
genuine version rollback. That would make Areas 1 and 2 correct only in the snapshot phase and
silently inert for the entire (much longer) CDC-streaming lifetime of a pipeline — the opposite of
what this document is for.

**Requirement:** `CDCHandler` must hold the current `position.Position` (or at minimum its
`SnapshotLowWatermarkLSN` and `SchemaHistory` fields) as part of its own state, seeded from the
position the connector was started with, and `buildPosition` must carry those fields forward
unchanged on every record, updating only `Type`/`LastLSN` per record and `SchemaHistory` itself
only at the moment Area 2's diff logic detects and records a new relation version. This is
equivalent in spirit to how `snapshot.Iterator` already threads `i.lastPosition` through
`buildRecord` (`iterator.go:150-153`) — the CDC side needs the same threading, which it does not
have today, and this document's Areas 1 and 2 are not actually implemented until it does. This is
called out as its own acceptance criterion below, separate from the watermark and schema-history
mechanisms themselves, because it is the kind of easy-to-omit wiring detail that would otherwise
only surface as a confusing intermittent regression during review or, worse, in production.

## Heartbeats

**The gap:** `Subscription.listen` (`subscription.go:168-215`) already sends a
`StandbyStatusUpdate` on a fixed timer (`StatusTimeout`, default 10s,
`subscription.go:43,133,171-180`) — but that update only **re-reports the last known
`walWritten`/`walFlushed` positions** (`sendStandbyStatusUpdate`, `subscription.go:382-423`). It
does not generate new WAL activity. Debezium's heartbeat problem is specifically: if the
_publication's_ tables are idle but unrelated WAL activity elsewhere in the database (other
tables, other publications, autovacuum, etc.) keeps growing, this subscriber never receives
`XLogData` for any of it (pgoutput only forwards changes matching the publication), so
`walWritten` never advances, and the slot's `restart_lsn`/`confirmed_flush_lsn` stays pinned,
holding WAL on disk indefinitely. The existing 10s timer resends stale numbers; it does not solve
the problem it looks like it solves.

**Design:** a dedicated heartbeat mechanism, decoupled from data-record flushing:

- A per-connector heartbeat table (`logrepl.heartbeat.enabled` default `false`,
  `logrepl.heartbeat.schema`/`logrepl.heartbeat.table` configurable, defaulting to a
  connector-managed `_conduit_heartbeat` table) that the connector's own connection `UPSERT`s a
  monotonic counter into on a timer (`logrepl.heartbeat.interval`, default 30s) — the same
  approach Debezium uses, and the one that reliably generates WAL entries pgoutput will actually
  deliver, because the heartbeat table is a member of the publication (this connector already
  manages publication membership programmatically, `internal.CreatePublication`,
  `publication.go`).
- **The heartbeat's LSN is tracked separately from the data-ack LSN, and is only safe to report
  when no data record is outstanding — never merely because the heartbeat message itself was
  "processed."** An earlier draft of this section gated heartbeat-driven advancement on "the
  heartbeat's `XLogData` was processed," which is wrong and reintroduces the exact sev-0 pattern
  this doc cites: `Handle` (`handler.go:119-153`) hands insert/update/delete records off to
  `CDCHandler`'s batch (`addToBatch`, `handler.go:264-279`) and the batching channel
  (`batchesCh`/`out`, `cdc.go:56,86-96`) well before the engine has acked them — `walFlushed` only
  ever advances via an explicit `Subscription.Ack(lsn)` call
  (`subscription.go:270-273`), driven by `CDCIterator.Ack` (`cdc.go:266-287`), which the SDK
  invokes only after the engine's persist-before-ack fix has durably written the position. A data
  record can be sitting in `recordBatch` or already handed to a slow destination, with a lower LSN
  than a heartbeat's, at the moment the heartbeat's own commit is observed. Reporting the
  heartbeat's LSN as flushed at that moment would tell Postgres it's safe to prune WAL past that
  unacked record — crash before the destination durably writes it, and the record is gone forever,
  unrecoverable because the WAL that would have redelivered it is already pruned. That is
  invariant 1/2/3 violated via a side channel, not a hypothetical.

  **The corrected gate:** the connector already tracks exactly the state needed to gate this
  correctly, without inventing new machinery. `Subscription.walWritten`
  (`subscription.go:55-58`) is updated in `handleXLogData` to the LSN of the most recent record
  actually **emitted** downstream (`writtenLSN`, returned non-zero only for
  insert/update/delete, `handler.go:129-143`, `subscription.go:261-263`); `walFlushed`
  (`subscription.go:270-273`) is updated only on an actual engine ack. `sendStandbyStatusUpdate`
  already computes `walFlushed == s.walWritten` today, for an unrelated purpose — deciding whether
  it's safe to reply with `serverWALEnd` for keepalive (`subscription.go:392-394`,
  `replyWithWALEnd := walFlushed == s.walWritten && ...`). That comparison is _precisely_ "no
  emitted data record is currently unacked," and it is the correct, and only correct, gate for
  heartbeat-driven advancement too:
  - Track a separate `heartbeatObservedLSN`, updated when the heartbeat table's own `XLogData` is
    decoded (a heartbeat write is itself just another row change flowing through `Handle`,
    recognized by table name and short-circuited before reaching `addToBatch` — it never enters
    the data record path).
  - The reported `WALFlushPosition` in `sendStandbyStatusUpdate` is `walFlushed` **unless
    `walFlushed == walWritten`** (no data record outstanding, the existing check) **and
    `heartbeatObservedLSN > walFlushed`**, in which case it is safe to advance to
    `heartbeatObservedLSN` — because with no outstanding emitted-but-unacked record, there is
    nothing left whose durability the advancement could be lying about.
  - This is exactly the case heartbeats are meant to cover — a genuinely idle publication has no
    outstanding records by definition — so the fix costs the feature nothing: heartbeats still
    advance the watermark whenever there's truly no application traffic, and are correctly
    withheld the moment there is.
  - This replaces the flawed `max(dataWalFlushed, heartbeatWalFlushed-that-is-safe-to-report)`
    framing from the earlier draft, which asserted a guarantee ("never exceeds the durably
    persisted watermark") the mechanism as originally specified did not actually provide.
- Heartbeat writes are skipped (not queued, not retried into a backlog) if the connector is
  currently in snapshot mode or mid-teardown — heartbeats only run once CDC streaming is active,
  matching the constraint that nothing should perturb the pinned/resumed snapshot transaction
  state from Area 1.

## Transaction-boundary metadata

`CDCHandler.Handle` already tracks `BeginMessage`/`CommitMessage` LSNs for an internal ordering
check (`handler.go:144-149`) but discards them otherwise. This doc adds: a synthetic
begin-boundary and end-boundary record type (following the same `opencdc.Record` shape, with a
`postgres.transaction` metadata block: `{id, event_count, affected_relations}`), emitted only when
`logrepl.transactionMetadata.enabled` is `true` (default `false`, to avoid changing default output
shape for existing pipelines — a backward-compatibility concern per the constraints above). This is
boundary information only, matching the epic's explicit non-goal of cross-destination atomic
commit — stated in the config docs and README, not implied to be more.

## Operator-API observability

Ships as an acceptance criterion, not a follow-on, per the epic and v0.20 plan. Exposed via the
existing connector `Config`/status surfaces the SDK already ferries to the pipeline API — no new
transport, matching the "one code path, multiple front-ends" rule:

- **Snapshot progress**: per-table `{lastRead, snapshotEnd, percentComplete, resumedWithoutPin
  bool}` — the last field is the direct observability fix for Finding 1's degraded mode.
- **Replication lag**: `serverWALEnd - walFlushed`, already computed internally
  (`subscription.go:392-394`) — just needs to be surfaced, not recomputed.
- **Heartbeat staleness**: time since last heartbeat write succeeded and time since last heartbeat
  XLogData observed — two numbers, because a heartbeat write failing (connector→DB) is a different
  failure mode from a heartbeat write succeeding but not being delivered through the replication
  stream (DB→connector), and conflating them would hide which side broke.
- **Schema-drift decisions**: table, detected diff, policy applied, LSN, and (for `dlq`/`halt`) an
  acknowledgment endpoint.

`conduit pipeline inspect --json` consumes this surface as the acceptance criterion requires;
`docs/operations/` gets the two owed runbook entries (replication-slot bloat, heartbeat staleness)
before this ships, each with symptom → diagnosis → remediation, per the existing runbook standard.

## Failure-mode analysis

| Failure | Behavior after this design | Invariant(s) |
| --- | --- | --- |
| **Crash mid-snapshot** (`kill -9` while a `FetchWorker` cursor is open) | Resume reopens the cursor at `lastRead` (unchanged, already correct). `TXSnapshotID` is unavailable (Finding 1); resumed read runs unpinned, tagged `postgres.snapshot.resumed`, and CDC does not prune WAL past the recorded low watermark until snapshot completion — any UPDATE/DELETE in the resumed window is guaranteed to also arrive via CDC replay. No gap; possible harmless duplicate; explicitly observable via the operator API, not silent. | 1, 2, 3 |
| **Crash at the snapshot→CDC switchover boundary** (every `FetchWorker` has reached end-of-cursor, but the crash happens before `useCDCIterator`/`StartSubscriber` runs, so the persisted position is still `Type == TypeSnapshot`) | On resume, `CombinedIterator.initSnapshotIterator` (`combined.go:198-224`) sees `pos.Type == TypeSnapshot` and reconstructs the snapshot iterator and its `FetchWorker`s exactly as if the snapshot were still in progress. But every table's cursor range is now empty (`lastRead == snapshotEnd` for each, since all workers had already finished before the crash), so `createCursor`/`fetch` (`fetch_worker.go:201-229`) immediately hits `n == 0`, the worker's `tomb` completes with no records sent, `Iterator.data` closes (`iterator.go:192-206`), and `NextN` returns `ErrIteratorDone` on the first call (`iterator.go:97-106`) — `CombinedIterator.NextN` then calls `useCDCIterator` (`combined.go:117-138`) and CDC picks up cleanly from the recorded low-watermark LSN, which was never pruned because CDC hadn't started consuming before the crash. **This is correct today only as a side effect of the empty-range short-circuit, not by explicit design** — it is not gap-free by an intentional check, it happens to be gap-free because there is nothing left to read. Called out explicitly here (rather than left unanalyzed) precisely because a future change to the empty-cursor path (e.g. an optimization that skips re-validating an empty-range table) could silently break this without anyone noticing it was ever load-bearing. Requires its own chaos test (acceptance criterion below) rather than resting on this analysis alone. | 1, 2, 3 |
| **DDL during an active snapshot on the same table** | `ALTER TABLE` requires `ACCESS EXCLUSIVE`; the `FetchWorker`'s open cursor holds `ACCESS SHARE` for the transaction's duration, so the DDL blocks until the snapshot transaction commits (standard Postgres lock semantics — not something this connector controls). Documented as an operational caveat: a DDL on a currently-snapshotting table will queue behind the snapshot, not corrupt it. Once the snapshot transaction ends and the DDL proceeds, the next `RelationMessage` triggers the diff/policy path in Area 2. | 6 |
| **DDL during active CDC streaming (steady state)** | New `RelationMessage` observed, diffed against history, policy applied (`halt`/`dlq`/`evolve`) before any record using the new shape is emitted. Narrowing changes always halt or DLQ regardless of configured policy. | 6 |
| **Replication slot lost/dropped externally** (e.g. an operator runs `pg_drop_replication_slot`, or Postgres invalidates it due to `max_slot_wal_keep_size`) | `CreateSubscription`'s duplicate-check path only handles "already exists"; slot _absence_ on a resume that expects one is a distinct, currently under-handled case. This design adds an explicit check: if the configured slot name does not exist AND the position indicates CDC has already started (`position.Type == TypeCDC`), fail fast with a coded, actionable error (`slot lost, cannot resume without full re-snapshot`) rather than silently recreating a fresh slot at the current WAL position, which would silently skip every change between the last confirmed position and slot recreation — a real gap-class data-loss bug if allowed to happen quietly. Recovery requires an explicit operator-triggered full re-snapshot, never an automatic one. | 1, 2, 3 |
| **Long-running transaction upstream** (an app holds open a multi-hour transaction touching a subscribed table) | Postgres cannot advance the slot's `restart_lsn` past the open transaction's start regardless of what this connector does — WAL grows on the server for the duration. This is a Postgres-level constraint, not one this design can fix; the mitigation is observability (replication-lag/heartbeat-staleness surfaced early) plus a runbook entry naming this specific cause distinctly from a stalled/misbehaving connector, so an operator does not waste time debugging the connector for a problem that's actually an application-side long transaction. | (observability only; no invariant fix possible) |
| **Disk full on WAL (server-side)** | Out of this connector's control to prevent; what's in scope is not lying about flush state under backpressure. If the standby-status-update send itself fails (`subscription.go:404-419` returns an error), `listen` returns the error and the subscription tears down (`doneReplication`, `subscription.go:442-451`) rather than silently continuing with the wrong last-known state — already correct today; this design does not weaken it, and heartbeat writes get the same treatment (a failed heartbeat write surfaces as heartbeat-write staleness, not swallowed). | 1, 3 |
| **Txn boundaries: a large transaction spanning many `Begin`/`Commit` pairs mid-batch** | Boundary metadata (if enabled) is emitted per actual `BeginMessage`/`CommitMessage` pair observed, using the existing ordering check (`handler.go:144-149`) as the correctness backstop — an out-of-order commit already returns a hard error today (`handler.go:147-149`), which this design preserves rather than loosens for the sake of boundary-metadata convenience. | 4 |
| **TOAST/NULL conflation (Finding 2) reaching production undetected** | Fixed directly: `TupleDataTypeToast` is now distinguished from `TupleDataTypeNull` at the decode site, with a regression test asserting an UPDATE that changes one column while leaving a large TOASTed column untouched preserves (via omission + metadata marker) rather than nulls the unchanged column. | 6 |

## Upgrade / rollback

- **Position format**: add `Version int` (new field, `omitempty`-equivalent so an unversioned v0.14
  position deserializes with `Version == 0`), `SnapshotLowWatermarkLSN string`, and
  `SchemaHistory map[string]SchemaVersion`. All three are additive JSON fields — an existing
  `{"type":1,"snapshots":{...},"last_lsn":"0/1A2B3C4"}` position from a running v0.14 pipeline
  deserializes unchanged under the new struct (zero-value new fields), and the connector treats
  `Version == 0` as "no low watermark recorded, no schema history — behave exactly as v0.14 did
  until the next snapshot/DDL event naturally populates the new fields." No forced migration step;
  no pipeline needs to be paused to upgrade.
  Rollback (new connector version → old): a position written by the new version that has never
  hit a resumed-snapshot or DDL-diff path is byte-compatible with the old struct (extra JSON fields
  are ignored by the old connector's `json.Unmarshal`). A position that _has_ recorded a low
  watermark or schema history and is then read by an old connector binary loses that
  information silently on decode (old struct doesn't have the fields) — this is the one asymmetric
  case, and it is stated here rather than glossed over: **rolling back past a resumed-snapshot or
  DDL-diff event returns the connector to today's Finding-1/Finding-2 behavior**, not a crash. No
  worse than current production behavior, just not improved — acceptable for a minor-version
  rollback, and documented as such in the release notes.
- **Schema-history bound**: capped at the last N versions per table (default 10, configurable) so
  the position payload does not grow unboundedly across a connector's lifetime — an old connector
  reading a truncated history simply doesn't see it (falls back to "no prior relation," same as
  today's `RelationSet.Add` unconditional-overwrite behavior).
- **Heartbeat table**: connector-managed, created (`CREATE TABLE IF NOT EXISTS`) and added to the
  publication only when `logrepl.heartbeat.enabled=true`; disabling it later does not require a
  cleanup step (the table is small, inert, and already covered by the existing
  `logrepl.autoCleanup` teardown path pattern — extended here to optionally drop the heartbeat
  table alongside the slot/publication on connector deletion, defaulting to leave-it-in-place to
  avoid surprising drops of an operator-visible table).

## Decisions (DeVaris, 2026-08-03)

All four open questions below are RESOLVED. Recorded here rather than in a new document because
they answer questions this doc posed; the questions are kept intact underneath so the reasoning
that produced each answer stays readable.

1. **`logrepl.schemaDrift.policy` default = `halt`.** Confirmed: safety over silent continuity.
   **This is a BREAKING CHANGE** and must ship with a migration note and a deprecation plan per
   CLAUDE.md — pipelines upgrading to a DBZ-3 connector version will, for the first time, stop on
   a DDL that previously passed through silently. The note must say plainly what changed, how to
   restore the old behaviour (`evolve`), and why the default moved (silent drift is an
   invariant-6 violation). Acceptance: the note exists and is linked from the release notes, not
   buried in a changelog line.

2. **Rename detection: NOT built in this workstream.** Confirmed. `pgoutput` cannot distinguish a
   rename from drop+add, and the `REPLICA IDENTITY FULL` heuristic can still be wrong. Ship
   drop+add — correct, noisier. This is a decision that the heuristic is not worth its complexity,
   not a deferral; revisit only on a concrete user report.

3. **Heartbeat table: keep `_conduit_heartbeat`; defer collision handling.** Confirmed. The table
   stays visible to any other consumer of the same publication. Collision with a coexisting
   Debezium heartbeat table during a migration-in-progress is real but belongs to the KC-migration
   compatibility-report path. Carry it forward there so it is not lost.

4. **Benchi: connector-local measurement, `postgres→log` reference.** Confirmed. Two reasons this
   does NOT block on the repo-level benchi harness: that harness was retracted as unfit for engine
   comparison (ConduitIO/conduit#2748 — its metric is not comparable across engines and its A/A
   noise floor exceeded the effect), and `postgres→s3` is blocked on conduit-connector-s3#963
   (MinIO checksum). A connector-local before/after on the heartbeat and resumed-snapshot-tagging
   hot paths answers criterion 6 without inheriting the engine-benchmark problem. **Any number
   reported must cite an A/A floor beside it**, per the methodology in that retraction.

## Open questions for DeVaris

1. **Default for `logrepl.schemaDrift.policy`.** This doc proposes `halt` as the safe default
   (never silently evolve without an explicit opt-in). That is a behavior change in spirit — today
   there is no policy at all, drift just silently flows through — so existing pipelines upgrading
   to a DBZ-3 connector version will, for the first time, see pipelines _stop_ on a DDL that
   previously passed silently. Confirm `halt`-by-default is the intended tradeoff (safety over
   silent continuity) before this becomes an acceptance criterion.
2. **Rename detection.** Stated above as "cannot be distinguished from drop+add using `pgoutput`
   alone." An alternative is requiring `REPLICA IDENTITY FULL` plus a heuristic (same
   `TypeModifier`/`DataType`, adjacent ordinal position, no intervening DML on the "old" name) to
   _guess_ a rename with a confidence flag rather than always treating it as drop+add. This adds
   real complexity for a heuristic that can still be wrong. Recommend NOT building the heuristic in
   this workstream (ship drop+add, correct but noisier) — confirming that call before it's load-
   bearing on the acceptance criteria.
3. **Heartbeat table ownership and destination visibility.** The heartbeat table is a real
   Postgres table in the source database, visible to any other consumer of the same publication
   (including a Kafka-Connect-wrapper Debezium-Postgres pipeline reading the same DB, per Track A
   of the epic). Confirm the naming convention (`_conduit_heartbeat` by default) doesn't collide
   with a coexisting Debezium heartbeat table if both are pointed at the same publication during a
   migration-in-progress scenario — worth an explicit note in the KC-migration compatibility
   report path later, flagged here so it isn't lost.
4. **Benchi throughput comparison.** Acceptance criterion 6 (below) requires a benchi run since
   heartbeats and the resumed-snapshot tagging touch the hot path. Confirm the reference pipeline
   config to benchmark against (the existing `postgres→s3` or `postgres→log` template referenced
   in the v0.20 plan's tracked follow-ups) before Phase C implementation begins, so the benchmark
   isn't improvised at review time.

## Acceptance criteria (restated from the v0.20 plan, made concrete here)

1. This design doc reviewed and signed off by DeVaris (cross-repo Tier-1) before any DBZ-3
   implementation PR opens in this repo.
2. Crash-mid-snapshot chaos test: `kill -9` a connector process mid-`FetchWorker.fetch`, resume,
   assert no gap and that any duplicate is limited to the documented resumed-without-pin window,
   with `postgres.snapshot.resumed` metadata present on the affected records.
3. DDL-mid-stream test: an `ALTER TABLE ADD/DROP COLUMN` and a type change, each exercised against
   all three policies (`halt`/`dlq`/`evolve`), asserting narrowing changes never silently evolve.
4. TOAST/NULL regression test (Finding 2): an UPDATE touching one column while leaving a large
   `jsonb` column untouched must not null that column downstream; the test fails without the fix
   and passes with it, per the bug-fix-ships-with-its-test rule.
5. Heartbeat test: an idle publication with unrelated WAL activity elsewhere in the database shows
   `confirmed_flush_lsn` advancing via heartbeat when `walFlushed == walWritten` (no data record
   outstanding), and a dedicated chaos test asserts the reported flush position **never** advances
   to a heartbeat-observed LSN while a data record has been emitted (`walWritten`) but not yet
   acked (`walFlushed < walWritten`) — i.e. the corrected gate in [Heartbeats](#heartbeats) holds
   under concurrent heartbeat and data traffic, not just in the idle case (invariant 1, 2, 3).
6. Benchi throughput comparison attached to the implementation PR (heartbeats/snapshot changes
   touch the hot path) — see open question 4.
7. Runbook entries for replication-slot bloat and heartbeat staleness land in `docs/operations/`
   before ship.
8. Operator-API surface (`conduit pipeline inspect --json`) shows snapshot progress, replication
   lag, heartbeat staleness, schema-drift decisions — cross-checked, not eyeballed.
9. Position-format upgrade test: a v0.14-serialized position deserializes and resumes correctly
   under the new connector version with zero-value new fields.
10. Switchover-boundary chaos test: `kill -9` after every `FetchWorker` reaches end-of-cursor but
    before `useCDCIterator`/`StartSubscriber` runs; assert the resumed run transitions cleanly to
    CDC with no gap and no duplicate, exercising the empty-cursor-range short-circuit explicitly
    rather than relying on the analysis alone.
11. Position carry-forward test: after a snapshot→CDC handoff, assert every subsequent CDC-mode
    position (not just the first) carries `SnapshotLowWatermarkLSN` and the latest `SchemaHistory`
    forward unchanged except where Area 2's diff logic updates the latter — asserting
    `CDCHandler.buildPosition` threads this state rather than resetting it per record.

## Related

- `ConduitIO/conduit/docs/design-documents/20260722-debezium-compete-roadmap.md` — the epic this
  document re-scopes DBZ-3 from a one-paragraph summary into a buildable spec.
- `ConduitIO/conduit/CLAUDE.md` — data-integrity invariants 1–7; Tier-1 cross-repo sign-off rule;
  design-doc bar (problem, constraints, ≥2 alternatives, failure modes, upgrade/rollback,
  observability).
- Internal plan: `v020-execution-plan.md`, Workstream 7 — sequencing behind WS0 (chaos-CI gate)
  and WS6 (DBZ-2 engine-side correctness suite); this connector's own Tier-1 review is separate
  from and does not inherit Conduit-core's standing admin-merge authorization.
- `docs/design-documents/20260715-dlq-record-visibility.md` (Conduit core, issue #2640) — the
  deferred Tier-1 DLQ record-store gap this document's `dlq` policy option inherits rather than
  building a parallel Postgres-specific store.
- Source cited throughout: `source/logrepl/combined.go`, `source/logrepl/internal/subscription.go`,
  `source/logrepl/internal/relationset.go`, `source/logrepl/handler.go`, `source/snapshot/iterator.go`,
  `source/snapshot/fetch_worker.go`, `source/position/position.go`,
  `source/logrepl/internal/replication_slot.go`, `source/schema/avro.go`.

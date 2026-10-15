// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbzoffset

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Querier is the part of pgx the preflight uses. *pgxpool.Pool and *pgx.Conn
// satisfy it. It must be a regular (not a replication) connection.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Target is what the import is applied to: the pipeline's own slot,
// publication and tables, and the server's identity.
type Target struct {
	// SlotName and PublicationName are Conduit's (logrepl.slotName,
	// logrepl.publicationName).
	SlotName        string
	PublicationName string
	// Tables are the pipeline's tables. A name without a schema means
	// "public", as everywhere else in this connector.
	Tables []string
	// SystemIdentifier is IDENTIFY_SYSTEM's systemid, read by the caller on
	// a replication connection (no extra privilege is needed for it).
	SystemIdentifier string
}

// SlotState is what the preflight read from Conduit's slot.
type SlotState struct {
	ConfirmedFlushLSN pglogrepl.LSN // F0
	RestartLSN        pglogrepl.LSN
	WALStatus         string
}

// The first minor release of each major that carries "Fix possibility of
// logical decoding partial transaction changes" (B1). PG 18 has it in every
// release; below 14 is refused as a policy (server_unsupported).
var firstFixedRelease = map[int]int{
	14: 140013,
	15: 150008,
	16: 160004,
	17: 170000,
}

// Fix texts for the checks that need Postgres.
const (
	fixFromFuture   = "The offset is from another server, or from one restored from a backup. Point `url` at the primary Debezium read from, and re-export the offset."
	fixUnpatched    = "Upgrade Postgres to at least 14.13, 15.8, 16.4 or 17.0 (whichever is your major), then redo the cutover."
	fixUnsupported  = "Upgrade Postgres to 14.13 or later."
	fixDBMismatch   = "Point `url` at the database Debezium read from, and re-export the offset with `conduit migrate kafka-connect`."
	fixSlotMissing  = "Go back to Debezium with the manual runbook, gated by `conduit migrate kafka-connect --check-rollback`, then redo the cutover. Import mode never creates a slot."
	fixSlotPlugin   = "Go back to Debezium with the manual runbook (`conduit migrate kafka-connect --check-rollback`), then redo the cutover; the slot must use pgoutput."
	fixSlotActive   = "Stop the other consumer of the slot."
	fixInvalidated  = "The slot lost its WAL. Re-snapshot (a new pipeline with `snapshotMode: initial`)."
	fixSlotAhead    = "Something consumed Conduit's slot past Debezium's offset. Go back to Debezium with the manual runbook (`conduit migrate kafka-connect --check-rollback`): Debezium's slot still holds everything from its offset. Then cut over again. If Debezium's slot is lost, re-snapshot."
	fixSlotDatabase = "Fix `url`: the slot belongs to another database."
	fixPublication  = "Repair the publication, then redo the cutover with a new fresh slot created after the repair. Never add tables to a publication that an existing slot will decode from: pgoutput reads the historic catalog and silently skips their earlier WAL."
	fixNotCaptured  = "Remove those tables from this pipeline and add them in a separate pipeline with a snapshot."
)

// Preflight runs the checks of D5 that need Postgres, in the S5 order, at
// the connector's Open (and in the CLI's dry run, with the same code):
//
//  5. not a standby, and lsn_commit <= pg_current_wal_lsn()
//     (CodeOffsetFromFuture);
//  7. the server carries the partial-decoding fix (CodeServerUnpatched,
//     CodeServerUnsupported);
//  8. the database identity (S3) and the S13 proof: current_database(),
//     the system identifier, and Debezium's slot exists here with
//     confirmed_flush_lsn >= C and is not active (CodeDatabaseMismatch,
//     CodeSlotActive);
//  10. Conduit's publication exists and covers the pipeline's tables
//     (CodePublicationMismatch);
//  11. the pipeline's tables were captured by Debezium
//     (CodeTablesNotCaptured);
//  12. Conduit's slot exists here, is pgoutput, inactive, not temporary, not
//     invalidated, and F <= Lc (the CodeSlot* codes).
//
// It never writes anything and never creates a slot or a publication.
// Checks 1 to 4 and 6 ran in ParseSetting; check 9 (Debezium's settings) is
// the CLI's.
func Preflight(ctx context.Context, q Querier, imp Import, t Target) (SlotState, error) {
	lc := imp.Offset.LSNCommit

	// Q4: v0.21 never reuses Debezium's slot. Checked here too, so the CLI's
	// dry run, which shares Preflight, refuses what the connector refuses.
	if t.SlotName == imp.Setting.DebeziumSlotName {
		return SlotState{}, newError(CodeSlotReuseUnsupported, "logrepl.slotName",
			"Use the fresh slot `conduit migrate kafka-connect --cutover` created (`--slot-strategy fresh`). Reusing Debezium's slot arrives in a later release.",
			"logrepl.slotName %q is Debezium's slot", t.SlotName)
	}

	// 5.
	var inRecovery bool
	var current pglogrepl.LSN
	if err := q.QueryRow(ctx, "SELECT pg_is_in_recovery(), CASE WHEN pg_is_in_recovery() THEN '0/0'::pg_lsn ELSE pg_current_wal_lsn() END").Scan(&inRecovery, &current); err != nil {
		return SlotState{}, fmt.Errorf("import preflight: failed to read the WAL position: %w", err)
	}
	if inRecovery {
		return SlotState{}, newError(CodeOffsetFromFuture, SettingPath, fixFromFuture,
			"the server is a standby (pg_is_in_recovery() is true); the import runs against the primary only")
	}
	if lc > current {
		return SlotState{}, newError(CodeOffsetFromFuture, SettingPath+".offset.lsn_commit", fixFromFuture,
			"lsn_commit %s is ahead of this server's current WAL position %s", lc, current)
	}

	// 7.
	var versionNum int
	if err := q.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&versionNum); err != nil {
		return SlotState{}, fmt.Errorf("import preflight: failed to read server_version_num: %w", err)
	}
	if err := checkServerVersion(versionNum); err != nil {
		return SlotState{}, err
	}

	// 8.
	if err := checkIdentity(ctx, q, imp, t); err != nil {
		return SlotState{}, err
	}

	// Resolve the pipeline's table names the way the connector does (one
	// quoted identifier, through the session's search_path), so the checks
	// below compare the tables the pipeline will actually read.
	resolved, err := resolveTables(ctx, q, t.Tables)
	if err != nil {
		return SlotState{}, err
	}

	// 10.
	if err := checkPublication(ctx, q, t.PublicationName, resolved); err != nil {
		return SlotState{}, err
	}

	// 11.
	if err := checkTablesCaptured(imp.Setting.Tables, resolved); err != nil {
		return SlotState{}, err
	}

	// 12.
	return checkConduitSlot(ctx, q, lc, t.SlotName)
}

func checkServerVersion(v int) error {
	major := v / 10000
	if major < 14 {
		return newError(CodeServerUnsupported, SettingPath, fixUnsupported,
			"Postgres %d (server_version_num %d) is not supported for an import", major, v)
	}
	if first, ok := firstFixedRelease[major]; ok && v < first {
		return newError(CodeServerUnpatched, SettingPath, fixUnpatched,
			"Postgres server_version_num %d predates the fix for partially decoded transactions "+
				"(first fixed: %d); a new slot could start mid-transaction and lose its head", v, first)
	}
	return nil
}

type slotRow struct {
	plugin, slotType, database string
	active, temporary          bool
	activePID                  *int32
	appName                    string
	confirmed, restart         *pglogrepl.LSN
	walStatus, invalidation    string
}

// readSlot reads one slot. Columns that do not exist on every supported
// major (invalidation_reason is PG 17+) are read through to_jsonb.
func readSlot(ctx context.Context, q Querier, name string) (slotRow, bool, error) {
	const query = `SELECT s.plugin, s.slot_type, COALESCE(s.database::text, ''),
		s.active, s.temporary, s.active_pid, COALESCE(a.application_name, ''),
		s.confirmed_flush_lsn, s.restart_lsn,
		COALESCE(s.wal_status, ''), COALESCE(to_jsonb(s)->>'invalidation_reason', '')
	FROM pg_replication_slots s
	LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid
	WHERE s.slot_name = $1`
	var r slotRow
	var plugin *string
	err := q.QueryRow(ctx, query, name).Scan(&plugin, &r.slotType, &r.database,
		&r.active, &r.temporary, &r.activePID, &r.appName,
		&r.confirmed, &r.restart, &r.walStatus, &r.invalidation)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotRow{}, false, nil
	}
	if err != nil {
		return slotRow{}, false, fmt.Errorf("import preflight: failed to read replication slot %q: %w", name, err)
	}
	if plugin != nil {
		r.plugin = *plugin
	}
	return r, true, nil
}

func (r slotRow) activeHolder() string {
	pid := "unknown"
	if r.activePID != nil {
		pid = fmt.Sprint(*r.activePID)
	}
	return fmt.Sprintf("pid %s, application_name %q", pid, r.appName)
}

func checkIdentity(ctx context.Context, q Querier, imp Import, t Target) error {
	var db string
	if err := q.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
		return fmt.Errorf("import preflight: failed to read current_database(): %w", err)
	}
	if db != imp.Setting.Database {
		return newError(CodeDatabaseMismatch, SettingPath+".database", fixDBMismatch,
			"the offset is for database %q, but the connector is connected to %q", imp.Setting.Database, db)
	}
	if t.SystemIdentifier != imp.Setting.SystemIdentifier {
		return newError(CodeDatabaseMismatch, SettingPath+".systemIdentifier", fixDBMismatch,
			"the offset is for system identifier %s, but this server is %s", imp.Setting.SystemIdentifier, t.SystemIdentifier)
	}

	// S13: an offset from another server, a clone or a promoted standby
	// cannot have moved this server's Debezium slot to C.
	name := imp.Setting.DebeziumSlotName
	r, ok, err := readSlot(ctx, q, name)
	if err != nil {
		return err
	}
	path := SettingPath + ".debeziumSlotName"
	switch {
	case !ok:
		return newError(CodeDatabaseMismatch, path, fixDBMismatch,
			"Debezium's replication slot %q does not exist on this server", name)
	case r.database != db:
		return newError(CodeDatabaseMismatch, path, fixDBMismatch,
			"Debezium's replication slot %q belongs to database %q, not %q", name, r.database, db)
	case r.active:
		// D3: Debezium must be stopped before Conduit reads (fresh mode).
		return newError(CodeSlotActive, path, "Stop the Debezium connector (`PUT /connectors/{name}/stop`) and wait for its slot to become inactive.",
			"Debezium's replication slot %q is still in use (%s)", name, r.activeHolder())
	case r.plugin != imp.Setting.Plugin:
		// S4: the partial rule's proof rests on the plugin Debezium used.
		// The slot is the ground truth; the setting must agree with it.
		return newError(CodePluginUnsupported, SettingPath+".plugin", fixPluginUnsupported,
			"the setting says Debezium used plugin %q, but its slot %q uses %q", imp.Setting.Plugin, name, r.plugin)
	case r.confirmed == nil || *r.confirmed < imp.ConsistentPoint:
		return newError(CodeDatabaseMismatch, path, fixDBMismatch,
			"Debezium's replication slot %q has confirmed_flush_lsn %v, below Conduit's consistent point %s: "+
				"it never passed the cutover point on this server", name, lsnOrNull(r.confirmed), imp.ConsistentPoint)
	}
	return nil
}

func lsnOrNull(l *pglogrepl.LSN) string {
	if l == nil {
		return "NULL"
	}
	return l.String()
}

// qualify gives a Debezium table name its schema, "public" when it has none.
// The CLI writes Debezium's list schema-qualified.
func qualify(table string) string {
	if strings.Contains(table, ".") {
		return table
	}
	return "public." + table
}

// resolveTables maps the pipeline's table names to "schema.table" as
// Postgres resolves them. The connector quotes each name as one identifier
// (internal.WrapSQLIdent), so resolution follows the session's search_path.
func resolveTables(ctx context.Context, q Querier, tables []string) ([]string, error) {
	out := make([]string, 0, len(tables))
	for _, tbl := range tables {
		var name *string
		if err := q.QueryRow(ctx, `SELECT n.nspname || '.' || c.relname FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass($1)`,
			internal.WrapSQLIdent(tbl)).Scan(&name); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("import preflight: failed to resolve table %q: %w", tbl, err)
		}
		if name == nil {
			return nil, newError(CodeTablesNotCaptured, "tables", fixNotCaptured, "table %q does not exist", tbl)
		}
		out = append(out, *name)
	}
	return out, nil
}

func checkPublication(ctx context.Context, q Querier, publication string, tables []string) error {
	var exists bool
	if err := q.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication).Scan(&exists); err != nil {
		return fmt.Errorf("import preflight: failed to read publication %q: %w", publication, err)
	}
	if !exists {
		return newError(CodePublicationMismatch, "logrepl.publicationName", fixPublication,
			"publication %q does not exist; import mode never creates one", publication)
	}
	rows, err := q.Query(ctx, "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1", publication)
	if err != nil {
		return fmt.Errorf("import preflight: failed to read publication %q: %w", publication, err)
	}
	covered := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("import preflight: failed to read publication %q: %w", publication, err)
		}
		covered[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("import preflight: failed to read publication %q: %w", publication, err)
	}
	var missing []string
	for _, tbl := range tables {
		if !covered[tbl] {
			missing = append(missing, tbl)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return newError(CodePublicationMismatch, "logrepl.publicationName", fixPublication,
			"publication %q does not cover the pipeline's tables %s", publication, strings.Join(missing, ", "))
	}
	return nil
}

func checkTablesCaptured(captured, pipeline []string) error {
	set := map[string]bool{}
	for _, c := range captured {
		set[qualify(c)] = true
	}
	var missing []string
	for _, p := range pipeline {
		if !set[p] {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return newError(CodeTablesNotCaptured, "tables", fixNotCaptured,
			"Debezium did not capture %s, so its offset says nothing about them", strings.Join(missing, ", "))
	}
	return nil
}

func checkConduitSlot(ctx context.Context, q Querier, lc pglogrepl.LSN, name string) (SlotState, error) {
	r, ok, err := readSlot(ctx, q, name)
	if err != nil {
		return SlotState{}, err
	}
	const path = "logrepl.slotName"
	var db string
	if ok {
		if err := q.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
			return SlotState{}, fmt.Errorf("import preflight: failed to read current_database(): %w", err)
		}
	}
	switch {
	case !ok:
		return SlotState{}, newError(CodeSlotMissing, path, fixSlotMissing, "replication slot %q does not exist", name)
	case r.database != db:
		return SlotState{}, newError(CodeSlotDatabase, path, fixSlotDatabase,
			"replication slot %q belongs to database %q, not %q", name, r.database, db)
	case r.slotType != "logical" || r.plugin != PluginPgoutput:
		return SlotState{}, newError(CodeSlotPlugin, path, fixSlotPlugin,
			"replication slot %q is a %s slot with plugin %q; this connector reads pgoutput", name, r.slotType, r.plugin)
	case r.active:
		return SlotState{}, newError(CodeSlotActive, path, fixSlotActive,
			"replication slot %q is in use (%s)", name, r.activeHolder())
	case r.temporary:
		return SlotState{}, newError(CodeSlotPlugin, path, fixSlotPlugin,
			"replication slot %q is temporary; it disappears with its session", name)
	case r.walStatus == "lost" || r.invalidation != "":
		return SlotState{}, newError(CodeSlotInvalidated, path, fixInvalidated,
			"replication slot %q is invalidated (wal_status %q, invalidation_reason %q)", name, r.walStatus, r.invalidation)
	case r.confirmed == nil || r.restart == nil:
		return SlotState{}, newError(CodeSlotInvalidated, path, fixInvalidated,
			"replication slot %q has no confirmed_flush_lsn or restart_lsn", name)
	case *r.confirmed > lc:
		// Invariant 3 (D5 check 5): Postgres re-sends only transactions
		// committing at or past F. With F > Lc, the changes in (Lc, F] are
		// neither re-sent nor covered by the offset. No override (Q2).
		return SlotState{}, newError(CodeSlotAhead, path, fixSlotAhead,
			"replication slot %q has confirmed_flush_lsn %s, past Debezium's lsn_commit %s", name, *r.confirmed, lc)
	}
	return SlotState{ConfirmedFlushLSN: *r.confirmed, RestartLSN: *r.restart, WALStatus: r.walStatus}, nil
}

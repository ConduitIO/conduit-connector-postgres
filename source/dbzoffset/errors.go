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
	"errors"
	"fmt"
)

// Code is a stable, machine-actionable error code (D11). Codes are a public
// contract: `conduit migrate kafka-connect` passes them through to its JSON
// output and maps them to exit codes. A code is never renamed or reused for a
// different cause.
type Code string

// The connector's import codes (D11). This package raises the offset and
// setting codes; the preflight and the stream wiring raise the slot, server
// and publication codes. They are all declared here so the CLI and the
// connector share one list.
const (
	// CodeOffsetInvalid: unparseable JSON, a wrong format, a missing field the
	// setting requires, an LSN or xid that does not fit.
	CodeOffsetInvalid Code = "postgres.import.offset_invalid"
	// CodeOffsetUnsupported: the offset has no lsn_commit (older than late
	// Debezium 1.x).
	CodeOffsetUnsupported Code = "postgres.import.offset_unsupported"
	// CodeSnapshotInProgress: an initial or blocking snapshot was running, or
	// it had completed but no streaming offset was committed yet (D7).
	CodeSnapshotInProgress Code = "postgres.import.snapshot_in_progress"
	// CodeIncrementalSnapshotInProgress: an incremental snapshot was running.
	CodeIncrementalSnapshotInProgress Code = "postgres.import.incremental_snapshot_in_progress"
	// CodeSnapshotModeConflict: the import is set with a snapshot mode other
	// than never.
	CodeSnapshotModeConflict Code = "postgres.import.snapshot_mode_conflict"
	// CodeCDCModeConflict: the import is set with a CDC mode other than
	// logrepl (S10).
	CodeCDCModeConflict Code = "postgres.import.cdc_mode_conflict"
	// CodeSlotReuseUnsupported: logrepl.slotName equals Debezium's slot, or
	// the CLI was given --slot-strategy reuse. The single code for every
	// reuse request in v0.21 (Q4).
	CodeSlotReuseUnsupported Code = "postgres.import.slot_reuse_unsupported"
	// CodeSlotMissing: Conduit's slot does not exist. Import mode never
	// creates one.
	CodeSlotMissing Code = "postgres.import.slot_missing"
	// CodeSlotPlugin: Conduit's slot is not pgoutput.
	CodeSlotPlugin Code = "postgres.import.slot_plugin"
	// CodeSlotActive: the slot is in use by another consumer.
	CodeSlotActive Code = "postgres.import.slot_active"
	// CodeSlotInvalidated: the slot lost its WAL.
	CodeSlotInvalidated Code = "postgres.import.slot_invalidated"
	// CodeSlotAhead: the slot's confirmed_flush_lsn is above lsn_commit, so
	// Postgres no longer re-sends changes the offset does not cover. No
	// override, in any version (Q2).
	CodeSlotAhead Code = "postgres.import.slot_ahead"
	// CodeSlotDatabase: the slot is on another database.
	CodeSlotDatabase Code = "postgres.import.slot_database"
	// CodeOffsetFromFuture: lsn_commit is above the server's current WAL, or
	// the server is a standby.
	CodeOffsetFromFuture Code = "postgres.import.offset_from_future"
	// CodeDatabaseMismatch: database, system identifier or the S13 slot proof
	// does not match this server.
	CodeDatabaseMismatch Code = "postgres.import.database_mismatch"
	// CodePluginUnsupported: Debezium's decoding plugin is absent, wal2json,
	// wal2json_streaming or unknown (S4).
	CodePluginUnsupported Code = "postgres.import.plugin_unsupported"
	// CodeServerUnpatched: the server is below the first minor release with
	// the partial-decoding fix (B1).
	CodeServerUnpatched Code = "postgres.import.server_unpatched"
	// CodeServerUnsupported: Postgres 13 or earlier (a policy choice, B1).
	CodeServerUnsupported Code = "postgres.import.server_unsupported"
	// CodePublicationMismatch: the publication is missing or does not cover
	// the pipeline's tables.
	CodePublicationMismatch Code = "postgres.import.publication_mismatch"
	// CodeTablesNotCaptured: the pipeline has tables Debezium did not capture.
	CodeTablesNotCaptured Code = "postgres.import.tables_not_captured"
)

// SettingPath is the config path every import error points at.
const SettingPath = "logrepl.import.debeziumOffset"

// Fix texts (D11, D7, S5). They are part of the error contract: an operator,
// or an agent, should be able to act on the message alone.
const (
	fixReexport = "Re-export the offset with `conduit migrate kafka-connect --dry-run`; " +
		"the setting is written by the CLI and is not meant to be edited by hand."
	fixOffsetUnsupported = "This offset has no `lsn_commit`. Run the connector on a Debezium version that " +
		"writes it (Debezium 2.x or 3.x; this connector's fixtures cover 2.7 and 3.2), let it commit an " +
		"offset after at least one transaction, then retry."
	fixSnapshotRunning  = "Let the Debezium snapshot finish and Debezium commit a streaming offset, then retry."
	fixSnapshotNoStream = "Debezium needs a committed transaction followed by a later record. Write two " +
		"separate transactions to a captured table, or set `heartbeat.action.query` to an insert or update on " +
		"a captured table. Wait for an offset flush (`offset.flush.interval.ms`), then retry."
	fixIncrementalSnapshot = "Let the incremental snapshot finish, or stop it with Debezium's `stop-snapshot` " +
		"signal, then retry."
	fixSlotAheadOffline = "Debezium's committed offset never reached Conduit's slot. Go back to Debezium with " +
		"the manual runbook (gated by `conduit migrate kafka-connect --check-rollback`): Debezium's slot still " +
		"holds everything from its offset. Then run the cutover again; it waits until the offset passes the slot."
	fixPluginUnsupported = "This offset is not importable. Re-snapshot (a new pipeline with " +
		"`snapshotMode: initial`), or dual-run Debezium and Conduit and cut over without an offset."
)

// Error is an import refusal. Its message starts with the code, as every
// coded error in this connector does (`postgres.import.slot_ahead: ...`), so
// the code survives any layer that only keeps the string. Callers that keep
// the value use errors.As or CodeOf.
type Error struct {
	// Code is the stable code.
	Code Code
	// Path is the config path the error is about, usually SettingPath, or a
	// field inside it such as "logrepl.import.debeziumOffset.offset.txId".
	Path string
	// Message says what is wrong, with the offending value.
	Message string
	// Fix says what to do about it.
	Fix string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Path)
	if e.Fix != "" {
		s += ". Fix: " + e.Fix
	}
	return s
}

// CodeOf returns the code of the first *Error in err's chain, or "" if there
// is none.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func newError(code Code, path, fix, format string, args ...any) *Error {
	return &Error{Code: code, Path: path, Message: fmt.Sprintf(format, args...), Fix: fix}
}

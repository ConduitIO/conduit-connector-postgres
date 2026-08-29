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

// Package chaospoint provides named fault-injection points for the DBZ-3
// process-kill chaos harness (test/chaos, ConduitIO/conduit-connector-postgres,
// see docs/design-documents for the harness design).
//
// Production code calls Reach(name) unconditionally at a small number of
// call sites. In the default build (no build tags) Reach is an empty
// function the compiler inlines away — this file, names.go, carries no
// build tag and is always compiled, but it declares only point-name
// constants, never behavior, so it cannot itself change what a released
// binary does.
//
// The behavior lives behind the conduitchaos build tag:
//   - chaospoint.go (!conduitchaos): Reach is a no-op. This is what every
//     released artifact ships, because .goreleaser.yml and
//     .github/workflows/publish.yml both build with no -tags.
//   - chaospoint_on.go (conduitchaos): Reach counts each reach of a named
//     point and, when PGCHAOS_PARK targets that point's Nth reach, announces
//     PARKED on stdout and blocks the calling goroutine forever. This is
//     what test/chaos links against to prove a kill lands at an exact,
//     provably-suspended point instead of a wall-clock guess.
//
// The build tag is a security boundary, not a style preference: an
// untagged, always-live env-var injector would ship a "park this connector
// forever" switch to every user, where a mistyped or hostile PGCHAOS_PARK
// becomes a production hang.
package chaospoint

// Point names identify a specific call site for PGCHAOS_PARK, in the form
// "<point>:<nth>" (e.g. "snapshot.fetch.row:3" parks at the 3rd reach of
// SnapshotFetchRow). Names are dotted strings rather than relying on Go
// identifier reflection, because they are also the human-typed half of that
// env var — anyone constructing PGCHAOS_PARK works from this file.
//
// Adding, renaming, or removing a call site must update this file in the
// same change: it is the only place the three call sites are enumerated
// together, and the chaos harness's CI job (go build/vet -tags conduitchaos)
// exists specifically so a rename here or at a call site fails loudly
// instead of silently breaking the harness.
const (
	// SnapshotFetchRow is reached in source/snapshot/fetch_worker.go's fetch
	// loop, once per row, immediately after the row is appended to the
	// pending batch. Parking here proves a kill mid-snapshot: the row is in
	// memory but not yet sent to the destination.
	SnapshotFetchRow = "snapshot.fetch.row"

	// PreStartSubscriber is reached as the first statement of
	// source/logrepl/combined.go's useCDCIterator, before the snapshot
	// iterator is torn down, before the low-watermark is re-seeded onto the
	// CDC handler, and before the CDC subscriber starts. Parking here proves
	// a kill exactly at the snapshot->CDC handoff, before any CDC-side state
	// (including the watermark carry-forward) has been touched.
	PreStartSubscriber = "combined.pre_start_subscriber"

	// StandbyStatusUpdate is reached in
	// source/logrepl/internal/subscription.go's sendStandbyStatusUpdate,
	// after walFlushed is loaded and the reply-with-WAL-end decision is
	// computed, but before either status update variant is written to the
	// replication connection. Parking here proves a kill between "the
	// engine has decided what the server should learn about flush progress"
	// and "the server actually learned it" — the window in which a
	// crash can leave the slot's confirmed_flush_lsn stale relative to what
	// was actually durably acked.
	StandbyStatusUpdate = "subscription.standby_status"

	// DriftVersionRecorded is reached as the first statement of
	// source/logrepl/handler.go's emitDriftMarker, before the marker record
	// exists. Parking here proves a kill after the new schema shape was
	// durably recorded in the position's schema history (RecordSchemaVersion)
	// but before any marker record was queued — the FM3 kill window of the B1
	// design doc (restart must halt again via driftAcrossRestart, and never
	// duplicate the marker).
	DriftVersionRecorded = "logrepl.drift_version_recorded"

	// DriftMarkerSeen is reached in test/chaos/child.go's read loop after a
	// drift marker was read from the connector but before it is appended to
	// the ledger: the marker is in the iterator's channel (delivered) but not
	// durably persisted — the FM2 kill window (restart resumes below the
	// marker and must halt again via driftAcrossRestart).
	DriftMarkerSeen = "child.drift_marker_seen"

	// DriftMarkerAppended is reached in test/chaos/child.go's read loop after
	// a drift marker was appended to the ledger (fsync-before-return) but
	// before it is acked — the FM1 kill window. The B1 design doc asserts this
	// window is observable (the marker record exists and is durably
	// checkpointed), not prevented: a restart resumes from the marker, which
	// IS the operator's approval.
	DriftMarkerAppended = "child.drift_marker_appended"
)

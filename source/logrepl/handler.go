// Copyright © 2022 Meroxa, Inc.
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

package logrepl

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	sdk "github.com/conduitio/conduit-connector-sdk"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jackc/pglogrepl"
)

// CDCHandler is responsible for handling logical replication messages,
// converting them to a record and sending them to a channel.
type CDCHandler struct {
	tableKeys   map[string]string
	relationSet *internal.RelationSet

	// batchSize is the largest number of records this handler will send at once.
	batchSize     int
	flushInterval time.Duration

	// recordBatch holds the batch that is currently being built.
	recordBatch     []opencdc.Record
	recordBatchLock sync.Mutex

	// out is a sending channel with batches of records.
	out            chan<- []opencdc.Record
	lastTXLSN      pglogrepl.LSN
	withAvroSchema bool
	keySchemas     map[string]cschema.Schema
	payloadSchemas map[string]cschema.Schema

	// basePosition holds the position the connector was started with. Its
	// carry-forward fields (currently SnapshotLowWatermarkLSN; DBZ-3 Area 2 will
	// add SchemaHistory) are threaded, unchanged, into every CDC-mode position by
	// buildPosition. Without this, buildPosition would mint a field-sparse
	// Position{Type: CDC, LastLSN} for every record and silently drop those
	// fields the instant the snapshot->CDC handoff completes — see the DBZ-3
	// design doc's "Position carry-forward is an implementation requirement"
	// section (acceptance criterion 11). It is written at construction and may be
	// re-seeded exactly once more at the snapshot->CDC handoff via
	// setBasePositionLowWatermark (see there for why the handoff re-seed is
	// required and why it is race-free). All reads happen on the single
	// subscription goroutine via Handle, which does not start until
	// StartSubscriber; both writes happen before that, so it needs no locking.
	basePosition position.Position

	// schemaDriftPolicy is the configured drift policy (halt unless configured
	// otherwise). Written at construction; read on the subscription goroutine
	// (handleRelation) and nowhere else, so it needs no locking.
	schemaDriftPolicy SchemaDriftPolicy

	// Drift-halt state (DBZ-3 B1, D3). The invariant: the halt error is
	// surfaced only after the marker record has been acked, because the engine
	// persists a record's position before acking it — an acked marker is proof
	// the new schema shape is checkpointed, which is exactly what makes a
	// restart an approval rather than a rollback.
	//
	// The SDK's batch middleware can run raw ReadN and raw Ack on different
	// goroutines, so the state lives here, on the handler, and each field has
	// exactly one writer:
	//
	//   - driftPending* is written on the subscription goroutine
	//     (handleRelation, when the policy halts on the drift) and read on the
	//     same goroutine (the next DML message). pgoutput delivers
	//     RelationMessage with XLogData.WALStart == 0 (verified on 2026-08-29;
	//     the relation message carries no WAL position on the wire), so the
	//     marker cannot be emitted with the relation message's LSN: it is
	//     emitted by the first DML that uses the new shape, which pgoutput
	//     always sends immediately after the relation message, and that DML's
	//     LSN becomes the marker's position (D1 deviation, documented in the
	//     B1 design doc's AC evidence). Between detection and emission nothing
	//     is special: the drift boundary has not been fixed yet, so records
	//     may still flow and be acked at positions below the future marker.
	//     driftPendingHistory is a DEEP COPY of the schema history taken at
	//     staging time, with the staged shape itself appended: the marker's
	//     position is built from it, never from the live history at emission,
	//     so a DDL that lands between staging and emission is not checkpointed
	//     by the first marker (review Blocker 1). The staged shape is
	//     deliberately NOT committed to basePosition at the sighting (re-review
	//     should-fix on the B1 drift policy): every regular record's position
	//     is serialized from basePosition, so a sighting-time commit would leak
	//     the shape into unrelated records' positions, get checkpointed
	//     (persist-before-ack), and dedupe the drift away on a restart before
	//     the drifted table's own DML. The commit happens at marker EMISSION.
	//     The marker fires only on a DML of the staged drifted relation itself
	//     (review should-fix 3): an unrelated table's DML flows normally until
	//     the drifted table's own next DML emits it.
	//   - driftMarkerLSN is written on the subscription goroutine
	//     (emitDriftMarker, after the marker is queued) and read on the engine
	//     goroutine (CDCIterator.Ack -> maybeArmDriftHalt, the D4 skip checks,
	//     and the F6 marker-preference selects). 0 means no marker is pending.
	//   - driftHaltArmed is written on the engine goroutine (Ack ->
	//     maybeArmDriftHalt) and read on the engine goroutine (NextN). It gates
	//     surfacing the error.
	//   - driftHaltErr is written by emitDriftMarker before driftMarkerLSN is
	//     published, and read only after driftHaltArmed is observed set; the
	//     sequentially-consistent atomic store/load orders the plain write
	//     before the read.
	//   - driftHaltCh is closed once, by the arming ack, to wake a NextN that
	//     is already blocked waiting for the marker's batch (F6) — without it,
	//     a halted pipeline whose read-ahead goroutine is inside one long
	//     NextN call would never see the error.
	//
	// No locks (D8): the atomics above order every read after its write.
	driftPendingRel     *pglogrepl.RelationMessage
	driftPendingKind    driftKind
	driftPendingDiff    internal.SchemaDiff
	driftPendingPrev    position.SchemaVersion
	driftPendingHash    string
	driftPendingHistory position.SchemaHistories
	driftMarkerLSN      atomic.Uint64
	driftHaltArmed      atomic.Bool
	driftHaltErr        error
	driftHaltCh         chan struct{}
}

func NewCDCHandler(
	ctx context.Context,
	rs *internal.RelationSet,
	tableKeys map[string]string,
	out chan<- []opencdc.Record,
	withAvroSchema bool,
	batchSize int,
	flushInterval time.Duration,
	startPosition position.Position,
	schemaDriftPolicy SchemaDriftPolicy,
) *CDCHandler {
	h := &CDCHandler{
		tableKeys:         tableKeys,
		relationSet:       rs,
		recordBatch:       make([]opencdc.Record, 0, batchSize),
		out:               out,
		withAvroSchema:    withAvroSchema,
		keySchemas:        make(map[string]cschema.Schema),
		payloadSchemas:    make(map[string]cschema.Schema),
		batchSize:         batchSize,
		flushInterval:     flushInterval,
		basePosition:      startPosition,
		schemaDriftPolicy: schemaDriftPolicy,
		driftHaltCh:       make(chan struct{}),
	}

	go h.scheduleFlushing(ctx)

	return h
}

func (h *CDCHandler) scheduleFlushing(ctx context.Context) {
	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.flush(ctx)
		}
	}
}

func (h *CDCHandler) flush(ctx context.Context) {
	h.recordBatchLock.Lock()
	defer h.recordBatchLock.Unlock()

	if len(h.recordBatch) == 0 {
		return
	}

	select {
	case <-ctx.Done():
		sdk.Logger(ctx).Warn().
			Err(ctx.Err()).
			Int("records", len(h.recordBatch)).
			Msg("CDCHandler flushing records cancelled")
	case h.out <- h.recordBatch:
		sdk.Logger(ctx).Debug().
			Int("records", len(h.recordBatch)).
			Msg("CDCHandler sending batch of records")
		h.recordBatch = make([]opencdc.Record, 0, h.batchSize)
	}
}

// Handle is the handler function that receives all logical replication messages.
// Returns non-zero LSN when a record was emitted for the message.
func (h *CDCHandler) Handle(ctx context.Context, m pglogrepl.Message, lsn pglogrepl.LSN) (pglogrepl.LSN, error) {
	sdk.Logger(ctx).Trace().
		Str("lsn", lsn.String()).
		Str("messageType", m.Type().String()).
		Msg("handler received pglogrepl.Message")

	switch m := m.(type) {
	case *pglogrepl.RelationMessage:
		// We have to add the Relations to our Set so that we can decode our own output
		// D2: when the drift policy halts on this relation message, the marker is
		// staged (not yet emitted) — pgoutput sends the RelationMessage with
		// WALStart 0, so it has no usable LSN. The marker is emitted by the first
		// DML that uses the new shape, and the DML cases below return that LSN,
		// which advances the subscription's walWritten past the marker so the next
		// standby status update can ack it without tripping the walFlushed >
		// walWritten guard (subscription.go:402-404).
		_, _ = h.handleRelation(ctx, m, lsn)
		return 0, nil
	case *pglogrepl.InsertMessage:
		if err := h.handleInsert(ctx, m, lsn); err != nil {
			return 0, fmt.Errorf("logrepl handler insert: %w", err)
		}
		return lsn, nil
	case *pglogrepl.UpdateMessage:
		if err := h.handleUpdate(ctx, m, lsn); err != nil {
			return 0, fmt.Errorf("logrepl handler update: %w", err)
		}
		return lsn, nil
	case *pglogrepl.DeleteMessage:
		if err := h.handleDelete(ctx, m, lsn); err != nil {
			return 0, fmt.Errorf("logrepl handler delete: %w", err)
		}
		return lsn, nil
	case *pglogrepl.BeginMessage:
		h.lastTXLSN = m.FinalLSN
	case *pglogrepl.CommitMessage:
		if h.lastTXLSN != 0 && h.lastTXLSN != m.CommitLSN {
			return 0, fmt.Errorf("out of order commit %s, expected %s", m.CommitLSN, h.lastTXLSN)
		}
	}

	return 0, nil
}

// handleInsert formats a Record with INSERT event data from Postgres and sends
// it to the output channel.
func (h *CDCHandler) handleInsert(
	ctx context.Context,
	msg *pglogrepl.InsertMessage,
	lsn pglogrepl.LSN,
) error {
	// D1/D2 deviation: the drift marker is emitted here, at the LSN of the
	// first DML that uses the new shape — pgoutput delivers the RelationMessage
	// with WALStart 0, so the relation message itself has no usable position.
	// This DML always follows its relation message (pgoutput emits the
	// relation message immediately before the first DML using it), so the
	// marker LSN is the earliest real WAL position at the drift boundary.
	// The emission is gated on this DML's relation being the staged drifted
	// one (should-fix 3): an unrelated table's DML flows normally below.
	h.emitPendingDriftMarkerIfMatching(ctx, lsn, msg.RelationID)

	if h.driftMarkerPending() {
		// D4: once the drift marker is emitted, emit nothing — not even for
		// other relations. A record past the marker LSN would let positions
		// advance beyond the marker, and the drifted table's skipped DML below
		// the resume point would then be lost on the operator-approved restart
		// (invariant 3). Skipping is safe: every position up to and including
		// the marker's LSN is at or below the first new-shape DML, so a restart
		// resumes before it and the skip repeats, with the restart-approval
		// admitting the shape. Skipping also avoids emitting old-shape
		// projections through a schema the connector no longer believes in
		// (invariant 6).
		sdk.Logger(ctx).Debug().
			Str("lsn", lsn.String()).
			Str("relationID", fmt.Sprintf("%d", msg.RelationID)).
			Msg("skipping record after schema-drift marker (D4: the marker, then nothing)")
		return nil
	}

	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return fmt.Errorf("failed getting relation %v: %w", msg.RelationID, err)
	}

	// Backfill the shape's FirstSeenLSN (RecordSchemaVersion saw only the
	// relation message's WALStart 0): the first DML using a shape is its true
	// first-seen position, which the drift-halt message reports. See
	// position.SetFirstSeenLSN.
	h.basePosition.SetFirstSeenLSN(relationKey(rel), lsn.String())

	newValues, err := h.relationSet.Values(msg.RelationID, msg.Tuple)
	if err != nil {
		return fmt.Errorf("failed to decode new values: %w", err)
	}

	if err := h.updateAvroSchema(ctx, rel); err != nil {
		return fmt.Errorf("failed to update avro schema: %w", err)
	}

	rec := sdk.Util.Source.NewRecordCreate(
		h.buildPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(newValues, rel.RelationName),
		h.buildRecordPayload(newValues),
	)
	h.attachSchemas(rec, rel.RelationName)
	h.addToBatch(ctx, rec)

	return nil
}

// handleUpdate formats a record with UPDATE event data from Postgres and sends
// it to the output channel.
func (h *CDCHandler) handleUpdate(
	ctx context.Context,
	msg *pglogrepl.UpdateMessage,
	lsn pglogrepl.LSN,
) error {
	h.emitPendingDriftMarkerIfMatching(ctx, lsn, msg.RelationID)

	if h.driftMarkerPending() {
		// D4: see handleInsert. Global across relations for the same reason.
		sdk.Logger(ctx).Debug().
			Str("lsn", lsn.String()).
			Str("relationID", fmt.Sprintf("%d", msg.RelationID)).
			Msg("skipping record after schema-drift marker (D4: the marker, then nothing)")
		return nil
	}

	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return err
	}

	// Backfill the shape's FirstSeenLSN — see handleInsert.
	h.basePosition.SetFirstSeenLSN(relationKey(rel), lsn.String())

	newValues, err := h.relationSet.Values(msg.RelationID, msg.NewTuple)
	if err != nil {
		return fmt.Errorf("failed to decode new values: %w", err)
	}

	if err := h.updateAvroSchema(ctx, rel); err != nil {
		return fmt.Errorf("failed to update avro schema: %w", err)
	}

	oldValues, err := h.relationSet.Values(msg.RelationID, msg.OldTuple)
	if err != nil {
		// this is not a critical error, old values are optional, just log it
		// we use level "trace" intentionally to not clog up the logs in production
		sdk.Logger(ctx).Trace().Err(err).Msg("could not parse old values from UpdateMessage")
	}

	// Invariant 6: RelationSet.Values omits columns that arrived as unchanged
	// TOASTed values (Postgres sends no data for them) instead of reporting
	// them as NULL. If the old tuple has the real value — which happens with
	// REPLICA IDENTITY FULL, where OldTuple carries the full previous row —
	// backfill it into newValues so the emitted payload reflects the
	// (unchanged) value rather than dropping the field. With the default
	// REPLICA IDENTITY, OldTuple only has key columns, so non-key TOASTed
	// columns stay omitted from newValues; that is the documented fallback,
	// never a silent NULL.
	for col, oldVal := range oldValues {
		if _, ok := newValues[col]; !ok {
			newValues[col] = oldVal
		}
	}

	rec := sdk.Util.Source.NewRecordUpdate(
		h.buildPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(newValues, rel.RelationName),
		h.buildRecordPayload(oldValues),
		h.buildRecordPayload(newValues),
	)
	h.attachSchemas(rec, rel.RelationName)
	h.addToBatch(ctx, rec)

	return nil
}

// handleDelete formats a record with DELETE event data from Postgres and sends
// it to the output channel. Deleted records only contain the key and no payload.
func (h *CDCHandler) handleDelete(
	ctx context.Context,
	msg *pglogrepl.DeleteMessage,
	lsn pglogrepl.LSN,
) error {
	h.emitPendingDriftMarkerIfMatching(ctx, lsn, msg.RelationID)

	if h.driftMarkerPending() {
		// D4: see handleInsert. Global across relations for the same reason.
		sdk.Logger(ctx).Debug().
			Str("lsn", lsn.String()).
			Str("relationID", fmt.Sprintf("%d", msg.RelationID)).
			Msg("skipping record after schema-drift marker (D4: the marker, then nothing)")
		return nil
	}

	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return err
	}

	// Backfill the shape's FirstSeenLSN — see handleInsert.
	h.basePosition.SetFirstSeenLSN(relationKey(rel), lsn.String())

	oldValues, err := h.relationSet.Values(msg.RelationID, msg.OldTuple)
	if err != nil {
		return fmt.Errorf("failed to decode old values: %w", err)
	}

	if err := h.updateAvroSchema(ctx, rel); err != nil {
		return fmt.Errorf("failed to update avro schema: %w", err)
	}

	rec := sdk.Util.Source.NewRecordDelete(
		h.buildPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(oldValues, rel.RelationName),
		h.buildRecordPayload(oldValues),
	)
	h.attachSchemas(rec, rel.RelationName)
	h.addToBatch(ctx, rec)

	return nil
}

// addToBatch the record to the output channel or detect the cancellation of the
// context and return the context error.
func (h *CDCHandler) addToBatch(ctx context.Context, rec opencdc.Record) {
	h.recordBatchLock.Lock()

	h.recordBatch = append(h.recordBatch, rec)
	currentBatchSize := len(h.recordBatch)

	sdk.Logger(ctx).Trace().
		Int("current_batch_size", currentBatchSize).
		Msg("CDCHandler added record to batch")

	h.recordBatchLock.Unlock()

	if currentBatchSize >= h.batchSize {
		h.flush(ctx)
	}
}

func (h *CDCHandler) buildRecordMetadata(rel *pglogrepl.RelationMessage) map[string]string {
	m := map[string]string{
		opencdc.MetadataCollection: rel.RelationName,
	}

	return m
}

// buildRecordKey takes the values from the message and extracts the key that
// matches the configured keyColumnName.
func (h *CDCHandler) buildRecordKey(values map[string]any, table string) opencdc.Data {
	keyColumn := h.tableKeys[table]
	key := make(opencdc.StructuredData)
	for k, v := range values {
		if keyColumn == k {
			key[k] = v
			break // TODO add support for composite keys
		}
	}
	return key
}

// buildRecordPayload takes the values from the message and extracts the payload
// for the record.
func (h *CDCHandler) buildRecordPayload(values map[string]any) opencdc.Data {
	if len(values) == 0 {
		return nil
	}
	return opencdc.StructuredData(values)
}

// buildPosition builds the position for a CDC record at the given LSN, carrying
// forward the DBZ-3 fields from basePosition unchanged (currently just
// SnapshotLowWatermarkLSN; Area 2 will also carry SchemaHistory here, and update
// it in place only when its diff logic records a new relation version). Only
// Type and LastLSN are set per record. Snapshots is intentionally NOT carried
// forward: it is snapshot-phase cursor state with no meaning in CDC mode, and
// copying it would bloat and change the shape of every CDC position.
//
// Invariant 2 (monotonic, crash-safe positions): the carry-forward must be
// unconditional per record. If any single CDC position dropped
// SnapshotLowWatermarkLSN, a restart landing on that position would regress to
// legacy (Version 0 / Finding-1) behavior even on a connector that has run well
// past its first snapshot — the intermittent regression the design doc calls out.
func (h *CDCHandler) buildPosition(lsn pglogrepl.LSN) opencdc.Position {
	return position.Position{
		Type:                    position.TypeCDC,
		LastLSN:                 lsn.String(),
		SnapshotLowWatermarkLSN: h.basePosition.SnapshotLowWatermarkLSN,
		SchemaHistory:           h.basePosition.SchemaHistory,
	}.ToSDKPosition()
}

// setBasePositionLowWatermark re-seeds the SnapshotLowWatermarkLSN carried
// forward by buildPosition (DBZ-3 Area 1, acceptance criterion 11). It is called
// once, at the snapshot->CDC handoff, before the subscription goroutine starts —
// see CDCIterator.SetSnapshotLowWatermarkLSN for the concurrency contract and the
// reason the handoff re-seed is load-bearing (on a first run the watermark is not
// known when the handler is constructed, only after the slot is created, so it
// must be re-applied before CDC positions are built).
func (h *CDCHandler) setBasePositionLowWatermark(lsn string) {
	h.basePosition.SnapshotLowWatermarkLSN = lsn
}

// driftKind classifies what a RelationMessage means relative to everything
// known about that table's shape.
//
// It is returned by handleRelation so the decision is assertable in a test
// rather than only observable in a log line, and it is the seam the drift policy
// (Area 2 step 3) attaches to: halt/dlq/evolve is a function of this kind plus
// SchemaDiff.IsIncompatible.
type driftKind int

const (
	// driftNone: the shape matches the last durable version. The common case —
	// Postgres re-sends a RelationMessage after a reconnect and when a new
	// subscriber attaches.
	driftNone driftKind = iota
	// driftInitial: first shape ever recorded for this table. Not drift; there
	// is nothing to compare against.
	driftInitial
	// driftInProcess: the shape changed while this process was running, so the
	// full column-level diff is available.
	driftInProcess
	// driftAcrossRestart: the shape differs from the last durable version but
	// this process saw no earlier shape, so the change happened while the
	// connector was down. Only the hash survived, so the affected columns are
	// not recoverable.
	driftAcrossRestart
)

// relationKey identifies a table in the durable schema history.
//
// Namespace+name, not RelationID: the ID is a pg_class OID, which a
// drop-and-recreate changes, and history keyed by it would silently start over
// for what an operator considers the same table. The name is also what appears
// in an operator-facing message.
func relationKey(r *pglogrepl.RelationMessage) string {
	return r.Namespace + "." + r.RelationName
}

// columnIdentities projects a RelationMessage onto the identity triple the
// schema hash is computed over. Kept in lockstep with diffRelations' notion of
// column identity — if the two disagreed, a shape could pass the hash
// comparison while the diff reported drift, or the reverse.
func columnIdentities(r *pglogrepl.RelationMessage) []position.ColumnIdentity {
	out := make([]position.ColumnIdentity, 0, len(r.Columns))
	for _, c := range r.Columns {
		out = append(out, position.ColumnIdentity{
			Name:         c.Name,
			DataType:     c.DataType,
			TypeModifier: c.TypeModifier,
		})
	}
	return out
}

// handleRelation caches a relation and reports schema drift against both the
// in-memory cache (drift within this process) and the durable history carried
// in the position (drift across a restart).
//
// The two are not redundant. The in-memory diff describes exactly what changed
// but is empty on every process start; the durable history survives restarts but
// stores only a hash, so it can prove the shape changed without saying how. A
// DDL applied while the connector was down is visible ONLY through the second.
//
// This step detects, reports, and — under the drift policy (DBZ-3 B1, Area 2
// step 3) — acts: when the policy halts on this drift, it records the new shape
// (already done below), emits the D1 marker record whose position carries the
// new shape at this RelationMessage's LSN, and returns that LSN so Handle can
// report it as written (D2). The halt itself is acked-gated (D3): the error
// surfaces only after the marker's ack.
//
// Concurrency: basePosition is read and written only here and in
// setBasePositionLowWatermark. Both run before or on the single subscription
// goroutine (see the basePosition field comment), so no locking is needed. The
// SchemaHistory map is handed to buildPosition by reference, but ToSDKPosition
// marshals to JSON eagerly, so no live reference to it ever escapes into an
// emitted position.
//
// The return value is the marker's LSN when a marker was emitted (D2), 0
// otherwise.
func (h *CDCHandler) handleRelation(ctx context.Context, r *pglogrepl.RelationMessage, lsn pglogrepl.LSN) (driftKind, pglogrepl.LSN) {
	diff := h.relationSet.Update(r)

	key := relationKey(r)
	hash := position.HashColumnSet(columnIdentities(r))

	// Read before any commit: RecordSchemaVersion mutates what LastSchemaVersion
	// returns.
	prev, hadHistory := h.basePosition.LastSchemaVersion(key)
	// A pure form of RecordSchemaVersion's "did the shape change" test. The
	// commit itself is deferred: accepted shapes (the non-halt branch below)
	// are recorded here, but a staged drift is recorded only at marker
	// EMISSION, never at the sighting — the live history must not absorb a
	// shape the drift decision rejected (re-review should-fix; see staging).
	changed := !hadHistory || prev.ColumnSetHash != hash

	var kind driftKind
	switch {
	case !changed:
		// Same shape as the last durable version. Postgres re-sends a
		// RelationMessage after a reconnect and when a new subscriber attaches,
		// so this is the common case and must stay silent.
		kind = driftNone
	case !hadHistory:
		// First shape ever recorded for this table — on a first run, or on the
		// first run after upgrading from a position that predates the history.
		// Nothing to compare against, so this is not drift.
		sdk.Logger(ctx).Debug().
			Str("table", key).
			Str("schema_hash", hash).
			Msg("recorded initial schema version for table")
		kind = driftInitial
	case diff.HasDrift():
		// Drift within this process: the full diff is available.
		sdk.Logger(ctx).Warn().
			Str("table", key).
			Str("lsn", lsn.String()).
			Bool("incompatible", diff.IsIncompatible()).
			Msg("schema drift detected: " + diff.String())
		kind = driftInProcess
	default:
		// The shape differs from the last durable version but this process has
		// no earlier shape to diff against: the change happened while the
		// connector was not running. Only the hash is retained, so this reports
		// THAT the schema changed, not which columns.
		sdk.Logger(ctx).Warn().
			Str("table", key).
			Str("lsn", lsn.String()).
			Str("previous_schema_hash", prev.ColumnSetHash).
			Str("previous_seen_lsn", prev.FirstSeenLSN).
			Str("current_schema_hash", hash).
			Msg("schema of table changed while the connector was not running; " +
				"the change happened before this process started, so the affected " +
				"columns cannot be reported — compare against your DDL history")
		kind = driftAcrossRestart
	}

	if !h.haltsOnDrift(kind, diff) {
		// Accepted shape (a re-sent relation message, the first sighting, or an
		// evolve-compatible change): commit it to the live history now.
		// Halt-worthy sightings never reach this line, so the live history
		// cannot absorb a shape the drift decision rejected.
		h.basePosition.RecordSchemaVersion(key, hash, lsn.String())
		return kind, 0
	}

	if h.driftMarkerPending() || h.driftPendingRel != nil {
		// FM8 chaospoint: reached after the second shape was classified as
		// halt-worthy and before the skip return. Parking here proves a kill
		// with the second sighting processed in-run while the first marker is
		// still pending — the in-run half of the FM8 window (AC9 parks the
		// down-variant at DriftMarkerAppended instead). No-op outside the
		// conduitchaos build.
		chaospoint.Reach(chaospoint.DriftVersionSkipped)

		// FM8 / AC9: a second DDL while a halt is already pending (marker
		// emitted, not yet acked) or detected (relation message seen, marker
		// not yet emitted). The version is deliberately NOT committed to the
		// live history: the first marker's staging snapshot predates this
		// sighting, so the marker never checkpointed it, and committing it
		// here would let a subsequent unrelated record's position serialize it
		// and dedupe the drift away on a restart (re-review should-fix). No
		// durable state claims this shape, so the restart re-delivers this
		// relation message and re-derives it as drift. Emitting a second
		// marker now would grow the history twice per halt and break the
		// "exactly one marker per halt" contract. Return no LSN: nothing new
		// was emitted.
		sdk.Logger(ctx).Warn().
			Str("table", key).
			Str("lsn", lsn.String()).
			Str("schema_hash", hash).
			Msg("schema changed again while a drift halt is pending; version " +
				"not committed, no second marker emitted (it will halt on restart)")
		return kind, 0
	}

	// Stage the halt. The marker is NOT emitted here: pgoutput sends the
	// RelationMessage with WALStart 0, so the relation message has no usable
	// position, and a marker position of 0/0 would both break the acked-gated
	// halt (the ack comparison) and produce a checkpoint with LSN 0. The
	// marker is emitted by the first DML that uses the new shape (see
	// emitDriftMarker), which pgoutput always sends immediately after the
	// relation message.
	//
	// The D5 error message is built at EMISSION time, not here: its "observed
	// at LSN" is the marker's LSN (the first new-shape DML), which only exists
	// once that DML arrives. Building it here with the relation message's
	// WALStart 0 produced a misleading "observed at LSN 0/0" (verified in the
	// B1 chaos harness, AC7 scenario).
	//
	// basePosition is deliberately NOT touched here (re-review should-fix):
	// the staged shape lives only in the staging snapshot below until the
	// marker is emitted. Every regular record's position is serialized from
	// basePosition (buildPosition) and checkpointed persist-before-ack, so a
	// sighting-time commit would leak the shape into unrelated records'
	// positions — and a restart from one of them, before the drifted table's
	// own DML, would dedupe the replayed relation message (driftNone) and
	// admit the drift with no halt, no approval, no disclosure.
	h.driftPendingRel = r
	h.driftPendingKind = kind
	h.driftPendingDiff = diff
	h.driftPendingPrev = prev
	h.driftPendingHash = hash
	// Snapshot the schema history at STAGING time, deep-copied, with the
	// staged shape itself appended. The marker is emitted later, by the first
	// new-shape DML, and its position must reflect exactly the state the halt
	// decision was made against — including the shape that decision is about
	// (review Blocker 1 on the B1 drift policy: a marker built from live
	// history at emission would checkpoint DDLs that landed after staging and
	// silently admit them on the restart).
	h.driftPendingHistory = h.basePosition.SchemaHistory.Clone()
	h.driftPendingHistory[key] = append(h.driftPendingHistory[key], position.SchemaVersion{ColumnSetHash: hash, FirstSeenLSN: lsn.String()})
	return kind, 0
}

// emitPendingDriftMarkerIfMatching emits the staged drift marker, but only on
// a DML of the staged drifted relation itself (adversarial-review should-fix 3
// on the B1 drift policy): firing on ANY DML would checkpoint the marker at an
// unrelated record's LSN and then drop that record (D4) — e.g. a drift staged
// on users could ride an orders insert, losing the orders record and
// mislabeling the marker. An unrelated DML is emitted normally and the marker
// stays pending for the drifted table's own next DML, which pgoutput always
// sends right after its relation message. Once the marker is emitted this is a
// no-op and the D4 skip in the caller handles the drop, so the skip still
// fires before any relation lookup (an unknown RelationID must not error at
// the gate — the D4 skip covers it).
//
// The marker's position is built from the schema-history snapshot taken at
// STAGING time (driftPendingHistory), never from live history at emission:
// handleRelation records every version it sees into basePosition.SchemaHistory
// the moment it sees it, so a second DDL landing between staging and emission
// would otherwise be checkpointed by the first marker and silently admitted on
// the restart (review Blocker 1). The staged snapshot keeps the marker
// position identical to the state the halt decision was made against.
func (h *CDCHandler) emitPendingDriftMarkerIfMatching(ctx context.Context, lsn pglogrepl.LSN, relationID uint32) {
	if h.driftPendingRel == nil {
		return
	}
	rel, err := h.relationSet.Get(relationID)
	if err != nil {
		// Relation unknown (a message whose RelationMessage never arrived).
		// Cannot prove this is the staged table: the marker stays pending and
		// this DML flows on; if it WAS the staged table's DML, the marker
		// fires on the next DML that resolves to it.
		return
	}
	if relationKey(rel) != relationKey(h.driftPendingRel) {
		return
	}
	h.emitPendingDriftMarker(ctx, lsn)
}

// emitPendingDriftMarker emits the staged drift marker at the LSN of the DML
// that triggered it, with the schema-history snapshot taken at staging time.
// Called only from emitPendingDriftMarkerIfMatching after the relation gate
// matched; the DML itself is never emitted (the D4 skip in the caller handles
// it).
func (h *CDCHandler) emitPendingDriftMarker(ctx context.Context, lsn pglogrepl.LSN) {
	if h.driftPendingRel == nil {
		return
	}
	h.emitDriftMarker(
		ctx, h.driftPendingRel, lsn,
		h.driftPendingKind, h.driftPendingDiff, h.driftPendingPrev, h.driftPendingHash,
		h.driftPendingHistory,
	)
	h.driftPendingRel = nil
	h.driftPendingHistory = nil
}

// haltsOnDrift decides whether the drift policy stops records for this drift.
//
// evolve halts only on incompatible (narrowing) changes and accepts additive
// ones silently; everything else halts on any drift. driftAcrossRestart always
// halts, even under evolve, because the diff is unavailable — there is no way
// to verify the change was additive-only (the connector cannot prove a column
// it never saw was merely added).
func (h *CDCHandler) haltsOnDrift(kind driftKind, diff internal.SchemaDiff) bool {
	switch kind {
	case driftNone, driftInitial:
		return false
	case driftAcrossRestart:
		return true
	}
	// driftInProcess
	if h.schemaDriftPolicy == SchemaDriftPolicyEvolve {
		return diff.IsIncompatible()
	}
	// halt — and dlq, which is rejected at config validation but fails closed
	// here for direct CDCConfig users — stops on any drift.
	return true
}

// emitDriftMarker builds and queues the D1 marker record and publishes the
// acked-gated halt state (D3 step 1: record version, build marker, hand it to
// the batch channel, set the pending flag).
//
// The marker is OperationCreate with nil key and nil payload; the evidence is
// all in the metadata (D1). Its position is built explicitly — never via
// buildPosition — from the schema-history SNAPSHOT taken at staging time
// (history; review Blocker 1 on the B1 drift policy): lsn is the LSN of the
// first DML that uses the new shape, not the RelationMessage's — pgoutput
// delivers the RelationMessage with WALStart 0 (verified 2026-08-29), so the
// relation message carries no usable position and the marker is emitted by
// the DML that follows it (see handleRelation/emitPendingDriftMarkerIfMatching).
// The snapshot contains exactly the state the halt decision was made against;
// a DDL sighted after staging (a stacked DDL) is never committed to the live
// history (the FM8 guard skips before any commit) and is not checkpointed by
// this marker, so the restart re-delivers its relation message and halts again
// instead of silently admitting it. The staged shape itself is committed to
// the live history HERE — at emission, never at the sighting (re-review
// should-fix on the B1 drift policy): any record emitted before the marker
// serializes basePosition, so a sighting-time commit would leak the shape
// into unrelated positions and dedupe the drift away on a restart before the
// drifted table's own DML. The snapshot receives the same FirstSeenLSN
// backfill as the live history (the commit below sets it directly from the
// marker LSN), so the marker's position never carries a "0/0" first-seen
// (FM7). Below the marker LSN everything was acked in FIFO order and at or
// above it nothing will be emitted (D4). Handle returns the same LSN for that
// DML, which advances the subscription's walWritten (D2), so the marker's ack
// can never trip the walFlushed > walWritten guard (subscription.go).
func (h *CDCHandler) emitDriftMarker(
	ctx context.Context,
	r *pglogrepl.RelationMessage,
	lsn pglogrepl.LSN,
	kind driftKind,
	diff internal.SchemaDiff,
	prev position.SchemaVersion,
	hash string,
	history position.SchemaHistories,
) pglogrepl.LSN {
	// FM3 chaospoint: first statement, before the marker record exists in
	// memory. Parking here proves a kill after the drift was staged (the new
	// shape held in the in-memory staging snapshot, not yet committed or
	// checkpointed anywhere) but before any marker record was queued. Nothing
	// durable claims the shape, so the restart re-delivers the relation
	// message and re-derives the drift — AC4's "restart halts, no dup marker".
	// No-op outside the conduitchaos build.
	chaospoint.Reach(chaospoint.DriftVersionRecorded)

	key := relationKey(r)
	// Commit the staged shape to the LIVE history at EMISSION — the one point
	// where the connector stops emitting below the marker (D4), so nothing
	// after it serializes a position that would have been built without the
	// shape (re-review should-fix: sighting-time commit would leak the staged
	// shape into unrelated records' positions, checkpointed persist-before-ack,
	// and a restart from one would dedupe the drift away). The prev passed to
	// newDriftHaltError is the PREVIOUS shape, which was already backfilled by
	// its own first DML, so the error message's "first seen at LSN" is real for
	// both sides of the arrow.
	h.basePosition.RecordSchemaVersion(key, hash, lsn.String())
	// The marker position is built from the STAGING snapshot below; backfill
	// its new shape's FirstSeenLSN too (the live commit above set it directly
	// from the marker LSN, but the snapshot's staged copy still carries the
	// relation message's placeholder), or the marker would checkpoint the
	// "0/0" placeholder the relation message left (FM7).
	history.SetFirstSeenLSN(key, lsn.String())
	// The D5 error is built here, at the marker's real LSN — the first DML
	// that uses the new shape — never at the relation message's (WALStart 0).
	haltErr := newDriftHaltError(kind, key, diff, prev, hash, lsn)
	metadata := map[string]string{
		MetadataSchemaDrift:       "true",
		MetadataSchemaDriftTable:  key,
		MetadataSchemaDriftLSN:    lsn.String(),
		MetadataSchemaDriftPolicy: string(h.schemaDriftPolicy),
	}
	if kind == driftInProcess {
		// The diff exists only for in-process drift; for driftAcrossRestart the
		// metadata must not fabricate one (FM7/AC7).
		metadata[MetadataSchemaDriftNarrowing] = strconv.FormatBool(diff.IsIncompatible())
		metadata[MetadataSchemaDriftDiff] = diff.String()
	}

	rec := sdk.Util.Source.NewRecordCreate(
		position.Position{
			Type:                    position.TypeCDC,
			LastLSN:                 lsn.String(),
			SnapshotLowWatermarkLSN: h.basePosition.SnapshotLowWatermarkLSN,
			SchemaHistory:           history, // the staging-time snapshot, not live history (Blocker 1)
		}.ToSDKPosition(),
		metadata,
		nil,
		nil,
	)
	h.addToBatch(ctx, rec)

	// Publish the pending-marker state AFTER the marker is queued, so a reader
	// that observes a non-zero driftMarkerLSN (or the armed flag, or the closed
	// driftHaltCh) can never miss the marker itself. The plain driftHaltErr
	// write is ordered before the atomic store; readers observe it after
	// driftHaltArmed or driftHaltCh (see the field comment).
	h.driftHaltErr = haltErr
	h.driftMarkerLSN.Store(uint64(lsn))
	return lsn
}

// driftMarkerPending reports whether a drift marker has been emitted but not
// yet acked. It stays true after the halt arms, which is what keeps D4's skip
// (and the F6 marker preference) active for the rest of the run.
func (h *CDCHandler) driftMarkerPending() bool {
	return h.driftMarkerLSN.Load() != 0
}

// maybeArmDriftHalt arms the halt once the engine acks at or past the marker's
// LSN (D3 step 3). The engine acks in FIFO order, so the marker's own ack is
// the first ack that reaches it; the `>=` comparison also covers a
// defensive/re-ordered ack. Arming is one-shot: the first ack wins, and the
// close of driftHaltCh wakes any NextN already blocked waiting for the marker's
// batch (F6).
//
// Invariant 1: arming is acked-gated, never sighting-gated — the engine
// persists a position before acking it, so an acked marker is proof the
// checkpoint is durable, which is exactly what makes the restart an approval.
func (h *CDCHandler) maybeArmDriftHalt(lsn pglogrepl.LSN) {
	markerLSN := pglogrepl.LSN(h.driftMarkerLSN.Load())
	if markerLSN == 0 || lsn < markerLSN {
		return
	}
	if h.driftHaltArmed.CompareAndSwap(false, true) {
		close(h.driftHaltCh)
	}
}

// driftHaltError returns the terminal D5 error once the halt is armed, nil
// otherwise. NextN calls this on entry and the blocked select receives from
// driftHaltCh; both read driftHaltErr only after the armed flag is observed
// set, which orders the write in emitDriftMarker (see the field comment).
func (h *CDCHandler) driftHaltError() error {
	if h.driftHaltArmed.Load() {
		return h.driftHaltErr
	}
	return nil
}

// updateAvroSchema generates and stores avro schema based on the relation's row
// when usage of avro schema is requested.
func (h *CDCHandler) updateAvroSchema(ctx context.Context, rel *pglogrepl.RelationMessage) error {
	if !h.withAvroSchema {
		return nil
	}
	// Payload schema
	avroPayloadSch, err := schema.Avro.ExtractLogrepl(rel.RelationName+"_payload", rel)
	if err != nil {
		return fmt.Errorf("failed to extract payload schema: %w", err)
	}
	ps, err := sdkschema.Create(
		ctx,
		cschema.TypeAvro,
		avroPayloadSch.Name(),
		[]byte(avroPayloadSch.String()),
	)
	if err != nil {
		return fmt.Errorf("failed creating payload schema for relation %v: %w", rel.RelationName, err)
	}
	h.payloadSchemas[rel.RelationName] = ps

	// Key schema
	avroKeySch, err := schema.Avro.ExtractLogrepl(rel.RelationName+"_key", rel, h.tableKeys[rel.RelationName])
	if err != nil {
		return fmt.Errorf("failed to extract key schema: %w", err)
	}
	ks, err := sdkschema.Create(
		ctx,
		cschema.TypeAvro,
		avroKeySch.Name(),
		[]byte(avroKeySch.String()),
	)
	if err != nil {
		return fmt.Errorf("failed creating key schema for relation %v: %w", rel.RelationName, err)
	}
	h.keySchemas[rel.RelationName] = ks

	return nil
}

func (h *CDCHandler) attachSchemas(rec opencdc.Record, relationName string) {
	if !h.withAvroSchema {
		return
	}
	cschema.AttachPayloadSchemaToRecord(rec, h.payloadSchemas[relationName])
	cschema.AttachKeySchemaToRecord(rec, h.keySchemas[relationName])
}

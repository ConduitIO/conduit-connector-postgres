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
	"reflect"
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
	// (decideDriftOnDelivery, emitDriftMarker) and nowhere else, so it needs
	// no locking.
	schemaDriftPolicy SchemaDriftPolicy

	// Drift-decision state (DBZ-3 B1; #335). Both maps are read and written
	// only on the subscription goroutine (Handle), so they need no locking.
	//
	// A RelationMessage is NOT a drift decision. pgoutput sends one before the
	// first change of a relation in each decoding session, and on a restart
	// that resumes inside a transaction it re-sends the whole transaction, so
	// it replays Relation messages with the shape the table had at that point
	// (the historic catalog), including shapes older than the one the
	// checkpoint already records. The subscription then skips the changes the
	// checkpoint covers (internal.ResumePoint), but their Relation messages
	// still arrive. Deciding drift on the Relation message would read a
	// replayed pre-ALTER shape as a change made while the connector was down
	// and halt, even under evolve (#335). The decision is therefore made on
	// the first change of the relation that is actually delivered to this
	// handler, against the shape that change is decoded with: a shape that
	// only precedes already-delivered (skipped) changes is never evaluated.
	//
	//   - undecidedRelations holds the IDs of relations whose cached shape has
	//     arrived but has not yet been decided by a delivered change.
	//   - seenShapes holds every shape this process has seen per relation ID,
	//     by column-set hash, including replayed ones. It is the diff base:
	//     when the last durable shape's hash is here, its columns are known
	//     exactly (the hash covers the same identity the diff compares), so
	//     the drift gets a full column diff. Pruned to the accepted shape on
	//     every accepted decision, so it holds at most the shapes sighted
	//     since the last decision.
	undecidedRelations map[uint32]struct{}
	seenShapes         map[uint32]map[string]*pglogrepl.RelationMessage

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
	//   - driftMarkerLSN is written on the subscription goroutine
	//     (emitDriftMarker, after the marker is queued) and read on the engine
	//     goroutine (CDCIterator.Ack -> maybeArmDriftHalt, the D4 skip checks,
	//     and the F6 marker-preference selects). 0 means no marker is pending.
	//     The marker is emitted by the delivered change that decided the
	//     drift, at that change's LSN and key: pgoutput delivers
	//     RelationMessage with XLogData.WALStart == 0 (verified 2026-08-29), so
	//     the relation message has no usable position (D1 deviation,
	//     documented in the B1 design doc's AC evidence).
	//   - driftMarkerKey is the marker's change key (transaction commit LSN,
	//     ordinal): the key of the change that triggered it. It is a plain
	//     field written by emitDriftMarker BEFORE the atomic store to
	//     driftMarkerLSN and read by maybeArmDriftHalt only after that atomic
	//     is observed non-zero, so the store/load pair orders the write before
	//     the read (same scheme as driftHaltErr). The halt arms on this key,
	//     never on the LSN: see maybeArmDriftHalt. It is the deciding change's
	//     key, not the key in the marker's position, which is the predecessor
	//     (#338).
	//   - driftMarkerPos is the marker's parsed position, written next to
	//     driftMarkerKey under the same ordering. The marker's position key is
	//     the deciding change's predecessor, which the previous record's
	//     position can share, so an ack is recognized as the marker's by the
	//     position's content, not its key (see maybeArmDriftHalt).
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
	driftMarkerLSN atomic.Uint64
	driftMarkerKey internal.ChangeKey
	driftMarkerPos position.Position
	driftHaltArmed atomic.Bool
	driftHaltErr   error
	driftHaltCh    chan struct{}

	// changeKey returns the key of the change being handled (see
	// internal.Subscription.CurrentChange). Set once before the subscription
	// starts and called only from Handle, on the subscription goroutine. Nil
	// in tests that drive the handler without a subscription; positions then
	// carry no key and resume with the legacy rule.
	changeKey func() internal.ChangeKey
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

		undecidedRelations: make(map[uint32]struct{}),
		seenShapes:         make(map[uint32]map[string]*pglogrepl.RelationMessage),
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
		// Cache the shape so the changes that follow can be decoded. The drift
		// decision waits for the first delivered change that uses it (#335;
		// see decideDriftOnDelivery). The marker that decision may emit rides
		// that change's LSN, which makes the subscription record the marker's
		// change key as the last emitted one, and keep it there for the changes
		// D4 then skips (see the invariant at the emitted assignment in
		// internal/subscription.go).
		h.handleRelation(m)
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
	if h.skipAfterDriftMarker(ctx, msg.RelationID, lsn) {
		return nil
	}
	if _, marker := h.decideDriftOnDelivery(ctx, msg.RelationID, lsn); marker {
		// D4: the change that decided a halting drift carries the marker
		// instead of its own record (the marker, then nothing).
		return nil
	}

	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return fmt.Errorf("failed getting relation %v: %w", msg.RelationID, err)
	}

	// Backfill the shape's FirstSeenLSN if it is the "0/0" placeholder. Shapes
	// are now recorded at the deciding change's LSN, but a position written by
	// an earlier build may still carry the relation message's WALStart 0. The
	// first DML using a shape is its true first-seen position, which the
	// drift-halt message reports. See position.SetFirstSeenLSN.
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
	if h.skipAfterDriftMarker(ctx, msg.RelationID, lsn) {
		return nil
	}
	if _, marker := h.decideDriftOnDelivery(ctx, msg.RelationID, lsn); marker {
		return nil // D4: see handleInsert
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
	if h.skipAfterDriftMarker(ctx, msg.RelationID, lsn) {
		return nil
	}
	if _, marker := h.decideDriftOnDelivery(ctx, msg.RelationID, lsn); marker {
		return nil // D4: see handleInsert
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
	return h.buildPositionAt(lsn, h.currentChangeKey())
}

// buildPositionAt is buildPosition with an explicit change key. A key that is
// not Known leaves the position without one (the legacy resume rule).
func (h *CDCHandler) buildPositionAt(lsn pglogrepl.LSN, key internal.ChangeKey) opencdc.Position {
	var commit string
	var seq uint64
	if key.Known() {
		commit, seq = key.CommitLSN.String(), key.Seq
	}
	return position.Position{
		Type:                    position.TypeCDC,
		LastLSN:                 lsn.String(),
		TxCommitLSN:             commit,
		TxSeq:                   seq,
		SnapshotLowWatermarkLSN: h.basePosition.SnapshotLowWatermarkLSN,
		SchemaHistory:           h.basePosition.SchemaHistory,
	}.ToSDKPosition()
}

// currentChangeKey returns the key of the change being handled (its
// transaction's commit LSN and its ordinal within that transaction, from the
// subscription), or the zero key when unknown. Every CDC position carries the
// key so a restart can tell which re-sent changes were already delivered
// (#331; see internal.ChangeKey and internal.ResumePoint).
//
// Invariant 2: (TxCommitLSN, TxSeq) increases strictly record by record in
// stream order, even when LastLSN does not (interleaved transactions) or
// repeats (rows of one multi-row insert). The drift marker is the one record
// whose position carries a key below its own change's (#338).
func (h *CDCHandler) currentChangeKey() internal.ChangeKey {
	if h.changeKey == nil {
		return internal.ChangeKey{}
	}
	return h.changeKey()
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

// driftKind classifies the shape a delivered change is decoded with, relative
// to everything known about that table's shape.
//
// classifyDrift returns it so the decision is assertable in a test rather than
// only observable in a log line, and it is the seam the drift policy (Area 2
// step 3) attaches to: halt/dlq/evolve is a function of this kind plus
// SchemaDiff.IsIncompatible.
type driftKind int

const (
	// driftNone: the shape matches the last durable version. The common case —
	// Postgres re-sends a RelationMessage after a reconnect, when a new
	// subscriber attaches, and when a restart replays a transaction.
	driftNone driftKind = iota
	// driftInitial: first shape ever recorded for this table. Not drift; there
	// is nothing to compare against.
	driftInitial
	// driftInProcess: the shape differs from the last durable version and this
	// process has seen a RelationMessage with exactly that durable shape (in
	// this run, or replayed by Postgres on resume), so the full column-level
	// diff from the durable shape is available.
	driftInProcess
	// driftAcrossRestart: the shape differs from the last durable version and
	// this process never saw the durable shape, so the change happened while
	// the connector was down. Only the hash survived, so the affected columns
	// are not recoverable.
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
// schema hash is computed over. Kept in lockstep with internal.DiffRelations' notion of
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

// handleRelation caches a RelationMessage so the changes that follow can be
// decoded, and records that its shape awaits a drift decision. It decides
// nothing: see decideDriftOnDelivery for why the decision waits for a
// delivered change (#335).
//
// The shape is cached even after a drift marker: decoding must keep working,
// and the D4 skip, not the cache, is what stops records.
func (h *CDCHandler) handleRelation(r *pglogrepl.RelationMessage) {
	h.relationSet.Update(r)
	h.undecidedRelations[r.RelationID] = struct{}{}

	shapes := h.seenShapes[r.RelationID]
	if shapes == nil {
		shapes = make(map[string]*pglogrepl.RelationMessage)
		h.seenShapes[r.RelationID] = shapes
	}
	shapes[position.HashColumnSet(columnIdentities(r))] = r
}

// skipAfterDriftMarker implements D4: once a drift marker has been emitted,
// every change is dropped, for every relation, without being decided. It
// reports whether the change was skipped.
//
// It runs before any relation lookup that could fail, so a change whose
// relation is unknown cannot error here. A change whose relation has an
// undecided shape that differs from the last durable one is the FM8 case (a
// second DDL while a halt is pending): that shape is deliberately neither
// decided nor committed to the history. Nothing durable claims it, so the
// restart re-delivers its Relation message and change and decides it then.
func (h *CDCHandler) skipAfterDriftMarker(ctx context.Context, relationID uint32, lsn pglogrepl.LSN) bool {
	if !h.driftMarkerPending() {
		return false
	}
	if _, undecided := h.undecidedRelations[relationID]; undecided {
		if rel, err := h.relationSet.Get(relationID); err == nil {
			key := relationKey(rel)
			hash := position.HashColumnSet(columnIdentities(rel))
			if prev, ok := h.basePosition.LastSchemaVersion(key); ok && prev.ColumnSetHash != hash {
				// FM8 chaospoint: a second shape reached a delivered change
				// while the first marker is pending. Parking here proves a kill
				// with the second shape seen in-run and not committed — the
				// in-run half of the FM8 window (AC9 parks the down-variant at
				// DriftMarkerAppended instead). No-op outside the conduitchaos
				// build.
				chaospoint.Reach(chaospoint.DriftVersionSkipped)
				sdk.Logger(ctx).Warn().
					Str("table", key).
					Str("lsn", lsn.String()).
					Str("schema_hash", hash).
					Msg("schema changed again while a drift halt is pending; version " +
						"not committed, no second marker emitted (it will halt on restart)")
			}
		}
		// Warn once per sighting. Nothing is decided after a marker anyway.
		delete(h.undecidedRelations, relationID)
	}
	// D4: once the drift marker is emitted, emit nothing — not even for other
	// relations. A record past the marker would let positions advance beyond
	// it, and the drifted table's skipped changes below the resume point
	// would then be lost on the operator-approved restart (invariant 3).
	// Skipping is safe: every position up to and including the marker's is at
	// or below the first new-shape change, so a restart resumes before it and
	// the skip repeats, with the restart-approval admitting the shape.
	// Skipping also avoids emitting old-shape projections through a schema the
	// connector no longer believes in (invariant 6).
	sdk.Logger(ctx).Debug().
		Str("lsn", lsn.String()).
		Str("relationID", strconv.FormatUint(uint64(relationID), 10)).
		Msg("skipping record after schema-drift marker (D4: the marker, then nothing)")
	return true
}

// decideDriftOnDelivery makes the drift decision for the relation of a change
// that is being delivered, if that relation's shape is still undecided. It
// returns the drift kind (driftNone when there was nothing to decide) and
// whether a drift marker was emitted in place of the change (D4: the caller
// then drops the change).
//
// Why here and not on the RelationMessage (#335): on a restart inside a
// transaction, Postgres re-sends the whole transaction, Relation messages
// included, and decodes it with the historic catalog. After an
// in-transaction ALTER TABLE, the replay starts with the pre-ALTER shape,
// while the checkpoint may already record the post-ALTER one. The
// subscription skips the changes the checkpoint covers, but not their
// Relation messages. Only the shape a delivered change is decoded with is a
// shape the connector is about to emit data through, so only that shape is
// decided. A replayed shape that only precedes skipped changes was decided by
// the run that delivered them.
//
// A real revert is still caught: its Relation message is followed by a
// delivered change, which is decided against the durable shape like any other
// (B1 AC5).
//
// Invariant 6: every change is delivered through a shape that was decided and
// accepted by the configured policy. Invariant 3: deferring the decision
// cannot drop a change; a change is skipped only by the resume point or, after
// a marker, by D4.
func (h *CDCHandler) decideDriftOnDelivery(ctx context.Context, relationID uint32, lsn pglogrepl.LSN) (driftKind, bool) {
	if _, undecided := h.undecidedRelations[relationID]; !undecided {
		return driftNone, false
	}
	rel, err := h.relationSet.Get(relationID)
	if err != nil {
		// Unreachable: a relation is marked undecided only after it is cached.
		// The caller's own lookup reports the error.
		return driftNone, false
	}
	delete(h.undecidedRelations, relationID)

	kind, diff, prev, hash := h.classifyDrift(ctx, rel, lsn)
	if !h.haltsOnDrift(kind, diff) {
		// Accepted shape (an unchanged or replayed shape, the first sighting,
		// or an evolve-compatible change): commit it to the live history, so
		// this change's position and every later one carry it.
		h.basePosition.RecordSchemaVersion(relationKey(rel), hash, lsn.String())
		// The accepted shape is the only diff base needed from here on.
		h.seenShapes[relationID] = map[string]*pglogrepl.RelationMessage{hash: rel}
		return kind, false
	}

	h.emitDriftMarker(ctx, rel, lsn, kind, diff, prev, hash)
	return kind, true
}

// classifyDrift compares the shape a delivered change is decoded with against
// the last durable shape of its table.
//
// Two sources are combined. The durable history in the position survives
// restarts but stores only a hash, so it can prove the shape changed without
// saying how. The shapes this process has seen (seenShapes) carry the
// columns: when one of them has the durable shape's hash, the diff from the
// durable shape is exact, because the hash covers the same column identity
// the diff compares. A DDL applied while the connector was down, with no
// replay of the old shape, is visible only through the history.
//
// Concurrency: basePosition and seenShapes are read and written only on the
// subscription goroutine (see the basePosition field comment).
func (h *CDCHandler) classifyDrift(
	ctx context.Context,
	r *pglogrepl.RelationMessage,
	lsn pglogrepl.LSN,
) (driftKind, internal.SchemaDiff, position.SchemaVersion, string) {
	key := relationKey(r)
	hash := position.HashColumnSet(columnIdentities(r))
	prev, hadHistory := h.basePosition.LastSchemaVersion(key)

	switch {
	case hadHistory && prev.ColumnSetHash == hash:
		// Same shape as the last durable version: a re-sent or replayed
		// Relation message, or no DDL at all. Must stay silent.
		return driftNone, internal.SchemaDiff{}, prev, hash
	case !hadHistory:
		// First shape ever recorded for this table — on a first run, or on the
		// first run after upgrading from a position that predates the history.
		// Nothing to compare against, so this is not drift.
		sdk.Logger(ctx).Debug().
			Str("table", key).
			Str("schema_hash", hash).
			Msg("recorded initial schema version for table")
		return driftInitial, internal.SchemaDiff{}, prev, hash
	}

	if base, ok := h.seenShapes[r.RelationID][prev.ColumnSetHash]; ok {
		if diff := internal.DiffRelations(base, r); diff.HasDrift() {
			sdk.Logger(ctx).Warn().
				Str("table", key).
				Str("lsn", lsn.String()).
				Bool("incompatible", diff.IsIncompatible()).
				Msg("schema drift detected: " + diff.String())
			return driftInProcess, diff, prev, hash
		}
	}

	// The shape differs from the last durable version and this process never
	// saw the durable shape: the change happened while the connector was not
	// running. Only the hash is retained, so this reports THAT the schema
	// changed, not which columns.
	sdk.Logger(ctx).Warn().
		Str("table", key).
		Str("lsn", lsn.String()).
		Str("previous_schema_hash", prev.ColumnSetHash).
		Str("previous_seen_lsn", prev.FirstSeenLSN).
		Str("current_schema_hash", hash).
		Msg("schema of table changed while the connector was not running; " +
			"the change happened before this process started, so the affected " +
			"columns cannot be reported — compare against your DDL history")
	return driftAcrossRestart, internal.SchemaDiff{}, prev, hash
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
// It runs on the delivered change that decided the drift (see
// decideDriftOnDelivery), so lsn and the change key are that change's: the
// marker takes its place in the stream (D4 drops the change itself in this
// run). The marker's position carries the key one below that change (#338), so
// the approving restart resumes at the change and delivers it, decoded against
// the approved shape. pgoutput
// delivers the RelationMessage with WALStart 0 (verified 2026-08-29), so the
// relation message has no usable position of its own.
//
// The marker is OperationCreate with nil key and nil payload; the evidence is
// all in the metadata (D1). Its position carries the new shape: the shape is
// committed to the live history here, at emission, and the position is built
// from it. Nothing emitted before the marker carries the shape (it was
// undecided until now), and nothing after it is emitted (D4), so the shape
// cannot leak into an unrelated record's checkpoint and dedupe the drift away
// on a restart. A shape sighted after the marker is never committed (FM8, see
// skipAfterDriftMarker), so the marker cannot checkpoint it either (review
// Blocker 1). Everything emitted before the marker has a change key below the
// marker's and is acked ahead of it (FIFO acks); nothing at or after the
// marker in stream order is emitted (D4). That is a statement about change
// keys, not LSNs: with interleaved transactions an earlier-emitted change can
// carry a higher LSN than the marker, which is why the halt arms on the key
// (maybeArmDriftHalt). Handle returns the same LSN for the change, so the
// subscription records the marker's key as the last emitted one
// (internal/subscription.go) and holds the slot advance until the marker is
// acked.
func (h *CDCHandler) emitDriftMarker(
	ctx context.Context,
	r *pglogrepl.RelationMessage,
	lsn pglogrepl.LSN,
	kind driftKind,
	diff internal.SchemaDiff,
	prev position.SchemaVersion,
	hash string,
) {
	// FM3 chaospoint: first statement, before the marker record exists in
	// memory or the shape is committed anywhere. Parking here proves a kill
	// after the drift was decided but before any marker record was queued.
	// Nothing durable claims the shape, so the restart re-delivers the
	// relation message and re-derives the drift — AC4's "restart halts, no dup
	// marker". No-op outside the conduitchaos build.
	chaospoint.Reach(chaospoint.DriftVersionRecorded)

	key := relationKey(r)
	h.basePosition.RecordSchemaVersion(key, hash, lsn.String())
	// The D5 error is built at the marker's LSN — the first delivered change
	// that uses the new shape. The prev passed in is the PREVIOUS durable
	// shape, so the message's "first seen at LSN" is real for both sides of
	// the arrow.
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

	// #338: the marker's position carries the key one below the deciding
	// change, so the approving restart resumes AT that change instead of past
	// it. Everything emitted before the marker has a key at or below the
	// predecessor and was acked ahead of the marker (FIFO), so the exact resume
	// skips precisely that and delivers the deciding change, now decoded
	// against the approved shape. The LSN stays the deciding change's own: it
	// is what the slot's confirmed_flush_lsn can reach, and it is below the
	// transaction's commit LSN, so Postgres re-sends the transaction.
	//
	// Invariant 3: the deciding change is re-delivered, not dropped.
	// Invariant 1: the flush gate compares the last emitted key (the deciding
	// change, or a later D4-skipped one) with the last acked key (this
	// predecessor), so it stays closed until the deciding change itself is
	// delivered and acked after the restart.
	markerKey := h.currentChangeKey()
	rec := sdk.Util.Source.NewRecordCreate(h.buildPositionAt(lsn, markerKey.Predecessor()), metadata, nil, nil)
	h.addToBatch(ctx, rec)

	// Publish the pending-marker state AFTER the marker is queued, so a reader
	// that observes a non-zero driftMarkerLSN (or the armed flag, or the closed
	// driftHaltCh) can never miss the marker itself. The plain driftHaltErr
	// write is ordered before the atomic store; readers observe it after
	// driftHaltArmed or driftHaltCh (see the field comment).
	h.driftHaltErr = haltErr
	h.driftMarkerKey = markerKey // before the atomic store below
	if mp, err := position.ParseSDKPosition(rec.Position); err == nil {
		h.driftMarkerPos = mp
	}
	h.driftMarkerLSN.Store(uint64(lsn))
	h.warnIfMarkerStaysUnacked(ctx, key)
}

// driftMarkerPending reports whether a drift marker has been emitted but not
// yet acked. It stays true after the halt arms, which is what keeps D4's skip
// (and the F6 marker preference) active for the rest of the run.
func (h *CDCHandler) driftMarkerPending() bool {
	return h.driftMarkerLSN.Load() != 0
}

// maybeArmDriftHalt arms the halt once the engine acks the marker or a change
// after it in stream order (D3 step 3). Arming is one-shot: the first such ack
// wins, and the close of driftHaltCh wakes any NextN already blocked waiting
// for the marker's batch (F6).
//
// The comparison is on change keys (transaction commit LSN, ordinal), never on
// LSNs. The engine acks in the order records were emitted, and the change key
// increases strictly in emission order (internal.ChangeKey), so the first ack
// whose key is at or past the marker's key is the marker's own. Change LSNs do
// not have that property: with interleaved transactions a change emitted
// before the marker can carry a higher LSN than the marker (the transaction
// that wrote it committed first), and arming on `lsn >= markerLSN` let the ack
// of such a change arm the halt before the marker was delivered, stranding the
// marker and the approval checkpoint it carries (#334 review).
//
// When either key is unknown (a handler driven without a subscription, which
// stamps no keys) the comparison falls back to LSNs, the pre-#331 behavior.
//
// Invariant 1: arming is acked-gated, never sighting-gated: the engine
// persists a position before acking it, so an acked marker is proof the
// checkpoint is durable, which is exactly what makes the restart an approval.
//
// #338: the marker's position carries the key one below the deciding change,
// which is also the key of the record delivered just before the marker (when
// the deciding change is not the first of its transaction). An ack's key
// therefore cannot tell the marker's ack from that record's. The position's
// content can: the marker's position records the new schema shape in its
// SchemaHistory, which the previous record's does not. An ack matches when its
// parsed Type, LastLSN, TxCommitLSN, TxSeq and SchemaHistory equal the
// marker's. The comparison is semantic, not byte-exact, so an engine or
// middleware that re-marshals the position (key order, whitespace) still
// arms; a byte comparison turned that into a silent stall, because D4 emits
// nothing after the marker and no later ack can arm instead. An ack with a key
// at or past the deciding change's key still arms, as before.
func (h *CDCHandler) maybeArmDriftHalt(lsn pglogrepl.LSN, key internal.ChangeKey, pos position.Position) {
	markerLSN := pglogrepl.LSN(h.driftMarkerLSN.Load())
	if markerLSN == 0 {
		return
	}
	// driftMarkerKey and driftMarkerPos are safe to read: the atomic load
	// above observed the store that followed their writes.
	if !samePosition(pos, h.driftMarkerPos) {
		if markerKey := h.driftMarkerKey; markerKey.Known() && key.Known() {
			if key.Before(markerKey) {
				return
			}
		} else if lsn < markerLSN {
			return
		}
	}
	if h.driftHaltArmed.CompareAndSwap(false, true) {
		close(h.driftHaltCh)
	}
}

// samePosition reports whether two CDC positions are semantically equal on
// the fields that identify a marker: type, LSN, change key and schema history
// (nil and empty histories are equal). A zero Position matches nothing real.
func samePosition(a, b position.Position) bool {
	if a.Type != position.TypeCDC || b.Type != position.TypeCDC {
		return false
	}
	if a.LastLSN != b.LastLSN || a.TxCommitLSN != b.TxCommitLSN || a.TxSeq != b.TxSeq {
		return false
	}
	if len(a.SchemaHistory) == 0 && len(b.SchemaHistory) == 0 {
		return true
	}
	return reflect.DeepEqual(a.SchemaHistory, b.SchemaHistory)
}

// driftMarkerWarnAfter is how long a drift marker may stay unacked before the
// connector says so. A variable so tests can shorten it.
var driftMarkerWarnAfter = 30 * time.Second

// warnIfMarkerStaysUnacked logs once if the halt has not armed
// driftMarkerWarnAfter after the marker was emitted. Until the marker is
// acked nothing else is emitted (D4) and the slot is held, so a pipeline that
// never acks the marker (for example because the position it returns is not
// the one the marker carried) waits silently while WAL accumulates.
func (h *CDCHandler) warnIfMarkerStaysUnacked(ctx context.Context, key string) {
	after := driftMarkerWarnAfter
	time.AfterFunc(after, func() {
		if h.driftHaltArmed.Load() {
			return
		}
		sdk.Logger(ctx).Warn().
			Str("table", key).
			Stringer("marker_lsn", pglogrepl.LSN(h.driftMarkerLSN.Load())).
			Dur("after", after).
			Msg("schema drift marker has not been acked; the halt cannot surface and the slot is held. " +
				"The marker's position must be acked unchanged (type, LSN, change key, schema history)")
	})
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

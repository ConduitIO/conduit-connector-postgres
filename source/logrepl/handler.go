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
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
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
) *CDCHandler {
	h := &CDCHandler{
		tableKeys:      tableKeys,
		relationSet:    rs,
		recordBatch:    make([]opencdc.Record, 0, batchSize),
		out:            out,
		withAvroSchema: withAvroSchema,
		keySchemas:     make(map[string]cschema.Schema),
		payloadSchemas: make(map[string]cschema.Schema),
		batchSize:      batchSize,
		flushInterval:  flushInterval,
		basePosition:   startPosition,
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
		// The returned drift kind is not acted on yet; reporting is Area 2 step 2,
		// the halt/dlq/evolve policy is step 3.
		_ = h.handleRelation(ctx, m, lsn)
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
	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return fmt.Errorf("failed getting relation %v: %w", msg.RelationID, err)
	}

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
	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return err
	}

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
	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return err
	}

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
// This step detects and reports. It does not yet halt, DLQ, or evolve — that is
// the drift policy (Area 2 step 3), where making halt the default is a breaking
// change that owes a migration note.
//
// Concurrency: basePosition is read and written only here and in
// setBasePositionLowWatermark. Both run before or on the single subscription
// goroutine (see the basePosition field comment), so no locking is needed. The
// SchemaHistory map is handed to buildPosition by reference, but ToSDKPosition
// marshals to JSON eagerly, so no live reference to it ever escapes into an
// emitted position.
func (h *CDCHandler) handleRelation(ctx context.Context, r *pglogrepl.RelationMessage, lsn pglogrepl.LSN) driftKind {
	diff := h.relationSet.Update(r)

	key := relationKey(r)
	hash := position.HashColumnSet(columnIdentities(r))

	// Read before recording: RecordSchemaVersion mutates what LastSchemaVersion
	// returns.
	prev, hadHistory := h.basePosition.LastSchemaVersion(key)
	changed := h.basePosition.RecordSchemaVersion(key, hash, lsn.String())

	switch {
	case !changed:
		// Same shape as the last durable version. Postgres re-sends a
		// RelationMessage after a reconnect and when a new subscriber attaches,
		// so this is the common case and must stay silent.
		return driftNone
	case !hadHistory:
		// First shape ever recorded for this table — on a first run, or on the
		// first run after upgrading from a position that predates the history.
		// Nothing to compare against, so this is not drift.
		sdk.Logger(ctx).Debug().
			Str("table", key).
			Str("schema_hash", hash).
			Msg("recorded initial schema version for table")
		return driftInitial
	case diff.HasDrift():
		// Drift within this process: the full diff is available.
		sdk.Logger(ctx).Warn().
			Str("table", key).
			Str("lsn", lsn.String()).
			Bool("incompatible", diff.IsIncompatible()).
			Msg("schema drift detected: " + diff.String())
		return driftInProcess
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
		return driftAcrossRestart
	}
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

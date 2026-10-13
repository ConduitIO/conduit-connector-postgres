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
	pginternal "github.com/conduitio/conduit-connector-postgres/internal"
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

	// truncateWarns rate-limits the TRUNCATE warning, one entry per table.
	// Only touched on the subscription goroutine (Handle). now is replaceable
	// in tests.
	truncateWarns map[string]truncateWarn
	now           func() time.Time

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
		h.relationSet.Add(m)
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
	case *pglogrepl.TruncateMessage:
		// Not propagated: OpenCDC has no truncate operation. Logging only, no
		// change to what is emitted or acked (#344; a configurable policy is planned).
		h.warnTruncate(ctx, m, lsn)
		return 0, nil
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
	//
	// Only a full old row ('O') can backfill. A key-only old tuple ('K', sent
	// when the identity columns change under DEFAULT or USING INDEX) carries
	// NULL for every non-identity column, so backfilling from it would turn
	// "unchanged" into a fabricated NULL, exactly what the omission avoids
	// (invariant 6).
	if msg.OldTupleType == pglogrepl.UpdateMessageTupleTypeOld {
		for col, oldVal := range oldValues {
			if _, ok := newValues[col]; !ok {
				newValues[col] = oldVal
			}
		}
	}

	rec := sdk.Util.Source.NewRecordUpdate(
		h.buildPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(newValues, rel.RelationName),
		h.buildRecordBefore(msg.OldTupleType, oldValues),
		h.buildRecordPayload(newValues),
	)
	h.attachSchemas(rec, rel.RelationName)
	h.addToBatch(ctx, rec)

	return nil
}

// handleDelete formats a record with DELETE event data from Postgres and sends
// it to the output channel. The record always carries the key. It carries a
// Before payload only under REPLICA IDENTITY FULL, where Postgres sends the
// whole old row; see buildRecordBefore.
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

	key := h.buildRecordKey(oldValues, rel.RelationName)
	// Invariant 3: a delete with a null key cannot be applied downstream (the
	// Postgres destination turns it into `WHERE id IS NULL`, matches nothing
	// and acks), so the row would silently survive. Open refuses tables whose
	// replica identity does not cover the key column; this catches the same
	// condition if the identity is changed while the pipeline runs.
	if err := h.requireDeleteKey(key, rel.RelationName, lsn); err != nil {
		return err
	}

	rec := sdk.Util.Source.NewRecordDelete(
		h.buildPosition(lsn),
		h.buildRecordMetadata(rel),
		key,
		h.buildRecordBefore(msg.OldTupleType, oldValues),
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

// buildRecordBefore returns the Before payload of an UPDATE or DELETE.
//
// Postgres sends the old row in one of two shapes. 'O' (REPLICA IDENTITY FULL)
// is the whole previous row, and is emitted as Before unchanged. 'K' (DEFAULT
// or USING INDEX) carries only the identity columns; pgoutput still sends
// every column, with NULL for the rest. Emitting that as Before would report
// NULL for columns that were not NULL (invariant 6), and under Avro the SDK
// encodes Before against the table's payload schema, where those columns are
// not nullable, so the encode fails and the source halts on every restart
// (conduit-connector-postgres#379). A 'K' tuple holds nothing the key does not
// already carry, except the old key of a key-changing UPDATE, so Before is
// left empty. A missing old tuple (UPDATE that changed no identity column)
// also gives an empty Before.
//
// The 'K'/'O' distinction is decided here, where the default-mode record is
// built, and not in RelationSet.Values: other consumers of the decoded tuple
// need the per-column information it carries.
func (h *CDCHandler) buildRecordBefore(oldTupleType uint8, oldValues map[string]any) opencdc.Data {
	// UpdateMessageTupleTypeOld and DeleteMessageTupleTypeOld are both 'O'.
	if oldTupleType != pglogrepl.DeleteMessageTupleTypeOld {
		return nil
	}
	return h.buildRecordPayload(oldValues)
}

// requireDeleteKey returns an error coded ErrorCodeReplicaIdentityExcludesKey
// when the key of a delete is missing or null. A primary-key column is never
// NULL in a stored row, so a null key on a delete means the old tuple did not
// include the key column: the table's REPLICA IDENTITY is USING INDEX on an
// index without it (conduit-connector-postgres#380).
//
// It only checks relations the source resolved a key column for at Open. A
// relation without one (for example a partition leaf, which pgoutput reports
// under its own name) already gets an empty key on every operation; that is a
// separate limitation and this check does not change it.
func (h *CDCHandler) requireDeleteKey(key opencdc.Data, table string, lsn pglogrepl.LSN) error {
	keyColumn, known := h.tableKeys[table]
	if !known || keyColumn == "" {
		return nil
	}
	if sd, ok := key.(opencdc.StructuredData); ok {
		if v, present := sd[keyColumn]; present && v != nil {
			return nil
		}
	}
	// The slot can only be moved past a whole transaction: advancing to an LSN
	// inside it replays it from the start, and the commit LSN itself is not past
	// the transaction (it is the start of the commit record). So the operator
	// needs the change LSN (to find the change) and the first LSN after the
	// commit record start (to skip the transaction). The latter is printed as a
	// literal because pg_lsn + integer only exists from Postgres 14. See "Delete
	// With a Missing Key" in the README.
	where := fmt.Sprintf("WAL LSN %s", lsn)
	skip := "advance the slot past that transaction"
	if h.lastTXLSN != 0 {
		where = fmt.Sprintf("WAL LSN %s, in the transaction that commits at LSN %s", lsn, h.lastTXLSN)
		skip = fmt.Sprintf("advance the slot past that transaction with pg_replication_slot_advance('<slot>', '%s') "+
			"(Postgres 11 or later; see the README for Postgres 10)", nextLSN(h.lastTXLSN))
	}
	return fmt.Errorf(
		"%s: delete on table %q (%s) arrived without a value for key column %q, so it cannot be applied downstream; "+
			"the table's REPLICA IDENTITY does not include the key column. "+
			"Fix: ALTER TABLE %s REPLICA IDENTITY FULL (or DEFAULT, or USING INDEX on a unique index that includes %q). "+
			"This change is already in the WAL without its key: after fixing the identity, stop the pipeline, "+
			"%s (this skips every change in the transaction, not only the delete), and reconcile the deleted row in the destination by hand",
		ErrorCodeReplicaIdentityExcludesKey, table, where, keyColumn, pginternal.WrapSQLIdent(table), keyColumn, skip)
}

// nextLSN returns the LSN one byte after l, in the X/Y hex form Postgres
// reads. Skipping a transaction needs an LSN just past its commit record
// start; the operator writes it as a literal, since pg_lsn arithmetic is not
// available before Postgres 14. A low part of FFFFFFFF carries into the high
// part.
func nextLSN(l pglogrepl.LSN) string {
	return (l + 1).String()
}

// buildRecordPayload takes the values from the message and extracts the payload
// for the record.
func (h *CDCHandler) buildRecordPayload(values map[string]any) opencdc.Data {
	if len(values) == 0 {
		return nil
	}
	return opencdc.StructuredData(values)
}

// buildPosition builds the position of the CDC record at lsn. It carries
// the record's change key (its transaction's commit LSN and its ordinal
// within that transaction, from the subscription), which is what lets a
// restart tell which re-sent changes were already delivered (#331; see
// internal.ChangeKey and internal.ResumePoint).
//
// Invariant 2: (TxCommitLSN, TxSeq) increases strictly record by record in
// stream order, even when LastLSN does not (interleaved transactions) or
// repeats (rows of one multi-row insert).
func (h *CDCHandler) buildPosition(lsn pglogrepl.LSN) opencdc.Position {
	p := position.Position{
		Type:    position.TypeCDC,
		LastLSN: lsn.String(),
	}
	if h.changeKey != nil {
		if k := h.changeKey(); k.Known() {
			p.TxCommitLSN = k.CommitLSN.String()
			p.TxSeq = k.Seq
		}
	}
	return p.ToSDKPosition()
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

// truncateWarnInterval is the minimum time between two warnings for the same
// table, so a storm of TRUNCATEs cannot flood the log.
const truncateWarnInterval = time.Minute

// truncateWarn is the rate-limit state of one table.
type truncateWarn struct {
	warnedAt   time.Time // when the last warning for the table was logged
	suppressed uint64    // TRUNCATEs swallowed by the rate limit since then
}

// warnTruncate tells the operator that a TRUNCATE happened and was NOT
// propagated: the rows it removed are still in the destination. Until a policy
// exists (a configurable policy is planned) this is the only signal. The
// connector exposes no metrics, so each warning carries "suppressed", the number
// of TRUNCATEs of that table the rate limit swallowed since the previous
// warning; a warning therefore stands for 1+suppressed TRUNCATEs.
//
// State is bounded: on each warning, entries whose window has expired are
// dropped, so it holds only tables truncated within the last interval. The
// price is that the suppressed count of a burst is reported only by the next
// warning for that table; if the table is never truncated again, or its entry
// is evicted first, the tail of the burst is not reported. The first TRUNCATE of
// every burst is always logged, so the operator is told at least once. This is
// deliberately not fixed with a timer: it would add a goroutine to a handler
// that has none, for a count that only refines a warning already delivered.
func (h *CDCHandler) warnTruncate(ctx context.Context, m *pglogrepl.TruncateMessage, lsn pglogrepl.LSN) {
	if h.truncateWarns == nil {
		h.truncateWarns = map[string]truncateWarn{}
	}
	now := time.Now
	if h.now != nil {
		now = h.now
	}
	t := now()

	for _, id := range m.RelationIDs {
		table := fmt.Sprintf("relation_id=%d", id)
		if rel, err := h.relationSet.Get(id); err == nil {
			table = rel.Namespace + "." + rel.RelationName
		}

		st, ok := h.truncateWarns[table]
		if ok && t.Sub(st.warnedAt) < truncateWarnInterval {
			st.suppressed++
			h.truncateWarns[table] = st
			continue
		}

		for k, v := range h.truncateWarns {
			if t.Sub(v.warnedAt) >= truncateWarnInterval {
				delete(h.truncateWarns, k)
			}
		}
		h.truncateWarns[table] = truncateWarn{warnedAt: t}
		sdk.Logger(ctx).Warn().
			Str("table", table).
			Stringer("lsn", lsn).
			Bool("cascade", m.Option&pglogrepl.TruncateOptionCascade != 0).
			Bool("restart_identity", m.Option&pglogrepl.TruncateOptionRestartIdentity != 0).
			Uint64("suppressed", st.suppressed).
			Msg("TRUNCATE was NOT propagated: the rows it removed still exist downstream. " +
				"No record is emitted for TRUNCATE; a configurable policy is planned. " +
				"See the Limitations section of the connector README.")
	}
}

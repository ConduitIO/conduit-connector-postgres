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

package debezium

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jackc/pglogrepl"
)

// Metadata keys of the record shape between this source and the
// debezium.encode processor (design: Record shape between source and
// processor). They are the keys the processor reads
// (ConduitIO/conduit pkg/plugin/processor/builtin/impl/debezium/envelope.go).
const (
	MetaTopic       = "debezium.topic"
	MetaOp          = "debezium.op"
	MetaDescriptor  = "debezium.descriptor"
	MetaOldKey      = "debezium.old_key"
	MetaSourcePfx   = "debezium.source."
	MetaTxID        = "debezium.tx.id"
	MetaTxTotal     = "debezium.tx.total_order"
	MetaTxCollOrder = "debezium.tx.data_collection_order"
)

const connectorName = "postgresql"

// catalogAttempts bounds the catalog reads for one relation shape when the
// catalog and the Relation message disagree (a DDL raced the read).
const catalogAttempts = 3

// Builder turns pgoutput changes into Debezium-mode records: typed values
// in the Go types the debezium.encode processor accepts, every column of the
// relation (nil for NULL), and the debezium.* metadata.
//
// A Builder is used from the subscription goroutine only and is not safe for
// concurrent use. It holds no position state: the caller sets the record's
// position, so positions, acks and resume are those of the default mode.
type Builder struct {
	s Settings
	q Querier

	// shapes is the latest Relation message per relation ID; tables are the
	// shapes built so far. A Relation message drops the built table, and the
	// next change of that relation reads the catalog again (lazily, so a
	// replayed shape that only precedes skipped changes costs nothing).
	shapes map[uint32]*pglogrepl.RelationMessage
	tables map[uint32]*table
	db     string

	tx tx
	// lastCommitEnd is the end LSN of the last commit seen in this run, the
	// first element of source.sequence. Zero until the first commit: the
	// first transaction after a start writes null there (documented
	// difference: Debezium restores it from its offset).
	lastCommitEnd pglogrepl.LSN

	sleep func(time.Duration)
}

type tx struct {
	active     bool
	xid        uint32
	commitTime time.Time
	total      int64
	coll       map[string]int64
}

// NewBuilder returns a Builder. q is a regular (not replication) connection
// pool used for catalog reads.
func NewBuilder(s Settings, q Querier) *Builder {
	return &Builder{
		s:      s,
		q:      q,
		shapes: map[uint32]*pglogrepl.RelationMessage{},
		tables: map[uint32]*table{},
		sleep:  time.Sleep,
	}
}

// Relation records the shape the following changes of the relation are
// encoded with.
func (b *Builder) Relation(rel *pglogrepl.RelationMessage) {
	b.shapes[rel.RelationID] = rel
	delete(b.tables, rel.RelationID)
}

// Begin starts a transaction: its xid and commit time go into the source
// block of each of its changes, and the transaction counters restart.
func (b *Builder) Begin(m *pglogrepl.BeginMessage) {
	b.tx = tx{active: true, xid: m.Xid, commitTime: m.CommitTime, coll: map[string]int64{}}
}

// Commit ends the transaction.
func (b *Builder) Commit(m *pglogrepl.CommitMessage) {
	b.tx = tx{}
	b.lastCommitEnd = m.TransactionEndLSN
}

// Insert builds the record of an insert (op c).
func (b *Builder) Insert(ctx context.Context, m *pglogrepl.InsertMessage, lsn pglogrepl.LSN) (opencdc.Record, error) {
	t, err := b.table(ctx, m.RelationID)
	if err != nil {
		return opencdc.Record{}, err
	}
	after, err := t.row(m.Tuple, nil, b.s.UnavailableValuePlaceholder)
	if err != nil {
		return opencdc.Record{}, err
	}
	rec := opencdc.Record{
		Operation: opencdc.OperationCreate,
		Key:       t.key(after),
		Payload:   opencdc.Change{After: opencdc.StructuredData(after)},
	}
	rec.Metadata, err = b.metadata(t, "c", lsn, 1)
	return rec, err
}

// Update builds the record of an update: op u, or, when the old tuple
// carries a key that differs from the new one, a primary-key change (op u
// with the old key in debezium.old_key and the old row as before; the
// encoder expands it into delete, tombstone, create).
//
// before is the full old row under REPLICA IDENTITY FULL, the D15-filled old
// row for a primary-key change under DEFAULT, and null otherwise; it is
// never synthesized from the new row.
func (b *Builder) Update(ctx context.Context, m *pglogrepl.UpdateMessage, lsn pglogrepl.LSN) (opencdc.Record, error) {
	t, err := b.table(ctx, m.RelationID)
	if err != nil {
		return opencdc.Record{}, err
	}
	var old map[string]any
	if m.OldTuple != nil {
		if old, err = t.oldRow(m.OldTuple, m.OldTupleType, b.s.UnavailableValuePlaceholder); err != nil {
			return opencdc.Record{}, err
		}
	}
	var full map[string]any
	if m.OldTupleType == pglogrepl.UpdateMessageTupleTypeOld {
		full = old
	}
	after, err := t.row(m.NewTuple, full, b.s.UnavailableValuePlaceholder)
	if err != nil {
		return opencdc.Record{}, err
	}
	rec := opencdc.Record{
		Operation: opencdc.OperationUpdate,
		Key:       t.key(after),
		Payload:   opencdc.Change{After: opencdc.StructuredData(after)},
	}
	events := 1
	if old != nil && len(t.keyIdx) > 0 {
		oldKey := t.key(old)
		if err := t.requireKey(oldKey, lsn, "update"); err != nil {
			return opencdc.Record{}, err
		}
		if !sameKey(t, oldKey, rec.Key) {
			// Primary-key change: Debezium numbers the delete and the create
			// as two events of the transaction.
			events = 2
			text, err := t.keyJSON(oldKey.(opencdc.StructuredData))
			if err != nil {
				return opencdc.Record{}, err
			}
			rec.Payload.Before = opencdc.StructuredData(old)
			rec.Metadata, err = b.metadata(t, "u", lsn, events)
			if err != nil {
				return opencdc.Record{}, err
			}
			rec.Metadata[MetaOldKey] = text
			return rec, nil
		}
	}
	if full != nil {
		rec.Payload.Before = opencdc.StructuredData(full)
	}
	rec.Metadata, err = b.metadata(t, "u", lsn, events)
	return rec, err
}

// Delete builds the record of a delete (op d). The key comes from the old
// tuple; a table with a key whose delete carries none fails with
// CodeDeleteKeyUnavailable rather than emitting a delete for a null key.
func (b *Builder) Delete(ctx context.Context, m *pglogrepl.DeleteMessage, lsn pglogrepl.LSN) (opencdc.Record, error) {
	t, err := b.table(ctx, m.RelationID)
	if err != nil {
		return opencdc.Record{}, err
	}
	if m.OldTuple == nil {
		if len(t.keyIdx) > 0 {
			return opencdc.Record{}, t.keyUnavailable(lsn, "delete")
		}
		return opencdc.Record{}, &Error{Code: CodeDeleteKeyUnavailable,
			Message: fmt.Sprintf("table %s.%s: delete at %s carries no old row", t.schemaName, t.tableName, lsn),
			Fix:     "Set REPLICA IDENTITY FULL on the table, or give it a primary key."}
	}
	old, err := t.oldRow(m.OldTuple, m.OldTupleType, b.s.UnavailableValuePlaceholder)
	if err != nil {
		return opencdc.Record{}, err
	}
	key := t.key(old)
	if err := t.requireKey(key, lsn, "delete"); err != nil {
		return opencdc.Record{}, err
	}
	rec := opencdc.Record{
		Operation: opencdc.OperationDelete,
		Key:       key,
		Payload:   opencdc.Change{Before: opencdc.StructuredData(old)},
	}
	rec.Metadata, err = b.metadata(t, "d", lsn, 1)
	return rec, err
}

// Skipped counts a change the subscription skips on a resume (delivered
// before the restart) or under an import's partial rule (delivered by
// Debezium), so the transaction's later events keep the numbers Debezium
// gives them: Debezium restores its transaction counters from its offset.
// A primary-key change counts two events, as in metadata.
func (b *Builder) Skipped(ctx context.Context, m pglogrepl.Message, lsn pglogrepl.LSN) error {
	var relID uint32
	events := int64(1)
	switch m := m.(type) {
	case *pglogrepl.InsertMessage:
		relID = m.RelationID
	case *pglogrepl.DeleteMessage:
		relID = m.RelationID
	case *pglogrepl.UpdateMessage:
		relID = m.RelationID
		if m.OldTuple != nil {
			t, err := b.table(ctx, relID)
			if err != nil {
				return err
			}
			if t.keyChanged(m.OldTuple, m.NewTuple) {
				events = 2
			}
		}
	default:
		return nil // truncates and the rest are not events here
	}
	rel, ok := b.shapes[relID]
	if !ok {
		return fmt.Errorf("debezium mode: no relation message for relation %d", relID)
	}
	if !b.tx.active {
		return fmt.Errorf("debezium mode: change at %s outside a transaction", lsn)
	}
	b.tx.total += events
	b.tx.coll[rel.Namespace+"."+rel.RelationName] += events
	return nil
}

// table returns the built shape of a relation, reading the catalog the
// first time a change uses the shape.
func (b *Builder) table(ctx context.Context, relID uint32) (*table, error) {
	if t, ok := b.tables[relID]; ok {
		return t, nil
	}
	rel, ok := b.shapes[relID]
	if !ok {
		return nil, fmt.Errorf("debezium mode: no relation message for relation %d", relID)
	}
	var lastErr error
	for attempt := 0; attempt < catalogAttempts; attempt++ {
		if attempt > 0 {
			b.sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		// A fresh type map per read, so an enum's new members are seen.
		types := map[uint32]*PGType{}
		cat, db, err := readTable(ctx, b.q, relID, types)
		if err != nil {
			return nil, err
		}
		// Types of columns the catalog no longer has (a replayed shape).
		for _, c := range rel.Columns {
			if _, ok := types[c.DataType]; !ok {
				if err := readTypeOnly(ctx, b.q, c.DataType, types); err != nil {
					return nil, err
				}
			}
		}
		t, err := newTable(rel, cat, types, &b.s)
		if err != nil {
			if CodeOf(err) == CodeCatalogSkew {
				lastErr = err
				continue
			}
			return nil, err
		}
		if err := t.checkIdentity(rel, cat); err != nil {
			return nil, err
		}
		b.db = db
		b.tables[relID] = t
		return t, nil
	}
	return nil, lastErr
}

// metadata is the debezium.* metadata of one change. events is the number of
// Debezium events the change becomes (2 for a primary-key change), by which
// the transaction counters advance; the record carries the first event's
// orders.
func (b *Builder) metadata(t *table, op string, lsn pglogrepl.LSN, events int) (opencdc.Metadata, error) {
	if !b.tx.active {
		return nil, fmt.Errorf("debezium mode: change at %s outside a transaction", lsn)
	}
	seq0 := "null"
	if b.lastCommitEnd != 0 {
		seq0 = strconv.Quote(strconv.FormatUint(uint64(b.lastCommitEnd), 10))
	}
	ct := b.tx.commitTime
	us := ct.UnixMicro()
	md := opencdc.Metadata{
		opencdc.MetadataCollection: t.schemaName + "." + t.tableName,
		MetaTopic:                  t.topic,
		MetaOp:                     op,
		MetaDescriptor:             t.descriptor,

		MetaSourcePfx + "version":   b.s.SourceVersion(),
		MetaSourcePfx + "connector": connectorName,
		MetaSourcePfx + "name":      b.s.TopicPrefix,
		MetaSourcePfx + "ts_ms":     strconv.FormatInt(ct.UnixMilli(), 10),
		MetaSourcePfx + "snapshot":  boolFalse,
		MetaSourcePfx + "db":        b.db,
		MetaSourcePfx + "sequence":  "[" + seq0 + "," + strconv.Quote(strconv.FormatUint(uint64(lsn), 10)) + "]",
		MetaSourcePfx + "ts_us":     strconv.FormatInt(us, 10),
		MetaSourcePfx + "ts_ns":     strconv.FormatInt(us*1000, 10),
		MetaSourcePfx + "schema":    t.schemaName,
		MetaSourcePfx + "table":     t.tableName,
		MetaSourcePfx + "txId":      strconv.FormatUint(uint64(b.tx.xid), 10),
		MetaSourcePfx + "lsn":       strconv.FormatUint(uint64(lsn), 10),
	}
	coll := t.schemaName + "." + t.tableName
	total, collOrder := b.tx.total+1, b.tx.coll[coll]+1
	b.tx.total += int64(events)
	b.tx.coll[coll] += int64(events)
	if b.s.ProvideTransactionMetadata {
		md[MetaTxID] = strconv.FormatUint(uint64(b.tx.xid), 10) + ":" + strconv.FormatUint(uint64(lsn), 10)
		md[MetaTxTotal] = strconv.FormatInt(total, 10)
		md[MetaTxCollOrder] = strconv.FormatInt(collOrder, 10)
	}
	return md, nil
}

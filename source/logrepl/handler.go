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
	"sync/atomic"
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
	batchSize int
	// flushInterval time.Duration

	// recordsBatch holds the batch that is currently being built.
	recordsBatch     *internal.Blocking[opencdc.Record]
	flushImmediately atomic.Bool

	// out is a sending channel with batches of records.
	out            chan<- []opencdc.Record
	lastTXLSN      pglogrepl.LSN
	withAvroSchema bool
	keySchemas     map[string]cschema.Schema
	payloadSchemas map[string]cschema.Schema
}

func NewCDCHandler(
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
		recordsBatch:   internal.NewBlocking[opencdc.Record](nil, internal.WithCapacity(batchSize)),
		out:            out,
		withAvroSchema: withAvroSchema,
		keySchemas:     make(map[string]cschema.Schema),
		payloadSchemas: make(map[string]cschema.Schema),
		batchSize:      batchSize,
		// flushInterval:  flushInterval,
	}

	return h
}

func (h *CDCHandler) FlushASAP() {
	h.flushImmediately.Store(true)
}

func (h *CDCHandler) flush(ctx context.Context) error {
	recs := h.recordsBatch.Clear()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.out <- recs:
		sdk.Logger(ctx).Trace().
			Int("records", len(recs)).
			Msg("CDCHandler sending batch of records")
		return nil
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

	return h.addToBatch(ctx, rec)
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

	rec := sdk.Util.Source.NewRecordUpdate(
		h.buildPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(newValues, rel.RelationName),
		h.buildRecordPayload(oldValues),
		h.buildRecordPayload(newValues),
	)
	h.attachSchemas(rec, rel.RelationName)

	return h.addToBatch(ctx, rec)
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

	return h.addToBatch(ctx, rec)
}

// addToBatch the record to the output channel or detect the cancellation of the
// context and return the context error.
func (h *CDCHandler) addToBatch(ctx context.Context, rec opencdc.Record) error {
	err := h.recordsBatch.Offer(rec)
	if err != nil {
		return fmt.Errorf("failed to flush records: %w", err)
	}

	if h.recordsBatch.IsFull() || h.flushImmediately.Load() {
		return h.flush(ctx)
	}

	return nil
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

// buildPosition stores the LSN in position and converts it to bytes.
func (*CDCHandler) buildPosition(lsn pglogrepl.LSN) opencdc.Position {
	return position.Position{
		Type:    position.TypeCDC,
		LastLSN: lsn.String(),
	}.ToSDKPosition()
}

// updateAvroSchema generates and stores avro schema based on the relation's row,
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

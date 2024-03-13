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

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
)

// CDCHandler is responsible for handling logical replication messages,
// converting them to a record and sending them to a channel.
type CDCHandler struct {
	tableKeys   map[string]string
	relationSet *internal.RelationSet
	out         chan<- sdk.Record
}

func NewCDCHandler(
	rs *internal.RelationSet,
	tableKeys map[string]string,
	out chan<- sdk.Record,
) *CDCHandler {
	return &CDCHandler{
		tableKeys:   tableKeys,
		relationSet: rs,
		out:         out,
	}
}

// Handle is the handler function that receives all logical replication messages.
func (h *CDCHandler) Handle(ctx context.Context, m pglogrepl.Message, lsn pglogrepl.LSN) error {
	sdk.Logger(ctx).Trace().
		Str("lsn", lsn.String()).
		Str("messageType", m.Type().String()).
		Msg("handler received pglogrepl.Message")

	switch m := m.(type) {
	case *pglogrepl.RelationMessage:
		// We have to add the Relations to our Set so that we can
		// decode our own output
		h.relationSet.Add(m)
	case *pglogrepl.InsertMessage:
		err := h.handleInsert(ctx, m, lsn)
		if err != nil {
			return fmt.Errorf("logrepl handler insert: %w", err)
		}
	case *pglogrepl.UpdateMessage:
		err := h.handleUpdate(ctx, m, lsn)
		if err != nil {
			return fmt.Errorf("logrepl handler update: %w", err)
		}
	case *pglogrepl.DeleteMessage:
		err := h.handleDelete(ctx, m, lsn)
		if err != nil {
			return fmt.Errorf("logrepl handler delete: %w", err)
		}
	}

	return nil
}

// handleInsert formats a Record with INSERT event data from Postgres and sends
// it to the output channel.
func (h *CDCHandler) handleInsert(
	ctx context.Context,
	msg *pglogrepl.InsertMessage,
	lsn pglogrepl.LSN,
) (err error) {
	rel, err := h.relationSet.Get(msg.RelationID)
	if err != nil {
		return err
	}

	newValues, err := h.relationSet.Values(msg.RelationID, msg.Tuple)
	if err != nil {
		return fmt.Errorf("failed to decode new values: %w", err)
	}

	rec := sdk.Util.Source.NewRecordCreate(
		LSNToPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(newValues, rel.RelationName),
		sdk.StructuredData(newValues),
	)
	return h.send(ctx, rec)
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

	oldValues, err := h.relationSet.Values(msg.RelationID, msg.OldTuple)
	if err != nil {
		// this is not a critical error, old values are optional, just log it
		// we use level "trace" intentionally to not clog up the logs in production
		sdk.Logger(ctx).Trace().Err(err).Msg("could not parse old values from UpdateMessage")
	}

	rec := sdk.Util.Source.NewRecordUpdate(
		LSNToPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(newValues, rel.RelationName),
		sdk.StructuredData(oldValues),
		sdk.StructuredData(newValues),
	)
	return h.send(ctx, rec)
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

	rec := sdk.Util.Source.NewRecordDelete(
		LSNToPosition(lsn),
		h.buildRecordMetadata(rel),
		h.buildRecordKey(oldValues, rel.RelationName),
	)
	return h.send(ctx, rec)
}

// send the record to the output channel or detect the cancellation of the
// context and return the context error.
func (h *CDCHandler) send(ctx context.Context, rec sdk.Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.out <- rec:
		return nil
	}
}

func (h *CDCHandler) buildRecordMetadata(relation *pglogrepl.RelationMessage) map[string]string {
	return map[string]string{
		MetadataPostgresTable: relation.RelationName,
	}
}

// buildRecordKey takes the values from the message and extracts the key that
// matches the configured keyColumnName.
func (h *CDCHandler) buildRecordKey(values map[string]any, table string) sdk.Data {
	keyColumn := h.tableKeys[table]
	key := make(sdk.StructuredData)
	for k, v := range values {
		if keyColumn == k {
			key[k] = v
			break // TODO add support for composite keys
		}
	}
	return key
}

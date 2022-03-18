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
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
)

type action string

var (
	actionInsert action = "insert"
	actionUpdate action = "update"
	actionDelete action = "delete"
)

type CDCHandler struct {
	keyColumn   string
	columns     map[string]bool // columns can be used to filter only specific columns
	relationSet *internal.RelationSet
	out         chan<- sdk.Record
}

func NewCDCHandler(
	rs *internal.RelationSet,
	keyColumn string,
	columns []string,
	out chan<- sdk.Record,
) *CDCHandler {
	var columnSet map[string]bool
	if len(columns) > 0 {
		columnSet = make(map[string]bool)
		for _, col := range columns {
			columnSet[col] = true
		}
	}
	return &CDCHandler{
		keyColumn:   keyColumn,
		columns:     columnSet,
		relationSet: rs,
		out:         out,
	}
}

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
			return fmt.Errorf("logrepl handler update: %w", err)
		}
	}

	return nil
}

// handleInsert formats a Record with INSERT event data from Postgres and
// inserts it into the records buffer for later reading.
func (h *CDCHandler) handleInsert(
	ctx context.Context,
	msg *pglogrepl.InsertMessage,
	lsn pglogrepl.LSN,
) (err error) {
	rel, err := h.relationSet.Get(pgtype.OID(msg.RelationID))
	if err != nil {
		return err
	}

	newValues, err := h.relationSet.Values(pgtype.OID(msg.RelationID), msg.Tuple)
	if err != nil {
		return fmt.Errorf("failed to decode new values: %w", err)
	}

	rec := h.buildRecord(actionInsert, rel, newValues, lsn)
	return h.send(ctx, rec)
}

// handleUpdate formats a record with a UPDATE event data from Postgres and
// inserts it into the records buffer for later reading.
func (h *CDCHandler) handleUpdate(
	ctx context.Context,
	msg *pglogrepl.UpdateMessage,
	lsn pglogrepl.LSN,
) error {
	rel, err := h.relationSet.Get(pgtype.OID(msg.RelationID))
	if err != nil {
		return err
	}

	newValues, err := h.relationSet.Values(pgtype.OID(msg.RelationID), msg.NewTuple)
	if err != nil {
		return fmt.Errorf("failed to decode new values: %w", err)
	}

	rec := h.buildRecord(actionUpdate, rel, newValues, lsn)
	return h.send(ctx, rec)
}

// handleDelete formats a record with a delete event data from Postgres.
// delete events only send along the primary key of the table.
func (h *CDCHandler) handleDelete(
	ctx context.Context,
	msg *pglogrepl.DeleteMessage,
	lsn pglogrepl.LSN,
) error {
	rel, err := h.relationSet.Get(pgtype.OID(msg.RelationID))
	if err != nil {
		return err
	}

	oldValues, err := h.relationSet.Values(pgtype.OID(msg.RelationID), msg.OldTuple)
	if err != nil {
		return fmt.Errorf("failed to decode old values: %w", err)
	}

	rec := h.buildRecord(actionDelete, rel, oldValues, lsn)
	// NB: Deletes shouldn't have payloads. Key + delete action is sufficient.
	rec.Payload = nil

	return h.send(ctx, rec)
}

func (h *CDCHandler) send(ctx context.Context, rec sdk.Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.out <- rec:
		return nil
	}
}

func (h *CDCHandler) buildRecord(
	action action,
	relation *pglogrepl.RelationMessage,
	values map[string]pgtype.Value,
	lsn pglogrepl.LSN,
) sdk.Record {
	return sdk.Record{
		Position: LSNToPosition(lsn),
		Metadata: map[string]string{
			"action": string(action),
			"table":  relation.RelationName,
		},
		CreatedAt: time.Now(),
		Key:       h.buildRecordKey(values),
		Payload:   h.buildRecordPayload(values),
	}
}

// withKey takes the values from the message and extracts a key that matches
// the configured keyColumnName.
func (h *CDCHandler) buildRecordKey(values map[string]pgtype.Value) sdk.Data {
	key := sdk.StructuredData{}
	for k, v := range values {
		if h.keyColumn == k {
			key[k] = v.Get()
		}
	}
	return key
}

// withPayload takes a record and a map of values and formats a payload for
// the record and then returns the record with that payload attached.
func (h *CDCHandler) buildRecordPayload(values map[string]pgtype.Value) sdk.Data {
	payload := sdk.StructuredData{}
	for k, v := range values {
		// filter columns if columns are specified
		if h.columns == nil || h.columns[k] {
			value := v.Get()
			payload[k] = value
		}
	}
	return payload
}

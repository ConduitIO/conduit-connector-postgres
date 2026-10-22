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

package logrepl

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func traceCtx(t *testing.T) (context.Context, *bytes.Buffer) {
	t.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	t.Cleanup(func() { zerolog.SetGlobalLevel(prev) })

	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.TraceLevel)
	return logger.WithContext(context.Background()), &buf
}

// TestNewCombinedIterator_BadPositionLogDoesNotContainPosition: a snapshot
// position holds primary key values, so a position that fails to parse must
// not be written to the log.
func TestNewCombinedIterator_BadPositionLogDoesNotContainPosition(t *testing.T) {
	is := is.New(t)
	ctx, buf := traceCtx(t)

	const secret = "customer-SECRET-4711"
	_, err := NewCombinedIterator(ctx, nil, Config{
		Position: opencdc.Position(`{"type":"s","snapshots":{"orders":{"last_read":"` + secret + `"`), // truncated JSON
	})
	is.True(err != nil)

	out := buf.String()
	is.True(strings.Contains(out, "failed to parse position"))
	is.True(strings.Contains(out, `"position_length"`))
	is.True(!strings.Contains(out, secret))
}

// TestHandler_OldValuesLogDoesNotContainRowData: when the old tuple of an
// UPDATE cannot be decoded, the trace line must not carry the decode error,
// which can include the value that failed to decode.
func TestHandler_OldValuesLogDoesNotContainRowData(t *testing.T) {
	is := is.New(t)
	ctx, buf := traceCtx(t)

	const secret = "SECRET-not-an-int-4711"
	ch := make(chan []opencdc.Record, 1)
	h := NewCDCHandler(ctx, newRelationSetForToastTests(), map[string]string{"toast_test": "id"}, ch, false, 1, time.Hour, position.Position{}, SchemaDriftPolicyHalt)

	err := h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: &pglogrepl.TupleData{
			ColumnNum: 3,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte(secret)}, // not an int8
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("a")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("b")},
			},
		},
		NewTuple: &pglogrepl.TupleData{
			ColumnNum: 3,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("c")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("d")},
			},
		},
	}, 1)
	is.NoErr(err)

	out := buf.String()
	is.True(strings.Contains(out, "could not parse old values from UpdateMessage"))
	is.True(!strings.Contains(out, secret))
}

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

package internal

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// A value Postgres sent that cannot be decoded is record data. The decoder's
// error repeats it, or part of it (strconv's `parsing "..."`, an array
// element, a range bound, a coordinate), and the handler wraps this error into
// "failed to decode new/old values", which reaches the DLQ, the API and CLI
// output: only the column, the size and a hash may remain.
func TestRelationSet_DecodeErrorDoesNotContainTheValue(t *testing.T) {
	for name, tc := range map[string]struct {
		oid   uint32
		value string
	}{
		"int8":          {pgtype.Int8OID, "SECRET-4711"},
		"short value":   {pgtype.Int8OID, "x"},
		"int8 array":    {pgtype.Int8ArrayOID, "{1,SECRET-4711}"},
		"timestamptz[]": {pgtype.TimestamptzArrayOID, "{2020-01-01 00:00:00+00,SECRET-4711}"},
		"int4range":     {pgtype.Int4rangeOID, "[1,SECRET-4711)"},
		"point":         {pgtype.PointOID, "(1,SECRET-4711)"},
		"interval":      {pgtype.IntervalOID, "1 SECRET-4711"},
		"timestamptz":   {pgtype.TimestamptzOID, "SECRET-4711"},
		"uuid":          {pgtype.UUIDOID, "SECRET-4711"},
		"numeric":       {pgtype.NumericOID, "SECRET-4711"},
		"bool":          {pgtype.BoolOID, "SECRET-4711"},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			rs := NewRelationSet()
			rs.Add(&pglogrepl.RelationMessage{
				RelationID:   1,
				RelationName: "orders",
				ColumnNum:    1,
				Columns:      []*pglogrepl.RelationMessageColumn{{Name: "c", DataType: tc.oid}},
			})
			_, err := rs.Values(1, &pglogrepl.TupleData{
				ColumnNum: 1,
				Columns:   []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeText, Data: []byte(tc.value)}},
			})
			if err == nil {
				t.Skip("the value decodes; nothing to hide")
			}
			msg := err.Error()
			is.True(strings.Contains(msg, `"c"`))
			is.True(strings.Contains(msg, "value hash "))
			is.True(strings.Contains(msg, "details not shown"))
			is.True(!strings.Contains(msg, "SECRET"))
			is.True(!strings.Contains(msg, "4711"))
			var cause interface{ Unwrap() error }
			is.True(errors.As(err, &cause)) // the chain is kept
		})
	}
}

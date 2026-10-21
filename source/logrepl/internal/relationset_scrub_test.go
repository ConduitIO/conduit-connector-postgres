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
// error repeats it (strconv's `parsing "..."`), and the handler wraps this
// error into "failed to decode new/old values", which reaches the DLQ, the API
// and CLI output: only the column, the size and a hash may remain.
func TestRelationSet_DecodeErrorDoesNotContainTheValue(t *testing.T) {
	is := is.New(t)
	rs := NewRelationSet()
	rs.Add(&pglogrepl.RelationMessage{
		RelationID:   1,
		RelationName: "orders",
		ColumnNum:    1,
		Columns:      []*pglogrepl.RelationMessageColumn{{Name: "qty", DataType: pgtype.Int8OID}},
	})

	for _, value := range []string{"SECRET-4711", "x"} {
		_, err := rs.Values(1, &pglogrepl.TupleData{
			ColumnNum: 1,
			Columns:   []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeText, Data: []byte(value)}},
		})
		is.True(err != nil)
		msg := err.Error()
		is.True(strings.Contains(msg, `"qty"`))
		is.True(strings.Contains(msg, "value hash "))
		is.True(!strings.Contains(msg, "SECRET"))
		is.True(!strings.Contains(msg, `"`+value+`"`))
		var cause interface{ Unwrap() error }
		is.True(errors.As(err, &cause)) // the chain is kept
	}
}

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
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

// keyText names a row in errors that reach the DLQ, the API and CLI output:
// the key columns and a hash of their values, never the values.
func TestKeyText_DoesNotContainKeyValues(t *testing.T) {
	is := is.New(t)

	rel := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{
		{Name: "id", Flags: 1},
		{Name: "note", Flags: 0},
	}}
	row := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("customer-SECRET-4711")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("note-SECRET")},
	}}

	got := keyText(rel, row)
	is.True(strings.HasPrefix(got, "columns id, key hash "))
	is.True(!strings.Contains(got, "SECRET"))
	is.Equal(keyText(rel, &pglogrepl.TupleData{}), "") // no key columns: nothing to name
}

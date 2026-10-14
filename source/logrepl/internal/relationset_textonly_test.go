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
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// TestRelationSet_TSVectorIsText: every constructor of RelationSet (the CDC
// iterator calls NewRelationSetWithEncoding) decodes tsvector and tsvector[] to
// the text Postgres sent. pgx >= 5.9 would decode a pgtype.TSVector.
func TestRelationSet_TSVectorIsText(t *testing.T) {
	ctors := map[string]func() *RelationSet{
		"NewRelationSet":             NewRelationSet,
		"NewRelationSetWithEncoding": func() *RelationSet { return NewRelationSetWithEncoding(0) },
	}
	for name, newSet := range ctors {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)

			rs := newSet()
			rs.Add(&pglogrepl.RelationMessage{
				RelationID:   1,
				RelationName: "ts",
				ColumnNum:    3,
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int8OID},
					{Name: "v", DataType: pgtype.TSVectorOID},
					{Name: "va", DataType: pgtype.TSVectorArrayOID},
				},
			})

			values, err := rs.Values(1, &pglogrepl.TupleData{
				ColumnNum: 3,
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte(`'a':1A,2B 'cat':3`)},
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte(`{{'a','b'},{'c','d'}}`)},
				},
			})
			is.NoErr(err)
			is.Equal(values["v"], `'a':1A,2B 'cat':3`)
			is.Equal(values["va"], `{{'a','b'},{'c','d'}}`)
		})
	}
}

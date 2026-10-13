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

package types_test

import (
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// TestRegisterTextOnly: tsvector and tsvector[] decode to the text Postgres
// sent, byte for byte, whatever pgx version decodes them. Without the
// registration pgx >= 5.9 returns a pgtype.TSVector struct and this connector
// line would emit {"Lexemes":...} instead of text.
func TestRegisterTextOnly(t *testing.T) {
	tests := []struct {
		name string
		oid  uint32
		text string
	}{
		{"scalar", pgtype.TSVectorOID, `'a':1A,2B 'cat':3 'it''s' 'back\\slash'`},
		{"empty", pgtype.TSVectorOID, ``},
		{"array", pgtype.TSVectorArrayOID, `{"'a':1 'b':2",'c'}`},
		{"array with NULL and empty element", pgtype.TSVectorArrayOID, `{NULL,""}`},
		{"multi dimensional array", pgtype.TSVectorArrayOID, `{{'a','b'},{'c','d'}}`},
		{"array with lower bound", pgtype.TSVectorArrayOID, `[0:1]={'a','b'}`},
	}

	m := pgtype.NewMap()
	types.RegisterTextOnly(m)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			typ, ok := m.TypeForOID(tc.oid)
			is.True(ok)

			got, err := typ.Codec.DecodeValue(m, tc.oid, pgtype.TextFormatCode, []byte(tc.text))
			is.NoErr(err)
			is.Equal(got, tc.text)
		})
	}

	t.Run("NULL", func(t *testing.T) {
		is := is.New(t)

		typ, ok := m.TypeForOID(pgtype.TSVectorOID)
		is.True(ok)

		got, err := typ.Codec.DecodeValue(m, pgtype.TSVectorOID, pgtype.TextFormatCode, nil)
		is.NoErr(err)
		is.Equal(got, nil)
	})
}

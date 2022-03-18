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

package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/matryer/is"
)

func TestRelationSet(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, conn)

	_, messages := setupSubscription(ctx, t, conn.Config().Config, table)

	// insert to get new message into publication
	query := `INSERT INTO %s (id, column1, column2, column3)
		VALUES (6, 'bizz', 456, false)`
	_, err := conn.Exec(ctx, fmt.Sprintf(query, table))
	is.NoErr(err)

	<-messages                                       // skip first message, it's a begin message
	rel := (<-messages).(*pglogrepl.RelationMessage) // second message is a relation
	ins := (<-messages).(*pglogrepl.InsertMessage)   // third one is the insert
	<-messages                                       // fourth one is the commit

	t.Run("values with unknown relation", func(t *testing.T) {
		// if we try to get values before adding info about the relation it will fail
		rs := NewRelationSet(conn.ConnInfo())

		got, err := rs.Get(pgtype.OID(rel.RelationID))
		is.True(err != nil)
		is.Equal(got, nil)

		values, err := rs.Values(pgtype.OID(ins.RelationID), ins.Tuple)
		is.True(err != nil)
		is.Equal(values, nil)
	})

	t.Run("values with known relation", func(t *testing.T) {
		rs := NewRelationSet(conn.ConnInfo())

		rs.Add(rel)
		gotRel, err := rs.Get(pgtype.OID(rel.RelationID))
		is.NoErr(err)
		is.Equal(gotRel, rel)

		values, err := rs.Values(pgtype.OID(ins.RelationID), ins.Tuple)
		is.NoErr(err)
		is.Equal(values, map[string]pgtype.Value{
			"id": &pgtype.Int8{
				Int:    6,
				Status: pgtype.Present,
			},
			"key": &pgtype.Bytea{
				Bytes:  nil,
				Status: pgtype.Null,
			},
			"column1": &pgtype.Varchar{
				String: "bizz",
				Status: pgtype.Present,
			},
			"column2": &pgtype.Int4{
				Int:    456,
				Status: pgtype.Present,
			},
			"column3": &pgtype.Bool{
				Bool:   false,
				Status: pgtype.Present,
			},
		})
	})
	// now
}

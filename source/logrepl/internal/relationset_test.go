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

func TestRelationSetUnregisteredType(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	rs := NewRelationSet(conn.ConnInfo())

	got, err := rs.Get(pgtype.OID(1234567))
	is.True(err != nil)
	is.Equal(got, nil)
}

func TestRelationSetAllTypes(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	table := setupTableAllTypes(ctx, t, conn)
	_, messages := setupSubscription(ctx, t, conn.Config().Config, table)
	insertRowAllTypes(ctx, t, conn, table)

	<-messages                                       // skip first message, it's a begin message
	rel := (<-messages).(*pglogrepl.RelationMessage) // second message is a relation
	ins := (<-messages).(*pglogrepl.InsertMessage)   // third one is the insert
	<-messages                                       // fourth one is the commit

	rs := NewRelationSet(conn.ConnInfo())

	rs.Add(rel)
	gotRel, err := rs.Get(pgtype.OID(rel.RelationID))
	is.NoErr(err)
	is.Equal(gotRel, rel)

	values, err := rs.Values(pgtype.OID(ins.RelationID), ins.Tuple)
	is.NoErr(err)
	isValuesAllTypes(is, values)
}

// setupTableAllTypes creates a new table with all types and returns its name.
func setupTableAllTypes(ctx context.Context, t *testing.T, conn test.Querier) string {
	is := is.New(t)
	table := test.RandomIdentifier(t)
	// TODO adjust test to run for all remaining types
	query := `
		CREATE TABLE %s (
		  col_bigint        bigint PRIMARY KEY,
		  col_bigserial     bigserial,
		  col_bit           bit(8),
		  col_varbit        varbit(8),
		  col_boolean       boolean,
		  col_box           box,
		  col_bytea         bytea
		--col_char          char(10),
		--col_varchar       varchar(10),
		--col_cidr          cidr,
		--col_circle        circle,
		--col_date          date,
		--col_float8        float8,
		--col_inet          inet,
		--col_int           int,
		--col_interval      interval TBD,
		--col_json          json,
		--col_jsonb         jsonb,
		--col_line          line,
		--col_lseg          lseg,
		--col_macaddr       macaddr,
		--col_money         money,
		--col_numeric       numeric(8,2),
		--col_path          path,
		--col_pg_lsn        pg_lsn,
		--col_point         point,
		--col_polygon       polygon,
		--col_float4        float4,
		--col_int2          int2,
		--col_serial2       serial2,
		--col_serial4       serial4,
		--col_text          text,
		--col_time          time,
		--col_timetz        timetz,
		--col_timestamp     timestamp,
		--col_timestamptz   timestamptz,
		--col_tsquery       tsquery,
		--col_tsvector      tsvector,
		--col_txid_snapshot txid_snapshot,
		--col_uuid          uuid,
		--col_xml           xml
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %s`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})
	return table
}

func insertRowAllTypes(ctx context.Context, t *testing.T, conn test.Querier, table string) {
	is := is.New(t)
	query := `
		INSERT INTO %s (
		  col_bigint,
		  col_bigserial,
		  col_bit,
		  col_varbit,
		  col_boolean,
		  col_box,
		  col_bytea
		-- col_char,
		-- col_varchar,
		-- col_cidr,
		-- col_circle,
		-- col_date,
		-- col_float8,
		-- col_inet,
		-- col_int,
		-- col_json,
		-- col_jsonb,
		-- col_line,
		-- col_lseg,
		-- col_macaddr,
		-- col_money,
		-- col_numeric,
		-- col_path
		-- col_pg_lsn
		-- col_point
		-- col_polygon
		-- col_float4
		-- col_int2
		-- col_serial2
		-- col_serial4
		-- col_text
		-- col_time
		-- col_timetz
		-- col_timestamp
		-- col_timestamptz
		-- col_tsquery
		-- col_tsvector
		-- col_txid_snapshot
		-- col_uuid
		-- col_xml
		) VALUES (
		  1,
		  2,
		  B'00000011',
		  B'00000100',
		  true,
		  '(6,7),(8,9)',
		  '\x0A'
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func isValuesAllTypes(is *is.I, got map[string]pgtype.Value) {
	want := map[string]pgtype.Value{
		"col_bigint": &pgtype.Int8{
			Int:    1,
			Status: pgtype.Present,
		},
		"col_bigserial": &pgtype.Int8{
			Int:    2,
			Status: pgtype.Present,
		},
		"col_bit": &pgtype.Bit{
			Bytes:  []byte{3},
			Len:    8,
			Status: pgtype.Present,
		},
		"col_varbit": &pgtype.Varbit{
			Bytes:  []byte{4},
			Len:    8,
			Status: pgtype.Present,
		},
		"col_boolean": &pgtype.Bool{
			Bool:   true,
			Status: pgtype.Present,
		},
		"col_box": &pgtype.Box{
			P: [2]pgtype.Vec2{
				{8, 9}, {6, 7},
			},
			Status: pgtype.Present,
		},
		"col_bytea": &pgtype.Bytea{
			Bytes:  []byte{10},
			Status: pgtype.Present,
		},
	}
	is.Equal(got, want)
}

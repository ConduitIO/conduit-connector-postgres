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
	"math/big"
	"net"
	"testing"
	"time"

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
	query := `
		CREATE TABLE %s (
		  col_bit           bit(8),
		  col_varbit        varbit(8),
		  col_boolean       boolean,
		  col_box           box,
		  col_bytea         bytea,
		  col_char          char(3),
		  col_varchar       varchar(10),
		  col_cidr          cidr,
		  col_circle        circle,
		  col_date          date,
		  col_float4        float4,
		  col_float8        float8,
		  col_inet          inet,
		  col_int2          int2,
		  col_int4          int4,
		  col_int8          int8,
		  col_interval      interval,
		  col_json          json,
		  col_jsonb         jsonb,
		  col_line          line,
		  col_lseg          lseg,
		  col_macaddr       macaddr,
		  col_macaddr8      macaddr8,
		  col_money         money,
		  col_numeric       numeric(8,2),
		  col_path          path,
		  col_pg_lsn        pg_lsn,
		  col_pg_snapshot   pg_snapshot,
		  col_point         point,
		  col_polygon       polygon,
		  col_serial2       serial2,
		  col_serial4       serial4,
		  col_serial8       serial8,
		  col_text          text,
		  col_time          time,
		  col_timetz        timetz,
		  col_timestamp     timestamp,
		  col_timestamptz   timestamptz,
		  col_tsquery       tsquery,
		  col_tsvector      tsvector,
		  col_uuid          uuid,
		  col_xml           xml
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
		  col_bit,
		  col_varbit,
		  col_boolean,
		  col_box,
		  col_bytea,
		  col_char,
		  col_varchar,
		  col_cidr,
		  col_circle,
		  col_date,
		  col_float4,
		  col_float8,
		  col_inet,
		  col_int2,
		  col_int4,
		  col_int8,
		  col_interval,
		  col_json,
		  col_jsonb,
		  col_line,
		  col_lseg,
		  col_macaddr,
		  col_macaddr8,
		  col_money,
		  col_numeric,
		  col_path,
		  col_pg_lsn,
		  col_pg_snapshot,
		  col_point,
		  col_polygon,
		  col_serial2,
		  col_serial4,
		  col_serial8,
		  col_text,
		  col_time,
		  col_timetz,
		  col_timestamp,
		  col_timestamptz,
		  col_tsquery,
		  col_tsvector,
		  col_uuid,
		  col_xml
		) VALUES (
		  B'00000001',                                -- col_bit
		  B'00000010',                                -- col_varbit
		  true,                                       -- col_boolean
		  '(3,4),(5,6)',                              -- col_box
		  '\x07',                                     -- col_bytea
		  '8',                                        -- col_char
		  '9',                                        -- col_varchar
		  '192.168.100.128/25',                       -- col_cidr
		  '<(11,12),13>',                             -- col_circle
		  '2022-03-14',                               -- col_date
		  15,                                         -- col_float4
		  16.16,                                      -- col_float8
		  '192.168.0.17',                             -- col_inet
		  32767,                                      -- col_int2
		  2147483647,                                 -- col_int4
		  9223372036854775807,                        -- col_int8
		  '18 seconds',                               -- col_interval
		  '{"foo":"bar"}',                            -- col_json
		  '{"foo":"baz"}',                            -- col_jsonb
		  '{19,20,21}',                               -- col_line
		  '((22,23),(24,25))',                        -- col_lseg
		  '08:00:2b:01:02:26',                        -- col_macaddr
		  '08:00:2b:01:02:03:04:27',                  -- col_macaddr8
		  '$28',                                      -- col_money
		  '292929.29',                                -- col_numeric
		  '[(30,31),(32,33),(34,35)]',                -- col_path
		  '36/37',                                    -- col_pg_lsn
		  '10:20:10,14,15',                           -- col_pg_snapshot
		  '(38,39)',                                  -- col_point
		  '((40,41),(42,43),(44,45))',                -- col_polygon
		  32767,                                      -- col_serial2
		  2147483647,                                 -- col_serial4
		  9223372036854775807,                        -- col_serial8
		  'foo bar baz',                              -- col_text
		  '04:05:06.789',                             -- col_time
		  '04:05:06.789-08',                          -- col_timetz
		  '2022-03-14 15:16:17',                      -- col_timestamp
		  '2022-03-14 15:16:17-08',                   -- col_timestamptz
		  'fat & (rat | cat)',                        -- col_tsquery
		  'a fat cat sat on a mat and ate a fat rat', -- col_tsvector
		  'bd94ee0b-564f-4088-bf4e-8d5e626caf66',     -- col_uuid
		  '<foo>bar</foo>'                            -- col_xml
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func isValuesAllTypes(is *is.I, got map[string]pgtype.Value) {
	want := map[string]pgtype.Value{
		"col_bit": &pgtype.Bit{
			Bytes:  []byte{0b01},
			Len:    8,
			Status: pgtype.Present,
		},
		"col_varbit": &pgtype.Varbit{
			Bytes:  []byte{0b10},
			Len:    8,
			Status: pgtype.Present,
		},
		"col_boolean": &pgtype.Bool{
			Bool:   true,
			Status: pgtype.Present,
		},
		"col_box": &pgtype.Box{
			P:      [2]pgtype.Vec2{{X: 5, Y: 6}, {X: 3, Y: 4}},
			Status: pgtype.Present,
		},
		"col_bytea": &pgtype.Bytea{
			Bytes:  []byte{0x07},
			Status: pgtype.Present,
		},
		"col_char": &pgtype.BPChar{
			String: "8  ", // blank padded char
			Status: pgtype.Present,
		},
		"col_varchar": &pgtype.Varchar{
			String: "9",
			Status: pgtype.Present,
		},
		"col_cidr": &pgtype.CIDR{
			IPNet: &net.IPNet{
				IP:   net.IPv4(192, 168, 100, 128).To4(),
				Mask: net.CIDRMask(25, 32),
			},
			Status: pgtype.Present,
		},
		"col_circle": &pgtype.Circle{
			P:      pgtype.Vec2{X: 11, Y: 12},
			R:      13,
			Status: pgtype.Present,
		},
		"col_date": &pgtype.Date{
			Time:             time.Date(2022, 3, 14, 0, 0, 0, 0, time.UTC),
			Status:           pgtype.Present,
			InfinityModifier: pgtype.None,
		},
		"col_float4": &pgtype.Float4{
			Float:  15,
			Status: pgtype.Present,
		},
		"col_float8": &pgtype.Float8{
			Float:  16.16,
			Status: pgtype.Present,
		},
		"col_inet": &pgtype.Inet{
			IPNet: &net.IPNet{
				IP:   net.IPv4(192, 168, 0, 17).To4(),
				Mask: net.CIDRMask(32, 32),
			},
			Status: pgtype.Present,
		},
		"col_int2": &pgtype.Int2{
			Int:    32767,
			Status: pgtype.Present,
		},
		"col_int4": &pgtype.Int4{
			Int:    2147483647,
			Status: pgtype.Present,
		},
		"col_int8": &pgtype.Int8{
			Int:    9223372036854775807,
			Status: pgtype.Present,
		},
		"col_interval": &pgtype.Interval{
			Microseconds: 18000000,
			Days:         0,
			Months:       0,
			Status:       pgtype.Present,
		},
		"col_json": &pgtype.JSON{
			Bytes:  []byte(`{"foo":"bar"}`),
			Status: pgtype.Present,
		},
		"col_jsonb": &pgtype.JSONB{
			Bytes:  []byte(`{"foo": "baz"}`),
			Status: pgtype.Present,
		},
		"col_line": &pgtype.Line{
			A:      19,
			B:      20,
			C:      21,
			Status: pgtype.Present,
		},
		"col_lseg": &pgtype.Lseg{
			P:      [2]pgtype.Vec2{{X: 22, Y: 23}, {X: 24, Y: 25}},
			Status: pgtype.Present,
		},
		"col_macaddr": &pgtype.Macaddr{
			Addr: func() net.HardwareAddr {
				mac, _ := net.ParseMAC("08:00:2b:01:02:26")
				return mac
			}(),
			Status: pgtype.Present,
		},
		"col_macaddr8": &pgtype.Unknown{
			String: "08:00:2b:01:02:03:04:27",
			Status: pgtype.Present,
		},
		"col_money": &pgtype.Unknown{
			String: "$28.00",
			Status: pgtype.Present,
		},
		"col_numeric": &pgtype.Numeric{
			Int:              big.NewInt(29292929),
			Exp:              -2,
			Status:           pgtype.Present,
			NaN:              false,
			InfinityModifier: pgtype.None,
		},
		"col_path": &pgtype.Path{
			P:      []pgtype.Vec2{{X: 30, Y: 31}, {X: 32, Y: 33}, {X: 34, Y: 35}},
			Closed: false,
			Status: pgtype.Present,
		},
		"col_pg_lsn": &pgtype.Unknown{
			String: "36/37",
			Status: pgtype.Present,
		},
		"col_pg_snapshot": &pgtype.Unknown{
			String: "10:20:10,14,15",
			Status: pgtype.Present,
		},
		"col_point": &pgtype.Point{
			P:      pgtype.Vec2{X: 38, Y: 39},
			Status: pgtype.Present,
		},
		"col_polygon": &pgtype.Polygon{
			P:      []pgtype.Vec2{{X: 40, Y: 41}, {X: 42, Y: 43}, {X: 44, Y: 45}},
			Status: pgtype.Present,
		},
		"col_serial2": &pgtype.Int2{
			Int:    32767,
			Status: pgtype.Present,
		},
		"col_serial4": &pgtype.Int4{
			Int:    2147483647,
			Status: pgtype.Present,
		},
		"col_serial8": &pgtype.Int8{
			Int:    9223372036854775807,
			Status: pgtype.Present,
		},
		"col_text": &pgtype.Text{
			String: "foo bar baz",
			Status: pgtype.Present,
		},
		"col_time": &pgtype.Time{
			Microseconds: time.Date(1970, 1, 1, 4, 5, 6, 789000000, time.UTC).UnixMicro(),
			Status:       pgtype.Present,
		},
		"col_timetz": &pgtype.Unknown{
			String: "04:05:06.789-08",
			Status: pgtype.Present,
		},
		"col_timestamp": &pgtype.Timestamp{
			Time:             time.Date(2022, 3, 14, 15, 16, 17, 0, time.UTC),
			Status:           pgtype.Present,
			InfinityModifier: pgtype.None,
		},
		"col_timestamptz": &pgtype.Timestamptz{
			Time:             time.Date(2022, 3, 14, 15+8, 16, 17, 0, time.FixedZone("", 0)),
			Status:           pgtype.Present,
			InfinityModifier: pgtype.None,
		},
		"col_tsquery": &pgtype.Unknown{
			String: "'fat' & ( 'rat' | 'cat' )",
			Status: pgtype.Present,
		},
		"col_tsvector": &pgtype.Unknown{
			String: "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'",
			Status: pgtype.Present,
		},
		"col_uuid": &pgtype.UUID{
			Bytes:  [16]byte{0xbd, 0x94, 0xee, 0x0b, 0x56, 0x4f, 0x40, 0x88, 0xbf, 0x4e, 0x8d, 0x5e, 0x62, 0x6c, 0xaf, 0x66},
			Status: pgtype.Present,
		},
		"col_xml": &pgtype.Unknown{
			String: "<foo>bar</foo>",
			Status: pgtype.Present,
		},
	}
	is.Equal(got, want)
}

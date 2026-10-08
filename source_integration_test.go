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

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Open(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	// Be sure primary key discovering works correctly on
	// table names with capital letters
	tableName := strings.ToUpper(test.RandomIdentifier(t))
	test.SetupTestTableWithName(ctx, t, conn, tableName)

	slotName := "conduitslot1"
	publicationName := "conduitpub1"

	s := NewSource()
	err := sdk.Util.ParseConfig(
		ctx,
		map[string]string{
			"url":                     test.RepmgrConnString,
			"tables":                  tableName,
			"snapshotMode":            "initial",
			"cdcMode":                 "logrepl",
			"logrepl.slotName":        slotName,
			"logrepl.publicationName": publicationName,
		},
		s.Config(),
		Connector.NewSpecification().SourceParams,
	)
	is.NoErr(err)

	err = s.Open(ctx, nil)
	is.NoErr(err)

	defer func() {
		is.NoErr(logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
			URL:             test.RepmgrConnString,
			SlotName:        slotName,
			PublicationName: publicationName,
		}))
		is.NoErr(s.Teardown(ctx))
	}()
}

func TestSource_ParseConfig(t *testing.T) {
	testCases := []struct {
		name    string
		cfg     config.Config
		wantErr bool
	}{
		{
			name: "valid postgres replication slot name",
			cfg: config.Config{
				"url":              "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
				"tables":           "table1,table2",
				"cdcMode":          "logrepl",
				"logrepl.slotName": "valid_slot_name",
			},
			wantErr: false,
		}, {
			name: "invalid postgres replication slot name",
			cfg: config.Config{
				"url":              "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
				"tables":           "table1,table2",
				"cdcMode":          "logrepl",
				"logrepl.slotName": "invalid:slot.name",
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			var cfg source.Config
			err := sdk.Util.ParseConfig(context.Background(), tc.cfg, cfg, Connector.NewSpecification().SourceParams)

			if tc.wantErr {
				is.True(err != nil)
				return
			}
			is.NoErr(err)
		})
	}
}

func TestSource_Read(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := setupSourceTable(ctx, t, conn)
	insertSourceRow(ctx, t, conn, table)

	s := NewSource()
	err := sdk.Util.ParseConfig(
		ctx,
		map[string]string{
			"url":          test.RepmgrConnString,
			"tables":       table,
			"snapshotMode": "initial",
			"cdcMode":      "logrepl",
		},
		s.Config(),
		Connector.NewSpecification().SourceParams,
	)
	is.NoErr(err)

	err = s.Open(ctx, nil)
	is.NoErr(err)

	recs, err := s.ReadN(ctx, 1)
	is.NoErr(err)

	fmt.Println(recs)
}

// setupSourceTable creates a new table with all types and returns its name.
func setupSourceTable(ctx context.Context, t *testing.T, conn test.Querier) string {
	is := is.New(t)
	table := test.RandomIdentifier(t)
	// todo still need to support:
	// bit, varbit, box, char(n), cidr, circle, inet, interval, line, lseg,
	// macaddr, macaddr8, money, path, pg_lsn, pg_snapshot, point, polygon,
	// time, timetz, tsquery, tsvector, xml
	query := `
		CREATE TABLE %s (
  		  id 				bigserial PRIMARY KEY,
		  col_boolean       boolean,
		  col_bytea         bytea,
		  col_varchar       varchar(10),
		  col_date          date,
		  col_float4        float4,
		  col_float8        float8,
		  col_int2          int2,
		  col_int4          int4,
		  col_int8          int8,
		  col_json          json,
		  col_jsonb         jsonb,
		  col_numeric       numeric(8,2),
		  col_serial2       serial2,
		  col_serial4       serial4,
		  col_serial8       serial8,
		  col_text          text,
		  col_timestamp     timestamp,
		  col_timestamptz   timestamptz,
		  col_uuid          uuid
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

func insertSourceRow(ctx context.Context, t *testing.T, conn test.Querier, table string) {
	is := is.New(t)
	query := `
		INSERT INTO %s (
		  col_boolean,
		  col_bytea,
		  col_varchar,
		  col_date,
		  col_float4,
		  col_float8,
		  col_int2,
		  col_int4,
		  col_int8,
		  col_json,
		  col_jsonb,
		  col_numeric,
		  col_serial2,
		  col_serial4,
		  col_serial8,
		  col_text,
		  col_timestamp,
		  col_timestamptz,
		  col_uuid
		) VALUES (
		  true,                                       -- col_boolean
		  '\x07',                                     -- col_bytea
		  '9',                                        -- col_varchar
		  '2022-03-14',                               -- col_date
		  15,                                         -- col_float4
		  16.16,                                      -- col_float8
		  32767,                                      -- col_int2
		  2147483647,                                 -- col_int4
		  9223372036854775807,                        -- col_int8
		  '{"foo": "bar"}',                            -- col_json
		  '{"foo": "baz"}',                            -- col_jsonb
		  '292929.29',                                -- col_numeric
		  32767,                                      -- col_serial2
		  2147483647,                                 -- col_serial4
		  9223372036854775807,                        -- col_serial8
		  'foo bar baz',                              -- col_text
		  '2022-03-14 15:16:17',                      -- col_timestamp
		  '2022-03-14 15:16:17-08',                   -- col_timestamptz
		  'bd94ee0b-564f-4088-bf4e-8d5e626caf66'     -- col_uuid
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

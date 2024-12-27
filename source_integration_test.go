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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func TestSource_Read(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)

	tableName := prepareSourceIntegrationTestTable(ctx, t)
	slotName := "conduitslot1"
	publicationName := "conduitpub1"

	s := NewSource()
	err := s.Configure(
		ctx,
		map[string]string{
			"url":                     test.RepmgrConnString,
			"tables":                  tableName,
			"snapshotMode":            "initial",
			"cdcMode":                 "logrepl",
			"logrepl.slotName":        slotName,
			"logrepl.publicationName": publicationName,
		},
	)
	is.NoErr(err)

	err = s.Open(ctx, nil)
	is.NoErr(err)
	t.Cleanup(func() {
		is.NoErr(logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
			URL:             test.RepmgrConnString,
			SlotName:        slotName,
			PublicationName: publicationName,
		}))
		is.NoErr(s.Teardown(ctx))
	})

	gotRecord, err := s.Read(ctx)
	is.NoErr(err)
	err = s.Ack(ctx, gotRecord.Position)
	is.NoErr(err)
	assertRecordOK(is, tableName, gotRecord)

	insertRowNotNullColumnsOnly(ctx, t, tableName, 3)
	insertRowAllColumns(ctx, t, tableName, 4)

	gotRecord, err = s.Read(ctx)
	is.NoErr(err)
	err = s.Ack(ctx, gotRecord.Position)
	is.NoErr(err)
	assertRecordOK(is, tableName, gotRecord)
}

func assertRecordOK(is *is.I, tableName string, gotRecord opencdc.Record) {
	is.True(gotRecord.Key != nil)
	is.True(gotRecord.Payload.After != nil)

	payloadSchemaSubject, err := gotRecord.Metadata.GetPayloadSchemaSubject()
	is.NoErr(err)
	is.Equal(tableName+"_payload", payloadSchemaSubject)
	payloadSchemaVersion, err := gotRecord.Metadata.GetPayloadSchemaVersion()
	is.NoErr(err)
	is.Equal(1, payloadSchemaVersion)

	keySchemaSubject, err := gotRecord.Metadata.GetKeySchemaSubject()
	is.NoErr(err)
	is.Equal(tableName+"_key", keySchemaSubject)
	keySchemaVersion, err := gotRecord.Metadata.GetKeySchemaVersion()
	is.NoErr(err)
	is.Equal(1, keySchemaVersion)
}

func prepareSourceIntegrationTestTable(ctx context.Context, t *testing.T) string {
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	table := test.RandomIdentifier(t)

	query := fmt.Sprintf(`CREATE TABLE %s (
    id                      bigserial PRIMARY KEY,
    col_bytea               bytea,
    col_bytea_not_null      bytea NOT NULL,
    col_varchar             varchar(10),
    col_varchar_not_null    varchar(10) NOT NULL,
    col_date                date,
    col_date_not_null       date NOT NULL,
    col_float4              float4,
    col_float4_not_null     float4 NOT NULL,
    col_float8              float8,
    col_float8_not_null     float8 NOT NULL,
    col_int2                int2,
    col_int2_not_null       int2 NOT NULL,
    col_int4                int4,
    col_int4_not_null       int4 NOT NULL,
    col_int8                int8,
    col_int8_not_null       int8 NOT NULL,
    col_numeric             numeric(8,2),
    -- col_numeric_not_null    numeric(8,2) NOT NULL,
    col_text                text,
    col_text_not_null       text NOT NULL,
    col_timestamp           timestamp,
    col_timestamp_not_null  timestamp NOT NULL,
    col_timestamptz         timestamptz,
    col_timestamptz_not_null timestamptz NOT NULL,
    col_uuid                uuid,
    col_uuid_not_null       uuid NOT NULL
)`, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %s`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})

	insertRowNotNullColumnsOnly(ctx, t, table, 1)
	insertRowAllColumns(ctx, t, table, 2)

	return table
}

func insertRowNotNullColumnsOnly(ctx context.Context, t *testing.T, table string, rowNumber int) {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	query := fmt.Sprintf(
		`INSERT INTO %s (
			col_bytea_not_null,
			col_varchar_not_null,
			col_date_not_null,
			col_float4_not_null,
			col_float8_not_null,
			col_int2_not_null,
			col_int4_not_null,
			col_int8_not_null,
			-- col_numeric_not_null,
			col_text_not_null,
			col_timestamp_not_null,
			col_timestamptz_not_null,
			col_uuid_not_null
		) VALUES (
			'%s'::bytea,   			-- col_bytea_not_null
			'foo-%v',               -- col_varchar_not_null
			now(),                  -- col_date_not_null
			%f,                  	-- col_float4_not_null
			%f,                  	-- col_float8_not_null
			%d,                  	-- col_int2_not_null
			%d,                  	-- col_int4_not_null
			%d,                  	-- col_int8_not_null
			-- %f,                  	-- col_numeric_not_null
			'bar-%v',               -- col_text_not_null
			now(),                  -- col_timestamp_not_null
			now(),                  -- col_timestamptz_not_null
			gen_random_uuid()       -- col_uuid_not_null
		)`,
		table,
		fmt.Sprintf("col_bytea_-%v", rowNumber),
		rowNumber,
		float32(rowNumber)/10,
		float64(rowNumber)/10,
		rowNumber%32768,
		rowNumber,
		rowNumber,
		float64(100+rowNumber)/10,
		rowNumber,
	)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func insertRowAllColumns(ctx context.Context, t *testing.T, table string, rowNumber int) {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	query := fmt.Sprintf(
		`INSERT INTO %s (
         col_bytea, col_bytea_not_null,
         col_varchar, col_varchar_not_null,
         col_date, col_date_not_null,
         col_float4, col_float4_not_null,
         col_float8, col_float8_not_null,
         col_int2, col_int2_not_null,
         col_int4, col_int4_not_null,
         col_int8, col_int8_not_null,
         -- col_numeric, col_numeric_not_null,
         col_text, col_text_not_null,
         col_timestamp, col_timestamp_not_null,
         col_timestamptz, col_timestamptz_not_null,
         col_uuid, col_uuid_not_null
      ) VALUES (
         '%s'::bytea, '%s'::bytea,
         'foo-%v', 'foo-%v',
         now(), now(),
         %f, %f,
         %f, %f,
         %d, %d,
         %d, %d,
         %d, %d,
         -- %f, %f,
         'bar-%v', 'bar-%v',
         now(), now(),
         now(), now(),
         gen_random_uuid(), gen_random_uuid()
      )`,
		table,
		fmt.Sprintf("col_bytea_-%v", rowNumber), fmt.Sprintf("col_bytea_-%v", rowNumber),
		rowNumber, rowNumber,
		float32(rowNumber)/10, float32(rowNumber)/10,
		float64(rowNumber)/10, float64(rowNumber)/10,
		rowNumber%32768, rowNumber%32768,
		rowNumber, rowNumber,
		rowNumber, rowNumber,
		float64(100+rowNumber)/10, float64(100+rowNumber)/10,
		rowNumber, rowNumber,
	)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

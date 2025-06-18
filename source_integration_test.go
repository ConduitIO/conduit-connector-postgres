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
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestSource_ReadN_Snapshot_CDC(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)

	tableName := createTableWithManyTypes(ctx, t)
	// Snapshot data
	insertRowNotNullColumnsOnly(ctx, t, tableName, 1)
	insertRowAllColumns(ctx, t, tableName, 2)

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
	t.Cleanup(func() {
		is.NoErr(logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
			URL:             test.RepmgrConnString,
			SlotName:        slotName,
			PublicationName: publicationName,
		}))
		is.NoErr(s.Teardown(ctx))
	})

	// Read and ack the 2 snapshots records
	snapshotRecs, err := s.ReadN(ctx, 2)
	is.NoErr(err)
	is.Equal(2, len(snapshotRecs))

	err = s.Ack(ctx, snapshotRecs[0].Position)
	is.NoErr(err)
	err = s.Ack(ctx, snapshotRecs[1].Position)
	is.NoErr(err)

	// Verify snapshot records
	assertRecordOK(is, tableName, snapshotRecs[0], 1, true)
	assertRecordOK(is, tableName, snapshotRecs[1], 2, false)

	// CDC data
	insertRowNotNullColumnsOnly(ctx, t, tableName, 3)
	insertRowAllColumns(ctx, t, tableName, 4)

	// Read, ack and verify the first CDC record
	cdcRecs, err := s.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(1, len(cdcRecs))
	err = s.Ack(ctx, cdcRecs[0].Position)
	is.NoErr(err)
	assertRecordOK(is, tableName, cdcRecs[0], 3, true)

	// Read, ack and verify the second CDC record
	cdcRecs, err = s.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(1, len(cdcRecs))
	err = s.Ack(ctx, cdcRecs[0].Position)
	is.NoErr(err)
	assertRecordOK(is, tableName, cdcRecs[0], 4, false)
}

func assertRecordOK(is *is.I, tableName string, gotRecord opencdc.Record, rowNum int, notNullOnly bool) {
	is.Helper()

	is.True(gotRecord.Key != nil)
	is.True(gotRecord.Payload.After != nil)

	assertSchemaPresent(is, tableName, gotRecord)
	assertPayloadOK(is, gotRecord, rowNum, notNullOnly)
}

func assertPayloadOK(is *is.I, record opencdc.Record, rowNum int, notNullOnly bool) {
	is.Helper()

	sch, err := schema.Get(
		context.TODO(),
		assert(record.Metadata.GetPayloadSchemaSubject()),
		assert(record.Metadata.GetPayloadSchemaVersion()),
	)
	is.NoErr(err)

	got := opencdc.StructuredData{}
	err = sch.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	want := expectedRecord(rowNum, notNullOnly)

	is.Equal("", cmp.Diff(want, got, test.BigRatComparer)) // -want, +got
}

func assertSchemaPresent(is *is.I, tableName string, gotRecord opencdc.Record) {
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

func createTableWithManyTypes(ctx context.Context, t *testing.T) string {
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	// Verify we can discover the primary key even when the table name
	// contains capital letters.
	table := strings.ToUpper(test.RandomIdentifier(t))

	query := fmt.Sprintf(`CREATE TABLE %q (
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
    col_numeric_not_null    numeric(8,2) NOT NULL,
    col_text                text,
    col_text_not_null       text NOT NULL,
    col_timestamp           timestamp,
    col_timestamp_not_null  timestamp NOT NULL,
    col_timestamptz         timestamptz,
    col_timestamptz_not_null timestamptz NOT NULL,
    col_uuid                uuid,
    col_uuid_not_null       uuid NOT NULL,
    col_json                json,
    col_json_not_null       json NOT NULL,
    col_jsonb               jsonb,
    col_jsonb_not_null      jsonb NOT NULL,
    col_bool                bool,
    col_bool_not_null       bool NOT NULL,
    col_serial              serial,
    col_serial_not_null     serial NOT NULL,
    col_smallserial         smallserial,
    col_smallserial_not_null smallserial NOT NULL,
    col_bigserial           bigserial,
    col_bigserial_not_null  bigserial NOT NULL
)`, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %q`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})

	return table
}

func insertRowNotNullColumnsOnly(ctx context.Context, t *testing.T, table string, rowNumber int) {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	rec := generateRecord(rowNumber, true)

	query := fmt.Sprintf(
		`INSERT INTO %q (
        id,
         col_bytea_not_null,
         col_varchar_not_null,
         col_date_not_null,
         col_float4_not_null,
         col_float8_not_null,
         col_int2_not_null,
         col_int4_not_null,
         col_int8_not_null,
         col_numeric_not_null,
         col_text_not_null,
         col_timestamp_not_null,
         col_timestamptz_not_null,
         col_uuid_not_null,
         col_json_not_null,
         col_jsonb_not_null,
         col_bool_not_null,
         col_serial_not_null,
         col_smallserial_not_null,
         col_bigserial_not_null
      ) VALUES (
        %d,
         '%s'::bytea,
         '%s',
         '%s'::date,
         %f,
         %f,
         %d,
         %d,
         %d,
         %f,
         '%s',
         '%s'::timestamp,
         '%s'::timestamptz,
         '%s'::uuid,
         '%s'::json,
         '%s'::jsonb,
         %t,
         %d,
         %d,
         %d
      )`,
		table,
		rowNumber,
		rec["col_bytea_not_null"],
		rec["col_varchar_not_null"],
		rec["col_date_not_null"],
		rec["col_float4_not_null"],
		rec["col_float8_not_null"],
		rec["col_int2_not_null"],
		rec["col_int4_not_null"],
		rec["col_int8_not_null"],
		rec["col_numeric_not_null"],
		rec["col_text_not_null"],
		rec["col_timestamp_not_null"],
		rec["col_timestamptz_not_null"],
		rec["col_uuid_not_null"],
		rec["col_json_not_null"],
		rec["col_jsonb_not_null"],
		rec["col_bool_not_null"],
		rec["col_serial_not_null"],
		rec["col_smallserial_not_null"],
		rec["col_bigserial_not_null"],
	)

	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func insertRowAllColumns(ctx context.Context, t *testing.T, table string, rowNumber int) {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	rec := generateRecord(rowNumber, false)

	query := fmt.Sprintf(
		`INSERT INTO %q (
         id,
         col_bytea, col_bytea_not_null,
         col_varchar, col_varchar_not_null,
         col_date, col_date_not_null,
         col_float4, col_float4_not_null,
         col_float8, col_float8_not_null,
         col_int2, col_int2_not_null,
         col_int4, col_int4_not_null,
         col_int8, col_int8_not_null,
         col_numeric, col_numeric_not_null,
         col_text, col_text_not_null,
         col_timestamp, col_timestamp_not_null,
         col_timestamptz, col_timestamptz_not_null,
         col_uuid, col_uuid_not_null,
         col_json, col_json_not_null,
         col_jsonb, col_jsonb_not_null,
         col_bool, col_bool_not_null,
         col_serial, col_serial_not_null,
         col_smallserial, col_smallserial_not_null,
         col_bigserial, col_bigserial_not_null
      ) VALUES (
        %d,
         '%s'::bytea, '%s'::bytea,
         '%s', '%s',
         '%s'::date, '%s'::date,
         %f, %f,
         %f, %f,
         %d, %d,
         %d, %d,
         %d, %d,
         %f, %f,
         '%s', '%s',
         '%s'::timestamp, '%s'::timestamp,
         '%s'::timestamptz, '%s'::timestamptz,
         '%s'::uuid, '%s'::uuid,
         '%s'::json, '%s'::json,
         '%s'::jsonb, '%s'::jsonb,
         %t, %t,
         %d, %d,
         %d, %d,
         %d, %d
      )`,
		table,
		rowNumber,
		rec["col_bytea"], rec["col_bytea_not_null"],
		rec["col_varchar"], rec["col_varchar_not_null"],
		rec["col_date"], rec["col_date_not_null"],
		rec["col_float4"], rec["col_float4_not_null"],
		rec["col_float8"], rec["col_float8_not_null"],
		rec["col_int2"], rec["col_int2_not_null"],
		rec["col_int4"], rec["col_int4_not_null"],
		rec["col_int8"], rec["col_int8_not_null"],
		rec["col_numeric"], rec["col_numeric_not_null"],
		rec["col_text"], rec["col_text_not_null"],
		rec["col_timestamp"], rec["col_timestamp_not_null"],
		rec["col_timestamptz"], rec["col_timestamptz_not_null"],
		rec["col_uuid"], rec["col_uuid_not_null"],
		rec["col_json"], rec["col_json_not_null"],
		rec["col_jsonb"], rec["col_jsonb_not_null"],
		rec["col_bool"], rec["col_bool_not_null"],
		rec["col_serial"], rec["col_serial_not_null"],
		rec["col_smallserial"], rec["col_smallserial_not_null"],
		rec["col_bigserial"], rec["col_bigserial_not_null"],
	)

	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func expectedRecord(rowNumber int, notNullOnly bool) opencdc.StructuredData {
	rec := generateRecord(rowNumber, notNullOnly)

	for key, value := range rec {
		if value != nil {
			rec[key] = normalizeNotNullValue(key, value)
		} else {
			rec[key] = normalizeNullValue(key, value)
		}
	}

	return rec
}

func normalizeNullValue(key string, value interface{}) interface{} {
	normalized := value
	if strings.Contains(key, "_uuid") {
		normalized = ""
	}

	return normalized
}

func normalizeNotNullValue(key string, value interface{}) interface{} {
	normalized := value
	switch {
	case strings.Contains(key, "_bytea"),
		strings.Contains(key, "_json"),
		strings.Contains(key, "_jsonb"):
		normalized = []uint8(value.(string))
	case strings.Contains(key, "_numeric"):
		val := new(big.Rat)
		val.SetString(fmt.Sprintf("%v", value))
		normalized = val
	case strings.Contains(key, "_date"):
		val := assert(time.Parse("2006-01-02", value.(string)))
		normalized = val
	}

	return normalized
}

func generateRecord(rowNumber int, notNullOnly bool) opencdc.StructuredData {
	tsLayout := "2006-01-02 15:04:05.000000"

	rowTS, _ := time.Parse("2006-01-02 15:04:05", "2022-01-21 17:04:05")
	rowTS = rowTS.Add(time.Duration(rowNumber) * time.Hour)

	rowUUID := fmt.Sprintf("a74a9875-978e-4832-b1b8-6b0f8793a%03d", rowNumber)

	id := int64(rowNumber)
	rec := opencdc.StructuredData{
		"id":                       id,
		"col_bytea":                fmt.Sprintf("col_bytea_%v", rowNumber),
		"col_bytea_not_null":       fmt.Sprintf("col_bytea_%v", rowNumber),
		"col_varchar":              fmt.Sprintf("foo-%v", rowNumber),
		"col_varchar_not_null":     fmt.Sprintf("foo-%v", rowNumber),
		"col_date":                 rowTS.Format("2006-01-02"),
		"col_date_not_null":        rowTS.Format("2006-01-02"),
		"col_float4":               float32(rowNumber) / 10,
		"col_float4_not_null":      float32(rowNumber) / 10,
		"col_float8":               float64(rowNumber) / 10,
		"col_float8_not_null":      float64(rowNumber) / 10,
		"col_int2":                 rowNumber % 32768,
		"col_int2_not_null":        rowNumber % 32768,
		"col_int4":                 rowNumber,
		"col_int4_not_null":        rowNumber,
		"col_int8":                 id,
		"col_int8_not_null":        id,
		"col_numeric":              float64(100+rowNumber) / 10,
		"col_numeric_not_null":     float64(100+rowNumber) / 10,
		"col_text":                 fmt.Sprintf("bar-%v", rowNumber),
		"col_text_not_null":        fmt.Sprintf("bar-%v", rowNumber),
		"col_timestamp":            rowTS.Format(tsLayout),
		"col_timestamp_not_null":   rowTS.Format(tsLayout),
		"col_timestamptz":          rowTS.Format(tsLayout),
		"col_timestamptz_not_null": rowTS.Format(tsLayout),
		"col_uuid":                 rowUUID,
		"col_uuid_not_null":        rowUUID,
		"col_json":                 fmt.Sprintf(`{"key": "value-%v"}`, rowNumber),
		"col_json_not_null":        fmt.Sprintf(`{"key": "value-%v"}`, rowNumber),
		"col_jsonb":                fmt.Sprintf(`{"key": "value-%v"}`, rowNumber),
		"col_jsonb_not_null":       fmt.Sprintf(`{"key": "value-%v"}`, rowNumber),
		"col_bool":                 rowNumber%2 == 0,
		"col_bool_not_null":        rowNumber%2 == 0,
		"col_serial":               rowNumber,
		"col_serial_not_null":      rowNumber,
		"col_smallserial":          rowNumber,
		"col_smallserial_not_null": rowNumber,
		"col_bigserial":            id,
		"col_bigserial_not_null":   id,
	}

	if notNullOnly {
		for key := range rec {
			if !strings.HasSuffix(key, "_not_null") && !strings.HasSuffix(key, "serial") && key != "id" {
				rec[key] = nil
			}
		}
	}

	return rec
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

func assert[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

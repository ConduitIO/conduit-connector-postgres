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

	"github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
	"github.com/shopspring/decimal"
)

type readTestCase struct {
	name        string
	notNullOnly bool
	snapshot    bool
	cdc         bool
	opDelete    bool
}

func TestSource_ReadN(t *testing.T) {
	testCases := []readTestCase{
		{
			name:        "snapshot not only only",
			notNullOnly: true,
			snapshot:    true,
		},
		{
			name:        "snapshot with nullable values",
			notNullOnly: false,
			snapshot:    true,
		},
		{
			name:        "cdc not only only",
			notNullOnly: true,
			cdc:         true,
		},
		{
			name:        "cdc with nullable values",
			notNullOnly: false,
			cdc:         true,
		},

		{
			name:        "delete cdc data not only only",
			notNullOnly: true,
			cdc:         true,
			opDelete:    true,
		},
		{
			name:        "delete cdc data with nullable values",
			notNullOnly: false,
			cdc:         true,
			opDelete:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runReadTest(t, tc)
		})
	}
}

func runReadTest(t *testing.T, tc readTestCase) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	tableName := createTableWithManyTypes(ctx, t)

	if tc.snapshot {
		insertRow(ctx, is, conn, tableName, 1, tc.notNullOnly)
	}

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

	if tc.snapshot {
		// Read, ack, and assert the snapshot record is OK
		rec := readAndAck(ctx, is, s)
		assertRecordOK(is, tableName, rec, 1, tc.notNullOnly)
	}

	if tc.cdc {
		insertRow(ctx, is, conn, tableName, 1, tc.notNullOnly)

		// Read, ack, and verify the CDC record
		rec := readAndAck(ctx, is, s)
		assertRecordOK(is, tableName, rec, 1, tc.notNullOnly)
	}

	if tc.opDelete {
		// https://github.com/ConduitIO/conduit-connector-postgres/issues/301
		t.Skip("Skipping delete test, see GitHub issue ")
		deleteRow(ctx, is, conn, tableName, 1)
		rec := readAndAck(ctx, is, s)
		is.Equal(opencdc.OperationDelete, rec.Operation)
	}
}

func readAndAck(ctx context.Context, is *is.I, s sdk.Source) opencdc.Record {
	recs, err := s.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(1, len(recs))

	err = s.Ack(ctx, recs[0].Position)
	is.NoErr(err)

	return recs[0]
}

func createTableWithManyTypes(ctx context.Context, t *testing.T) string {
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	// Verify we can discover the primary key even when the table name
	// contains capital letters.
	table := strings.ToUpper(test.RandomIdentifier(t))

	query := fmt.Sprintf(`CREATE TABLE %q (
    id                      integer PRIMARY KEY,
    col_bytea               bytea,
    col_bytea_not_null      bytea NOT NULL,
    col_varchar             varchar(30),
    col_varchar_not_null    varchar(30) NOT NULL,
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

// insertRow inserts a row using the values provided by generatePayloadData.
// if notNullOnly is true, only NOT NULL columns are inserted.
func insertRow(ctx context.Context, is *is.I, conn *pgx.Conn, table string, rowNumber int, notNullOnly bool) {
	rec := generatePayloadData(rowNumber, false)

	var columns []string
	var values []interface{}
	for key, value := range rec {
		// the database generates serial values
		if strings.Contains(key, "serial") {
			continue
		}
		// check if only NOT NULL columns are needed (names ends in _not_null)
		// id is an exception
		if notNullOnly && !strings.HasSuffix(key, "_not_null") && key != "id" {
			continue
		}

		columns = append(columns, key)
		// col_numeric is a big.Rat, so we convert it to a string
		if strings.HasPrefix(key, "col_numeric") {
			values = append(values, decimalString(value))
		} else {
			values = append(values, value)
		}
	}

	query, args, err := squirrel.Insert(internal.WrapSQLIdent(table)).
		Columns(columns...).
		Values(values...).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()

	is.NoErr(err)

	_, err = conn.Exec(ctx, query, args...)
	is.NoErr(err)
}

func deleteRow(ctx context.Context, is *is.I, conn *pgx.Conn, table string, rowNumber int) {
	query, args, err := squirrel.Delete(internal.WrapSQLIdent(table)).
		Where(squirrel.Eq{"id": rowNumber}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	is.NoErr(err)

	_, err = conn.Exec(ctx, query, args...)
	is.NoErr(err)
}

func generatePayloadData(id int, notNullOnly bool) opencdc.StructuredData {
	// Add a time zone offset
	rowTS := assert(time.Parse(time.RFC3339, fmt.Sprintf("2022-01-21T17:04:05+%02d:00", id)))
	rowTS = rowTS.Add(time.Duration(id) * time.Hour)

	rowUUID := assert(uuid.Parse(fmt.Sprintf("a74a9875-978e-4832-b1b8-6b0f8793a%03d", id)))
	idInt64 := int64(id)
	numericVal := big.NewRat(int64(100+id), 10)

	rec := opencdc.StructuredData{
		"id": id,

		"col_bytea":            []uint8(fmt.Sprintf("col_bytea_%v", id)),
		"col_bytea_not_null":   []uint8(fmt.Sprintf("col_bytea_not_null_%v", id)),
		"col_varchar":          fmt.Sprintf("col_varchar_%v", id),
		"col_varchar_not_null": fmt.Sprintf("col_varchar_not_null_%v", id),
		"col_text":             fmt.Sprintf("col_text_%v", id),
		"col_text_not_null":    fmt.Sprintf("col_text_not_null_%v", id),

		"col_uuid":          rowUUID,
		"col_uuid_not_null": rowUUID,

		"col_json":           []uint8(fmt.Sprintf(`{"key": "json-value-%v"}`, id)),
		"col_json_not_null":  []uint8(fmt.Sprintf(`{"key": "json-not-value-%v"}`, id)),
		"col_jsonb":          []uint8(fmt.Sprintf(`{"key": "jsonb-value-%v"}`, id)),
		"col_jsonb_not_null": []uint8(fmt.Sprintf(`{"key": "jsonb-not-value-%v"}`, id)),

		"col_float4":          float32(id) / 10,
		"col_float4_not_null": float32(id) / 10,
		"col_float8":          float64(id) / 10,
		"col_float8_not_null": float64(id) / 10,
		"col_int2":            id % 32768,
		"col_int2_not_null":   id % 32768,
		"col_int4":            id,
		"col_int4_not_null":   id,
		"col_int8":            idInt64,
		"col_int8_not_null":   idInt64,

		"col_numeric":          numericVal,
		"col_numeric_not_null": numericVal,

		// NB: these values are not used in insert queries, but we assume
		// the test rows will always be inserted in order, i.e.,
		// test row 1, then test row 2, etc.
		"col_serial":               id,
		"col_serial_not_null":      id,
		"col_smallserial":          id,
		"col_smallserial_not_null": id,
		"col_bigserial":            idInt64,
		"col_bigserial_not_null":   idInt64,

		"col_date":                 rowTS.Truncate(24 * time.Hour),
		"col_date_not_null":        rowTS.Truncate(24 * time.Hour),
		"col_timestamp":            rowTS.UTC(),
		"col_timestamp_not_null":   rowTS.UTC(),
		"col_timestamptz":          rowTS,
		"col_timestamptz_not_null": rowTS,

		"col_bool":          id%2 == 0,
		"col_bool_not_null": id%2 == 1,
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

func decimalString(v interface{}) string {
	return decimal.NewFromBigRat(v.(*big.Rat), 2).String()
}

// assertRecordOK asserts that the input record has a schema and that its payload
// is what we expect (based on the ID and what columns are included).
func assertRecordOK(is *is.I, tableName string, gotRecord opencdc.Record, id int, notNullOnly bool) {
	is.Helper()

	is.True(gotRecord.Key != nil)
	is.True(gotRecord.Payload.After != nil)

	assertSchemaPresent(is, tableName, gotRecord)
	assertPayloadOK(is, gotRecord, id, notNullOnly)
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

// assertPayloadOK decodes the record's payload and asserts that the payload
// is what we expect (based on the ID and what columns are included).
func assertPayloadOK(is *is.I, record opencdc.Record, rowNum int, notNullOnly bool) {
	is.Helper()

	sch, err := schema.Get(
		context.Background(),
		assert(record.Metadata.GetPayloadSchemaSubject()),
		assert(record.Metadata.GetPayloadSchemaVersion()),
	)
	is.NoErr(err)

	got := opencdc.StructuredData{}
	err = sch.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	want := expectedData(rowNum, notNullOnly)

	is.Equal("", cmp.Diff(want, got, test.BigRatComparer)) // expected different payload (-want, +got)
}

// expectedData creates an opencdc.StructuredData with expected keys and values
// based on the ID and the columns (NOT NULL columns only or all columns).
// Its output is different generatePayloadData, because certain values are written
// into the test table as one type, but read as another (e.g., we use UUID objects
// when inserting test data, but they are read as strings).
func expectedData(id int, notNullOnly bool) opencdc.StructuredData {
	rec := generatePayloadData(id, notNullOnly)

	for key, value := range rec {
		// UUID are written as byte arrays but read as strings.
		if strings.HasPrefix(key, "col_uuid") && value != nil {
			rec[key] = value.(uuid.UUID).String()
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

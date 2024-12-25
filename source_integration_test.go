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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func TestSource_Read(t *testing.T) {
	is := is.New(t)
	ctx, _ := context.WithTimeout(test.Context(t), 3000*time.Second)

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

	insertRow(ctx, t, tableName, 2)

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

	query := fmt.Sprintf(`
		CREATE TABLE %s (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean,
		column4 varchar(256) NOT NULL,
		column5 integer NOT NULL,
		column6 boolean NOT NULL
	)`, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %s`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})

	insertRow(ctx, t, table, 1)

	return table
}

func insertRow(ctx context.Context, t *testing.T, table string, rowNumber int) {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	query := fmt.Sprintf(
		`INSERT INTO %s (key, column1, column2, column3, column4, column5, column6)
		VALUES ('%v', null, null, null, 'foo-%v', %d, false)`,
		table,
		rowNumber,
		rowNumber,
		100+rowNumber,
	)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

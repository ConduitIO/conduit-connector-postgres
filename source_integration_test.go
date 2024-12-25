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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func TestSource_Read(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	tableName := test.SetupTestTable(ctx, t, conn)
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

	for i := 0; i < 4; i++ {
		gotRecord, err := s.Read(ctx)
		is.NoErr(err)

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
}

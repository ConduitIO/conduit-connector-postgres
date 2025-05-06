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

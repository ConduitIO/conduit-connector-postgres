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

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func TestSource_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	tableName := test.SetupTestTable(ctx, t, conn)

	s := NewSource()
	err := s.Configure(
		ctx,
		map[string]string{
			"url":                     test.RepmgrConnString,
			"tables":                  tableName,
			"snapshotMode":            "initial",
			"cdcMode":                 "logrepl",
			"tableKeys":               "'id",
			"logrepl.slotName":        "conduitslot1",
			"logrepl.publicationName": "conduitpub1",
		},
	)
	is.NoErr(err)

	err = s.Open(ctx, nil)
	is.NoErr(err)

	defer func() {
		is.NoErr(s.Teardown(ctx))
	}()
}

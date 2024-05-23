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

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

func TestSource_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	tableName := test.SetupTestTable(ctx, t, conn)

	s := NewSource()
	err := s.Configure(
		ctx,
		map[string]string{
			"url":   test.RegularConnString,
			"table": "{{ index .Metadata \"opencdc.collection\" }}",
		},
	)
	is.NoErr(err)
	err = s.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := s.Teardown(ctx)
		is.NoErr(err)
	}()
}

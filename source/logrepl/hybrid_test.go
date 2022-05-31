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

package logrepl

import (
	"context"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"

	"github.com/matryer/is"
)

func TestHybridSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h := createTestHybridIterator(ctx, t)

	go func() {
		conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
		_, err := conn.Exec(ctx, fmt.Sprintf(`insert into %s values ( 4, null, null,
		null, null, null, '{"biz":"baz"}')`, h.config.TableName))
		is.NoErr(err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`insert into %s values ( 5, null, null,
		null, null, null, '{"fiz":"buzz"}')`, h.config.TableName))
		is.NoErr(err)
	}()

	count := 0
	for count < 4 {
		rec, err := h.Next(ctx)
		is.NoErr(err)
		count++
		t.Logf("%v\n", rec)
	}
}

func createTestHybridIterator(ctx context.Context, t *testing.T) *Hybrid {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTableV2(ctx, t, conn)

	h, err := NewHybridIterator(ctx, conn, Config{
		TableName:       table,
		SlotName:        table,
		PublicationName: table,
		KeyColumnName:   "h",
		Columns:         []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		SnapshotMode:    "initial",
	})
	is.NoErr(err)
	t.Cleanup(func() {
		is.NoErr(h.Teardown(ctx))
	})
	return h
}

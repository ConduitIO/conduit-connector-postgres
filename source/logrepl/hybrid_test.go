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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"

	"github.com/matryer/is"
)

func TestHybridSnapshot(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h := createTestHybridIterator(ctx, t)

	count := 0
	for count < 2 {
		rec, err := h.Next(ctx)
		is.NoErr(err)
		count++
		t.Logf("\n%v\n", rec)
	}
}

func createTestHybridIterator(ctx context.Context, t *testing.T) *Hybrid {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	_, err := conn.Exec(ctx, `create temporary table foo( a int2, b int4, c int8, d varchar, e text, f date, g json)`)
	is.NoErr(err)

	_, err = conn.Exec(context.Background(), `insert into foo values (0, 1, 2, 'abc	', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`)
	is.NoErr(err)

	_, err = conn.Exec(context.Background(), `insert into foo values (3, null, null, null, null, null, '{"foo":"bar"}')`)
	is.NoErr(err)

	h, err := NewHybridIterator(ctx, conn, Config{
		TableName:       "foo",
		SlotName:        "foo",
		PublicationName: "foo",
		KeyColumnName:   "key",
		Columns:         []string{"a", "b", "c", "d", "e", "f", "g"},
		SnapshotMode:    "initial",
	})
	is.NoErr(err)
	t.Cleanup(func() {
		is.NoErr(h.Teardown(ctx))
	})
	return h
}

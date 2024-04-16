// Copyright © 2024 Meroxa, Inc.
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

package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func Test_Iterator(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	pgurl := "postgres://repl:repl@127.0.0.1:5432/demo"
	pool, err := pgxpool.New(ctx, pgurl)
	is.NoErr(err)

	p := position.Position{
		Type:     position.TypeSnapshot,
		Snapshot: make(position.SnapshotPositions),
	}.ToSDKPosition()

	i, err := NewIterator(ctx, pool.Config().ConnString(), Config{
		Position: p,
		Tables:   []string{"orders"},
		TablesKeys: map[string]string{
			"orders": "id",
		},
	},
	)
	is.NoErr(err)

	for {
		r, err := i.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				t.Logf("iterator is done")
				break
			}
			t.Fatalf("iterator exited: %v", err)
		}
		v, err := json.Marshal(r)
		is.NoErr(err)

		pos, err := position.ParseSDKPosition(r.Position)
		is.NoErr(err)

		t.Logf("%s\n", string(v))
		t.Logf("%+v\n\n\n", pos)
	}
}

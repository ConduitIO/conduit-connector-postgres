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

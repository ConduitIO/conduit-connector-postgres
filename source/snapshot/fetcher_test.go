package snapshot

import (
	"context"
	"fmt"
	"testing"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func Test_FetcherRun(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	pgurl := "postgres://repl:repl@127.0.0.1:5432/demo"
	is.True(pgurl != "")

	pool, err := pgxpool.New(ctx, pgurl)
	is.NoErr(err)

	r := make(chan sdk.Record)
	f := NewFetcher(pool, r, FetcherConfig{
		Table: "orders",
		Key:   "id",
		Position: Position{
			Type: TypeSnapshot,
			Snapshot: map[string]SnapshotPosition{
				"orders": {
					LastRead: 4,
					SnapshotEnd: 9,
				},
			},
		},
	})

	is.NoErr(f.Validate(ctx))

	// Todo handle err properly here
	go f.Run(ctx)

	stdoutConsumer(t, f, r)


	for {
		select {
		case <-f.Done():
			return
		case fr := <-r:
			fmt.Printf("got %+v\n", fr)

		}
	}
}


func stdoutConsumer(t *testing.T, f *Fetcher, in chan sdk.Record) {
	t.Helper()

	is := is.New(t)

	// Basically what the snapshotter will do except from multiple fetchers
	for {
		select {
		case <-f.Done():
			return
		case r := <-in:
			v, err := json.Marshal(r)
			is.NoErr(err)

			pos, err := ParseSDKPosition(r.Position)
			is.NoErr(err)

			fmt.Printf("%s\n", string(v))
			fmt.Printf("%+v\n\n\n", pos)
		}
	}
}
package logrepl

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestCombinedIterator(t *testing.T) {
	is := is.New(t)
	pool := test.ConnectPool(context.Background(), t, test.RepmgrConnString)

	t.Run("should switch into CDC mode when snapshot finished", func(t *testing.T) {
		ctx := context.Background()

		table := test.SetupTestTable(ctx, t, pool)
		// TODO: create test snapshot

		conn, err := pool.Acquire(ctx)
		is.NoErr(err)
		ci, err := NewCombinedIterator(ctx, conn.Conn(), Config{})
		is.NoErr(err)

		for i := 0; i < 5; i++ {
			// 5th call should return ErrSnapshotComplete
			_, err := ci.Next(ctx)
			if err != nil {
				is.True(errors.Is(err, ErrSnapshotComplete))
				break
			}
		}

		// insert a record so that CDC reads something off
		insert := `INSERT INTO %s (id, column1, column2, column3)
			VALUES (6, 'bizz', 456, false)`
		_, err = pool.Exec(ctx, insert)
		is.NoErr(err)

		// should detect next CDC record
		rec, err := ci.Next(ctx)
		is.NoErr(err)

		is.Equal(rec, sdk.Record{
			Key: sdk.StructuredData{"id": int64(6)},
			Metadata: map[string]string{
				"table":  table,
				"action": "insert",
			},
			Payload: sdk.StructuredData{
				"id":      int64(6),
				"column1": "bizz",
				"column2": int32(456),
				"column3": false,
				"key":     nil,
			},
		})
		// assert that it equals the record we made above
	})
}

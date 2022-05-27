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
	"reflect"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/jackc/pgx/v4"
	"github.com/matryer/is"
)

func TestCopyTo(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, "postgres://repmgr:repmgrmeroxa@localhost:5432/meroxadb?sslmode=disable&replication=database")

	_, err := conn.Exec(ctx, `create temporary table foo( a int2, b int4, c int8, d varchar, e text, f date, g json)`)
	is.NoErr(err)

	_, err = conn.Exec(context.Background(), `insert into foo values (0, 1, 2, 'abc	', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`)
	is.NoErr(err)

	_, err = conn.Exec(context.Background(), `insert into foo values (3, null, null, null, null, null, '{"foo":"bar"}')`)
	is.NoErr(err)

	// --------------
	// TODO make sure to filter by table_schema
	// rows, err := conn.Query(ctx, "SELECT column_name, udt_name FROM information_schema.columns WHERE table_name = 'foo' ORDER BY ordinal_position")
	// is.NoErr(err)
	// --------------

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       "REPEATABLE",
		AccessMode:     "READ",
		DeferrableMode: "",
	})
	is.NoErr(err)
	t.Cleanup(func() { is.NoErr(tx.Commit(ctx)) })

	w, err := NewCopyDataWriter(ctx, conn, Config{TableName: "foo"})
	is.NoErr(err)
	t.Cleanup(func() { w.Teardown(ctx) })

	go w.Copy(ctx)

	now := time.Now()
	count := 0
	for count < 2 {
		rec, err := w.Next(ctx)
		is.NoErr(err)
		is.True(rec.CreatedAt.After(now))
		count++
	}
}

func TestCopyWriter_Copy(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, "postgres://repmgr:repmgrmeroxa@localhost:5432/meroxadb?sslmode=disable&replication=database")

	_, err := conn.Exec(ctx, `create temporary table foo( a int2, b int4, c int8, d varchar, e text, f date, g json)`)
	is.NoErr(err)

	_, err = conn.Exec(context.Background(), `insert into foo values (0, 1, 2, 'abc	', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`)
	is.NoErr(err)

	_, err = conn.Exec(context.Background(), `insert into foo values (3, null, null, null, null, null, '{"foo":"bar"}')`)
	is.NoErr(err)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   "REPEATABLE",
		AccessMode: "READ",
	})
	is.NoErr(err)
	defer tx.Commit(ctx)

	w, err := NewCopyDataWriter(ctx, tx.Conn(), Config{TableName: "foo"})
	is.NoErr(err)

	go w.Copy(ctx)

	now := time.Now()
	count := 0
	for count < 2 {
		rec, err := w.Next(ctx)
		is.NoErr(err)
		is.True(rec.CreatedAt.After(now))
		count++
	}
}

func TestCopyDataWriter_Next(t *testing.T) {
	is := is.New(t)
	type args struct {
		config Config
	}
	tests := []struct {
		name       string
		args       args
		setupQuery func(conn *pgx.Conn)
		wantErr    bool
		want       sdk.Record
	}{
		{
			name: "should parse a copy line into a record",
			args: args{
				config: Config{
					TableName: "foo",
				},
			},
			setupQuery: func(conn *pgx.Conn) {
				_, err := conn.Exec(context.Background(), `create temporary table foo(a int2, b int4, c int8, d varchar, e text, f date, g json)`)
				is.NoErr(err)
				_, err = conn.Exec(context.Background(), `insert into foo 
				values (0, 1, 2, 'abc	', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`)
				is.NoErr(err)
			},
			wantErr: false,
			want: sdk.Record{
				Position: nil,                  // TODO
				Metadata: map[string]string{},  // TODO
				Key:      sdk.StructuredData{}, // TODO
				Payload: sdk.StructuredData{
					"a": int16(0),
					"b": int32(1),
					"c": int64(2),
					"d": string(`abc\t`),
					"e": string("efg"),
					"f": time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
					"g": map[string]any{
						"abc": string("def"),
						"foo": string("bar"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
			now := time.Now()
			tt.setupQuery(conn)

			w, err := NewCopyDataWriter(ctx, conn, tt.args.config)
			is.NoErr(err)
			go w.Copy(ctx)

			got, err := w.Next(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("CopyDataWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			is.True(got.CreatedAt.After(now))
			got.CreatedAt = time.Time{} // TODO
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("wanted: %v - got: %v", tt.want, got)
			}
		})
	}
}

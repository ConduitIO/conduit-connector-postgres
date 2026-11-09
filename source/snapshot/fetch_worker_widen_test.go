// Copyright © 2026 Meroxa, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"gopkg.in/tomb.v2"
)

var widenTableSeq atomic.Int64

// widenWorker is a worker for a table (id bigint PRIMARY KEY, i int, t text)
// whose catalog read says every column is NOT NULL, with its fields.
func widenWorker() (*FetchWorker, []pgconn.FieldDescription) {
	table := fmt.Sprintf("widen_test_%d", widenTableSeq.Add(1))
	f := &FetchWorker{
		conf:     FetchConfig{Table: table, Key: "id", WithAvroSchema: true, Position: position.Position{Type: position.TypeInitial}},
		notNull:  schema.NotNull{"id": true, "i": true, "t": true},
		tableOID: 7,
	}
	fields := []pgconn.FieldDescription{
		{Name: "id", TableOID: 7, DataTypeOID: pgtype.Int8OID},
		{Name: "i", TableOID: 7, DataTypeOID: pgtype.Int4OID},
		{Name: "t", TableOID: 7, DataTypeOID: pgtype.TextOID},
	}
	return f, fields
}

// row runs one row through what fetch does with it.
func (f *FetchWorker) row(ctx context.Context, fields []pgconn.FieldDescription, values []any) (FetchData, error) {
	if err := f.widenForNull(ctx, fields, values); err != nil {
		return FetchData{}, err
	}
	if err := f.extractSchemas(ctx, fields); err != nil {
		return FetchData{}, err
	}
	return f.buildFetchData(fields, values)
}

// TestFetchWorker_WidensNullInNotNullColumn: a snapshot row with NULL in a
// column the catalog read says is NOT NULL (a SET NOT NULL committed after
// the data snapshot was taken) widens that column's field, registers a new
// version and delivers null; it is never a zero value and never stops the
// snapshot.
func TestFetchWorker_WidensNullInNotNullColumn(t *testing.T) {
	is := is.New(t)
	var logs bytes.Buffer
	ctx := zerolog.New(&logs).WithContext(context.Background())
	f, fields := widenWorker()

	first, err := f.row(ctx, fields, []any{int64(1), int32(10), "secret-a"})
	is.NoErr(err)
	is.True(!strings.Contains(string(first.PayloadSchema.Bytes), `"null"`))

	second, err := f.row(ctx, fields, []any{int64(2), nil, "secret-b"})
	is.NoErr(err)
	is.True(second.PayloadSchema.Version > first.PayloadSchema.Version)
	is.True(strings.Contains(string(second.PayloadSchema.Bytes), `{"name":"i","type":["null","int"],"default":null}`))
	is.True(strings.Contains(string(second.PayloadSchema.Bytes), `{"name":"t","type":"string"}`))
	is.Equal(first.KeySchema.Version, second.KeySchema.Version) // the key did not change

	encoded, err := second.PayloadSchema.Marshal(map[string]any(second.Payload))
	is.NoErr(err)
	var got map[string]any
	is.NoErr(second.PayloadSchema.Unmarshal(encoded, &got))
	v, present := got["i"]
	is.True(present)
	is.Equal(v, nil)

	third, err := f.row(ctx, fields, []any{int64(3), nil, "secret-c"})
	is.NoErr(err)
	is.Equal(third.PayloadSchema.Version, second.PayloadSchema.Version)

	out := logs.String()
	is.Equal(strings.Count(out, schema.ErrorCodeAvroNullInNotNullColumn), 1) // once per column
	is.True(strings.Contains(out, `"level":"warn"`))
	is.True(strings.Contains(out, `"table":"`+f.conf.Table+`"`))
	is.True(strings.Contains(out, `"column":"i"`))
	is.True(!strings.Contains(out, "secret")) // never a value
}

// TestFetchWorker_WidenRejected: when the registry rejects the widened
// schema, the row fails with the code and an actionable fix, and the worker
// keeps its schemas.
func TestFetchWorker_WidenRejected(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	f, fields := widenWorker()
	rejected := errors.New("Schema being registered is incompatible with an earlier schema for subject")
	f.createSchema = func(ctx context.Context, typ cschema.Type, subject string, b []byte) (cschema.Schema, error) {
		if strings.Contains(string(b), `"null"`) {
			return cschema.Schema{}, rejected
		}
		return sdkschema.Create(ctx, typ, subject, b)
	}
	first, err := f.row(ctx, fields, []any{int64(1), int32(10), "secret"})
	is.NoErr(err)

	_, err = f.row(ctx, fields, []any{int64(2), nil, "secret"})
	is.True(errors.Is(err, rejected))
	is.True(strings.HasPrefix(err.Error(), schema.ErrorCodeAvroNullInNotNullColumn+": "))
	is.True(strings.Contains(err.Error(), `"`+f.conf.Table+`"`))
	is.True(strings.Contains(err.Error(), `["i"]`))
	is.True(strings.Contains(err.Error(), f.conf.Table+"_payload"))
	is.True(strings.Contains(err.Error(), "BACKWARD"))
	is.True(!strings.Contains(err.Error(), "secret"))
	is.True(f.notNull["i"])                                        // not widened
	is.Equal(f.payloadSchema.Version, first.PayloadSchema.Version) // schemas restored
}

// TestFetchWorker_Run_WidensNullInNotNullColumn drives the race through Run:
// the worker's catalog read says c is NOT NULL (a SET NOT NULL committed
// after the data snapshot was taken, which the catalog read sees while the
// rows do not), and a row holds NULL in c.
func TestFetchWorker_Run_WidensNullInNotNullColumn(t *testing.T) {
	var (
		ctx  = test.Context(t)
		is   = is.New(t)
		pool = test.ConnectPool(ctx, t, test.RegularConnString)
		out  = make(chan []FetchData)
		tt   = &tomb.Tomb{}
	)
	table := fmt.Sprintf("widen_run_%d_%d", time.Now().UnixMicro()%1000000, widenTableSeq.Add(1))
	_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, c int)", table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+table) })
	_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 10), (2, NULL)", table))
	is.NoErr(err)
	var oid uint32
	is.NoErr(pool.QueryRow(ctx, "SELECT $1::regclass::oid", table).Scan(&oid))

	f := NewFetchWorker(pool, out, FetchConfig{Table: table, Key: "id", WithAvroSchema: true})
	f.notNull, f.tableOID = schema.NotNull{"id": true, "c": true}, oid // what the catalog read saw
	tt.Go(func() error {
		runCtx := tt.Context(ctx)
		defer close(out)
		if err := f.Validate(runCtx); err != nil {
			return err
		}
		return f.Run(runCtx)
	})
	var got []FetchData
	for data := range out {
		got = append(got, data...)
	}
	is.NoErr(tt.Wait()) // the snapshot does not stop
	is.Equal(len(got), 2)

	is.True(strings.Contains(string(got[0].PayloadSchema.Bytes), `{"name":"c","type":"int"}`))
	is.True(strings.Contains(string(got[1].PayloadSchema.Bytes), `{"name":"c","type":["null","int"],"default":null}`))
	is.True(got[1].PayloadSchema.Version > got[0].PayloadSchema.Version)
	for i, want := range []any{10, nil} {
		encoded, err := got[i].PayloadSchema.Marshal(map[string]any(got[i].Payload))
		is.NoErr(err)
		var decoded map[string]any
		is.NoErr(got[i].PayloadSchema.Unmarshal(encoded, &decoded))
		is.Equal(decoded["c"], want)
	}
}

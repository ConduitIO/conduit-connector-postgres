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
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/pgutil"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/matryer/is"
)

func TestCopyTo(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, "postgres://repmgr:repmgrmeroxa@localhost:5432/meroxadb?sslmode=disable&replication=database")

	_, err := conn.Exec(ctx, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g json
	)`)
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
	defer tx.Commit(ctx)

	err = CreateReplicationSlot(ctx, conn.PgConn())
	is.NoErr(err)

	fds, err := getFieldDescriptions(ctx, conn.PgConn(), []string{"foo"})
	is.NoErr(err)

	decoders := make([]decoderValue, len(fds))
	for i, fd := range fds {
		decoders[i] = oidToDecoderValue(pgtype.OID(fd.DataTypeOID))
	}

	for _, d := range decoders {
		fmt.Printf("%T\n", d)
	}
	// var buf bytes.Buffer

	w := &CopyDataWriter{
		fieldDescriptions: fds,
		decoders:          decoders,
		connInfo:          conn.ConnInfo(),
	}
	res, err := conn.PgConn().CopyTo(context.Background(), w, "copy foo to stdout")
	is.NoErr(err)

	fmt.Println(res.RowsAffected())
	fmt.Println("-------------")
	// fmt.Println(buf.String())
	//
	// for row := 0; row < int(res.RowsAffected()); row++ {
	// 	line, err := buf.ReadBytes('\n')
	// 	is.NoErr(err)
	// 	tokens := bytes.Split(line, []byte{'\t'})
	// 	for i, decoder := range decoders {
	// 		token := tokens[i]
	// 		if bytes.Equal(token, []byte(`\N`)) {
	// 			token = nil
	// 		}
	//
	// 		err = decoder.DecodeText(conn.ConnInfo(), token)
	// 		is.NoErr(err)
	//
	// 		fmt.Println(string(fds[i].Name), decoder.Get())
	// 	}
	// 	fmt.Println("===")
	// }
}

type CopyDataWriter struct {
	fieldDescriptions []pgproto3.FieldDescription
	decoders          []decoderValue
	connInfo          *pgtype.ConnInfo
}

func (w *CopyDataWriter) Write(line []byte) (n int, err error) {

	tokens := bytes.Split(line, []byte{'\t'})
	for i, decoder := range w.decoders {
		token := tokens[i]
		if bytes.Equal(token, []byte(`\N`)) {
			token = nil
		}

		err = decoder.DecodeText(w.connInfo, token)
		if err != nil {
			return 0, err
		}

		fmt.Println(string(w.fieldDescriptions[i].Name), decoder.Get())
	}
	fmt.Println("===")

	return len(line), nil
}

func CreateReplicationSlot(ctx context.Context, conn *pgconn.PgConn) error {
	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		"myslot",
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true, // replication slot is dropped when we disconnect
			SnapshotAction: "USE_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		// If creating the replication slot fails with code 42710, this means
		// the replication slot already exists.
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42710" {
			return err
		}
	}

	_ = result
	return nil
}

func getFieldDescriptions(ctx context.Context, conn *pgconn.PgConn, table pgx.Identifier) ([]pgproto3.FieldDescription, error) {
	mrr := conn.Exec(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", table.Sanitize()))
	hasNext := mrr.NextResult()
	if !hasNext {
		err := mrr.Close()
		if err == nil {
			// shouldn't happen, just to be safe
			err = errors.New("MultiResultReader did not contain any results")
		}
		return nil, err
	}

	rr := mrr.ResultReader()
	fds := rr.FieldDescriptions()

	// need to copy slice since it is reused for other queries after rr is closed
	fdsCopy := make([]pgproto3.FieldDescription, len(fds))
	copy(fdsCopy, fds)

	_, err := rr.Close()
	if err != nil {
		_ = mrr.Close()
		return nil, err
	}

	err = mrr.Close()
	if err != nil {
		return nil, err
	}

	return fdsCopy, nil
}

type decoderValue interface {
	pgtype.Value
	pgtype.TextDecoder
}

func oidToDecoderValue(id pgtype.OID) decoderValue {
	t, ok := pgutil.OIDToPgType(id).(decoderValue)
	if !ok {
		// not all pg types implement pgtype.Value and pgtype.TextDecoder
		return &pgtype.Unknown{}
	}
	return t
}

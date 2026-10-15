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

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/debezium"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestSource_DebeziumRestartMidTransaction restarts a Debezium-mode source
// from the position of a record inside a transaction. The rest of the
// transaction arrives exactly once (the default mode's exact resume; the
// mode does not change positions), and its events keep the transaction
// orders the uninterrupted run gives them, a primary-key change counting
// two.
func TestSource_DebeziumRestartMidTransaction(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	name := fmt.Sprintf("dbzrestart_%d", time.Now().UnixNano()%1e9)
	_, err := conn.Exec(ctx, "CREATE TABLE "+name+" (id int PRIMARY KEY, v text NOT NULL)")
	is.NoErr(err)

	cfg := map[string]string{
		"url": test.RepmgrConnString, "tables": name, "cdcMode": "logrepl", "snapshotMode": "never",
		"logrepl.slotName": name, "logrepl.publicationName": name,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
		"debezium.enabled": on, "debezium.topic.prefix": "dbz", "debezium.provide.transaction.metadata": on,
	}
	open := func(pos opencdc.Position) (sdk.Source, context.CancelFunc) {
		src := Connector.NewSource()
		is.NoErr(sdk.Util.ParseConfig(ctx, cfg, src.Config(), Connector.NewSpecification().SourceParams))
		octx, cancel := context.WithCancel(ctx)
		is.NoErr(src.Open(octx, pos))
		return src, cancel
	}
	stop := func(src sdk.Source, cancel context.CancelFunc) {
		cancel()
		is.NoErr(src.Teardown(context.Background()))
	}
	t.Cleanup(func() {
		bg := context.Background()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			if _, err := conn.Exec(bg, "SELECT pg_drop_replication_slot($1)", name); err == nil {
				break
			}
		}
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+name)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+name)
	})
	read := func(src sdk.Source, n int) []opencdc.Record {
		var out []opencdc.Record
		deadline := time.Now().Add(time.Minute)
		for len(out) < n {
			if time.Now().After(deadline) {
				t.Fatalf("got %d of %d records", len(out), n)
			}
			rctx, c := context.WithTimeout(ctx, time.Second)
			recs, _ := src.ReadN(rctx, n-len(out))
			c()
			out = append(out, recs...)
		}
		return out
	}

	src, cancel := open(nil)
	tx, err := conn.Begin(ctx)
	is.NoErr(err)
	for _, q := range []string{
		"INSERT INTO " + name + " VALUES (1, 'a')",
		"UPDATE " + name + " SET id = 10 WHERE id = 1", // primary-key change: two events
		"INSERT INTO " + name + " VALUES (2, 'b')",
		"INSERT INTO " + name + " VALUES (3, 'c')",
		"DELETE FROM " + name + " WHERE id = 2",
	} {
		_, err := tx.Exec(ctx, q)
		is.NoErr(err)
	}
	is.NoErr(tx.Commit(ctx))

	first := read(src, 3)
	wantTotal := []string{"1", "2", "4", "5", "6"}
	for i, r := range first {
		is.Equal(r.Metadata[debezium.MetaTxTotal], wantTotal[i])
	}
	// Checkpoint at the third record, mid-transaction, then restart from it.
	for _, r := range first {
		is.NoErr(src.Ack(ctx, r.Position))
	}
	stop(src, cancel)

	src, cancel = open(first[2].Position)
	defer stop(src, cancel)
	rest := read(src, 2)
	for i, r := range rest {
		want := wantTotal[len(first)+i]
		is.Equal(r.Metadata[debezium.MetaTxTotal], want)
		is.Equal(r.Metadata[debezium.MetaTxCollOrder], want)
		is.Equal(r.Metadata[debezium.MetaSourcePfx+"txId"], first[0].Metadata[debezium.MetaSourcePfx+"txId"])
		is.True(strings.HasPrefix(r.Metadata[debezium.MetaSourcePfx+"sequence"], "[null,")) // no commit seen since the start
	}
	is.Equal(rest[0].Key, opencdc.StructuredData{"id": int32(3)})
	is.Equal(rest[1].Metadata[debezium.MetaOp], "d")
	is.Equal(rest[1].Key, opencdc.StructuredData{"id": int32(2)})

	// Nothing after the transaction: no duplicate of the skipped part.
	rctx, c := context.WithTimeout(ctx, 2*time.Second)
	more, _ := src.ReadN(rctx, 1)
	c()
	is.Equal(len(more), 0)
}

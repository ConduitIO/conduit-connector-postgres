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
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v4"

	"github.com/matryer/is"
)

func TestHybridContextCancellation(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	cfg := conn.Config()
	cfg.RuntimeParams["replication"] = "database"
	replconn, err := pgx.ConnectConfig(ctx, cfg)
	is.NoErr(err)

	table := test.SetupTestTableV2(ctx, t, conn)
	ctx, cancel := context.WithCancel(ctx)

	h, err := NewHybridIterator(ctx, replconn, Config{
		TableName:       table,
		SlotName:        table,
		PublicationName: table,
		KeyColumnName:   "h",
		Columns:         []string{"a", "b", "c", "d", "e", "f", "g"},
		SnapshotMode:    "initial",
	})
	is.NoErr(err)
	t.Cleanup(func() {
		err := h.Teardown(ctx)
		is.True(errors.Is(err, context.Canceled))
	})

	count := 0
	for count < 2 {
		_, err = h.Next(ctx)
		is.NoErr(err)
		count++
	}

	go func() {
		cancel()
	}()

	select {
	case <-h.Done(ctx):
	case <-time.After(time.Millisecond * 500):
		is.Fail()
	}
}

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

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestSource_Truncate_WarnsAndKeepsDelivering is the regression test for the
// v0.20.1 part of #344: a multi-table TRUNCATE between two inserts is not
// propagated (no record), but it must be reported with a WARN that names the
// tables and the LSN, and the inserts around it must still be delivered.
func TestSource_Truncate_WarnsAndKeepsDelivering(t *testing.T) {
	is := is.New(t)
	logs := &lockedBuffer{}
	ctx := zerolog.New(logs).WithContext(context.Background())
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	a := fmt.Sprintf("trunc_a_%d", time.Now().UnixNano()%100000000)
	b := fmt.Sprintf("trunc_b_%d", time.Now().UnixNano()%100000000)
	for _, tb := range []string{a, b} {
		_, err := conn.Exec(ctx, "CREATE TABLE "+tb+" (id bigint primary key, v text)")
		is.NoErr(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1)", a)
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+a)
		_, _ = conn.Exec(bg, "DROP TABLE "+a+", "+b)
	})

	src := Connector.NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url": test.RepmgrConnString, "tables": a + "," + b, "cdcMode": "logrepl",
		"logrepl.slotName": a, "logrepl.publicationName": a,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, src.Config(), Connector.NewSpecification().SourceParams))

	openCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	is.NoErr(src.Open(openCtx, nil))
	defer func() { cancel(); _ = src.Teardown(context.Background()) }()

	exec := func(q string) {
		_, err := conn.Exec(ctx, q)
		is.NoErr(err)
	}
	exec("INSERT INTO " + a + " VALUES (1, 'before')")
	exec("TRUNCATE " + a + ", " + b)
	exec("INSERT INTO " + b + " VALUES (2, 'after')")

	var keys []string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && len(keys) < 2 {
		rctx, c := context.WithTimeout(ctx, time.Second)
		recs, _ := src.ReadN(rctx, 10)
		c()
		for _, r := range recs {
			keys = append(keys, r.Metadata["opencdc.collection"]+"/"+string(r.Key.Bytes()))
		}
	}
	is.Equal(len(keys), 2) // both inserts delivered, TRUNCATE emitted nothing
	is.True(strings.HasPrefix(keys[0], a+"/"))
	is.True(strings.HasPrefix(keys[1], b+"/"))

	out := logs.String()
	for _, tb := range []string{a, b} {
		is.True(strings.Contains(out, `"table":"public.`+tb+`"`)) // names the table
	}
	is.True(strings.Contains(out, "TRUNCATE was NOT propagated"))
	is.True(strings.Contains(out, `"lsn":"`)) // names the LSN
	is.True(strings.Contains(out, `"truncates_total":1`))
}

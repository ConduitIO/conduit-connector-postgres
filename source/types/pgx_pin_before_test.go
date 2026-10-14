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

package types_test

// TestPgxPinBeforeImages pins the Before image and the key of UPDATE (with a
// primary key change) and DELETE records under REPLICA IDENTITY FULL, for the
// types pgx decodes differently from one release to the next. The Before image
// is decoded by the same function as the After image, but it is a separate
// record field and a separate code path in the handler.
//
// Regenerate with -update-pgx-pin, only after a deliberate change.

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/pgxpin"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
)

const pinBeforeGoldenFile = "testdata/pgx_pin_before.golden.json"

func TestPgxPinBeforeImages(t *testing.T) {
	if os.Getenv(pinZoneEnv) == "" {
		// The host time zone shows in the Go value of every timestamptz, so
		// the test always runs in a child process with a fixed one.
		runPinChild(t, "^TestPgxPinBeforeImages$", "UTC")
		return
	}
	runPinBeforeImages(t)
}

func runPinBeforeImages(t *testing.T) {
	t.Helper()
	ctx := test.Context(t)
	pgxpin.Lock(ctx, t, test.RegularConnString)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)

	const table, slot = "pgxpin_before", "pgxpin_before_slot"

	cleanup := func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = logrepl.Cleanup(cctx, logrepl.CleanupConfig{URL: pool.Config().ConnString(), SlotName: slot, PublicationName: slot})
	}
	cleanup()
	for _, q := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, tsv tsvector, tsva tsvector[], n numeric, nn numeric[], ts timestamptz, tsa timestamptz[], ia int4[], ta text[], j jsonb, iv interval, d date)",
		"ALTER TABLE " + table + " REPLICA IDENTITY FULL",
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	it, err := logrepl.NewCDCIterator(ctx, pool, logrepl.CDCConfig{
		Tables: []string{table}, TableKeys: map[string]string{table: "id"},
		PublicationName: slot, SlotName: slot, BatchSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = it.Teardown(cctx)
		cleanup()
	})
	if err := it.StartSubscriber(ctx); err != nil {
		t.Fatal(err)
	}

	stmts := []string{
		`INSERT INTO ` + table + ` VALUES (1, 'a fat cat', '{"''a'':1 ''b'':2",NULL}', 123456789012345678901234567890.123456789, '{1.5,2,NULL}', '2022-03-14 15:16:17.123456-08', '{"2022-03-14 15:16:17-08",infinity}', '{{1,2},{3,4}}', '{a,"b c",NULL}', '{"x": 12345678901234567890}', '1 year 2 mons 3 days 04:05:06.789', '0001-01-01 BC')`,
		`UPDATE ` + table + ` SET tsv = '''z'':9', tsva = '[0:1]={''q'',''r''}', n = 7, nn = '{}', ts = 'infinity', tsa = NULL, ia = '[0:2]={7,8,9}', ta = '{}', j = 'null', iv = '-1 mons', d = 'infinity' WHERE id = 1`,
		`UPDATE ` + table + ` SET id = 2 WHERE id = 1`,
		`DELETE FROM ` + table + ` WHERE id = 2`,
		`INSERT INTO ` + table + ` (id) VALUES (3)`,
		`UPDATE ` + table + ` SET tsv = '' WHERE id = 3`,
		`DELETE FROM ` + table + ` WHERE id = 3`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	// The PK change is emitted as one record; count what comes.
	const want = 7
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var recs []opencdc.Record
	for len(recs) < want {
		rr, err := it.NextN(rctx, want-len(recs))
		if err != nil {
			t.Fatalf("got %d records: %v", len(recs), err)
		}
		recs = append(recs, rr...)
	}

	got := make([]map[string]string, 0, len(recs))
	for _, r := range recs {
		before, _ := r.Payload.Before.(opencdc.StructuredData)
		after, _ := r.Payload.After.(opencdc.StructuredData)
		key, _ := r.Key.(opencdc.StructuredData)
		norm := opencdc.Record{Operation: r.Operation, Key: r.Key, Payload: r.Payload}
		got = append(got, map[string]string{
			"op":     r.Operation.String(),
			"key":    dump(key),
			"before": dump(before),
			"after":  dump(after),
			"json":   safely(func() (string, error) { return string(norm.Bytes()), nil }),
		})
	}

	if *updatePgxPin {
		b, err := json.MarshalIndent(got, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(pinBeforeGoldenFile, append(b, '\n'), 0o644); err != nil { //nolint:gosec // test data
			t.Fatal(err)
		}
		return
	}
	comparePinBefore(t, got)
}

func comparePinBefore(t *testing.T, got []map[string]string) {
	t.Helper()
	raw, err := os.ReadFile(pinBeforeGoldenFile)
	if err != nil {
		t.Fatalf("read golden: %v (create it with -update-pgx-pin)", err)
	}
	var want []map[string]string
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(got) {
		t.Fatalf("got %d records, golden has %d", len(got), len(want))
	}
	for i := range got {
		keys := make([]string, 0, len(got[i]))
		for k := range got[i] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if got[i][k] != want[i][k] {
				t.Errorf("record %d, %s differs\n got: %s\nwant: %s", i, k, got[i][k], want[i][k])
			}
		}
	}
}

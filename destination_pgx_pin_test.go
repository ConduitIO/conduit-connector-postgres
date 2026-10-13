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

// TestDestination_PgxPin pins what lands in Postgres when the destination
// writes the records the source emits. The inputs are the JSON forms of the
// records in source/types/testdata/pgx_pin.golden.json (captured on pgx v5.7.5
// with connector v0.14.3), decoded the way a pipeline hands them to a
// destination. The result of each write, and the table afterwards read back as
// text, are compared to testdata/destination_pgx_pin.golden.json.
//
// Regenerate only after a deliberate change of what the destination writes:
//
//	go test . -run TestDestination_PgxPin -update-pgx-pin
//
// Needs the test Postgres (see test/docker-compose.yml).

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/pgxpin"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var updateDestPgxPin = flag.Bool("update-pgx-pin", false, "rewrite testdata/destination_pgx_pin.golden.json")

const destPinGolden = "testdata/destination_pgx_pin.golden.json"

// serverError matches the part of an error that comes from the server.
var serverError = regexp.MustCompile(`ERROR: .*\(SQLSTATE [0-9A-Z]{5}\)`)

func TestDestination_PgxPin(t *testing.T) {
	ctx := test.Context(t)
	pgxpin.Lock(ctx, t, test.RegularConnString)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	raw, err := os.ReadFile("source/types/testdata/pgx_pin.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var src map[string]json.RawMessage
	if err := json.Unmarshal(raw, &src); err != nil {
		t.Fatal(err)
	}

	d := NewDestination()
	if err := sdk.Util.ParseConfig(ctx,
		map[string]string{
			"url":   test.RegularConnString,
			"table": `{{ index .Metadata "opencdc.collection" }}`,
		},
		d.Config(),
		Connector.NewSpecification().DestinationParams,
	); err != nil {
		t.Fatal(err)
	}
	if err := d.Open(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Teardown(context.Background()) })

	got := map[string]any{}
	for _, c := range pgxpin.Cases {
		var rows []struct {
			JSON string `json:"json"`
		}
		if err := json.Unmarshal(src["UTC/avro=false/cdc/"+c.Name], &rows); err != nil {
			// The source fails on this type (the error is pinned there), so
			// there is no record to write.
			continue
		}

		for _, s := range c.Setup {
			if _, err := conn.Exec(ctx, s); err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
		}
		table := "pgxpin_d_" + c.Name
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, c.Typ)); err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

		results := make([]string, 0, len(rows))
		for _, row := range rows {
			// Decode the record the way a pipeline delivers it: structured
			// data with JSON scalars.
			var wire struct {
				Key     map[string]any `json:"key"`
				Payload struct {
					After map[string]any `json:"after"`
				} `json:"payload"`
			}
			if err := json.Unmarshal([]byte(row.JSON), &wire); err != nil {
				// The source's JSON is not valid JSON (NaN, Infinity), so
				// no destination can receive it as such.
				results = append(results, "input is not valid JSON")
				continue
			}
			// See #402: the result depends on the CPU, not on pgx.
			if beyondFloatPrecision(wire.Payload.After) {
				results = append(results, "skipped: integer beyond float64 precision")
				continue
			}
			rec := opencdc.Record{
				Operation: opencdc.OperationCreate,
				Metadata:  opencdc.Metadata{opencdc.MetadataCollection: table},
				Key:       opencdc.StructuredData(wire.Key),
				Payload:   opencdc.Change{After: opencdc.StructuredData(wire.Payload.After)},
			}

			results = append(results, writeOne(ctx, d, rec, table, c.Name))
		}

		stored := readStored(ctx, t, conn, table)

		got[c.Name] = map[string]any{"results": results, "stored": stored}
	}

	for k, v := range writeNative(ctx, t, d, conn) {
		got[k] = v
	}

	// Normalize through JSON so that key order does not matter.
	gb, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var gv map[string]any
	_ = json.Unmarshal(gb, &gv)

	if *updateDestPgxPin {
		b, _ := json.MarshalIndent(gv, "", " ")
		if err := os.WriteFile(destPinGolden, append(b, '\n'), 0o644); err != nil { //nolint:gosec // test data
			t.Fatal(err)
		}
		t.Logf("wrote %d cases to %s", len(gv), destPinGolden)
		return
	}
	compareDestPinGolden(t, gv)
}

// beyondFloatPrecision reports whether v holds an integer-valued number that
// a JSON decoder cannot have read exactly (2^53 to 2^64). Writing it to an
// integer column converts a float64 out of the int64 range, and that
// conversion is defined by the CPU (saturates on arm64, wraps on amd64), so
// what lands in the table says nothing about pgx. The bug itself, which pgx
// 5.7.5 and 5.9.2 share, is tracked in
// https://github.com/ConduitIO/conduit-connector-postgres/issues/402.
func beyondFloatPrecision(v any) bool {
	switch x := v.(type) {
	case float64:
		a := math.Abs(x)
		return a == math.Trunc(a) && a >= 1<<53 && a <= 1<<64
	case []any:
		return slices.ContainsFunc(x, beyondFloatPrecision)
	case map[string]any:
		for _, e := range x {
			if beyondFloatPrecision(e) {
				return true
			}
		}
	}
	return false
}

// writeOne writes rec and describes the result. pgx words client-side errors
// differently from release to release, so what is pinned is that the write
// fails, and for a server error which SQLSTATE and message it fails with.
func writeOne(ctx context.Context, d sdk.Destination, rec opencdc.Record, table, name string) string {
	n, err := d.Write(ctx, []opencdc.Record{rec})
	if err != nil {
		msg := "client-side error"
		if m := serverError.FindString(err.Error()); m != "" {
			msg = strings.ReplaceAll(m, table, "T_"+name)
		}
		return "write error: " + msg
	}
	return fmt.Sprintf("wrote %d", n)
}

// nativeValues are values as a source in the same process, or the Avro decoder,
// hands them to a destination: Go types instead of JSON scalars.
var nativeValues = []struct {
	name string
	typ  string
	val  any
}{
	{"bigrat", "numeric(10,2)", big.NewRat(3, 2)},
	{"bigrat_value", "numeric", *big.NewRat(-7, 8)},
	{"bigrat_nonterminating", "numeric(10,4)", big.NewRat(1, 3)},
	{"bigrat_int", "numeric", big.NewRat(42, 1)},
	{"time_timestamp", "timestamp", time.Date(2022, 3, 14, 15, 16, 17, 123456000, time.UTC)},
	{"time_timestamptz", "timestamptz", time.Date(2022, 3, 14, 15, 16, 17, 0, time.FixedZone("x", -8*3600))},
	{"time_date", "date", time.Date(2022, 3, 14, 0, 0, 0, 0, time.UTC)},
	{"bytes", "bytea", []byte{0, 1, 255}},
	{"string", "text", "héllo"},
	{"int32", "int4", int32(-5)},
	{"int64", "int8", int64(1 << 62)},
	{"int", "int8", 42},
	{"float32", "float4", float32(1.5)},
	{"float64", "float8", 2.25},
	{"float_to_numeric", "numeric", 1.5},
	{"bool", "boolean", true},
	{"map_jsonb", "jsonb", map[string]any{"a": []any{1.5, "x"}}},
	{"bytes_jsonb", "jsonb", []byte(`{"a": 1}`)},
	{"slice_int4", "int4[]", []any{int32(1), int32(2)}},
	{"slice_text", "text[]", []any{"a", "b"}},
	{"uuid_string", "uuid", "bd94ee0b-564f-4088-bf4e-8d5e626caf66"},
	{"uuid_array16", "uuid", [16]byte{0xbd, 0x94, 0xee, 0x0b, 0x56, 0x4f, 0x40, 0x88, 0xbf, 0x4e, 0x8d, 0x5e, 0x62, 0x6c, 0xaf, 0x66}},
	{"string_to_int", "int4", "42"},
	{"string_tsvector", "tsvector", "'a':1 'b':2"},
	{"string_interval", "interval", "1 day"},
	{"duration_interval", "interval", time.Hour},
	{"string_inet", "inet", "192.168.0.17"},
	{"string_timestamptz", "timestamptz", "2022-03-14T15:16:17Z"},
	{"nil", "text", nil},
}

// writeNative writes each of nativeValues to its own table and reads it back.
func writeNative(ctx context.Context, t *testing.T, d sdk.Destination, conn test.Querier) map[string]any {
	t.Helper()
	out := map[string]any{}
	for _, nv := range nativeValues {
		table := "pgxpin_dn_" + nv.name
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, nv.typ)); err != nil {
			t.Fatalf("%s: %v", nv.name, err)
		}
		t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

		rec := opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  opencdc.Metadata{opencdc.MetadataCollection: table},
			Key:       opencdc.StructuredData{"id": int64(1)},
			Payload:   opencdc.Change{After: opencdc.StructuredData{"v": nv.val}},
		}
		out["native/"+nv.name] = map[string]any{
			"results": []string{writeOne(ctx, d, rec, table, nv.name)},
			"stored":  readStored(ctx, t, conn, table),
		}
	}
	return out
}

// readStored returns the rows of table as "id: text" lines.
func readStored(ctx context.Context, t *testing.T, conn test.Querier, table string) []string {
	t.Helper()
	stored := []string{}
	qrows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, v::text FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatal(err)
	}
	defer qrows.Close()
	for qrows.Next() {
		var id int64
		var v *string
		if err := qrows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		if v == nil {
			stored = append(stored, fmt.Sprintf("%d: NULL", id))
		} else {
			stored = append(stored, fmt.Sprintf("%d: %s", id, *v))
		}
	}
	if err := qrows.Err(); err != nil {
		t.Fatal(err)
	}
	return stored
}

func compareDestPinGolden(t *testing.T, gv map[string]any) {
	t.Helper()
	wraw, err := os.ReadFile(destPinGolden)
	if err != nil {
		t.Fatalf("read golden: %v (create it with -update-pgx-pin)", err)
	}
	var wv map[string]any
	if err := json.Unmarshal(wraw, &wv); err != nil {
		t.Fatal(err)
	}

	names := map[string]struct{}{}
	for k := range gv {
		names[k] = struct{}{}
	}
	for k := range wv {
		names[k] = struct{}{}
	}
	sorted := make([]string, 0, len(names))
	for k := range names {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	diffs := 0
	for _, k := range sorted {
		g, _ := json.MarshalIndent(gv[k], "", " ")
		w, _ := json.MarshalIndent(wv[k], "", " ")
		if string(g) != string(w) {
			t.Errorf("%s differs\n got: %s\nwant: %s", k, g, w)
			diffs++
		}
	}
	t.Logf("compared %d cases, %d differences", len(sorted), diffs)
}

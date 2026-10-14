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

// TestPgxPin pins what the source emits for every Postgres type, so that a pgx
// upgrade cannot change a record unnoticed. It runs the real snapshot
// (snapshot.FetchWorker) and the real CDC iterator (logrepl.CDCIterator), with
// and without Avro schemas, and compares the Go value, the JSON of the record
// and the Avro bytes to testdata/pgx_pin.golden.json.
//
// The golden file was captured on release/v0.14.x with pgx v5.7.5 (the
// behavior of connector v0.14.3). A pgx change that alters any of them fails
// this test and has to be neutralised in source/types, not accepted by
// regenerating the file. Regenerate only after a deliberate, documented change
// of the emitted records:
//
//	go test ./source/types -run TestPgxPin -update-pgx-pin
//
// Needs the test Postgres (see test/docker-compose.yml).

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/internal/pgxpin"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/tomb.v2"
)

var updatePgxPin = flag.Bool("update-pgx-pin", false, "rewrite testdata/pgx_pin.golden.json")

const pinGoldenFile = "testdata/pgx_pin.golden.json"

// pinOut is the golden record of one row.
type pinOut struct {
	Key  string `json:"key"`
	Go   string `json:"go"`
	JSON string `json:"json"`
	Avro string `json:"avro,omitempty"`
	// Schema is the Avro payload schema, with the table name normalised.
	Schema string `json:"schema,omitempty"`
}

func TestPgxPin(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)

	if zone := os.Getenv(pinZoneEnv); zone != "" {
		// The parent holds the lock for the whole test.
		// Child process: the host time zone is set (TZ) by the parent, collect
		// what the connector emits under it and hand it back.
		pinWriteChild(t, collectPinZone(ctx, t, pool, zone))
		return
	}

	pgxpin.Lock(ctx, t, test.RegularConnString)

	// The host time zone decides what pgx, and the Avro library, make of a
	// timestamp, so the types that depend on it are replayed in a child process
	// per zone. time.Local cannot be switched inside this process: the
	// connector's goroutines read it.
	got := map[string]any{}
	for _, zone := range pgxpin.Zones {
		for k, v := range pinRunZone(t, zone) {
			got[k] = v
		}
	}

	// The Avro extractor decides which types the Avro variants can cover. Pin
	// its verdict per type too (a type that silently becomes supported or
	// unsupported changes what a pipeline does).
	for _, c := range pgxpin.Cases {
		got["avro-support/"+c.Name] = pinAvroSupport(ctx, t, pool, c)
	}

	if *updatePgxPin {
		writePinGolden(t, got)
		return
	}
	comparePinGolden(t, got)
}

const (
	pinZoneEnv = "PGXPIN_ZONE"
	pinOutEnv  = "PGXPIN_OUT"
)

// collectPinZone replays the cases under the process's time zone.
func collectPinZone(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone string) map[string]any {
	t.Helper()
	got := map[string]any{}
	for _, avro := range []bool{false, true} {
		var cases []pgxpin.Case
		for _, c := range pgxpin.Cases {
			if zone != "UTC" && !c.TZ {
				continue
			}
			if avro && !c.Avro {
				continue
			}
			cases = append(cases, c)
		}

		prefix := fmt.Sprintf("%s/avro=%v", zone, avro)
		for k, v := range pinSnapshot(ctx, t, pool, cases, avro) {
			got[prefix+"/snapshot/"+k] = v
		}
		for k, v := range pinCDC(ctx, t, pool, cases, avro) {
			got[prefix+"/cdc/"+k] = v
		}
	}
	return got
}

func pinWriteChild(t *testing.T, got map[string]any) {
	t.Helper()
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(pinOutEnv), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// pinRunZone runs this test binary again with TZ set to zone and returns what
// it collected.
func pinRunZone(t *testing.T, zone string) map[string]any {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.json")
	runPinChild(t, "^TestPgxPin$", zone, pinOutEnv+"="+out)

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// runPinChild runs this test binary again for the tests matching run, with TZ
// set to zone, and fails t with its output if it fails.
func runPinChild(t *testing.T, run, zone string, env ...string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	args := []string{"-test.run=" + run, "-test.count=1"}
	if *updatePgxPin {
		args = append(args, "-update-pgx-pin")
	}
	cmd := exec.Command(exe, args...)
	cmd.Env = append(append(os.Environ(), "TZ="+zone, pinZoneEnv+"="+zone), env...)
	if o, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("zone %s: %v\n%s", zone, err, o)
	}
}

func writePinGolden(t *testing.T, got map[string]any) {
	t.Helper()
	b, err := json.MarshalIndent(got, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pinGoldenFile, append(b, '\n'), 0o644); err != nil { //nolint:gosec // test data
		t.Fatal(err)
	}
	t.Logf("wrote %d entries to %s", len(got), pinGoldenFile)
}

func comparePinGolden(t *testing.T, got map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(pinGoldenFile)
	if err != nil {
		t.Fatalf("read golden: %v (create it with -update-pgx-pin)", err)
	}
	var want map[string]json.RawMessage
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}

	keys := map[string]struct{}{}
	for k := range got {
		keys[k] = struct{}{}
	}
	for k := range want {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	diffs := 0
	for _, k := range sorted {
		g, gok := got[k]
		w, wok := want[k]
		switch {
		case !gok:
			t.Errorf("%s: in golden, not produced", k)
			diffs++
		case !wok:
			t.Errorf("%s: produced, not in golden", k)
			diffs++
		default:
			// Round-trip both sides through JSON so that key order and
			// number formatting are the same.
			var gv, wv any
			gj, _ := json.Marshal(g)
			_ = json.Unmarshal(gj, &gv)
			_ = json.Unmarshal(w, &wv)
			gb, _ := json.MarshalIndent(gv, "", " ")
			wb, _ := json.MarshalIndent(wv, "", " ")
			if string(gb) != string(wb) {
				t.Errorf("%s differs\n got: %s\nwant: %s", k, gb, wb)
				diffs++
			}
		}
	}
	t.Logf("compared %d entries, %d differences", len(sorted), diffs)
}

// -- helpers ------------------------------------------------------------------

func pinTable(prefix string, c pgxpin.Case) string { return "pgxpin_" + prefix + "_" + c.Name }

func pinCreate(ctx context.Context, t *testing.T, pool *pgxpool.Pool, prefix string, c pgxpin.Case) string {
	t.Helper()
	table := pinTable(prefix, c)
	for _, s := range c.Setup {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("%s: setup %q: %v", c.Name, s, err)
		}
	}
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, c.Typ)); err != nil {
		t.Fatalf("%s: create table: %v", c.Name, err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
	return table
}

func pinInsert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, c pgxpin.Case) {
	t.Helper()
	// One INSERT per row keeps the rows in separate transactions, like a
	// pipeline source sees them.
	for i, v := range c.Vals {
		lit := "'" + strings.ReplaceAll(v, "'", "''") + "'"
		q := fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, %s::%s)", table, i+1, lit, c.Typ)
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("%s: insert %q: %v", c.Name, v, err)
		}
	}
	q := fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, NULL)", table, len(c.Vals)+1)
	if _, err := pool.Exec(ctx, q); err != nil {
		t.Fatal(err)
	}
}

func pinSnapshot(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cases []pgxpin.Case, avro bool) map[string]any {
	t.Helper()
	out := map[string]any{}
	for _, c := range cases {
		table := pinCreate(ctx, t, pool, "s", c)
		pinInsert(ctx, t, pool, table, c)

		fetched := make(chan []snapshot.FetchData)
		f := snapshot.NewFetchWorker(pool, fetched, snapshot.FetchConfig{
			Table:          table,
			Key:            "id",
			WithAvroSchema: avro,
			Position:       position.Position{Type: position.TypeInitial},
		})

		tt := &tomb.Tomb{}
		tt.Go(func() error {
			defer close(fetched)
			wctx := tt.Context(ctx)
			if err := f.Validate(wctx); err != nil {
				return err
			}
			return f.Run(wctx)
		})

		var data []snapshot.FetchData
		for d := range fetched {
			data = append(data, d...)
		}
		if err := tt.Wait(); err != nil {
			out[c.Name] = map[string]string{"error": normalizeTable(err.Error(), table, c.Name)}
			continue
		}

		rows := make([]pinOut, 0, len(data))
		for _, d := range data {
			rows = append(rows, pinRow(ctx, t, table, c.Name, d.Key, d.Payload, d.KeySchema, d.PayloadSchema, avro))
		}
		out[c.Name] = rows
	}
	return out
}

func pinCDC(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cases []pgxpin.Case, avro bool) map[string]any {
	t.Helper()
	var batch, alone []pgxpin.Case
	for _, c := range cases {
		if c.Isolate {
			alone = append(alone, c)
		} else {
			batch = append(batch, c)
		}
	}

	var (
		res map[string]any
		err error
	)
	if len(batch) > 0 {
		res, err = pinCDCRun(ctx, t, pool, batch, avro)
	}
	if err != nil {
		// One of the types broke the stream. Run them one by one so that the
		// failure is attributed to the type that caused it.
		t.Logf("CDC batch failed (%v), running the cases one by one", err)
		alone = cases
		res = nil
	}
	out := map[string]any{}
	for k, v := range res {
		out[k] = v
	}
	for _, c := range alone {
		one, err := pinCDCRun(ctx, t, pool, []pgxpin.Case{c}, avro)
		if err != nil {
			table := pinTable("c", c)
			out[c.Name] = map[string]string{"error": normalizeTable(err.Error(), table, c.Name)}
			continue
		}
		out[c.Name] = one[c.Name]
	}
	return out
}

func pinCDCRun(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cases []pgxpin.Case, avro bool) (map[string]any, error) {
	t.Helper()

	const slot = "pgxpin_slot"
	var (
		tables []string
		keys   = map[string]string{}
		byTbl  = map[string]pgxpin.Case{}
		want   int
	)
	for _, c := range cases {
		table := pinCreate(ctx, t, pool, "c", c)
		tables = append(tables, table)
		keys[table] = "id"
		byTbl[table] = c
		want += len(c.Vals) + 1
	}

	cleanup := func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = logrepl.Cleanup(cctx, logrepl.CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        slot,
			PublicationName: slot,
		})
	}
	cleanup()

	it, err := logrepl.NewCDCIterator(ctx, pool, logrepl.CDCConfig{
		Tables:          tables,
		TableKeys:       keys,
		PublicationName: slot,
		SlotName:        slot,
		WithAvroSchema:  avro,
		BatchSize:       100,
	})
	if err != nil {
		cleanup()
		return nil, err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = it.Teardown(cctx)
		cleanup()
	}()

	if err := it.StartSubscriber(ctx); err != nil {
		return nil, err
	}

	for _, table := range tables {
		pinInsert(ctx, t, pool, table, byTbl[table])
	}

	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var recs []opencdc.Record
	for len(recs) < want {
		rr, err := it.NextN(rctx, want-len(recs))
		if err != nil {
			return nil, err
		}
		recs = append(recs, rr...)
	}

	perTable := map[string][]opencdc.Record{}
	for _, r := range recs {
		coll, err := r.Metadata.GetCollection()
		if err != nil {
			return nil, err
		}
		perTable[coll] = append(perTable[coll], r)
	}

	out := map[string]any{}
	for table, rr := range perTable {
		c := byTbl[table]
		sort.Slice(rr, func(i, j int) bool {
			return rr[i].Key.(opencdc.StructuredData)["id"].(int64) < rr[j].Key.(opencdc.StructuredData)["id"].(int64)
		})
		rows := make([]pinOut, 0, len(rr))
		for _, r := range rr {
			var ks, ps cschema.Schema
			if avro {
				ks = pinSchema(ctx, t, r.Metadata.GetKeySchemaSubject, r.Metadata.GetKeySchemaVersion)
				ps = pinSchema(ctx, t, r.Metadata.GetPayloadSchemaSubject, r.Metadata.GetPayloadSchemaVersion)
			}
			after, _ := r.Payload.After.(opencdc.StructuredData)
			key, _ := r.Key.(opencdc.StructuredData)
			rows = append(rows, pinRow(ctx, t, table, c.Name, key, after, ks, ps, avro))
		}
		out[c.Name] = rows
	}
	return out, nil
}

func pinSchema(ctx context.Context, t *testing.T, subj func() (string, error), ver func() (int, error)) cschema.Schema {
	t.Helper()
	s, err := subj()
	if err != nil {
		t.Fatal(err)
	}
	v, err := ver()
	if err != nil {
		t.Fatal(err)
	}
	sch, err := sdkschema.Get(ctx, s, v)
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// pinRow renders one record the three ways the pin compares: the Go value, the
// JSON of the record (what a pipeline without Avro puts on the wire) and the
// Avro bytes (what the SDK encodes when the connector attaches a schema).
func pinRow(_ context.Context, _ *testing.T, table, name string, key, payload opencdc.StructuredData, ks, ps cschema.Schema, avro bool) pinOut {
	o := pinOut{
		Key: dump(key),
		Go:  dump(payload),
	}

	o.JSON = safely(func() (string, error) {
		rec := opencdc.Record{Operation: opencdc.OperationCreate, Key: key, Payload: opencdc.Change{After: payload}}
		return string(rec.Bytes()), nil
	})

	if avro {
		o.Schema = normalizeTable(string(ps.Bytes), table, name)
		o.Avro = "key=" + pinAvroBytes(ks, key) + " payload=" + pinAvroBytes(ps, payload)
	}
	return o
}

func pinAvroBytes(sch cschema.Schema, v any) string {
	return safely(func() (string, error) {
		srd, err := sch.Serde()
		if err != nil {
			return "", err
		}
		b, err := srd.Marshal(v)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(b), nil
	})
}

func pinAvroSupport(ctx context.Context, t *testing.T, pool *pgxpool.Pool, c pgxpin.Case) string {
	t.Helper()
	// Ask the same code path the source uses: build a snapshot worker for an
	// empty table and see whether it accepts Avro.
	table := pinCreate(ctx, t, pool, "a", c)
	fetched := make(chan []snapshot.FetchData)
	f := snapshot.NewFetchWorker(pool, fetched, snapshot.FetchConfig{
		Table: table, Key: "id", WithAvroSchema: true,
		Position: position.Position{Type: position.TypeInitial},
	})
	pinInsert(ctx, t, pool, table, pgxpin.Case{Typ: c.Typ, Vals: c.Vals[:1]})
	tt := &tomb.Tomb{}
	tt.Go(func() error {
		defer close(fetched)
		wctx := tt.Context(ctx)
		if err := f.Validate(wctx); err != nil {
			return err
		}
		return f.Run(wctx)
	})
	for d := range fetched {
		_ = d
	}
	if err := tt.Wait(); err != nil {
		// The wording of the extractor's error depends on what pgx knows about
		// the type, so pin that the extractor refuses it, not why.
		if strings.Contains(err.Error(), "failed to extract payload schema") {
			return "unsupported"
		}
		return "error: " + normalizeTable(err.Error(), table, c.Name)
	}
	return "ok"
}

var oidInMessage = regexp.MustCompile(`OID \d+`)

// normalizeTable makes an error or schema independent of the table name and of
// the OIDs Postgres assigned to user defined types in this run.
func normalizeTable(s, table, name string) string {
	return oidInMessage.ReplaceAllString(strings.ReplaceAll(s, table, "T_"+name), "OID N")
}

// safely runs f and turns an error or a panic into a string, because both are
// part of what a record does.
func safely(f func() (string, error)) (s string) {
	defer func() {
		if r := recover(); r != nil {
			s = fmt.Sprintf("panic: %v", r)
		}
	}()
	out, err := f()
	if err != nil {
		return "error: " + err.Error()
	}
	return out
}

// dump renders a value with its Go type, deterministically.
func dump(v any) string {
	var b strings.Builder
	dumpValue(&b, reflect.ValueOf(v))
	return b.String()
}

func dumpValue(b *strings.Builder, v reflect.Value) {
	if !v.IsValid() {
		b.WriteString("nil")
		return
	}
	if dumpKnown(b, v) {
		return
	}

	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		dumpPointer(b, v)
	case reflect.Map:
		dumpMap(b, v)
	case reflect.Slice, reflect.Array:
		dumpList(b, v)
	case reflect.Struct:
		dumpStruct(b, v)
	default:
		dumpScalar(b, v)
	}
}

// dumpKnown renders the types that need more than their kind, and reports
// whether v was one of them.
func dumpKnown(b *strings.Builder, v reflect.Value) bool {
	switch x := v.Interface().(type) {
	case time.Time:
		_, off := x.Zone()
		fmt.Fprintf(b, "time.Time(%s loc=%s off=%d)", x.Format(time.RFC3339Nano), x.Location(), off)
	case *big.Rat:
		if x == nil {
			b.WriteString("(*big.Rat)(nil)")
		} else {
			fmt.Fprintf(b, "*big.Rat(%s)", x.String())
		}
	case []byte:
		fmt.Fprintf(b, "%s(%x)", v.Type(), x)
	case float32:
		fmt.Fprintf(b, "float32(%s)", fmtFloat(float64(x), 32))
	case float64:
		fmt.Fprintf(b, "float64(%s)", fmtFloat(x, 64))
	default:
		return false
	}
	return true
}

func dumpPointer(b *strings.Builder, v reflect.Value) {
	if v.IsNil() {
		fmt.Fprintf(b, "%s(nil)", v.Type())
		return
	}
	if v.Kind() == reflect.Pointer {
		b.WriteString("&")
	}
	dumpValue(b, v.Elem())
}

func dumpMap(b *strings.Builder, v reflect.Value) {
	fmt.Fprintf(b, "%s{", v.Type())
	keys := v.MapKeys()
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "%v: ", k)
		dumpValue(b, v.MapIndex(k))
	}
	b.WriteString("}")
}

func dumpList(b *strings.Builder, v reflect.Value) {
	if v.Kind() == reflect.Slice && v.IsNil() {
		fmt.Fprintf(b, "%s(nil)", v.Type())
		return
	}
	fmt.Fprintf(b, "%s[", v.Type())
	for i := range v.Len() {
		if i > 0 {
			b.WriteString(", ")
		}
		dumpValue(b, v.Index(i))
	}
	b.WriteString("]")
}

func dumpStruct(b *strings.Builder, v reflect.Value) {
	fmt.Fprintf(b, "%s{", v.Type())
	first := true
	for i := range v.NumField() {
		f := v.Type().Field(i)
		if !f.IsExported() {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(b, "%s: ", f.Name)
		dumpValue(b, v.Field(i))
	}
	b.WriteString("}")
}

func dumpScalar(b *strings.Builder, v reflect.Value) {
	switch v.Kind() { //nolint:exhaustive // the rest is rendered by %v
	case reflect.String:
		fmt.Fprintf(b, "%s(%q)", v.Type(), v.String())
	case reflect.Bool:
		fmt.Fprintf(b, "%s(%v)", v.Type(), v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(b, "%s(%d)", v.Type(), v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fmt.Fprintf(b, "%s(%d)", v.Type(), v.Uint())
	default:
		fmt.Fprintf(b, "%s(%v)", v.Type(), v)
	}
}

func fmtFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case f == 0 && math.Signbit(f):
		return "-0"
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

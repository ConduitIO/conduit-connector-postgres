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
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/debezium"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// The Debezium golden test runs the golden workload (testdata/debezium/sql,
// copied from ConduitIO/conduit tests/debezium/sql at de14091d) against a
// database whose server-side settings differ from the pinned ones
// (TimeZone America/Los_Angeles, DateStyle 'SQL, DMY', as the capture
// server ran), reads the source's Debezium-mode records, and compares them
// with what real Debezium wrote for the same changes
// (testdata/debezium/golden, the streaming events of the json-numeric and
// json-txmeta captures from the same commit).
//
// It checks, per change: topic, op, key, before, after, the descriptor's
// key and value schemas byte for byte against the JsonConverter schemas,
// the source block's stable fields and its internal consistency, and the
// transaction block's orders. It renders the record values the way the
// JsonConverter does (decimal.format NUMERIC, replace.null.with.default,
// VariableScaleDecimal stripped of trailing zeros), because that is the
// encoder's job and the comparison is with converter output.
//
// Volatile fields are compared structurally only: LSNs, txIds and commit
// times depend on the server.

// on is a boolean setting's "true".
const on = "true"

const goldenTables = "customers,types,toasty,composite,full_ident,default_ident,nopk,numkey,uuidkey,defaults,temporal,sentinel"

// goldenEvent is one Kafka message of a capture.
type goldenEvent struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       *struct {
		Text string `json:"text"`
	} `json:"key"`
	Value *struct {
		Text string `json:"text"`
	} `json:"value"`
}

// event is one Debezium event in comparable form.
type event struct {
	topic     string
	op        string // c, u, d, or "tombstone"
	key       any    // rendered key payload, nil for a null key
	before    any
	after     any
	source    map[string]any
	tx        any // rendered transaction block or nil
	keySchema string
	valSchema string
	where     string
}

func TestSource_DebeziumGolden(t *testing.T) {
	for _, tc := range []struct{ compat, version string }{
		{"2.6", "2.7.4"},
		{"3.5", "3.7.0"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			got := runGoldenWorkload(t, tc.compat)
			compareGolden(t, tc.version, got)
		})
	}
}

// runGoldenWorkload creates a database for the workload, starts the source
// in the Debezium mode, runs the stream script and returns the records up to
// the sentinel's.
func runGoldenWorkload(t *testing.T, compat string) []opencdc.Record {
	t.Helper()
	is := is.New(t)
	ctx := test.Context(t)
	admin := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	name := fmt.Sprintf("dbzgold_%s_%d", strings.ReplaceAll(compat, ".", ""), time.Now().UnixNano()%1e9)
	_, err := admin.Exec(ctx, "CREATE DATABASE "+name)
	is.NoErr(err)
	t.Cleanup(func() {
		// Registered first, so it runs last: after the source and the slot.
		if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})
	// The capture server's settings; the source must not depend on them.
	_, err = admin.Exec(ctx, "ALTER DATABASE "+name+" SET timezone = 'America/Los_Angeles'")
	is.NoErr(err)
	_, err = admin.Exec(ctx, "ALTER DATABASE "+name+" SET datestyle = 'SQL, DMY'")
	is.NoErr(err)

	// The connector names tables relative to the search_path; the role used
	// here has its own search_path, which a database setting does not
	// override, so it is set on the connection.
	url := strings.Replace(test.RepmgrConnString, "/meroxadb?", "/"+name+"?search_path=golden,public&", 1)
	db := test.ConnectSimple(ctx, t, url)
	schema, stream := goldenStatements(t)
	for _, stmt := range schema {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	src := Connector.NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url":                                   url,
		"tables":                                goldenTables,
		"cdcMode":                               "logrepl",
		"snapshotMode":                          "never",
		"logrepl.slotName":                      name,
		"logrepl.publicationName":               name,
		"sdk.schema.extract.key.enabled":        "false",
		"sdk.schema.extract.payload.enabled":    "false",
		"debezium.enabled":                      on,
		"debezium.compat":                       compat,
		"debezium.topic.prefix":                 "dbz",
		"debezium.provide.transaction.metadata": on,
	}, src.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(src.Config().Validate(ctx))

	openCtx, cancel := context.WithCancel(ctx)
	is.NoErr(src.Open(openCtx, nil))
	t.Cleanup(func() {
		cancel()
		_ = src.Teardown(context.Background())
		bg := context.Background()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			if _, err := admin.Exec(bg, "SELECT pg_drop_replication_slot($1)", name); err == nil {
				break
			}
		}
		_ = db.Close(bg)
	})

	// Statement by statement: a multi-statement simple query would run the
	// statements outside BEGIN/COMMIT as one implicit transaction.
	for _, stmt := range stream {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	var recs []opencdc.Record
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the sentinel record; got %d records", len(recs))
		}
		rctx, c := context.WithTimeout(ctx, time.Second)
		batch, err := src.ReadN(rctx, 100)
		c()
		if err != nil && !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "context") {
			t.Fatalf("read: %v", err)
		}
		recs = append(recs, batch...)
		for _, r := range batch {
			is.NoErr(src.Ack(ctx, r.Position))
		}
		if n := len(recs); n > 0 && recs[n-1].Metadata[debezium.MetaTopic] == "dbz.golden.sentinel" {
			return recs
		}
	}
}

// goldenStatements returns the workload's schema statements (01 and the
// CREATE of 03) and its stream statements (02, with 03's inserts before the
// sentinel insert, as in the capture).
func goldenStatements(t *testing.T) (schema, stream []string) {
	t.Helper()
	var temporalDML []string
	for _, stmt := range sqlStatements(t, "testdata/debezium/sql/03-temporal.sql") {
		if !strings.HasPrefix(stmt, "CREATE") {
			temporalDML = append(temporalDML, stmt)
		}
	}
	schema = sqlStatements(t, "testdata/debezium/sql/01-schema.sql")
	for _, stmt := range sqlStatements(t, "testdata/debezium/sql/03-temporal.sql") {
		if strings.HasPrefix(stmt, "CREATE") {
			schema = append(schema, stmt)
		}
	}
	// The temporal inserts go right before the sentinel, as in the capture.
	stream = sqlStatements(t, "testdata/debezium/sql/02-stream.sql")
	last := stream[len(stream)-1]
	if !strings.Contains(last, "golden.sentinel") {
		t.Fatalf("02-stream.sql no longer ends with the sentinel insert: %s", last)
	}
	stream = append(append(stream[:len(stream)-1:len(stream)-1], temporalDML...), last)
	return schema, stream
}

// sqlStatements splits a script at semicolons that end a line, dropping
// comment lines.
func sqlStatements(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

func compareGolden(t *testing.T, version string, recs []opencdc.Record) {
	t.Helper()
	dir := "testdata/debezium/golden/" + version
	want := append(goldenEvents(t, dir+"/json-numeric/events.jsonl", false), goldenEvents(t, dir+"/json-numeric/temporal.jsonl", false)...)
	wantTx := append(goldenEvents(t, dir+"/json-txmeta/events.jsonl", true), goldenEvents(t, dir+"/json-txmeta/temporal.jsonl", true)...)

	var got []event
	for i, r := range recs {
		got = append(got, recordEvents(t, i, r)...)
	}

	wantBy, gotBy, txBy := groupEvents(want), groupEvents(got), groupEvents(wantTx)
	var groups []string
	for k := range wantBy {
		groups = append(groups, k)
	}
	for k := range gotBy {
		if _, ok := wantBy[k]; !ok {
			groups = append(groups, k)
		}
	}
	sort.Strings(groups)

	// Debezium txId to ours: the events of one transaction must be one
	// transaction here too.
	txMap := map[string]string{}
	for _, g := range groups {
		w, gt, wtx := wantBy[g], gotBy[g], txBy[g]
		if len(wtx) != len(w) {
			t.Errorf("%s: the json-txmeta capture has %d events, json-numeric %d", g, len(wtx), len(w))
			continue
		}
		if len(w) != len(gt) {
			t.Errorf("%s: Debezium wrote %d events, the source produced %d\nwant ops %v\ngot ops  %v", g, len(w), len(gt), ops(w), ops(gt))
			continue
		}
		for i := range w {
			compareEvent(t, fmt.Sprintf("%s[%d]", g, i), w[i], gt[i], wtx[i], txMap)
		}
	}
}

func ops(es []event) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.op)
	}
	return out
}

// groupEvents groups events by topic and key, in order. Debezium's order
// holds per key (one partition per key); records with a null key have no
// order, so they are sorted by their rendered after image.
func groupEvents(es []event) map[string][]event {
	out := map[string][]event{}
	for _, e := range es {
		k := e.topic + " key=" + canon(e.key)
		out[k] = append(out[k], e)
	}
	for k, v := range out {
		if strings.HasSuffix(k, " key=null") {
			sort.SliceStable(v, func(i, j int) bool { return canon(v[i].after) < canon(v[j].after) })
			out[k] = v
		}
	}
	return out
}

func canon(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<%v>", err)
	}
	return string(b)
}

func goldenEvents(t *testing.T, path string, base64Decimals bool) []event {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var raw []goldenEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var e goldenEvent
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		raw = append(raw, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(raw, func(i, j int) bool {
		if raw[i].Topic != raw[j].Topic {
			return raw[i].Topic < raw[j].Topic
		}
		if raw[i].Partition != raw[j].Partition {
			return raw[i].Partition < raw[j].Partition
		}
		return raw[i].Offset < raw[j].Offset
	})
	var out []event
	for _, g := range raw {
		e := event{topic: g.Topic, where: fmt.Sprintf("%s[%d]@%d", g.Topic, g.Partition, g.Offset)}
		if g.Key != nil {
			var k struct {
				Schema  json.RawMessage `json:"schema"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal([]byte(g.Key.Text), &k); err != nil {
				t.Fatal(err)
			}
			e.key = decodeNumber(t, k.Payload)
			e.keySchema = string(k.Schema)
			if base64Decimals {
				e.key = numericKey(t, e.keySchema, e.key)
			}
		}
		if g.Value == nil {
			e.op = "tombstone"
			out = append(out, e)
			continue
		}
		var v struct {
			Schema struct {
				Fields []json.RawMessage `json:"fields"`
			} `json:"schema"`
			Payload map[string]json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal([]byte(g.Value.Text), &v); err != nil {
			t.Fatal(err)
		}
		for _, f := range v.Schema.Fields {
			if bytes.HasSuffix(f, []byte(`,"field":"after"}`)) {
				e.valSchema = string(f[:len(f)-len(`,"field":"after"}`)]) + "}"
			}
		}
		_ = json.Unmarshal(v.Payload["op"], &e.op)
		e.before = decodeNumber(t, v.Payload["before"])
		e.after = decodeNumber(t, v.Payload["after"])
		e.source, _ = decodeNumber(t, v.Payload["source"]).(map[string]any)
		e.tx = decodeNumber(t, v.Payload["transaction"])
		out = append(out, e)
	}
	return out
}

func decodeNumber(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// schemaNode is a parsed Connect schema in JsonConverter form.
type schemaNode struct {
	Type       string            `json:"type"`
	Items      *schemaNode       `json:"items"`
	Fields     []schemaField     `json:"fields"`
	Optional   bool              `json:"optional"`
	Name       string            `json:"name"`
	Parameters map[string]string `json:"parameters"`
	Default    json.RawMessage   `json:"default"`
}

type schemaField struct {
	schemaNode
	Field string `json:"field"`
}

// parseDescriptor reads debezium.descriptor: {"version":1,"key":...,"value":...}.
func parseDescriptor(t *testing.T, i int, text string) (keyS, valS *schemaNode, keySchema, valSchema string) {
	t.Helper()
	var desc struct {
		Version int             `json:"version"`
		Key     json.RawMessage `json:"key"`
		Value   json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal([]byte(text), &desc); err != nil {
		t.Fatalf("record %d: descriptor: %v", i, err)
	}
	if desc.Version != 1 {
		t.Fatalf("record %d: descriptor version %d", i, desc.Version)
	}
	if string(desc.Key) != "null" {
		keyS = &schemaNode{}
		if err := json.Unmarshal(desc.Key, keyS); err != nil {
			t.Fatal(err)
		}
		keySchema = string(desc.Key)
	}
	valS = &schemaNode{}
	if err := json.Unmarshal(desc.Value, valS); err != nil {
		t.Fatal(err)
	}
	return keyS, valS, keySchema, string(desc.Value)
}

// recordEvents expands a record into the Debezium events the encoder makes
// of it: one, or delete, tombstone and create for a primary-key change, or
// delete and tombstone for a delete.
func recordEvents(t *testing.T, i int, r opencdc.Record) []event {
	t.Helper()
	md := r.Metadata
	keyS, valS, keySchema, valSchema := parseDescriptor(t, i, md[debezium.MetaDescriptor])

	src := map[string]any{}
	for k, v := range md {
		if name, ok := strings.CutPrefix(k, debezium.MetaSourcePfx); ok {
			src[name] = v
		}
	}
	var tx any
	var total, coll int64
	if id, ok := md[debezium.MetaTxID]; ok {
		total, _ = strconv.ParseInt(md[debezium.MetaTxTotal], 10, 64)
		coll, _ = strconv.ParseInt(md[debezium.MetaTxCollOrder], 10, 64)
		tx = map[string]any{"id": id, "total_order": json.Number(strconv.FormatInt(total, 10)), "data_collection_order": json.Number(strconv.FormatInt(coll, 10))}
	}
	base := event{topic: md[debezium.MetaTopic], source: src, tx: tx, keySchema: keySchema, valSchema: valSchema, where: fmt.Sprintf("record %d", i)}
	row := func(d opencdc.Data) any {
		if d == nil {
			return nil
		}
		sd, ok := d.(opencdc.StructuredData)
		if !ok {
			t.Fatalf("record %d: row is %T", i, d)
		}
		return render(t, valS, map[string]any(sd))
	}
	key := func(d opencdc.Data) any {
		if keyS == nil {
			if d != nil {
				t.Fatalf("record %d: key without a key schema", i)
			}
			return nil
		}
		sd, ok := d.(opencdc.StructuredData)
		if !ok {
			t.Fatalf("record %d: key is %T", i, d)
		}
		return render(t, keyS, map[string]any(sd))
	}
	op := md[debezium.MetaOp]
	switch {
	case op == "c":
		e := base
		e.op, e.key, e.after = "c", key(r.Key), row(r.Payload.After)
		return []event{e}
	case op == "u" && md[debezium.MetaOldKey] != "":
		oldKey := decodeNumber(t, json.RawMessage(md[debezium.MetaOldKey]))
		d := base
		d.op, d.key, d.before = "d", oldKey, row(r.Payload.Before)
		tomb := event{topic: base.topic, op: "tombstone", key: oldKey, keySchema: keySchema}
		c := base
		c.op, c.key, c.after = "c", key(r.Key), row(r.Payload.After)
		if tx != nil {
			c.tx = map[string]any{"id": md[debezium.MetaTxID], "total_order": json.Number(strconv.FormatInt(total+1, 10)), "data_collection_order": json.Number(strconv.FormatInt(coll+1, 10))}
		}
		return []event{d, tomb, c}
	case op == "u":
		e := base
		e.op, e.key, e.before, e.after = "u", key(r.Key), row(r.Payload.Before), row(r.Payload.After)
		return []event{e}
	case op == "d":
		e := base
		e.op, e.key, e.before = "d", key(r.Key), row(r.Payload.Before)
		return []event{e, {topic: base.topic, op: "tombstone", key: e.key, keySchema: keySchema}}
	}
	t.Fatalf("record %d: op %q", i, op)
	return nil
}

// render turns a record value into what the JsonConverter writes for it
// with decimal.format NUMERIC, decoded with UseNumber.
func render(t *testing.T, s *schemaNode, v any) any {
	t.Helper()
	if v == nil {
		if s.Default != nil {
			return decodeNumber(t, s.Default) // replace.null.with.default
		}
		return nil
	}
	switch s.Name {
	case "org.apache.kafka.connect.data.Decimal":
		return json.Number(v.(string))
	case "io.debezium.data.VariableScaleDecimal":
		n, scale := stripZeros(t, v.(string))
		return map[string]any{"scale": json.Number(strconv.Itoa(scale)), "value": base64.StdEncoding.EncodeToString(javaBytes(n))}
	}
	switch s.Type {
	case "int8", "int16", "int32", "int64":
		return json.Number(fmt.Sprint(v))
	case "float":
		return floatValue(float64(v.(float32)))
	case "double":
		return floatValue(v.(float64))
	case "boolean", "string":
		return v
	case "bytes":
		return base64.StdEncoding.EncodeToString(v.([]byte))
	case "array":
		var out []any
		for _, e := range v.([]any) {
			out = append(out, render(t, s.Items, e))
		}
		if out == nil {
			out = []any{}
		}
		return out
	case "struct":
		m := v.(map[string]any)
		out := map[string]any{}
		if len(m) != len(s.Fields) {
			t.Fatalf("struct %s: %d values for %d fields", s.Name, len(m), len(s.Fields))
		}
		for _, f := range s.Fields {
			fv, ok := m[f.Field]
			if !ok {
				t.Fatalf("struct %s: no value for field %q", s.Name, f.Field)
			}
			fs := f.schemaNode
			out[f.Field] = render(t, &fs, fv)
		}
		return out
	}
	t.Fatalf("unhandled schema type %q", s.Type)
	return nil
}

// floatValue is a float as compared: the golden writes NaN and infinities as
// strings and finite values as numbers in Java's notation.
type floatValue float64

func stripZeros(t *testing.T, text string) (*big.Int, int) {
	t.Helper()
	neg := strings.HasPrefix(text, "-")
	s := strings.TrimPrefix(text, "-")
	ip, fp, _ := strings.Cut(s, ".")
	fp = strings.TrimRight(fp, "0")
	n, ok := new(big.Int).SetString(ip+fp, 10)
	if !ok {
		t.Fatalf("decimal %q", text)
	}
	if neg {
		n.Neg(n)
	}
	return n, len(fp)
}

func javaBytes(n *big.Int) []byte {
	if n.Sign() >= 0 {
		b := n.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	m := new(big.Int).Neg(n)
	m.Sub(m, big.NewInt(1))
	b := m.Bytes()
	for i := range b {
		b[i] = ^b[i]
	}
	if len(b) == 0 || b[0]&0x80 == 0 {
		b = append([]byte{0xff}, b...)
	}
	return b
}

func compareEvent(t *testing.T, at string, want, got, wantTx event, txMap map[string]string) {
	t.Helper()
	at = fmt.Sprintf("%s (golden %s, %s)", at, want.where, got.where)
	if want.op != got.op {
		t.Errorf("%s: op %q, want %q", at, got.op, want.op)
		return
	}
	if want.keySchema != got.keySchema {
		t.Errorf("%s: key schema\n got %s\nwant %s", at, got.keySchema, want.keySchema)
	}
	if want.op == "tombstone" {
		return
	}
	if want.valSchema != got.valSchema {
		t.Errorf("%s: value schema\n got %s\nwant %s", at, got.valSchema, want.valSchema)
	}
	if d := diffValue("before", want.before, got.before); d != "" {
		t.Errorf("%s: %s", at, d)
	}
	if d := diffValue("after", want.after, got.after); d != "" {
		t.Errorf("%s: %s", at, d)
	}
	compareSource(t, at, want.source, got.source, txMap)
	compareTx(t, at, wantTx.tx, got.tx, got.source)
}

func diffValue(path string, want, got any) string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: got %#v, want %#v", path, got, want)
		}
		var keys []string
		for k := range w {
			keys = append(keys, k)
		}
		for k := range g {
			if _, ok := w[k]; !ok {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var out []string
		for _, k := range keys {
			if d := diffValue(path+"."+k, w[k], g[k]); d != "" {
				out = append(out, d)
			}
		}
		return strings.Join(out, "; ")
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return fmt.Sprintf("%s: got %#v, want %#v", path, got, want)
		}
		var out []string
		for i := range w {
			if d := diffValue(fmt.Sprintf("%s[%d]", path, i), w[i], g[i]); d != "" {
				out = append(out, d)
			}
		}
		return strings.Join(out, "; ")
	}
	if f, ok := got.(floatValue); ok {
		if floatMatches(want, float64(f)) {
			return ""
		}
		return fmt.Sprintf("%s: got float %v, want %v", path, float64(f), want)
	}
	if reflect.DeepEqual(want, got) {
		return ""
	}
	return fmt.Sprintf("%s: got %#v, want %#v", path, got, want)
}

func floatMatches(want any, got float64) bool {
	switch w := want.(type) {
	case string:
		switch w {
		case "NaN":
			return math.IsNaN(got)
		case "Infinity":
			return math.IsInf(got, 1)
		case "-Infinity":
			return math.IsInf(got, -1)
		}
	case json.Number:
		f, err := strconv.ParseFloat(string(w), 64)
		if err != nil {
			return false
		}
		if f == got {
			return true
		}
		// float32 columns: Java prints the shortest float32 text.
		return float32(f) == float32(got) && float64(float32(got)) == got
	}
	return false
}

// compareSource compares the stable fields exactly and the volatile ones by
// their relations.
func compareSource(t *testing.T, at string, want, got map[string]any, txMap map[string]string) {
	t.Helper()
	for _, f := range []string{"version", "connector", "name", "snapshot", "schema", "table"} {
		if fmt.Sprint(want[f]) != fmt.Sprint(got[f]) {
			t.Errorf("%s: source.%s %v, want %v", at, f, got[f], want[f])
		}
	}
	for _, f := range []string{"xmin", "origin", "origin_lsn"} {
		if _, ok := got[f]; ok {
			t.Errorf("%s: source.%s is set; Debezium writes null", at, f)
		}
	}
	if !strings.HasPrefix(fmt.Sprint(got["db"]), "dbzgold_") {
		t.Errorf("%s: source.db %v", at, got["db"])
	}
	n := func(k string) int64 {
		v, err := strconv.ParseInt(fmt.Sprint(got[k]), 10, 64)
		if err != nil {
			t.Errorf("%s: source.%s %v: %v", at, k, got[k], err)
		}
		return v
	}
	ms, us, ns := n("ts_ms"), n("ts_us"), n("ts_ns")
	if us/1000 != ms || ns != us*1000 {
		t.Errorf("%s: source ts_ms %d, ts_us %d, ts_ns %d disagree", at, ms, us, ns)
	}
	lsn := n("lsn")
	var seq []*string
	if err := json.Unmarshal([]byte(fmt.Sprint(got["sequence"])), &seq); err != nil || len(seq) != 2 || seq[1] == nil {
		t.Errorf("%s: source.sequence %v", at, got["sequence"])
	} else if *seq[1] != strconv.FormatInt(lsn, 10) {
		t.Errorf("%s: source.sequence[1] %s, lsn %d", at, *seq[1], lsn)
	} else if seq[0] != nil {
		if p, err := strconv.ParseInt(*seq[0], 10, 64); err != nil || p > lsn {
			t.Errorf("%s: source.sequence[0] %s is not a commit LSN at or before %d", at, *seq[0], lsn)
		}
	}
	wantTx, gotTx := fmt.Sprint(want["txId"]), fmt.Sprint(got["txId"])
	if prev, ok := txMap[wantTx]; ok && prev != gotTx {
		t.Errorf("%s: Debezium transaction %s is transactions %s and %s here", at, wantTx, prev, gotTx)
	}
	txMap[wantTx] = gotTx
}

func compareTx(t *testing.T, at string, want, got any, src map[string]any) {
	t.Helper()
	w, _ := want.(map[string]any)
	g, _ := got.(map[string]any)
	if w == nil || g == nil {
		t.Errorf("%s: transaction block %v, want %v", at, got, want)
		return
	}
	for _, f := range []string{"total_order", "data_collection_order"} {
		if fmt.Sprint(w[f]) != fmt.Sprint(g[f]) {
			t.Errorf("%s: transaction.%s %v, want %v", at, f, g[f], w[f])
		}
	}
	if id := fmt.Sprint(g["id"]); id != fmt.Sprint(src["txId"])+":"+fmt.Sprint(src["lsn"]) {
		t.Errorf("%s: transaction.id %s, want txId:lsn", at, id)
	}
}

// numericKey rewrites the Decimal fields of a key payload written with
// decimal.format BASE64 (the json-txmeta capture) into the NUMERIC form, so
// keys of both captures group alike.
func numericKey(t *testing.T, schemaText string, key any) any {
	t.Helper()
	var s schemaNode
	if err := json.Unmarshal([]byte(schemaText), &s); err != nil {
		t.Fatal(err)
	}
	m, ok := key.(map[string]any)
	if !ok {
		return key
	}
	for _, f := range s.Fields {
		if f.Name != "org.apache.kafka.connect.data.Decimal" {
			continue
		}
		str, ok := m[f.Field].(string)
		if !ok {
			continue
		}
		b, err := base64.StdEncoding.DecodeString(str)
		if err != nil {
			t.Fatal(err)
		}
		n := new(big.Int).SetBytes(b)
		if len(b) > 0 && b[0]&0x80 != 0 {
			n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
		}
		scale, _ := strconv.Atoi(f.Parameters["scale"])
		r := new(big.Rat).SetFrac(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
		m[f.Field] = json.Number(r.FloatString(scale))
	}
	return m
}

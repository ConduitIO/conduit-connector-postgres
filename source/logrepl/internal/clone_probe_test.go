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

package internal

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// cloneProbeSamples is a text value for every base type pgx registers,
// chosen to make the decoder allocate (non-empty slices, bounds, digits
// past int64). Array samples are derived from these.
var cloneProbeSamples = map[string]string{
	"bool": "t", "bytea": `\x0102`, "char": "a", "name": "n", "int8": "1", "int2": "1", "int4": "1",
	"text": "t", "oid": "1", "tid": "(1,2)", "xid": "1", "cid": "1", "xid8": "1",
	"json": `{"a":[1]}`, "jsonb": `{"a":[1]}`, "xml": "<a/>", "jsonpath": "$.a",
	"point": "(1,2)", "lseg": "[(1,2),(3,4)]", "path": "[(1,2),(3,4)]", "box": "(3,4),(1,2)",
	"polygon": "((1,2),(3,4),(5,6))", "line": "{1,2,3}", "circle": "<(1,2),3>",
	"cidr": "10.0.0.0/8", "inet": "10.0.0.1/8", "macaddr": "08:00:2b:01:02:03", "macaddr8": "08:00:2b:01:02:03:04:05",
	"float4": "1.5", "float8": "1.5", "unknown": "u", "aclitem": "a=r/b",
	"bpchar": "a", "varchar": "v", "date": "2024-01-02", "time": "01:02:03", "timestamp": "2024-01-02 01:02:03",
	"timestamptz": "2024-01-02 01:02:03+00", "interval": "1 day 01:02:03",
	"bit": "101", "varbit": "10110", "numeric": "123456789012345678901234567890.5",
	"uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "tsvector": "'a':1 'b':2",
	"int4range": "[1,5)", "int8range": "[1,5)", "numrange": "[1.5,123456789012345678901234567890)",
	"tsrange": `["2024-01-02 01:02:03","2024-01-03 01:02:03")`, "tstzrange": `["2024-01-02 01:02:03+00","2024-01-03 01:02:03+00")`,
	"daterange":      "[2024-01-02,2024-01-05)",
	"int4multirange": "{[1,5)}", "int8multirange": "{[1,5)}", "nummultirange": "{[1.5,123456789012345678901234567890)}",
	"tsmultirange": `{["2024-01-02 01:02:03","2024-01-03 01:02:03")}`, "tstzmultirange": `{["2024-01-02 01:02:03+00","2024-01-03 01:02:03+00")}`,
	"datemultirange": "{[2024-01-02,2024-01-05)}",
	"record":         "(1,a)",
}

// TestCloneValue_EveryDecodedType decodes a value of every type pgx
// registers (and its array), the way RelationSet does for a change, under
// both encodings, and requires types.CloneValue's copy to share no slice,
// map or pointer with the original (#425). A type added to pgx without a
// sample fails the test, so the list cannot fall behind.
func TestCloneValue_EveryDecodedType(t *testing.T) {
	m := pgtype.NewMap()
	for oid := uint32(0); oid < 70000; oid++ {
		ty, ok := m.TypeForOID(oid)
		if !ok {
			continue
		}
		base, isArray := strings.CutPrefix(ty.Name, "_")
		sample, ok := cloneProbeSamples[base]
		if !ok {
			t.Errorf("%s (oid %d): no sample; add one to cloneProbeSamples", ty.Name, oid)
			continue
		}
		if isArray {
			sample = `{"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(sample) + `"}`
		}
		for _, enc := range []types.Encoding{types.EncodingNative, types.EncodingAvro} {
			rs := NewRelationSetWithEncoding(enc)
			rs.Add(&pglogrepl.RelationMessage{RelationID: 1, RelationName: "t", ColumnNum: 1,
				Columns: []*pglogrepl.RelationMessageColumn{{Name: "c", DataType: oid, TypeModifier: -1}}})
			vals, err := rs.Values(1, &pglogrepl.TupleData{ColumnNum: 1, Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte(sample)}}})
			if err != nil {
				// Some types cannot be represented under one encoding
				// (for example Avro); that path errors before any record.
				t.Logf("%s (enc %d): %v", ty.Name, enc, err)
				continue
			}
			orig := vals["c"]
			clone := types.CloneValue(orig)
			if !reflect.DeepEqual(orig, clone) {
				t.Errorf("%s (enc %d): clone %#v differs from %#v", ty.Name, enc, clone, orig)
			}
			if path, shared := sharedMemory(reflect.ValueOf(orig), reflect.ValueOf(clone), "c"); shared {
				t.Errorf("%s (enc %d, %T): clone shares %s with the original", ty.Name, enc, orig, path)
			}
		}
	}
}

// Types whose references are shared by design and never mutated: the
// *time.Location of a time.Time, and the interned zone handle of a
// netip.Addr (a unique.Handle).
var probeImmutable = map[reflect.Type]bool{
	reflect.TypeOf(time.Time{}):    true,
	reflect.TypeOf(netip.Addr{}):   true,
	reflect.TypeOf(netip.Prefix{}): true,
}

// sharedMemory walks a and b in parallel, unexported fields included, and
// reports the first slice, map or pointer they share.
//
//nolint:gocyclo // one case per reflect kind
func sharedMemory(a, b reflect.Value, path string) (string, bool) {
	if !a.IsValid() || !b.IsValid() {
		return "", false
	}
	switch a.Kind() { //nolint:exhaustive // the other kinds hold no reference
	case reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return "", false
		}
		return sharedMemory(a.Elem(), b.Elem(), path)
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return "", false
		}
		if a.Pointer() == b.Pointer() {
			return path + " (pointer)", true
		}
		return sharedMemory(a.Elem(), b.Elem(), "*"+path)
	case reflect.Slice:
		if a.Len() == 0 || b.Len() == 0 {
			return "", false
		}
		if a.Pointer() == b.Pointer() {
			return path + " (slice)", true
		}
		for i := 0; i < a.Len() && i < b.Len(); i++ {
			if p, s := sharedMemory(a.Index(i), b.Index(i), path+"[i]"); s {
				return p, s
			}
		}
	case reflect.Array:
		for i := 0; i < a.Len(); i++ {
			if p, s := sharedMemory(a.Index(i), b.Index(i), path+"[i]"); s {
				return p, s
			}
		}
	case reflect.Map:
		if a.Len() == 0 {
			return "", false
		}
		if a.Pointer() == b.Pointer() {
			return path + " (map)", true
		}
		iter := a.MapRange()
		for iter.Next() {
			if p, s := sharedMemory(iter.Value(), b.MapIndex(iter.Key()), path+"[k]"); s {
				return p, s
			}
		}
	case reflect.Struct:
		if probeImmutable[a.Type()] {
			return "", false
		}
		for i := 0; i < a.NumField(); i++ {
			if p, s := sharedMemory(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name); s {
				return p, s
			}
		}
	}
	return "", false
}

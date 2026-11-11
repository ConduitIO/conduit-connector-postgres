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

package schema

import (
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// The schema a pipeline registered before json/jsonb became strings, and the
// one it registers after the upgrade, for the same table.
const (
	jsonSchemaBytes = `{"name":"t_payload","type":"record","fields":[{"name":"id","type":"long"},{"name":"j","type":"bytes"},{"name":"jb","type":"bytes"}]}`
)

// The bytes -> string change of json/jsonb is a promotion the Avro spec
// allows in both directions ("bytes is promotable to string", "string is
// promotable to bytes", Schema Resolution): the new schema is BACKWARD
// compatible (a reader on the new schema reads data written with the old
// one) and FORWARD compatible (a reader still on the old schema reads new
// data), so FULL too. This pins the verdict of the Avro library the
// connector uses, and that the resolution keeps every byte of the document.
func TestJSONStringSchema_CompatibleWithBytesSchema(t *testing.T) {
	is := is.New(t)

	newSch, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "j", DataTypeOID: pgtype.JSONOID, TypeModifier: -1},
		{Name: "jb", DataTypeOID: pgtype.JSONBOID, TypeModifier: -1},
	}, NotNull{"id": true, "j": true, "jb": true})
	is.NoErr(err)
	is.Equal(newSch.String(), `{"name":"t_payload","type":"record","fields":[{"name":"id","type":"long"},{"name":"j","type":"string"},{"name":"jb","type":"string"}]}`)
	oldSch, err := avro.Parse(jsonSchemaBytes)
	is.NoErr(err)

	sc := avro.NewSchemaCompatibility()
	is.NoErr(sc.Compatible(newSch, oldSch)) // BACKWARD: new reader, old writer
	is.NoErr(sc.Compatible(oldSch, newSch)) // FORWARD: old reader, new writer

	doc := `{"z": 1, "a": [12345678901234567890], "u": "日本 😀"}`

	// Data written before the upgrade, read with the new schema.
	oldData, err := avro.Marshal(oldSch, map[string]any{"id": int64(1), "j": []byte(doc), "jb": []byte(doc)})
	is.NoErr(err)
	resolved, err := sc.Resolve(newSch, oldSch)
	is.NoErr(err)
	var gotNew map[string]any
	is.NoErr(avro.Unmarshal(resolved, oldData, &gotNew))
	is.Equal(gotNew["j"], doc)
	is.Equal(gotNew["jb"], doc)

	// Data written after the upgrade, read by a consumer still on the old
	// schema.
	newData, err := avro.Marshal(newSch, map[string]any{"id": int64(1), "j": doc, "jb": doc})
	is.NoErr(err)
	is.Equal(newData, oldData) // the wire bytes are the same: both are length + raw bytes
	resolved, err = sc.Resolve(oldSch, newSch)
	is.NoErr(err)
	var gotOld map[string]any
	is.NoErr(avro.Unmarshal(resolved, newData, &gotOld))
	is.Equal(gotOld["j"], []byte(doc))
	is.Equal(gotOld["jb"], []byte(doc))
}

// The registered json/jsonb schema carries Debezium's connect.name and
// connect.version properties. They are not part of the Avro type, so the
// verdict above holds for the text a registry stores and hands back: the
// schema is BACKWARD, FORWARD and FULL compatible with the bytes schema of
// v0.20 and earlier, and with the plain "string" schema the json change
// registered before the properties were added. Its fingerprint (computed on
// the canonical form) is the one of the plain string schema, and data
// written with any of the three decodes with any other unchanged.
func TestJSONDebeziumNamedSchema_Compatible(t *testing.T) {
	is := is.New(t)

	extracted, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "j", DataTypeOID: pgtype.JSONOID, TypeModifier: -1},
		{Name: "jb", DataTypeOID: pgtype.JSONBOID, TypeModifier: -1},
	}, NotNull{"id": true, "j": true, "jb": true})
	is.NoErr(err)
	text, err := Text(extracted)
	is.NoErr(err)
	is.True(strings.Contains(string(text), `"connect.name":"io.debezium.data.Json"`))
	named, err := avro.Parse(string(text)) // as a registry hands it back
	is.NoErr(err)

	bytesSch, err := avro.Parse(jsonSchemaBytes)
	is.NoErr(err)
	plainSch, err := avro.Parse(`{"name":"t_payload","type":"record","fields":[{"name":"id","type":"long"},{"name":"j","type":"string"},{"name":"jb","type":"string"}]}`)
	is.NoErr(err)
	is.Equal(named.Fingerprint(), plainSch.Fingerprint())

	doc := `{"z": 1, "a": [12345678901234567890], "u": "日本 😀"}`
	values := map[string]avro.Schema{"bytes (v0.20)": bytesSch, "plain string": plainSch}
	for name, other := range values {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			sc := avro.NewSchemaCompatibility()
			is.NoErr(sc.Compatible(named, other)) // BACKWARD: new reader, old writer
			is.NoErr(sc.Compatible(other, named)) // FORWARD: old reader, new writer

			var otherValue any = doc
			if name == "bytes (v0.20)" {
				otherValue = []byte(doc)
			}
			otherData, err := avro.Marshal(other, map[string]any{"id": int64(1), "j": otherValue, "jb": otherValue})
			is.NoErr(err)
			newData, err := avro.Marshal(named, map[string]any{"id": int64(1), "j": doc, "jb": doc})
			is.NoErr(err)
			is.Equal(newData, otherData) // same wire bytes

			resolved, err := sc.Resolve(named, other)
			is.NoErr(err)
			var gotNew map[string]any
			is.NoErr(avro.Unmarshal(resolved, otherData, &gotNew))
			is.Equal(gotNew["j"], doc)
			is.Equal(gotNew["jb"], doc)

			resolved, err = sc.Resolve(other, named)
			is.NoErr(err)
			var gotOld map[string]any
			is.NoErr(avro.Unmarshal(resolved, newData, &gotOld))
			is.Equal(gotOld["j"], otherValue)
			is.Equal(gotOld["jb"], otherValue)
		})
	}
}

// A nullable json column changes twice in the same release: bytes becomes
// string, and the field becomes ["null", string] (#466). The registered
// version is BACKWARD compatible with the v0.20 bytes schema (a reader on the
// new schema reads old data, to the exact string) and not FORWARD compatible
// (a reader still on bytes cannot read the union), so not FULL. Rolling back
// re-registers bytes after the union, which is not BACKWARD compatible
// either: the README's upgrade notes say so, and this pins it.
func TestJSONNullableSchema_BackwardOnly(t *testing.T) {
	is := is.New(t)

	extracted, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "j", DataTypeOID: pgtype.JSONOID, TypeModifier: -1},
	}, NotNull{"id": true, "j": false})
	is.NoErr(err)
	text, err := Text(extracted)
	is.NoErr(err)
	is.True(strings.Contains(string(text), `"type":["null",{"type":"string","connect.name":"io.debezium.data.Json"`))
	newSch, err := avro.Parse(string(text)) // as a registry hands it back
	is.NoErr(err)
	oldSch, err := avro.Parse(`{"name":"t_payload","type":"record","fields":[{"name":"id","type":"long"},{"name":"j","type":"bytes"}]}`)
	is.NoErr(err)

	sc := avro.NewSchemaCompatibility()
	is.NoErr(sc.Compatible(newSch, oldSch)) // BACKWARD: new reader, old writer
	// Not FORWARD (old reader, new writer). The same check is the rollback's:
	// bytes registered after the union is not BACKWARD compatible.
	is.True(sc.Compatible(oldSch, newSch) != nil)

	doc := `{"z": 1, "u": "日本 😀"}`
	oldData, err := avro.Marshal(oldSch, map[string]any{"id": int64(1), "j": []byte(doc)})
	is.NoErr(err)
	resolved, err := sc.Resolve(newSch, oldSch)
	is.NoErr(err)
	var got map[string]any
	is.NoErr(avro.Unmarshal(resolved, oldData, &got))
	is.Equal(got["j"], doc) // old data resolves to the exact string
}

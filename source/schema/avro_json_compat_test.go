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
	})
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

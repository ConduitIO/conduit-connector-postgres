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

package debezium

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// Kafka Connect schema types, as JsonConverter names them.
const (
	typeInt16   = "int16"
	typeInt32   = "int32"
	typeInt64   = "int64"
	typeFloat32 = "float"
	typeFloat64 = "double"
	typeBoolean = "boolean"
	typeString  = "string"
	typeBytes   = "bytes"
	typeArray   = "array"
	typeStruct  = "struct"
)

// Logical type names Debezium uses.
const (
	nameDecimal              = "org.apache.kafka.connect.data.Decimal"
	nameVariableScaleDecimal = "io.debezium.data.VariableScaleDecimal"
	nameDate                 = "io.debezium.time.Date"
	nameTime                 = "io.debezium.time.Time"
	nameMicroTime            = "io.debezium.time.MicroTime"
	nameTimestamp            = "io.debezium.time.Timestamp"
	nameMicroTimestamp       = "io.debezium.time.MicroTimestamp"
	nameZonedTime            = "io.debezium.time.ZonedTime"
	nameZonedTimestamp       = "io.debezium.time.ZonedTimestamp"
	nameMicroDuration        = "io.debezium.time.MicroDuration"
	nameUUID                 = "io.debezium.data.Uuid"
	nameJSON                 = "io.debezium.data.Json"
	nameXML                  = "io.debezium.data.Xml"
	nameEnum                 = "io.debezium.data.Enum"
)

// param is one schema parameter. Parameters are an ordered list because
// Debezium's order is part of the schema text (scale before precision).
type param struct{ Key, Value string }

// Schema is the subset of a Kafka Connect schema this source writes into the
// descriptor. It is written in JsonConverter's schema form, in its key order:
// type, items or fields, optional, name, version, doc, parameters, default,
// field. Field order is column order (attnum) or key order, never map order.
type Schema struct {
	Type     string
	Items    *Schema
	Fields   []Field
	Optional bool
	Name     string
	Version  int // 0: none
	Doc      string
	Params   []param
	// Default is the schema default as a JSON literal in JsonConverter's
	// NUMERIC decimal form (decimals as numbers). Nil: no default.
	Default json.RawMessage
}

// Field is one struct field.
type Field struct {
	Name   string
	Schema *Schema
}

// MarshalJSON writes the schema in JsonConverter form.
func (s *Schema) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	s.write(&b, "")
	return b.Bytes(), nil
}

func (s *Schema) write(b *bytes.Buffer, fieldName string) {
	b.WriteString(`{"type":`)
	writeJSONString(b, s.Type)
	if s.Items != nil {
		b.WriteString(`,"items":`)
		s.Items.write(b, "")
	}
	if s.Type == typeStruct {
		b.WriteString(`,"fields":[`)
		for i, f := range s.Fields {
			if i > 0 {
				b.WriteByte(',')
			}
			f.Schema.write(b, f.Name)
		}
		b.WriteByte(']')
	}
	b.WriteString(`,"optional":`)
	b.WriteString(strconv.FormatBool(s.Optional))
	if s.Name != "" {
		b.WriteString(`,"name":`)
		writeJSONString(b, s.Name)
	}
	if s.Version != 0 {
		b.WriteString(`,"version":`)
		b.WriteString(strconv.Itoa(s.Version))
	}
	if s.Doc != "" {
		b.WriteString(`,"doc":`)
		writeJSONString(b, s.Doc)
	}
	if len(s.Params) > 0 {
		b.WriteString(`,"parameters":{`)
		for i, p := range s.Params {
			if i > 0 {
				b.WriteByte(',')
			}
			writeJSONString(b, p.Key)
			b.WriteByte(':')
			writeJSONString(b, p.Value)
		}
		b.WriteByte('}')
	}
	if s.Default != nil {
		b.WriteString(`,"default":`)
		b.Write(s.Default)
	}
	if fieldName != "" {
		b.WriteString(`,"field":`)
		writeJSONString(b, fieldName)
	}
	b.WriteByte('}')
}

func writeJSONString(b *bytes.Buffer, s string) {
	enc, _ := json.Marshal(s) // a Go string always marshals
	b.Write(enc)
}

// clone returns a shallow copy whose Optional flag can be changed without
// touching the original.
func (s *Schema) clone() *Schema {
	c := *s
	return &c
}

// MarshalDescriptor writes the debezium.descriptor metadata value:
//
//	{"version":1,"key":<schema>|null,"value":<schema>}
//
// (decision 17, 2026-10-15). key is nil for a table without a key.
func MarshalDescriptor(key, value *Schema) string {
	var b bytes.Buffer
	b.WriteString(`{"version":1,"key":`)
	if key == nil {
		b.WriteString("null")
	} else {
		key.write(&b, "")
	}
	b.WriteString(`,"value":`)
	value.write(&b, "")
	b.WriteByte('}')
	return b.String()
}

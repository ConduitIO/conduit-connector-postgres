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
	"fmt"
	"strconv"
	"strings"
)

// PGType is what the catalog says about a column's type.
type PGType struct {
	OID  uint32
	Name string // pg_type.typname, e.g. "int4", "_int4", "mood"
	// Kind is pg_type.typtype: 'b' base, 'e' enum, 'd' domain, 'c'
	// composite, 'r' range, 'm' multirange, 'p' pseudo.
	Kind byte
	// Elem is the element type of an array type, nil otherwise.
	Elem *PGType
	// EnumLabels are the members of an enum, in pg_enum.enumsortorder.
	EnumLabels []string
}

// paramScale is the Decimal scale parameter and VariableScaleDecimal's
// scale field.
const paramScale = "scale"

// Postgres type names (pg_type.typname) the mapping switches on.
const (
	pgInt2        = "int2"
	pgInt4        = "int4"
	pgInt8        = "int8"
	pgOid         = "oid"
	pgFloat4      = "float4"
	pgFloat8      = "float8"
	pgBool        = "bool"
	pgText        = "text"
	pgNumeric     = "numeric"
	pgMoney       = "money"
	pgDate        = "date"
	pgTime        = "time"
	pgTimetz      = "timetz"
	pgTimestamp   = "timestamp"
	pgTimestamptz = "timestamptz"
	pgInterval    = "interval"
	pgBytea       = "bytea"
	pgHstore      = "hstore"
	pgJSON        = "json"
	pgJSONB       = "jsonb"
	pgUUID        = "uuid"
	pgXML         = "xml"
	pgName        = "name"
	pgInt4Array   = "_int4"
	pgTextArray   = "_text"
)

// kind is the Debezium mapping of one Postgres type: its Connect schema and
// how to decode pgoutput's text form into the record value FromGo accepts.
type kind int

const (
	kindInt16 kind = iota + 1
	kindInt32
	kindInt64
	kindOID
	kindFloat32
	kindFloat64
	kindBool
	kindString // text, varchar, bpchar, inet, cidr, macaddr, citext, name
	kindXML
	kindDecimal         // numeric(p,s)
	kindVariableDecimal // numeric
	kindMoney
	kindDate
	kindTime      // time(p<=3): ms
	kindMicroTime // time(p>3 or unspecified): µs
	kindTimeTZ
	kindTimestamp      // timestamp(p<=3): ms
	kindMicroTimestamp // timestamp(p>3 or unspecified): µs
	kindTimestampTZ
	kindInterval
	kindUUID
	kindJSON
	kindBytes
	kindEnum
	kindHstore
	kindArray
)

// column is one column's mapping, built once per relation shape.
type column struct {
	name      string
	kind      kind
	elem      *column // arrays
	schema    *Schema
	typeName  string
	precision int // time, timestamp
	scale     int // numeric(p,s)
}

// typmod helpers. Postgres stores (precision << 16 | scale) + 4 for numeric,
// and the fractional precision directly for time/timestamp (-1 = unset).
func numericTypmod(typmod int32) (precision, scale int, ok bool) {
	if typmod < 4 {
		return 0, 0, false
	}
	t := typmod - 4
	return int((t >> 16) & 0xffff), int(t & 0xffff), true
}

// newColumn maps a column type to its Debezium kind and schema under the
// default handling modes (decimal precise, time adaptive, binary bytes,
// interval numeric, hstore json). Types Debezium does not map fail with
// CodeUnsupportedType: the source halts instead of guessing (invariant 6).
//
//nolint:gocyclo // one branch per Postgres type; the table is the point
func newColumn(name string, t *PGType, typmod int32, opts columnOptions) (*column, error) {
	c := &column{name: name, typeName: t.Name}
	s := &Schema{}
	switch {
	case t.Kind == 'e':
		c.kind = kindEnum
		s.Type, s.Name, s.Version = typeString, nameEnum, 1
		s.Params = []param{{"allowed", strings.Join(t.EnumLabels, ",")}}
	case t.Elem != nil:
		if t.Name == "_bytea" || t.Name == "_uuid" || t.Name == "_date" || t.Name == "_int8" || t.Name == pgInt4Array ||
			t.Name == "_int2" || t.Name == pgTextArray || t.Name == "_varchar" || t.Name == "_bpchar" || t.Name == "_numeric" ||
			t.Name == "_bool" || t.Name == "_float4" || t.Name == "_float8" || t.Name == "_timestamp" ||
			t.Name == "_timestamptz" || t.Name == "_jsonb" || t.Name == "_json" {
			elemMod := typmod // an array column's typmod is its element's
			elem, err := newColumn(name, t.Elem, elemMod, opts)
			if err != nil {
				return nil, err
			}
			es := elem.schema.clone()
			es.Optional = true
			es.Default = nil
			c.kind, c.elem = kindArray, elem
			s.Type, s.Items = typeArray, es
		} else {
			return nil, unsupportedType(name, t.Name)
		}
	default:
		switch t.Name {
		case pgInt2:
			c.kind, s.Type = kindInt16, typeInt16
		case pgInt4:
			c.kind, s.Type = kindInt32, typeInt32
		case pgInt8:
			c.kind, s.Type = kindInt64, typeInt64
		case pgOid:
			c.kind, s.Type = kindOID, typeInt64
		case pgFloat4:
			c.kind, s.Type = kindFloat32, typeFloat32
		case pgFloat8:
			c.kind, s.Type = kindFloat64, typeFloat64
		case pgBool:
			c.kind, s.Type = kindBool, typeBoolean
		case pgText, "varchar", "bpchar", pgName, "citext", "inet", "cidr", "macaddr", "macaddr8":
			c.kind, s.Type = kindString, typeString
		case pgXML:
			c.kind, s.Type, s.Name, s.Version = kindXML, typeString, nameXML, 1
		case pgNumeric:
			if p, sc, ok := numericTypmod(typmod); ok {
				c.kind, c.scale = kindDecimal, sc
				s.Type, s.Name, s.Version = typeBytes, nameDecimal, 1
				s.Params = []param{{paramScale, strconv.Itoa(sc)}, {"connect.decimal.precision", strconv.Itoa(p)}}
			} else {
				c.kind = kindVariableDecimal
				s.Type, s.Name, s.Version, s.Doc = typeStruct, nameVariableScaleDecimal, 1, "Variable scaled decimal"
				s.Fields = []Field{
					{Name: paramScale, Schema: &Schema{Type: typeInt32}},
					{Name: "value", Schema: &Schema{Type: typeBytes}},
				}
			}
		case pgMoney:
			c.kind, c.scale = kindMoney, opts.moneyFractionDigits
			s.Type, s.Name, s.Version = typeBytes, nameDecimal, 1
			s.Params = []param{{paramScale, strconv.Itoa(opts.moneyFractionDigits)}}
		case pgDate:
			c.kind, s.Type, s.Name, s.Version = kindDate, typeInt32, nameDate, 1
		case pgTime:
			c.precision = timePrecision(typmod)
			if c.precision <= 3 {
				c.kind, s.Type, s.Name, s.Version = kindTime, typeInt32, nameTime, 1
			} else {
				c.kind, s.Type, s.Name, s.Version = kindMicroTime, typeInt64, nameMicroTime, 1
			}
		case pgTimetz:
			c.kind, s.Type, s.Name, s.Version = kindTimeTZ, typeString, nameZonedTime, 1
		case pgTimestamp:
			c.precision = timePrecision(typmod)
			if c.precision <= 3 {
				c.kind, s.Type, s.Name, s.Version = kindTimestamp, typeInt64, nameTimestamp, 1
			} else {
				c.kind, s.Type, s.Name, s.Version = kindMicroTimestamp, typeInt64, nameMicroTimestamp, 1
			}
		case pgTimestamptz:
			c.kind, s.Type, s.Name, s.Version = kindTimestampTZ, typeString, nameZonedTimestamp, 1
		case pgInterval:
			c.kind, s.Type, s.Name, s.Version = kindInterval, typeInt64, nameMicroDuration, 1
		case pgUUID:
			c.kind, s.Type, s.Name, s.Version = kindUUID, typeString, nameUUID, 1
		case pgJSON, pgJSONB:
			c.kind, s.Type, s.Name, s.Version = kindJSON, typeString, nameJSON, 1
		case pgBytea:
			c.kind, s.Type = kindBytes, typeBytes
		case pgHstore:
			c.kind, s.Type, s.Name, s.Version = kindHstore, typeString, nameJSON, 1
		default:
			return nil, unsupportedType(name, t.Name)
		}
	}
	c.schema = s
	return c, nil
}

// timePrecision is the fractional precision of a time or timestamp column;
// an unspecified precision (typmod -1) is 6.
func timePrecision(typmod int32) int {
	if typmod < 0 || typmod > 6 {
		return 6
	}
	return int(typmod)
}

type columnOptions struct {
	moneyFractionDigits int
}

func unsupportedType(col, typeName string) error {
	return &Error{
		Code:    CodeUnsupportedType,
		Message: fmt.Sprintf("column %q has type %q, which the Debezium mode does not map in this version", col, typeName),
		Fix:     "Exclude the table from the pipeline, or run it in the default output mode. The supported types are listed in the connector's Debezium mode documentation.",
	}
}

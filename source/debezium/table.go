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
	"regexp"
	"strings"

	"github.com/jackc/pglogrepl"
)

// table is one relation shape in Debezium terms: the columns of the
// Relation message (the shape the changes are encoded with), enriched with
// the catalog's nullability and defaults, and the descriptor and topic.
type table struct {
	relID      uint32
	schemaName string
	tableName  string
	topic      string
	columns    []*tableColumn // Relation message order
	keyIdx     []int          // indexes into columns, in key order
	descriptor string
	keySchema  *Schema
}

type tableColumn struct {
	*column
	notNull bool
	// defaultValue is the decoded default, or nil; fill is what a required
	// column holds when an old tuple does not carry it (D15).
	defaultValue any
	hasDefault   bool
	// isKey marks a column of the record key.
	isKey bool
}

var topicInvalid = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

// sanitizeTopic replaces characters Kafka does not allow in a topic name
// with '_', as Debezium's topic naming strategy does.
func sanitizeTopic(s string) string {
	return topicInvalid.ReplaceAllString(s, "_")
}

// newTable builds the shape. Columns the catalog no longer has (a replayed
// Relation message from before a DROP COLUMN) are optional with no default;
// a column whose type differs between the catalog and the Relation message
// is a race with a DDL (CodeCatalogSkew), which the caller retries.
func newTable(rel *pglogrepl.RelationMessage, cat catalogTable, types map[uint32]*PGType, s *Settings) (*table, error) {
	t := &table{relID: rel.RelationID, schemaName: rel.Namespace, tableName: rel.RelationName}
	t.topic = sanitizeTopic(s.TopicPrefix + s.TopicDelimiter + rel.Namespace + s.TopicDelimiter + rel.RelationName)
	byName := map[string]catalogColumn{}
	for _, c := range cat.columns {
		byName[c.name] = c
	}
	value := &Schema{Type: typeStruct, Optional: true, Name: t.topic + ".Value"}
	for _, rc := range rel.Columns {
		cc, inCatalog := byName[rc.Name]
		if inCatalog && cc.typeOID != rc.DataType {
			return nil, &Error{Code: CodeCatalogSkew,
				Message: fmt.Sprintf("table %s.%s column %q: the catalog has type %d, the change stream %d",
					rel.Namespace, rel.RelationName, rc.Name, cc.typeOID, rc.DataType),
				Fix: "A DDL changed the column while the connector read the catalog; the read is retried, and a restart resolves it."}
		}
		pt, ok := types[rc.DataType]
		if !ok {
			return nil, unsupportedType(rc.Name, fmt.Sprintf("oid %d", rc.DataType))
		}
		col, err := newColumn(rc.Name, pt, rc.TypeModifier, columnOptions{moneyFractionDigits: s.MoneyFractionDigits})
		if err != nil {
			return nil, fmt.Errorf("table %s.%s: %w", rel.Namespace, rel.RelationName, err)
		}
		tc := &tableColumn{column: col}
		sch := col.schema.clone()
		if inCatalog {
			tc.notNull = cc.notNull
			sch.Default = col.schemaDefault(cc.defaultX)
			if sch.Default != nil {
				tc.hasDefault = true
				tc.defaultValue = defaultGoValue(col, cc.defaultX)
			}
		}
		sch.Optional = !tc.notNull
		col.schema = sch
		t.columns = append(t.columns, tc)
		value.Fields = append(value.Fields, Field{Name: rc.Name, Schema: sch})
	}
	idx := map[string]int{}
	for i, c := range t.columns {
		idx[c.name] = i
	}
	if len(cat.keyColumns) > 0 {
		key := &Schema{Type: typeStruct, Name: t.topic + ".Key"}
		for _, k := range cat.keyColumns {
			i, ok := idx[k]
			if !ok {
				return nil, &Error{Code: CodeCatalogSkew,
					Message: fmt.Sprintf("table %s.%s: key column %q is not in the change stream's shape", rel.Namespace, rel.RelationName, k),
					Fix:     "A DDL changed the primary key; the read is retried, and a restart resolves it."}
			}
			ks := t.columns[i].schema.clone()
			ks.Optional, ks.Default = false, nil
			key.Fields = append(key.Fields, Field{Name: k, Schema: ks})
			t.keyIdx = append(t.keyIdx, i)
			t.columns[i].isKey = true
		}
		t.keySchema = key
	}
	t.descriptor = MarshalDescriptor(t.keySchema, value)
	return t, nil
}

// defaultGoValue is the record value of a column default, used to fill a
// required column an old tuple does not carry (D15).
func defaultGoValue(c *column, expr string) any {
	if nowFunctions[expr] {
		switch c.kind {
		case kindDate:
			return int32(0)
		case kindTime:
			return int32(0)
		case kindMicroTime, kindTimestamp, kindMicroTimestamp:
			return int64(0)
		case kindTimestampTZ:
			return epochZoned
		}
		return nil
	}
	text := expr
	if m := castLiteral.FindStringSubmatch(expr); m != nil {
		text = strings.ReplaceAll(m[1], "''", "'")
	}
	if c.kind == kindBool && (text == boolTrue || text == boolFalse) {
		text = text[:1]
	}
	v, err := c.decode(text)
	if err != nil {
		return nil
	}
	return v
}

// zeroValue is what Debezium writes into a required column of a before
// image that Postgres did not send (REPLICA IDENTITY DEFAULT deletes and
// key changes, D15): the column default when it has a literal one, else the
// type's zero (golden: "" for text, 0 for integers and timestamps).
func (c *tableColumn) zeroValue() any {
	if c.hasDefault && c.defaultValue != nil {
		return c.defaultValue
	}
	switch c.kind {
	case kindInt16:
		return int16(0)
	case kindInt32, kindDate, kindTime:
		return int32(0)
	case kindInt64, kindOID, kindMicroTime, kindTimestamp, kindMicroTimestamp, kindInterval:
		return int64(0)
	case kindFloat32:
		return float32(0)
	case kindFloat64:
		return float64(0)
	case kindBool:
		return false
	case kindString, kindXML, kindUUID, kindJSON, kindEnum, kindHstore:
		return ""
	case kindTimeTZ:
		return "00:00:00Z"
	case kindTimestampTZ:
		return epochZoned
	case kindDecimal, kindVariableDecimal, kindMoney:
		return "0"
	case kindBytes:
		return []byte{}
	case kindArray:
		return []any{}
	}
	return nil
}

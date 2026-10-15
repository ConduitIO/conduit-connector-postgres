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
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jackc/pglogrepl"
)

// pgoutput tuple column kinds.
const (
	tupleNull      = 'n'
	tupleUnchanged = 'u'
	tupleText      = 't'
)

// row decodes a new tuple into a value for every column of the shape: nil
// for NULL, the placeholder for an unchanged TOAST value, or the backfilled
// value when full holds the whole old row (REPLICA IDENTITY FULL).
func (t *table) row(tup *pglogrepl.TupleData, full map[string]any, placeholder string) (map[string]any, error) {
	if tup == nil {
		return nil, fmt.Errorf("debezium mode: table %s.%s: change without a tuple", t.schemaName, t.tableName)
	}
	if len(tup.Columns) != len(t.columns) {
		return nil, t.columnCountSkew(len(tup.Columns))
	}
	out := make(map[string]any, len(t.columns))
	for i, tc := range t.columns {
		d := tup.Columns[i]
		switch d.DataType {
		case tupleNull:
			out[tc.name] = nil
		case tupleUnchanged:
			if v, ok := full[tc.name]; ok {
				// Invariant 6: a copy, so After does not share the value with
				// Before (postgres#425).
				out[tc.name] = cloneValue(v)
				continue
			}
			v, err := tc.placeholderValue(placeholder)
			if err != nil {
				return nil, err
			}
			out[tc.name] = v
		case tupleText:
			v, err := tc.decode(string(d.Data))
			if err != nil {
				return nil, fmt.Errorf("table %s.%s: %w", t.schemaName, t.tableName, err)
			}
			out[tc.name] = v
		default:
			return nil, fmt.Errorf("debezium mode: table %s.%s column %q: tuple data type %q is not text", t.schemaName, t.tableName, tc.name, d.DataType)
		}
	}
	return out, nil
}

// oldRow decodes an old tuple. A full old row ('O', REPLICA IDENTITY FULL)
// decodes like a new tuple. A key-only old row ('K') carries the identity
// columns; every other column is NULL in it, and Debezium writes the
// default or the type's zero into the required ones (D15) and null into the
// optional ones.
func (t *table) oldRow(tup *pglogrepl.TupleData, kind uint8, placeholder string) (map[string]any, error) {
	if kind != pglogrepl.UpdateMessageTupleTypeKey {
		return t.row(tup, nil, placeholder)
	}
	if len(tup.Columns) != len(t.columns) {
		return nil, t.columnCountSkew(len(tup.Columns))
	}
	out := make(map[string]any, len(t.columns))
	for i, tc := range t.columns {
		d := tup.Columns[i]
		if d.DataType == tupleText {
			v, err := tc.decode(string(d.Data))
			if err != nil {
				return nil, fmt.Errorf("table %s.%s: %w", t.schemaName, t.tableName, err)
			}
			out[tc.name] = v
			continue
		}
		// A key column stays nil: the D15 fill is for the columns the
		// identity does not carry, and a filled key would turn a missing old
		// key into a delete for key 0 (requireKey catches the nil).
		if tc.notNull && !tc.isKey {
			out[tc.name] = tc.zeroValue()
		} else {
			out[tc.name] = nil
		}
	}
	return out, nil
}

func (t *table) columnCountSkew(got int) error {
	return &Error{Code: CodeCatalogSkew,
		Message: fmt.Sprintf("table %s.%s: the tuple has %d columns, the relation shape %d", t.schemaName, t.tableName, got, len(t.columns)),
		Fix:     "Restart the pipeline; if it recurs, report it with the table's DDL history."}
}

// key is the record key: the key columns of row in key order, or nil for a
// table without a key.
func (t *table) key(row map[string]any) opencdc.Data {
	if len(t.keyIdx) == 0 || row == nil {
		return nil
	}
	k := make(opencdc.StructuredData, len(t.keyIdx))
	for _, i := range t.keyIdx {
		name := t.columns[i].name
		// Invariant 6: the key owns its value; the row holds it too
		// (postgres#425).
		k[name] = cloneValue(row[name])
	}
	return k
}

// keyChanged reports whether an update's old tuple carries a key that
// differs from the new one, comparing the key columns' text as sent. It is
// the skipped-change form of the comparison Update makes on decoded values;
// for one type, equal text is equal decoded value and back.
func (t *table) keyChanged(oldTup, newTup *pglogrepl.TupleData) bool {
	if len(t.keyIdx) == 0 || oldTup == nil || newTup == nil || len(oldTup.Columns) != len(t.columns) || len(newTup.Columns) != len(t.columns) {
		return false
	}
	for _, i := range t.keyIdx {
		o, n := oldTup.Columns[i], newTup.Columns[i]
		if o.DataType != n.DataType || !bytes.Equal(o.Data, n.Data) {
			return true
		}
	}
	return false
}

// requireKey fails when a key column of the old key is null: the old tuple
// did not carry the key (the replica identity does not cover it).
func (t *table) requireKey(key opencdc.Data, lsn pglogrepl.LSN, what string) error {
	if len(t.keyIdx) == 0 {
		return nil
	}
	sd, _ := key.(opencdc.StructuredData)
	for _, i := range t.keyIdx {
		if v, ok := sd[t.columns[i].name]; !ok || v == nil {
			return t.keyUnavailable(lsn, what)
		}
	}
	return nil
}

func (t *table) keyUnavailable(lsn pglogrepl.LSN, what string) error {
	return &Error{Code: CodeDeleteKeyUnavailable,
		Message: fmt.Sprintf("table %s.%s: the %s at %s carries no old key (the replica identity does not cover the primary key)",
			t.schemaName, t.tableName, what, lsn),
		Fix: "Set REPLICA IDENTITY DEFAULT or FULL on the table. The changes before the fix cannot be keyed; recreate the pipeline to resnapshot the table."}
}

// sameKey compares two keys column by column in the decoded Debezium form,
// as Debezium compares its key structs.
func sameKey(t *table, a, b opencdc.Data) bool {
	as, _ := a.(opencdc.StructuredData)
	bs, _ := b.(opencdc.StructuredData)
	for _, i := range t.keyIdx {
		n := t.columns[i].name
		if !reflect.DeepEqual(as[n], bs[n]) {
			return false
		}
	}
	return true
}

// checkIdentity refuses a table whose replica identity is an index that
// does not contain every primary key column (D19): its old tuples cannot
// yield the record key, and a key change that leaves the index untouched
// would be missed.
func (t *table) checkIdentity(rel *pglogrepl.RelationMessage, cat catalogTable) error {
	if rel.ReplicaIdentity != 'i' || len(cat.keyColumns) == 0 {
		return nil
	}
	flagged := map[string]bool{}
	for _, c := range rel.Columns {
		if c.Flags&1 != 0 {
			flagged[c.Name] = true
		}
	}
	for _, k := range cat.keyColumns {
		if !flagged[k] {
			return &Error{Code: CodeReplicaIdentityIndexNotKey,
				Message: fmt.Sprintf("table %s.%s: REPLICA IDENTITY USING INDEX does not contain primary key column %q, so its old rows cannot carry the record key",
					t.schemaName, t.tableName, k),
				Fix: "Set REPLICA IDENTITY DEFAULT or FULL on the table, or use an identity index that contains the primary key."}
		}
	}
	return nil
}

// placeholderValue is the unchanged-TOAST placeholder in the column's form:
// the text for string types, its UTF-8 bytes for bytea, and a one-element
// list for arrays of those (golden: toasty).
func (c *column) placeholderValue(p string) (any, error) {
	switch c.kind { //nolint:exhaustive // the rest has no placeholder form
	case kindString, kindXML, kindJSON, kindHstore, kindEnum, kindUUID:
		return p, nil
	case kindBytes:
		return []byte(p), nil
	case kindArray:
		v, err := c.elem.placeholderValue(p)
		if err != nil {
			return nil, err
		}
		return []any{v}, nil
	}
	return nil, &Error{Code: CodeUnsupportedType,
		Message: fmt.Sprintf("column %q (%s) holds an unchanged TOAST value, which has no Debezium placeholder form for this type", c.name, c.typeName),
		Fix:     "Set REPLICA IDENTITY FULL on the table so unchanged values are sent, or run the table in the default output mode."}
}

// keyJSON renders a key as the schemaless JSON payload the encoder reads
// from debezium.old_key: fields in key order, decimals as JSON numbers
// (decimal.format=NUMERIC), bytes as base64.
func (t *table) keyJSON(key opencdc.StructuredData) (string, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for n, i := range t.keyIdx {
		c := t.columns[i]
		if n > 0 {
			b.WriteByte(',')
		}
		writeJSONString(&b, c.name)
		b.WriteByte(':')
		if err := writePayload(&b, c.column, key[c.name]); err != nil {
			return "", err
		}
	}
	b.WriteByte('}')
	return b.String(), nil
}

func writePayload(b *bytes.Buffer, c *column, v any) error {
	if v == nil {
		b.WriteString("null")
		return nil
	}
	switch x := v.(type) {
	case int16:
		b.WriteString(strconv.FormatInt(int64(x), 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(x), 10))
	case int64:
		b.WriteString(strconv.FormatInt(x, 10))
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return outOfRange(c, fmt.Sprint(x), "a key cannot hold a non-finite float")
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return outOfRange(c, fmt.Sprint(x), "a key cannot hold a non-finite float")
		}
		b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case []byte:
		writeJSONString(b, base64.StdEncoding.EncodeToString(x))
	case string:
		switch c.kind { //nolint:exhaustive // the other string-valued kinds are JSON strings
		case kindDecimal, kindMoney:
			b.WriteString(x)
		case kindVariableDecimal:
			return writeVariableScale(b, c, x)
		default:
			writeJSONString(b, x)
		}
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writePayload(b, c.elem, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	default:
		return fmt.Errorf("debezium mode: column %q: no JSON form for %T", c.name, v)
	}
	return nil
}

// writeVariableScale writes a VariableScaleDecimal payload: the scale and
// the unscaled value as big-endian two's-complement bytes, as Debezium's
// VariableScaleDecimal.fromLogical writes them.
func writeVariableScale(b *bytes.Buffer, c *column, text string) error {
	unscaled, scale, ok := unscale(text)
	if !ok {
		return badText(c, text, nil)
	}
	b.WriteString(`{"scale":`)
	b.WriteString(strconv.Itoa(scale))
	b.WriteString(`,"value":`)
	writeJSONString(b, base64.StdEncoding.EncodeToString(twosComplement(unscaled)))
	b.WriteByte('}')
	return nil
}

// unscale splits a plain decimal literal into its unscaled integer and scale
// ("-1.50" is -150, 2).
func unscale(text string) (*big.Int, int, bool) {
	s := text
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	intPart, frac, _ := strings.Cut(s, ".")
	n, ok := new(big.Int).SetString(intPart+frac, 10)
	if !ok {
		return nil, 0, false
	}
	if neg {
		n.Neg(n)
	}
	return n, len(frac), true
}

// twosComplement is Java's BigInteger.toByteArray.
func twosComplement(n *big.Int) []byte {
	if n.Sign() >= 0 {
		b := n.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	// -n - 1, inverted.
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

// cloneValue deep-copies the containers a decoded value can hold ([]byte,
// and []any for arrays); everything else is immutable.
func cloneValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return bytes.Clone(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = cloneValue(e)
		}
		return out
	}
	return v
}

// readTypeOnly reads one type into types, for a column the catalog no
// longer has.
func readTypeOnly(ctx context.Context, q Querier, oid uint32, types map[uint32]*PGType) error {
	tx, err := q.Begin(ctx)
	if err != nil {
		return fmt.Errorf("debezium mode: catalog read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = readType(ctx, tx, oid, types)
	return err
}

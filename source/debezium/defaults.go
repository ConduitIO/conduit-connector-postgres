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
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// epochZoned is the ZonedTimestamp of the epoch: the zero of timestamptz
// and the schema default of a now()-like default.
const epochZoned = "1970-01-01T00:00:00.000000Z"

const (
	boolTrue  = "true"
	boolFalse = "false"
)

// literalDefault matches pg_get_expr output for a constant: 'text'::type,
// or a bare number or boolean.
var (
	castLiteral = regexp.MustCompile(`^'((?:[^']|'')*)'::[A-Za-z_][A-Za-z0-9_ ."\[\]()]*$`)
	bareLiteral = regexp.MustCompile(`^(-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?|true|false)$`)
	// Functions Debezium maps to the epoch value of a temporal column
	// (golden: DEFAULT now() on timestamptz is "1970-01-01T00:00:00.000000Z").
	nowFunctions = map[string]bool{
		"now()": true, "CURRENT_TIMESTAMP": true, "CURRENT_DATE": true, "CURRENT_TIME": true,
		"LOCALTIMESTAMP": true, "LOCALTIME": true, "transaction_timestamp()": true,
		"statement_timestamp()": true, "clock_timestamp()": true,
	}
)

// schemaDefault returns the schema default for a column as a JSON literal in
// JsonConverter form with NUMERIC decimals, or nil when the column has no
// default Debezium propagates. Literal defaults map to their value;
// now()-like defaults of temporal columns map to the epoch value; every
// other expression (nextval, other functions) has no schema default.
func (c *column) schemaDefault(expr string) json.RawMessage {
	if expr == "" {
		return nil
	}
	if nowFunctions[expr] {
		return c.epochDefault()
	}
	var text string
	if m := castLiteral.FindStringSubmatch(expr); m != nil {
		text = strings.ReplaceAll(m[1], "''", "'")
	} else if bareLiteral.MatchString(expr) {
		text = expr
		if c.kind == kindBool {
			text = map[string]string{boolTrue: "t", boolFalse: "f"}[expr]
		}
	} else {
		return nil
	}
	if c.kind == kindBool && (text == boolTrue || text == boolFalse) {
		text = text[:1]
	}
	v, err := c.decode(text)
	if err != nil {
		return nil
	}
	return jsonLiteral(c, v)
}

func (c *column) epochDefault() json.RawMessage {
	switch c.kind {
	case kindDate, kindTime, kindMicroTime, kindTimestamp, kindMicroTimestamp:
		return json.RawMessage("0")
	case kindTimestampTZ:
		return json.RawMessage(strconv.Quote(epochZoned))
	}
	return nil
}

// jsonLiteral renders a decoded value as a schema default. Kinds whose
// default form is not settled by the goldens have none (V).
func jsonLiteral(c *column, v any) json.RawMessage {
	switch c.kind {
	case kindInt16, kindInt32, kindInt64, kindOID, kindDate, kindTime, kindMicroTime, kindTimestamp,
		kindMicroTimestamp, kindInterval:
		return json.RawMessage(strconv.FormatInt(toInt64(v), 10))
	case kindBool:
		return json.RawMessage(strconv.FormatBool(v.(bool)))
	case kindString, kindXML, kindUUID, kindJSON, kindEnum, kindTimestampTZ, kindTimeTZ, kindHstore:
		b, _ := json.Marshal(v.(string))
		return b
	case kindDecimal, kindMoney:
		return json.RawMessage(v.(string))
	case kindBytes:
		b, _ := json.Marshal(base64.StdEncoding.EncodeToString(v.([]byte)))
		return b
	case kindFloat32:
		return json.RawMessage(strconv.FormatFloat(float64(v.(float32)), 'g', -1, 32))
	case kindFloat64:
		return json.RawMessage(strconv.FormatFloat(v.(float64), 'g', -1, 64))
	}
	return nil
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

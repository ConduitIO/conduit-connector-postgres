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

package types

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// ErrorCodeTemporalNotRepresentable prefixes the error returned when a date,
// timestamp or timestamptz is 'infinity' or '-infinity' and has to be emitted
// under an Avro schema, whose date and timestamp types cannot hold it.
const ErrorCodeTemporalNotRepresentable = "postgres.source.temporal_not_representable"

// TemporalNotRepresentableError reports an infinite date or timestamp that
// Avro cannot represent. Type is the Postgres type name and Value is the
// canonical text ("infinity" or "-infinity"). Table and Column are filled in
// by the caller, see Locate.
//
// Before this error existed, pgx's InfinityModifier (an int8) went into the
// Avro encoder as the integer 1 or -1, which is 1970-01-02 / 1969-12-31 for a
// date: silent corruption (Invariant 6).
type TemporalNotRepresentableError struct {
	Table  string
	Column string
	Type   string
	Value  string
}

func (e *TemporalNotRepresentableError) Error() string {
	where := ""
	switch {
	case e.Table != "" && e.Column != "":
		where = fmt.Sprintf(" in column %q of table %q", e.Column, e.Table)
	case e.Column != "":
		where = fmt.Sprintf(" in column %q", e.Column)
	}
	return fmt.Sprintf("%s: %s value %q%s cannot be encoded under an Avro schema, "+
		"so the source halts instead of changing it. Fix: set logrepl.withAvroSchema to false "+
		"to receive the strings \"infinity\" and \"-infinity\". Removing the row from the table "+
		"does not help once the change is in the WAL",
		ErrorCodeTemporalNotRepresentable, e.Type, e.Value, where)
}

const (
	typeDate        = "date"
	typeTimestamp   = "timestamp"
	typeTimestamptz = "timestamptz"

	infinityText    = "infinity"
	negInfinityText = "-infinity"
)

var temporalTypeNames = map[uint32]string{
	pgtype.DateOID:        typeDate,
	pgtype.TimestampOID:   typeTimestamp,
	pgtype.TimestamptzOID: typeTimestamptz,
}

// formatInfinity maps pgx's InfinityModifier for a date or timestamp column.
func formatInfinity(enc Encoding, typ string, m pgtype.InfinityModifier) (any, error) {
	var text string
	switch m {
	case pgtype.Infinity:
		text = infinityText
	case pgtype.NegativeInfinity:
		text = negInfinityText
	default:
		return nil, fmt.Errorf("unexpected %s infinity modifier %d", typ, int8(m))
	}
	if enc == EncodingAvro {
		return nil, &TemporalNotRepresentableError{Type: typ, Value: text}
	}
	return text, nil
}

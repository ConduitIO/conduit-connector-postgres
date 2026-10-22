// Copyright © 2024 Meroxa, Inc.
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
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgtype"
)

// ErrorCodeNumericNotRepresentable prefixes the error returned when a numeric
// value (NaN, Infinity, -Infinity) has to be emitted under an Avro schema,
// whose decimal type cannot hold it. Like the other connector codes it is
// embedded at the start of the message string.
const ErrorCodeNumericNotRepresentable = "postgres.source.numeric_not_representable"

// Canonical Postgres text forms of the numeric values that are not finite
// numbers. They are what the source emits for them when the records carry no
// Avro schema.
const (
	numericNaN         = "NaN"
	numericInfinity    = "Infinity"
	numericNegInfinity = "-Infinity"
)

// NumericNotRepresentableError reports a numeric value that the Avro decimal
// logical type cannot represent. Value is the canonical Postgres text form
// ("NaN", "Infinity" or "-Infinity"). Table and Column are filled in by the
// caller that knows them, see LocateNumeric; Row identifies the row by its
// primary key, see WithRow.
type NumericNotRepresentableError struct {
	Table  string
	Column string
	Value  string
	Row    string
}

func (e *NumericNotRepresentableError) Error() string {
	where := ""
	switch {
	case e.Table != "" && e.Column != "":
		where = fmt.Sprintf(" in column %q of table %q", e.Column, e.Table)
	case e.Column != "":
		where = fmt.Sprintf(" in column %q", e.Column)
	}
	if e.Row != "" {
		where += fmt.Sprintf(" (row %s)", e.Row)
	}
	// The advice must work for a change that is already in the WAL: removing
	// the row, a CHECK constraint or a view only help for rows that are not
	// there yet, and dropping the slot loses the position.
	return fmt.Sprintf("%s: numeric value %q%s cannot be encoded as an Avro decimal, "+
		"so the source halts instead of changing it. Fix: restart once with "+
		"logrepl.withAvroSchema=false from the same position, which delivers the value as the "+
		"string %q, and set it back to true as soon as that record has been delivered: without Avro a "+
		"finite numeric is a Go big.Rat, which a standalone connector cannot send to Conduit (#378), "+
		"so the next finite numeric would stop the pipeline (built-in pipelines are not affected). "+
		"On a standalone connector the SDK also fails the whole read batch on the first record it cannot "+
		"send, so if the record or its batch contains any finite numeric it is never delivered with Avro "+
		"off and this workaround does not help (#378). Until then JSON-serializing destinations receive "+
		"a finite numeric as the string \"3/2\". "+
		"For a row that is only in the snapshot, UPDATE it to a finite value and restart. Removing "+
		"the row does not help once the change is in the WAL, and do not drop the replication slot: "+
		"that loses the position",
		ErrorCodeNumericNotRepresentable, e.Value, where, e.Value)
}

// WithRow returns a copy of err with Row set when err is or wraps a
// *NumericNotRepresentableError, and err itself otherwise.
func WithRow(err error, row string) error {
	var nre *NumericNotRepresentableError
	if row == "" || !errors.As(err, &nre) {
		return err
	}
	c := *nre
	c.Row = row
	return &c
}

// LocateNumeric returns a copy of err with its table and column set, and true,
// when err is or wraps a *NumericNotRepresentableError. The copy is returned
// unwrapped so the error code stays at the start of the message. For any other
// error it returns err and false.
func LocateNumeric(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	var nre *NumericNotRepresentableError
	if !errors.As(err, &nre) {
		return err, false
	}
	located := *nre
	located.Table, located.Column = table, column
	return &located, true
}

// LocateError applies LocateTemporal and LocateNumeric: it is what a call site
// that formats a column uses to name the table and column in a coded error.
func LocateError(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	if located, ok := LocateTemporal(err, table, column); ok {
		return located, true
	}
	return LocateNumeric(err, table, column)
}

// LocateErrorSnapshot is LocateError for a value read by the snapshot, whose
// temporal error advice differs (the row can be fixed).
func LocateErrorSnapshot(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	if located, ok := LocateTemporalSnapshot(err, table, column); ok {
		return located, true
	}
	return LocateNumeric(err, table, column)
}

type NumericFormatter struct{}

// BigRatFromNumeric converts a finite pgtype.Numeric to a big.Rat. It never
// returns a nil *big.Rat without an error: NULL, NaN and +/-Infinity have no
// rational value, and a typed nil handed on as a record value is not a NULL
// (it passes `v == nil` checks and crashes the Avro encoder).
func (NumericFormatter) BigRatFromNumeric(num pgtype.Numeric) (*big.Rat, error) {
	if !num.Valid {
		return nil, errors.New("numeric is NULL, it has no rational value")
	}
	if text, ok := nonFinite(num); ok {
		return nil, &NumericNotRepresentableError{Value: text}
	}
	if num.Int == nil {
		return nil, errors.New("numeric has neither digits nor a special value")
	}
	driverVal, err := num.Value()
	if err != nil {
		return nil, err
	}
	text, ok := driverVal.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected %T for a numeric value", driverVal)
	}
	v, ok := new(big.Rat).SetString(text)
	if !ok {
		return nil, fmt.Errorf("numeric value (%d bytes) is not a valid number", len(text))
	}
	return v, nil
}

// nonFinite returns the canonical text of num if it is NaN or an infinity.
func nonFinite(num pgtype.Numeric) (string, bool) {
	switch {
	case num.NaN:
		return numericNaN, true
	case num.InfinityModifier == pgtype.Infinity:
		return numericInfinity, true
	case num.InfinityModifier == pgtype.NegativeInfinity:
		return numericNegInfinity, true
	}
	return "", false
}

// formatNumeric maps a numeric to its record value.
//
// Invariant 6: NaN and +/-Infinity are never turned into NULL or a number.
// Under Avro they halt with NumericNotRepresentableError; otherwise they are
// emitted as their canonical text. Only a real NULL is the untyped nil.
func formatNumeric(enc Encoding, num pgtype.Numeric) (any, error) {
	if !num.Valid {
		return nil, nil //nolint:nilnil // NULL is an untyped nil record value
	}
	if text, ok := nonFinite(num); ok {
		if enc == EncodingAvro {
			return nil, &NumericNotRepresentableError{Value: text}
		}
		return text, nil
	}
	r, err := Numeric.BigRatFromNumeric(num)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// formatNumericArray applies formatNumeric's rule for non-finite values to the
// elements of a numeric[]. Finite and NULL elements are returned exactly as pgx
// decoded them (a pgtype.Numeric, not a *big.Rat): that is what the source
// emitted before, and changing it would change the output of every finite
// numeric[] column.
func formatNumericArray(enc Encoding, v any) (any, error) {
	if _, ok := v.([]any); !ok {
		return v, nil // nil or a shape pgx does not produce for numeric[]: leave it alone
	}
	return mapArray(v, "numeric[]", func(e any) (any, error) {
		n, ok := e.(pgtype.Numeric)
		if !ok {
			return e, nil
		}
		if _, nonFin := nonFinite(n); !nonFin {
			return e, nil
		}
		return formatNumeric(enc, n)
	})
}

// FormatColumn is FormatFor for a column whose type modifier is known. It
// differs from FormatFor in one case: under EncodingAvro an unconstrained
// numeric (type modifier -1) is its exact decimal text, because that column's
// Avro schema is a string (see source/schema). Every other column is FormatFor.
func FormatColumn(enc Encoding, oid uint32, typeMod int32, v any) (any, error) {
	if enc == EncodingAvro && oid == pgtype.NumericOID && typeMod == -1 {
		return formatNumericText(v)
	}
	return FormatFor(enc, oid, v)
}

// formatNumericText returns the exact text Postgres uses for a numeric value:
// the digits as stored (scale included, 1.50 stays "1.50"), or "NaN",
// "Infinity" or "-Infinity". Nothing is rounded, and, unlike a big.Rat, nothing
// is lost to a repeating expansion. NULL is the untyped nil.
func formatNumericText(v any) (any, error) {
	var n pgtype.Numeric
	switch t := v.(type) {
	case nil:
		return nil, nil //nolint:nilnil // NULL is an untyped nil record value
	case pgtype.Numeric:
		n = t
	case *pgtype.Numeric:
		if t == nil {
			return nil, nil //nolint:nilnil // a nil pointer from pgx is NULL
		}
		n = *t
	default:
		return v, nil
	}
	if !n.Valid {
		return nil, nil //nolint:nilnil // NULL is an untyped nil record value
	}
	if text, ok := nonFinite(n); ok {
		return text, nil
	}
	if n.Int == nil {
		return nil, errors.New("numeric has neither digits nor a special value")
	}
	driverVal, err := n.Value()
	if err != nil {
		return nil, err
	}
	text, ok := driverVal.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected %T for a numeric value", driverVal)
	}
	return text, nil
}

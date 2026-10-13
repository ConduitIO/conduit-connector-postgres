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
// caller that knows them, see Locate.
type NumericNotRepresentableError struct {
	Table  string
	Column string
	Value  string
}

func (e *NumericNotRepresentableError) Error() string {
	where := ""
	switch {
	case e.Table != "" && e.Column != "":
		where = fmt.Sprintf(" in column %q of table %q", e.Column, e.Table)
	case e.Column != "":
		where = fmt.Sprintf(" in column %q", e.Column)
	}
	return fmt.Sprintf("%s: numeric value %q%s cannot be encoded as an Avro decimal, "+
		"so the source halts instead of changing it. Fix: constrain the column "+
		"(CHECK (%s NOT IN ('NaN', 'Infinity', '-Infinity'))), cast it to a value you choose "+
		"in a view or generated column, or set logrepl.withAvroSchema to false to receive "+
		"the strings \"NaN\", \"Infinity\" and \"-Infinity\"",
		ErrorCodeNumericNotRepresentable, e.Value, where, checkColumn(e.Column))
}

func checkColumn(c string) string {
	if c == "" {
		return "<column>"
	}
	return pgIdent(c)
}

// pgIdent quotes an identifier for use in the suggested SQL.
func pgIdent(s string) string {
	out := []byte{'"'}
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"')
		}
		out = append(out, s[i])
	}
	return string(append(out, '"'))
}

// Locate returns a copy of err with its table and column set, and true, when
// err is or wraps a *NumericNotRepresentableError or a
// *TemporalNotRepresentableError. The copy is returned unwrapped so the error
// code stays at the start of the message. For any other error it returns err
// and false.
func Locate(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	var nre *NumericNotRepresentableError
	if errors.As(err, &nre) {
		located := *nre
		located.Table, located.Column = table, column
		return &located, true
	}
	var tre *TemporalNotRepresentableError
	if errors.As(err, &tre) {
		located := *tre
		located.Table, located.Column = table, column
		return &located, true
	}
	return err, false
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
		return nil, fmt.Errorf("numeric %q is not a valid number", text)
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

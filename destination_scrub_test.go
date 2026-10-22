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

package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// The server and pgx quote the row's value in their messages. Errors reach the
// DLQ, the API and CLI output: the SQLSTATE and the names stay, the text goes.
func TestScrubExecError(t *testing.T) {
	for name, pgErr := range map[string]*pgconn.PgError{
		"22P02 invalid input": {Code: "22P02", Message: `invalid input syntax for type integer: "customer-SECRET-4711"`, ColumnName: "qty", TableName: "orders"},
		"22007 datetime":      {Code: "22007", Message: `invalid input syntax for type timestamp: "customer-SECRET-4711"`},
		"22008 overflow":      {Code: "22008", Message: `date/time field value out of range: "customer-SECRET-4711"`},
		"23505 unique":        {Code: "23505", Message: "duplicate key value violates unique constraint", Detail: "Key (email)=(customer-SECRET-4711) already exists.", ConstraintName: "orders_email_key"},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			err := scrubExecError(pgErr)
			is.True(strings.HasPrefix(err.Error(), "SQLSTATE "+pgErr.Code))
			is.True(!strings.Contains(err.Error(), "SECRET"))
			var got *pgconn.PgError
			is.True(errors.As(err, &got)) // the chain is kept
			is.Equal(got.Code, pgErr.Code)
		})
	}

	t.Run("names are kept", func(t *testing.T) {
		is := is.New(t)
		err := scrubExecError(&pgconn.PgError{Code: "22P02", Message: "x", ColumnName: "qty", TableName: "orders"})
		is.True(strings.Contains(err.Error(), `column "qty"`))
		is.True(strings.Contains(err.Error(), `table "orders"`))
	})
	t.Run("other server errors keep their text", func(t *testing.T) {
		is := is.New(t)
		err := &pgconn.PgError{Code: "42P01", Message: `relation "orders" does not exist`}
		is.Equal(scrubExecError(err), error(err))
		is.Equal(scrubExecError(nil), nil)
	})
	t.Run("pgx encode error", func(t *testing.T) {
		is := is.New(t)
		_, encErr := pgtype.NewMap().Encode(pgtype.Int8OID, pgtype.BinaryFormatCode, "customer-SECRET-4711", nil)
		is.True(encErr != nil)
		is.True(strings.Contains(encErr.Error(), "SECRET")) // the premise: pgx prints the value
		is.True(!strings.Contains(scrubExecError(encErr).Error(), "SECRET"))
	})
}

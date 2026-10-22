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
	"context"
	"errors"
	"fmt"
	"net"
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
		"23514 check":         {Code: "23514", Message: "new row for relation violates check constraint", Detail: "Failing row contains (1, customer-SECRET-4711).", ConstraintName: "orders_chk"},
		"P0001 trigger raise": {Code: "P0001", Message: "bad customer-SECRET-4711", Hint: "customer-SECRET-4711", Where: "PL/pgSQL function f() line 3 customer-SECRET-4711"},
		"42703 column":        {Code: "42703", Message: `column "customer-SECRET-4711" does not exist`},
		"XX000 unlisted":      {Code: "XX000", Message: "internal error customer-SECRET-4711", InternalQuery: "SELECT 'customer-SECRET-4711'"},
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
	t.Run("classification survives", func(t *testing.T) {
		is := is.New(t)
		err := scrubExecError(&pgconn.PgError{Code: "42P01", Message: `relation "orders" does not exist`})
		is.True(!strings.Contains(err.Error(), "relation"))
		is.True(strings.HasPrefix(err.Error(), "SQLSTATE 42P01"))
		var got *pgconn.PgError
		is.True(errors.As(err, &got))
		is.Equal(scrubExecError(nil), nil)
	})
	t.Run("errors without row values are kept", func(t *testing.T) {
		is := is.New(t)
		err := context.DeadlineExceeded
		is.Equal(scrubExecError(err), err)
		var ne net.Error = &net.OpError{Op: "read", Err: errors.New("connection reset")}
		is.Equal(scrubExecError(fmt.Errorf("send: %w", ne)).Error(), fmt.Sprintf("send: %v", ne))
	})
	t.Run("batch preprocessing error", func(t *testing.T) {
		is := is.New(t)
		_, encErr := pgtype.NewMap().Encode(pgtype.Int8OID, pgtype.BinaryFormatCode, "customer-SECRET-4711", nil)
		err := scrubExecError(fmt.Errorf("wrap: %w", encErr))
		is.True(!strings.Contains(err.Error(), "SECRET"))
	})
	t.Run("withholdEncodeError keeps nothing of the cause", func(t *testing.T) {
		is := is.New(t)
		err := withholdEncodeError(errors.New("cannot find encode plan for customer-SECRET-4711"))
		is.Equal(err.Error(), withheldEncodeMessage)
	})
	t.Run("pgx encode error", func(t *testing.T) {
		is := is.New(t)
		_, encErr := pgtype.NewMap().Encode(pgtype.Int8OID, pgtype.BinaryFormatCode, "customer-SECRET-4711", nil)
		is.True(encErr != nil)
		is.True(strings.Contains(encErr.Error(), "SECRET")) // the premise: pgx prints the value
		is.True(!strings.Contains(scrubExecError(encErr).Error(), "SECRET"))
	})
}

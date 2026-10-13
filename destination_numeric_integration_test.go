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
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// openNumericDestination opens a destination whose connection uses the given
// search_path ("" leaves the server default).
func openNumericDestination(ctx context.Context, t *testing.T, searchPath string) sdk.Destination {
	t.Helper()
	is := is.New(t)

	url := test.RegularConnString
	if searchPath != "" {
		url += "&search_path=" + searchPath
	}

	d := NewDestination()
	err := sdk.Util.ParseConfig(
		ctx,
		map[string]string{
			"url":   url,
			"table": "{{ index .Metadata \"opencdc.collection\" }}",
		},
		d.Config(),
		Connector.NewSpecification().DestinationParams,
	)
	is.NoErr(err)
	is.NoErr(d.Open(ctx))
	t.Cleanup(func() { is.NoErr(d.Teardown(ctx)) })
	return d
}

func writeAmount(ctx context.Context, d sdk.Destination, table string, id int, amt *big.Rat) error {
	_, err := d.Write(ctx, []opencdc.Record{{
		Position:  opencdc.Position("p"),
		Operation: opencdc.OperationCreate,
		Metadata:  map[string]string{opencdc.MetadataCollection: table},
		Key:       opencdc.StructuredData{"id": id},
		Payload:   opencdc.Change{After: opencdc.StructuredData{"amt": amt}},
	}})
	return err
}

func amountText(ctx context.Context, t *testing.T, conn *pgx.Conn, qualifiedTable string, id int) string {
	t.Helper()
	var got string
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT amt::text FROM %s WHERE id = $1", qualifiedTable), id).Scan(&got)
	is.New(t).NoErr(err)
	return got
}

// TestDestination_NumericScale_AlterColumn is the regression test for the
// stale scale cache: after ALTER COLUMN ... TYPE numeric(12,6) a running
// destination must store 1/3 as 0.333333, not the 0.33 it stored before.
//
// Invariant 6: schema handling never silently mangles data.
func TestDestination_NumericScale_AlterColumn(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := strings.ToLower(test.RandomIdentifier(t))
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int PRIMARY KEY, amt numeric(12,2))`, table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table)) })

	d := openNumericDestination(ctx, t, "")
	third := big.NewRat(1, 3)

	is.NoErr(writeAmount(ctx, d, table, 1, third))
	is.Equal(amountText(ctx, t, conn, fmt.Sprintf("%q", table), 1), "0.33")

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ALTER COLUMN amt TYPE numeric(12,6)`, table))
	is.NoErr(err)

	// Same destination instance, same process: no restart.
	is.NoErr(writeAmount(ctx, d, table, 2, third))
	is.Equal(amountText(ctx, t, conn, fmt.Sprintf("%q", table), 2), "0.333333")
}

// TestDestination_NumericScale_SameTableTwoSchemas: two tables with the same
// name and different scales in different schemas. Each destination resolves
// the table through its own search_path and must use that table's scale.
func TestDestination_NumericScale_SameTableTwoSchemas(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	base := strings.ToLower(test.RandomIdentifier(t))
	schemaA, schemaB := base+"_a", base+"_b"
	for schema, scale := range map[string]int{schemaA: 2, schemaB: 6} {
		_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %q`, schema))
		is.NoErr(err)
		t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema)) })
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q.t (id int PRIMARY KEY, amt numeric(12,%d))`, schema, scale))
		is.NoErr(err)
	}

	// Both orders, so the result cannot depend on which row a schema-blind
	// catalog query happens to return first.
	dA := openNumericDestination(ctx, t, schemaA)
	dB := openNumericDestination(ctx, t, schemaB)
	third := big.NewRat(1, 3)

	is.NoErr(writeAmount(ctx, dA, "t", 1, third))
	is.NoErr(writeAmount(ctx, dB, "t", 1, third))

	is.Equal(amountText(ctx, t, conn, fmt.Sprintf("%q.t", schemaA), 1), "0.33")
	is.Equal(amountText(ctx, t, conn, fmt.Sprintf("%q.t", schemaB), 1), "0.333333")
}

// TestDestination_NumericScale_Reduction: after the scale is reduced, the
// stored value must equal what Postgres itself stores for the exact value.
// 0.0049999 is chosen because rounding to the old scale first (0.005000) and
// then to the new one gives 0.01, while Postgres rounding the exact value
// gives 0.00. A second case checks overflow is an error, never a truncation.
func TestDestination_NumericScale_Reduction(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := strings.ToLower(test.RandomIdentifier(t))
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int PRIMARY KEY, amt numeric(12,6))`, table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table)) })

	d := openNumericDestination(ctx, t, "")
	exact := big.NewRat(49999, 10000000) // 0.0049999

	is.NoErr(writeAmount(ctx, d, table, 1, exact)) // warms any scale cache at 6
	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ALTER COLUMN amt TYPE numeric(12,2)`, table))
	is.NoErr(err)
	is.NoErr(writeAmount(ctx, d, table, 2, exact))

	var want string
	is.NoErr(conn.QueryRow(ctx, `SELECT '0.0049999'::numeric(12,2)::text`).Scan(&want)) // Postgres is the oracle
	is.Equal(want, "0.00")
	is.Equal(amountText(ctx, t, conn, fmt.Sprintf("%q", table), 2), want)

	// Precision overflow: Postgres errors, so must the write (and it must not ack).
	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ALTER COLUMN amt TYPE numeric(4,2)`, table))
	is.NoErr(err)
	err = writeAmount(ctx, d, table, 3, big.NewRat(123456, 100))
	is.True(err != nil)
	test.IsPgError(is, err, "22003") // numeric_value_out_of_range
}

// TestDestination_NumericScale_PostgresIsTheOracle writes values on and around
// rounding boundaries into constrained and unconstrained columns and requires
// the stored text to equal what Postgres stores for the same exact literal.
// An unconstrained NUMERIC used to be treated as scale 0 (5/2 was stored as 3).
func TestDestination_NumericScale_PostgresIsTheOracle(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := strings.ToLower(test.RandomIdentifier(t))
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %q (id int PRIMARY KEY, s0 numeric(12,0), s2 numeric(12,2), s6 numeric(12,6), unc numeric)`, table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table)) })

	d := openNumericDestination(ctx, t, "")
	vals := []struct {
		lit string
		rat *big.Rat
	}{
		{"2.5", big.NewRat(5, 2)},
		{"-2.5", big.NewRat(-5, 2)},
		{"0.005", big.NewRat(5, 1000)},
		{"-0.005", big.NewRat(-5, 1000)},
		{"0.0049999", big.NewRat(49999, 10000000)},
		{"0.0000005", big.NewRat(5, 10000000)},
		{"123456.789", big.NewRat(123456789, 1000)},
	}
	for i, v := range vals {
		id := i + 1
		_, err := d.Write(ctx, []opencdc.Record{{
			Position:  opencdc.Position("p"),
			Operation: opencdc.OperationCreate,
			Metadata:  map[string]string{opencdc.MetadataCollection: table},
			Key:       opencdc.StructuredData{"id": id},
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"s0": v.rat, "s2": v.rat, "s6": v.rat, "unc": v.rat,
			}},
		}})
		is.NoErr(err)

		var s0, s2, s6, unc, w0, w2, w6 string
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT s0::text, s2::text, s6::text, unc::text FROM %q WHERE id=$1`, table), id).
			Scan(&s0, &s2, &s6, &unc))
		is.NoErr(conn.QueryRow(ctx, `SELECT $1::numeric(12,0)::text, $1::numeric(12,2)::text, $1::numeric(12,6)::text`, v.lit).
			Scan(&w0, &w2, &w6))
		is.Equal(s0, w0)
		is.Equal(s2, w2)
		is.Equal(s6, w6)
		is.Equal(unc, v.lit) // unconstrained keeps the exact value
	}
}

// TestDestination_NumericScale_TooManyDigits: a rational that terminates but
// needs more than Postgres's 16383 fractional digits (1/2^20000) must not be
// sent in full, because Postgres rejects it with 22003 on every numeric column.
// The value is placed just above a rounding boundary so a wrong truncation
// would round the other way. 1/2^16383 is the longest expansion Postgres
// accepts and must still arrive exactly.
func TestDestination_NumericScale_TooManyDigits(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := strings.ToLower(test.RandomIdentifier(t))
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int PRIMARY KEY, s6 numeric(12,6), unc numeric)`, table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table)) })

	d := openNumericDestination(ctx, t, "")
	write := func(id int, v *big.Rat) error {
		_, err := d.Write(ctx, []opencdc.Record{{
			Position:  opencdc.Position("p"),
			Operation: opencdc.OperationCreate,
			Metadata:  map[string]string{opencdc.MetadataCollection: table},
			Key:       opencdc.StructuredData{"id": id},
			Payload:   opencdc.Change{After: opencdc.StructuredData{"s6": v, "unc": v}},
		}})
		return err
	}
	pow2 := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }

	// 0.0000005 + 2^-20000: 20000 fractional digits, just above the numeric(12,6)
	// rounding boundary, so Postgres must round it up to 0.000001.
	above := new(big.Rat).Add(big.NewRat(5, 10000000), new(big.Rat).SetFrac(big.NewInt(1), pow2(20000)))
	is.NoErr(write(1, above))
	var s6, unc string
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT s6::text, unc::text FROM %q WHERE id=1`, table)).Scan(&s6, &unc))
	is.Equal(s6, "0.000001")
	is.True(strings.HasPrefix(unc, "0.0000005000000")) // truncated, not rejected

	// Just below the boundary: must round down.
	below := new(big.Rat).Sub(big.NewRat(5, 10000000), new(big.Rat).SetFrac(big.NewInt(1), pow2(20000)))
	is.NoErr(write(2, below))
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT s6::text FROM %q WHERE id=2`, table)).Scan(&s6))
	is.Equal(s6, "0.000000")

	// The longest expansion Postgres accepts is stored exactly.
	longest := new(big.Rat).SetFrac(big.NewInt(1), pow2(16383))
	is.NoErr(write(3, longest))
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT length(split_part(unc::text, '.', 2)) FROM %q WHERE id=3`, table)).Scan(&unc))
	is.Equal(unc, "16383")
}

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
	"fmt"
	"math/big"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

// BenchmarkDestination_Write_Numeric measures the end-to-end cost of writing a
// 100-record batch with one NUMERIC(12,6) column holding a big.Rat, to a local
// Postgres. It is the number to compare when changing how Rats are formatted.
func BenchmarkDestination_Write_Numeric(b *testing.B) {
	ctx := b.Context()
	conn, err := pgx.Connect(ctx, test.RegularConnString)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx)

	table := "bench_numeric_scale"
	if _, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, table)); err != nil {
		b.Fatal(err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int PRIMARY KEY, amt numeric(12,6))`, table)); err != nil {
		b.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table)) }()

	d := openNumericBenchDestination(b)
	for _, rat := range []struct {
		name string
		v    *big.Rat
	}{
		{"terminating", big.NewRat(123456789, 1000000)},
		{"non-terminating", big.NewRat(1, 3)},
	} {
		recs := make([]opencdc.Record, 100)
		for i := range recs {
			recs[i] = opencdc.Record{
				Position:  opencdc.Position("p"),
				Operation: opencdc.OperationCreate,
				Metadata:  map[string]string{opencdc.MetadataCollection: table},
				Key:       opencdc.StructuredData{"id": i},
				Payload:   opencdc.Change{After: opencdc.StructuredData{"amt": rat.v}},
			}
		}
		b.Run(rat.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// Write mutates the payload (the key is deleted from it), so give
				// every iteration its own copy.
				batch := make([]opencdc.Record, len(recs))
				for i, r := range recs {
					r.Payload.After = opencdc.StructuredData{"amt": rat.v}
					batch[i] = r
				}
				if _, err := d.Write(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func openNumericBenchDestination(b *testing.B) sdk.Destination {
	b.Helper()
	ctx := b.Context()
	d := NewDestination()
	err := sdk.Util.ParseConfig(
		ctx,
		map[string]string{
			"url":   test.RegularConnString,
			"table": "{{ index .Metadata \"opencdc.collection\" }}",
		},
		d.Config(),
		Connector.NewSpecification().DestinationParams,
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := d.Open(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Teardown(ctx) })
	return d
}

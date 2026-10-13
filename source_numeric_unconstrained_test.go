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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// An unconstrained numeric used to get the Avro schema decimal(65535, 65531),
// which leaves four digits before the point: any value >= 10000 failed to
// encode and halted the pipeline (#375). It is now an Avro string holding the
// exact text of the value.
func TestSource_NumericUnconstrainedAvro(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	long := strings.Repeat("9", 1000) + "." + strings.Repeat("7", 60)
	vals := []string{
		"0", "0.000", "0.00000", "-0.0", "1.50", "100.00", "0.10", "-5.500", "0.000001", "9999.9999",
		"10000", "12345.678", "123456789012345678901234567890.123456789",
		"-99999999999999999999.5", long, "1" + strings.Repeat("0", 500),
		"NaN", "Infinity", "-Infinity",
	}

	for _, path := range []string{"snapshot", "cdc"} {
		for i, v := range vals {
			name := fmt.Sprintf("%s/%d", path, i)
			if len(v) < 30 {
				name = fmt.Sprintf("%s/%s", path, v)
			}
			t.Run(name, func(t *testing.T) {
				is := is.New(t)
				table := fmt.Sprintf("numu_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v numeric)", table))
				is.NoErr(err)
				dropReplication(ctx, t, table)
				t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
				insert := fmt.Sprintf("INSERT INTO %s VALUES (1, '%s')", table, v)
				mode := "never"
				if path == "snapshot" {
					mode = "initial"
					_, err = conn.Exec(ctx, insert)
					is.NoErr(err)
				}
				s := NewSource()
				is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
					"url": test.RepmgrConnString, "tables": table, "snapshotMode": mode,
					"cdcMode": "logrepl", "logrepl.slotName": table, "logrepl.publicationName": table,
				}, s.Config(), Connector.NewSpecification().SourceParams))
				is.NoErr(s.Open(ctx, nil))
				t.Cleanup(func() { _ = s.Teardown(context.Background()) })
				if path == "cdc" {
					_, err = conn.Exec(ctx, insert)
					is.NoErr(err)
				}

				rec, err := readOne(ctx, t, s)
				is.NoErr(err)
				got := payloadValue(ctx, t, rec, true)
				// What Postgres prints for the stored value.
				var want string
				is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT v::text FROM %s", table)).Scan(&want))
				is.Equal(got, want) // exact text, including scale
				fmt.Printf("UNC %s %q -> %q\n", path, truncate(v), truncate(fmt.Sprint(got)))
			})
		}
	}
}

func truncate(s string) string {
	if len(s) > 40 {
		return fmt.Sprintf("%s...(%d chars)", s[:20], len(s))
	}
	return s
}

// pg -> pg through the source (Avro) and this connector's destination. The
// destination receives the value as a string and must store exactly it.
func TestNumericUnconstrained_RoundTrip(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	long := strings.Repeat("9", 1000) + "." + strings.Repeat("7", 60)
	vals := []string{
		"1.50", "10000", "12345.678", "-99999999999999999999.5",
		"123456789012345678901234567890.123456789", long, "NaN", "Infinity", "-Infinity",
	}

	for _, dstType := range []string{"numeric", "numeric(900,70)", "text"} {
		for _, path := range []string{"snapshot", "cdc"} {
			t.Run(dstType+"/"+path, func(t *testing.T) {
				is := is.New(t)
				seq := numericTableSeq.Add(1)
				src := fmt.Sprintf("numrt_s_%d_%d", time.Now().UnixMicro()%1000000, seq)
				dst := fmt.Sprintf("numrt_d_%d_%d", time.Now().UnixMicro()%1000000, seq)
				for _, q := range []string{
					fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v numeric)", src),
					fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", dst, dstType),
				} {
					_, err := conn.Exec(ctx, q)
					is.NoErr(err)
				}
				dropReplication(ctx, t, src)
				t.Cleanup(func() {
					_, _ = conn.Exec(context.Background(), "DROP TABLE "+src+", "+dst)
				})
				insertAll := func() {
					for i, v := range vals {
						if skip(dstType, v) {
							continue
						}
						_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (%d, '%s')", src, i+1, v))
						is.NoErr(err)
					}
				}
				mode := "never"
				if path == "snapshot" {
					mode = "initial"
					insertAll()
				}

				s := NewSource()
				is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
					"url": test.RepmgrConnString, "tables": src, "snapshotMode": mode,
					"cdcMode": "logrepl", "logrepl.slotName": src, "logrepl.publicationName": src,
				}, s.Config(), Connector.NewSpecification().SourceParams))
				is.NoErr(s.Open(ctx, nil))
				t.Cleanup(func() { _ = s.Teardown(context.Background()) })
				if path == "cdc" {
					insertAll()
				}

				d := NewDestination()
				is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
					"url": test.RegularConnString, "table": dst,
				}, d.Config(), Connector.NewSpecification().DestinationParams))
				is.NoErr(d.Open(ctx))
				t.Cleanup(func() { _ = d.Teardown(context.Background()) })

				want := 0
				for _, v := range vals {
					if !skip(dstType, v) {
						want++
					}
				}
				copied := 0
				deadline := time.Now().Add(30 * time.Second)
				for copied < want && time.Now().Before(deadline) {
					recs, err := s.ReadN(ctx, 100)
					if errors.Is(err, sdk.ErrBackoffRetry) {
						time.Sleep(50 * time.Millisecond)
						continue
					}
					is.NoErr(err)
					for _, rec := range recs {
						n, err := d.Write(ctx, []opencdc.Record{rec})
						is.NoErr(err)
						is.Equal(n, 1)
						is.NoErr(s.Ack(ctx, rec.Position))
						copied++
					}
				}
				is.Equal(copied, want)

				if dstType == "numeric(900,70)" {
					// The column has a scale: Postgres applies it, so compare the values.
					var mismatches int
					is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(
						"SELECT count(*) FROM %s s JOIN %s d USING (id) WHERE s.v IS DISTINCT FROM d.v", src, dst)).Scan(&mismatches))
					is.Equal(mismatches, 0)
					var rowsCopied int
					is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+dst).Scan(&rowsCopied))
					is.Equal(rowsCopied, want)
					return
				}
				rows, err := conn.Query(ctx, fmt.Sprintf(
					"SELECT s.v::text, d.v::text FROM %s s JOIN %s d USING (id) ORDER BY id", src, dst))
				is.NoErr(err)
				defer rows.Close()
				n := 0
				for rows.Next() {
					var sv, dv string
					is.NoErr(rows.Scan(&sv, &dv))
					is.Equal(dv, sv) // exactly what Postgres stored in the source
					n++
				}
				is.NoErr(rows.Err())
				is.Equal(n, len(vals))
			})
		}
	}
}

// skip reports whether a destination column type cannot hold the value at all
// (not a limit of the connector): numeric(900,70) has at most 830 integer
// digits and no infinity.
func skip(dstType, v string) bool {
	return dstType == "numeric(900,70)" && (len(v) > 900 || v == "Infinity" || v == "-Infinity")
}

// Copyright © 2022 Meroxa, Inc.
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
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	// tables with capital letters should be quoted
	tableName := strings.ToUpper(test.RandomIdentifier(t))
	test.SetupTestTableWithName(ctx, t, conn, tableName)

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
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := d.Teardown(ctx)
		is.NoErr(err)
	}()

	tests := []struct {
		name   string
		record opencdc.Record
	}{
		{
			name: "snapshot",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationSnapshot,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 5000},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1":          "foo",
						"column2":          123,
						"column3":          true,
						"column4":          nil,
						"UppercaseColumn1": 222,
					},
				},
			},
		},
		{
			name: "create",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationCreate,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 5},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1":          "foo",
						"column2":          456,
						"column3":          false,
						"column4":          nil,
						"UppercaseColumn1": 333,
					},
				},
			},
		},
		{
			name: "insert on update (upsert)",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationUpdate,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 6},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1":          "bar",
						"column2":          567,
						"column3":          true,
						"column4":          nil,
						"UppercaseColumn1": 444,
					},
				},
			},
		},
		{
			name: "update on conflict",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationUpdate,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 1},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1":          "foobar",
						"column2":          567,
						"column3":          true,
						"column4":          nil,
						"UppercaseColumn1": 555,
					},
				},
			},
		},
		{
			name: "delete",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Operation: opencdc.OperationDelete,
				Key:       opencdc.StructuredData{"id": 4},
			},
		},
		{
			name: "write a big.Rat",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationSnapshot,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 123},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1":          "abcdef",
						"column2":          567,
						"column3":          true,
						"column4":          big.NewRat(123, 100),
						"UppercaseColumn1": 555,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is = is.New(t)
			id := tt.record.Key.(opencdc.StructuredData)["id"]

			i, err := d.Write(ctx, []opencdc.Record{tt.record})
			is.NoErr(err)
			is.Equal(i, 1)

			got, err := queryTestTable(ctx, conn, tableName, id)
			switch tt.record.Operation {
			case opencdc.OperationCreate, opencdc.OperationSnapshot, opencdc.OperationUpdate:
				is.NoErr(err)
				is.Equal(
					"",
					cmp.Diff(
						tt.record.Payload.After,
						got,
						cmp.Comparer(func(x, y *big.Rat) bool {
							return x.Cmp(y) == 0
						}),
					),
				) // -want, +got
			case opencdc.OperationDelete:
				is.Equal(err, pgx.ErrNoRows)
			}
		})
	}
}

// TestDestination_SchemaDriftMarkerNoop pins FM6/AC6: the drift marker record
// (keyless, payload-less OperationCreate carrying postgres.schema.drift
// metadata) must be an explicit no-op at the connector's own destination —
// never a silent wrong row (inserting it would produce `INSERT ... DEFAULT
// VALUES`). A marker mixed into a batch with real records must not disturb
// them, and the full batch count is still acked (the marker's position
// checkpoint flows end to end).
//
// Perturbation proof: removing the marker skip in Destination.Write turns the
// marker into an INSERT ... DEFAULT VALUES (or a NOT NULL constraint error) —
// this test fails on the row-count assertion.
func TestDestination_SchemaDriftMarkerNoop(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	tableName := strings.ToUpper(test.RandomIdentifier(t))
	test.SetupTestTableWithName(ctx, t, conn, tableName)

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
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)
	defer func() {
		is.NoErr(d.Teardown(ctx))
	}()

	marker := opencdc.Record{
		Position:  markerPosition(tableName),
		Operation: opencdc.OperationCreate,
		Metadata: map[string]string{
			opencdc.MetadataCollection:       tableName,
			logrepl.MetadataSchemaDrift:      "true",
			logrepl.MetadataSchemaDriftTable: tableName,
		},
		Key:     nil,
		Payload: opencdc.Change{After: nil},
	}

	// The seeded table has 4 rows; the marker alone must not add one.
	var count int
	err = conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %q`, tableName)).Scan(&count)
	is.NoErr(err)
	is.Equal(count, 4)

	i, err := d.Write(ctx, []opencdc.Record{marker})
	is.NoErr(err)
	is.Equal(i, 1)

	err = conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %q`, tableName)).Scan(&count)
	is.NoErr(err)
	is.Equal(count, 4)

	// Marker mixed with a real record in one batch: the real record is written,
	// the marker is skipped, and both are acked.
	realRec := opencdc.Record{
		Position:  opencdc.Position("real"),
		Operation: opencdc.OperationCreate,
		Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
		Key:       opencdc.StructuredData{"id": 9000},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"column1":          "real",
				"column2":          1,
				"column3":          true,
				"column4":          nil,
				"UppercaseColumn1": 1,
			},
		},
	}
	i, err = d.Write(ctx, []opencdc.Record{marker, realRec})
	is.NoErr(err)
	is.Equal(i, 2)

	got, err := queryTestTable(ctx, conn, tableName, 9000)
	is.NoErr(err)
	is.Equal("", cmp.Diff(realRec.Payload.After, got,
		cmp.Comparer(func(x, y *big.Rat) bool { return x.Cmp(y) == 0 })))

	err = conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %q`, tableName)).Scan(&count)
	is.NoErr(err)
	is.Equal(count, 5) // 4 seeded + the one real record; the marker wrote nothing
}

// openDriftTestDestination opens a destination routed by opencdc.collection
// against a freshly seeded table (4 rows, ids 1-4) and returns it with the
// table name and a separate verification connection.
func openDriftTestDestination(t *testing.T) (context.Context, sdk.Destination, string, *pgx.Conn) {
	t.Helper()
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	tableName := strings.ToUpper(test.RandomIdentifier(t))
	test.SetupTestTableWithName(ctx, t, conn, tableName)

	d := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(
		ctx,
		map[string]string{
			"url":   test.RegularConnString,
			"table": "{{ index .Metadata \"opencdc.collection\" }}",
		},
		d.Config(),
		Connector.NewSpecification().DestinationParams,
	))
	is.NoErr(d.Open(ctx))
	t.Cleanup(func() { _ = d.Teardown(ctx) })
	return ctx, d, tableName, conn
}

func driftTestPayload(v int) opencdc.StructuredData {
	return opencdc.StructuredData{
		"column1":          "real data",
		"column2":          v,
		"column3":          true,
		"column4":          nil,
		"UppercaseColumn1": 1,
	}
}

// TestDestination_DriftMetadataOnRealRecordIsWritten is the regression test
// for the silent-loss bug (invariant 3): Write skipped AND acked any record
// whose metadata had postgres.schema.drift=true, without checking that the
// record was shaped like the marker the source emits. A keyed,
// payload-bearing record carrying the key came back n=1, err=nil, with no row
// stored. Metadata is user-reachable (processors, other sources), so only the
// exact marker shape may be skipped.
//
// Perturbation proof: restoring the metadata-only check in Destination.Write
// fails every subtest on the row assertion.
func TestDestination_DriftMetadataOnRealRecordIsWritten(t *testing.T) {
	ctx, d, tableName, conn := openDriftTestDestination(t)

	meta := func() map[string]string {
		return map[string]string{
			opencdc.MetadataCollection:       tableName,
			logrepl.MetadataSchemaDrift:      "true",
			logrepl.MetadataSchemaDriftTable: tableName,
		}
	}

	t.Run("keyed create with payload", func(t *testing.T) {
		is := is.New(t)
		rec := opencdc.Record{
			Position: opencdc.Position("p1"), Operation: opencdc.OperationCreate, Metadata: meta(),
			Key:     opencdc.StructuredData{"id": 9001},
			Payload: opencdc.Change{After: driftTestPayload(11)},
		}
		n, err := d.Write(ctx, []opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(n, 1)
		got, err := queryTestTable(ctx, conn, tableName, 9001)
		is.NoErr(err) // the row must exist: acked implies stored
		is.Equal("", cmp.Diff(rec.Payload.After, got,
			cmp.Comparer(func(x, y *big.Rat) bool { return x.Cmp(y) == 0 })))
	})

	t.Run("snapshot with key and payload", func(t *testing.T) {
		is := is.New(t)
		rec := opencdc.Record{
			Position: opencdc.Position("p2"), Operation: opencdc.OperationSnapshot, Metadata: meta(),
			Key:     opencdc.StructuredData{"id": 9002},
			Payload: opencdc.Change{After: driftTestPayload(12)},
		}
		n, err := d.Write(ctx, []opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(n, 1)
		_, err = queryTestTable(ctx, conn, tableName, 9002)
		is.NoErr(err)
	})

	t.Run("update", func(t *testing.T) {
		is := is.New(t)
		rec := opencdc.Record{
			Position: opencdc.Position("p3"), Operation: opencdc.OperationUpdate, Metadata: meta(),
			Key:     opencdc.StructuredData{"id": 1},
			Payload: opencdc.Change{After: driftTestPayload(13)},
		}
		n, err := d.Write(ctx, []opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(n, 1)
		got, err := queryTestTable(ctx, conn, tableName, 1)
		is.NoErr(err)
		is.Equal(got["column1"], "real data")
	})

	t.Run("delete", func(t *testing.T) {
		is := is.New(t)
		rec := opencdc.Record{
			Position: opencdc.Position("p4"), Operation: opencdc.OperationDelete, Metadata: meta(),
			Key: opencdc.StructuredData{"id": 2},
		}
		n, err := d.Write(ctx, []opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(n, 1)
		_, err = queryTestTable(ctx, conn, tableName, 2)
		is.True(errors.Is(err, pgx.ErrNoRows)) // the delete was applied, not skipped
	})
}

// TestDestination_DriftMarkerAmongRealRecords pins that the genuine marker is
// still a no-op that is acked, in a batch where it sits between real records
// (one of which carries stray drift metadata): exactly the real records are
// written, nothing else, and the full count is acked.
func TestDestination_DriftMarkerAmongRealRecords(t *testing.T) {
	is := is.New(t)
	ctx, d, tableName, conn := openDriftTestDestination(t)

	count := func() int {
		var c int
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %q`, tableName)).Scan(&c))
		return c
	}
	is.Equal(count(), 4)

	marker := opencdc.Record{
		Position:  markerPosition(tableName),
		Operation: opencdc.OperationCreate,
		Metadata: map[string]string{
			opencdc.MetadataCollection:       tableName,
			logrepl.MetadataSchemaDrift:      "true",
			logrepl.MetadataSchemaDriftTable: tableName,
		},
	}
	mkReal := func(id int) opencdc.Record {
		return opencdc.Record{
			Position: opencdc.Position(fmt.Sprintf("r%d", id)), Operation: opencdc.OperationCreate,
			Metadata: map[string]string{opencdc.MetadataCollection: tableName},
			Key:      opencdc.StructuredData{"id": id},
			Payload:  opencdc.Change{After: driftTestPayload(id)},
		}
	}
	stray := mkReal(9103)
	stray.Metadata[logrepl.MetadataSchemaDrift] = "true"

	// The marker alone: acked, nothing written.
	n, err := d.Write(ctx, []opencdc.Record{marker})
	is.NoErr(err)
	is.Equal(n, 1)
	is.Equal(count(), 4)

	// Marker first, middle and last, around a stray-metadata record.
	n, err = d.Write(ctx, []opencdc.Record{marker, mkReal(9101), marker, mkReal(9102), stray, marker})
	is.NoErr(err)
	is.Equal(n, 6)
	is.Equal(count(), 7) // 4 seeded + 3 real; the three markers wrote nothing
	for _, id := range []int{9101, 9102, 9103} {
		_, err := queryTestTable(ctx, conn, tableName, id)
		is.NoErr(err)
	}
}

func TestDestination_Batch(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	tableName := strings.ToUpper(test.RandomIdentifier(t))
	test.SetupTestTableWithName(ctx, t, conn, tableName)

	d := NewDestination()

	err := sdk.Util.ParseConfig(
		ctx,
		map[string]string{"url": test.RegularConnString, "table": tableName},
		d.Config(),
		Connector.NewSpecification().DestinationParams,
	)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := d.Teardown(ctx)
		is.NoErr(err)
	}()

	records := []opencdc.Record{
		{
			Position:  opencdc.Position("foo1"),
			Operation: opencdc.OperationCreate,
			Key:       opencdc.StructuredData{"id": 5},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"column1":          "foo1",
					"column2":          1,
					"column3":          false,
					"column4":          nil,
					"UppercaseColumn1": 111,
				},
			},
		},
		{
			Position:  opencdc.Position("foo2"),
			Operation: opencdc.OperationCreate,
			Key:       opencdc.StructuredData{"id": 6},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"column1":          "foo2",
					"column2":          2,
					"column3":          true,
					"column4":          nil,
					"UppercaseColumn1": 222,
				},
			},
		},
		{
			Position:  opencdc.Position("foo3"),
			Operation: opencdc.OperationCreate,
			Key:       opencdc.StructuredData{"id": 7},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"column1":          "foo3",
					"column2":          3,
					"column3":          false,
					"column4":          nil,
					"UppercaseColumn1": 333,
				},
			},
		},
	}

	i, err := d.Write(ctx, records)
	is.NoErr(err)
	is.Equal(i, len(records))

	for _, rec := range records {
		got, err := queryTestTable(ctx, conn, tableName, rec.Key.(opencdc.StructuredData)["id"])
		is.NoErr(err)
		is.Equal(rec.Payload.After, got)
	}
}

func queryTestTable(ctx context.Context, conn test.Querier, tableName string, id any) (opencdc.StructuredData, error) {
	row := conn.QueryRow(
		ctx,
		fmt.Sprintf(`SELECT column1, column2, column3, column4, "UppercaseColumn1" FROM %q WHERE id = $1`, tableName),
		id,
	)

	var (
		col1          string
		col2          int
		col3          bool
		col4Str       *string
		uppercaseCol1 int
	)

	err := row.Scan(&col1, &col2, &col3, &col4Str, &uppercaseCol1)
	if err != nil {
		return nil, err
	}

	// Handle the potential nil case for col4
	var col4 interface{}
	if col4Str != nil {
		r := new(big.Rat)
		r.SetString(*col4Str)
		col4 = r
	}

	return opencdc.StructuredData{
		"column1":          col1,
		"column2":          col2,
		"column3":          col3,
		"column4":          col4,
		"UppercaseColumn1": uppercaseCol1,
	}, nil
}

// TestDestination_Write_CommitTimeError is the regression test for #342.
//
// A DEFERRABLE INITIALLY DEFERRED unique constraint is only checked when the
// batch's implicit transaction commits, i.e. after every statement result has
// been read successfully. That error surfaces only from BatchResults.Close.
// Write must report it (0 records written) instead of letting the engine ack
// records that were rolled back (invariant 1).
func TestDestination_Write_CommitTimeError(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %q (id int PRIMARY KEY, v int, CONSTRAINT %q UNIQUE (v) DEFERRABLE INITIALLY DEFERRED)`,
		table, table+"_u"))
	is.NoErr(err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE %q`, table))
		is.NoErr(err)
	})

	d := NewDestination()
	err = sdk.Util.ParseConfig(
		ctx,
		map[string]string{"url": test.RegularConnString, "table": table},
		d.Config(),
		Connector.NewSpecification().DestinationParams,
	)
	is.NoErr(err)
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	recs := []opencdc.Record{
		{Operation: opencdc.OperationCreate, Key: opencdc.StructuredData{"id": 1}, Payload: opencdc.Change{After: opencdc.StructuredData{"v": 5}}},
		{Operation: opencdc.OperationCreate, Key: opencdc.StructuredData{"id": 2}, Payload: opencdc.Change{After: opencdc.StructuredData{"v": 5}}},
	}

	n, err := d.Write(ctx, recs)

	var count int
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %q`, table)).Scan(&count))
	t.Logf("Write returned n=%d err=%v; rows stored=%d", n, err, count)

	is.True(err != nil) // commit-time failure must surface as an error
	is.Equal(n, 0)      // nothing was stored, so nothing may be reported written
	is.Equal(count, 0)
}

// markerPosition is a position the source writes for a schema-drift marker
// of table (SchemaDriftMarker set), the source-only signal the destination
// requires before it skips a marker.
func markerPosition(table string) opencdc.Position {
	return position.Position{Type: position.TypeCDC, LastLSN: "0/16B3748", TxCommitLSN: "0/16B3747",
		TxSeq: math.MaxUint64, SchemaDriftMarker: table}.ToSDKPosition()
}

// TestDestination_UnverifiedDriftMarkerIsRefused: a record with the drift
// metadata and the marker's shape, but a position the source did not write
// as a marker, is neither written nor acked. Before, it was skipped and
// acked on metadata alone (spoofable; #358).
func TestDestination_UnverifiedDriftMarkerIsRefused(t *testing.T) {
	is := is.New(t)
	ctx, d, tableName, conn := openDriftTestDestination(t)
	count := func() int {
		var c int
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %q", tableName)).Scan(&c))
		return c
	}
	before := count()
	meta := func(table string) map[string]string {
		return map[string]string{
			opencdc.MetadataCollection:       tableName,
			logrepl.MetadataSchemaDrift:      "true",
			logrepl.MetadataSchemaDriftTable: table,
		}
	}
	realRec := opencdc.Record{
		Position: opencdc.Position("real"), Operation: opencdc.OperationCreate,
		Metadata: map[string]string{opencdc.MetadataCollection: tableName},
		Key:      opencdc.StructuredData{"id": 9301},
		Payload:  opencdc.Change{After: driftTestPayload(1)},
	}
	for name, pos := range map[string]opencdc.Position{
		"foreign position (e.g. from Kafka)":    opencdc.Position("kafka-partition-0-offset-17"),
		"cdc position without the marker field": position.Position{Type: position.TypeCDC, LastLSN: "0/1"}.ToSDKPosition(),
		"marker position for another table":     markerPosition("public.other"),
		"snapshot position with the field":      position.Position{Type: position.TypeSnapshot, SchemaDriftMarker: tableName}.ToSDKPosition(),
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			spoof := opencdc.Record{Position: pos, Operation: opencdc.OperationCreate, Metadata: meta(tableName)}
			n, err := d.Write(ctx, []opencdc.Record{realRec, spoof})
			is.True(err != nil)
			is.True(strings.HasPrefix(err.Error(), ErrorCodeDriftMarkerUnverified+": "))
			is.Equal(n, 0)            // nothing acked
			is.Equal(count(), before) // nothing written, not even the real record before it
		})
	}
}

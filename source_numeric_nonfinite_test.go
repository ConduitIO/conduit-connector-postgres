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
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
)

// These tests drive a real source, with the default SDK middleware (so the
// records are Avro-encoded exactly as in a pipeline), over a numeric column
// that holds NaN, Infinity, -Infinity, NULL and an ordinary value (issue #368).
//
// Before the fix, every non-finite value panicked inside the Avro encoder with
// a nil pointer dereference, on the snapshot path and on the CDC path alike.
// The pipeline restarts at the same position and replays the row, so the
// panic loops forever (Invariant 3: the source could not make progress).

const numericErrorCode = "postgres.source.numeric_not_representable"

var numericTableSeq atomic.Int64

// dropReplication removes the slot and publication a source created under the
// given name. Registered before the source's own Teardown cleanup so it runs
// after it; a halted source may not have dropped them itself.
func dropReplication(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	// Dropping a slot needs the replication role the regular user lacks.
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	t.Cleanup(func() {
		ctx := context.Background()
		for i := 0; i < 20; i++ { // the slot stays active until its connection is gone
			_, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, name)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = conn.Exec(ctx, fmt.Sprintf(`DROP PUBLICATION IF EXISTS %q`, name))
	})
}

// readOne reads one record, retrying while the source has nothing yet. A panic
// is reported as a test failure of this case instead of killing the whole
// test binary, so each case in the matrix reports on its own.
func readOne(ctx context.Context, t *testing.T, s sdk.Source) (rec opencdc.Record, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("source panicked: %v", r)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		recs, err := s.ReadN(ctx, 1)
		switch {
		case errors.Is(err, sdk.ErrBackoffRetry):
			time.Sleep(50 * time.Millisecond)
			continue
		case err != nil:
			return opencdc.Record{}, err
		}
		return recs[0], nil
	}
}

// payloadValue returns the value of column v in the record's after payload,
// decoding the Avro bytes with the schema attached to the record.
func payloadValue(ctx context.Context, t *testing.T, rec opencdc.Record, avro bool) any {
	t.Helper()
	is := is.New(t)

	if !avro {
		sd, ok := rec.Payload.After.(opencdc.StructuredData)
		is.True(ok) // expected structured payload
		return sd["v"]
	}

	raw, ok := rec.Payload.After.(opencdc.RawData)
	is.True(ok) // expected an Avro-encoded payload
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	is.NoErr(err)
	version, err := rec.Metadata.GetPayloadSchemaVersion()
	is.NoErr(err)
	sch, err := schema.Get(ctx, subject, version)
	is.NoErr(err)
	var decoded map[string]any
	is.NoErr(sch.Unmarshal(raw.Bytes(), &decoded))
	return decoded["v"]
}

func TestSource_NumericNonFinite(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	values := []struct {
		name string
		sql  string
		// text is the canonical value the source emits without Avro, and the
		// value named by the error with Avro. Empty for the cases that are
		// not a non-finite number.
		text string
	}{
		{name: "NaN", sql: "'NaN'", text: "NaN"},
		{name: "Infinity", sql: "'Infinity'", text: "Infinity"},
		{name: "NegInfinity", sql: "'-Infinity'", text: "-Infinity"},
		{name: "NULL", sql: "NULL"},
		{name: "finite", sql: "'1.50'"},
	}

	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			for _, v := range values {
				name := fmt.Sprintf("%s/avro=%t/%s", path, avro, v.name)
				if avro && v.sql == "NULL" {
					// The Avro schema the source attaches has no nullable
					// fields, so the SDK cannot encode ANY NULL under it (#326).
					// That is independent of this fix; a NULL numeric is covered
					// below the SDK middleware instead, see
					// source/snapshot/fetch_worker_test.go and
					// source/logrepl/internal/relationset_test.go.
					continue
				}
				t.Run(name, func(t *testing.T) {
					is := is.New(t)
					id := numericTableSeq.Add(1)
					// An unconstrained numeric is an Avro string and holds NaN and
					// the infinities as text (#375). Only a constrained numeric(p,s)
					// is an Avro decimal that cannot, and only NaN fits in one.
					colType := "numeric"
					halts := avro && v.text == "NaN"
					if halts {
						colType = "numeric(8,2)"
					}
					table := fmt.Sprintf("nonfinite_%d_%d", time.Now().UnixMicro()%1000000, id)

					_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v %s)`, table, colType))
					is.NoErr(err)
					dropReplication(ctx, t, table)
					t.Cleanup(func() {
						_, err := conn.Exec(context.Background(), `DROP TABLE `+table)
						is.NoErr(err)
					})
					insert := fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, table, v.sql)

					snapshotMode := "never"
					if path == "snapshot" {
						snapshotMode = "initial"
						_, err = conn.Exec(ctx, insert)
						is.NoErr(err)
					}

					s := NewSource()
					is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
						"url":                     test.RepmgrConnString,
						"tables":                  table,
						"snapshotMode":            snapshotMode,
						"cdcMode":                 "logrepl",
						"logrepl.slotName":        table,
						"logrepl.publicationName": table,
						"logrepl.withAvroSchema":  fmt.Sprint(avro),
					}, s.Config(), Connector.NewSpecification().SourceParams))
					is.NoErr(s.Open(ctx, nil))
					t.Cleanup(func() { _ = s.Teardown(context.Background()) })

					if path == "cdc" {
						_, err = conn.Exec(ctx, insert)
						is.NoErr(err)
					}

					rec, err := readOne(ctx, t, s)

					if halts {
						// Avro decimal cannot hold it: halt with an actionable error.
						is.True(err != nil) // expected the source to halt
						msg := err.Error()
						is.True(strings.Contains(msg, numericErrorCode))             // stable error code
						is.True(strings.Contains(msg, table))                        // names the table
						is.True(strings.Contains(msg, `"v"`))                        // names the column
						is.True(strings.Contains(msg, fmt.Sprintf("%q", v.text)))    // names the value
						is.True(strings.Contains(msg, "withAvroSchema=false"))       // the fix that works from the WAL
						is.True(strings.Contains(msg, "(row columns id, key hash ")) // names the row by key columns and hash
						is.True(!strings.Contains(msg, "id=1"))
						return
					}

					is.NoErr(err)
					got := payloadValue(ctx, t, rec, avro)
					switch {
					case v.text != "":
						is.Equal(got, v.text) // canonical text, never NULL or 0
					case v.sql == "NULL":
						is.True(got == nil) // a real NULL stays an untyped NULL
					case avro:
						is.Equal(got, "1.50") // unconstrained numeric: exact text, scale kept
					default:
						r, ok := got.(*big.Rat)
						is.True(ok) // finite numerics are unchanged
						is.Equal(r.Cmp(big.NewRat(3, 2)), 0)
					}
				})
			}
		}
	}
}

// A constrained numeric(p,s) can hold NaN too (not Infinity: Postgres rejects
// it with 22003).
func TestSource_NumericNonFinite_Constrained(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := fmt.Sprintf("nonfinite_c_%d", time.Now().UnixMicro()%1000000)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v numeric(8,2))`, table))
	is.NoErr(err)
	dropReplication(ctx, t, table)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), `DROP TABLE `+table) })
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'NaN')`, table))
	is.NoErr(err)

	s := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url":                     test.RepmgrConnString,
		"tables":                  table,
		"snapshotMode":            "initial",
		"cdcMode":                 "logrepl",
		"logrepl.slotName":        table,
		"logrepl.publicationName": table,
	}, s.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(s.Open(ctx, nil))
	t.Cleanup(func() { _ = s.Teardown(context.Background()) })

	_, err = readOne(ctx, t, s)
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), numericErrorCode))
}

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

package types_test

import (
	"encoding/hex"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // the host-zone matrix must not depend on the CI image

	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// goldenZones are the host zones the golden tests are replayed under. UTC is
// the only one where the pre-fix code is right; Los_Angeles has the DST gap in
// March, Kolkata a half-hour offset without DST, Auckland a southern-hemisphere
// DST gap in September.
var goldenZones = []string{"UTC", "America/Los_Angeles", "Asia/Kolkata", "Pacific/Auckland"}

const goldenChildEnv = "PG_TYPES_GOLDEN_CHILD"

// goldenColumn is a column of the golden row: the Postgres type and the text
// the walsender prints for the value (what the CDC path decodes).
type goldenColumn struct {
	name string
	oid  uint32
	text string
}

var goldenRow = []goldenColumn{
	{"ts", pgtype.TimestampOID, "2024-01-02 03:04:05.123456"},
	// 02:30 does not exist on 2024-03-10 in Los_Angeles, nor on 2024-09-29 in
	// Auckland; a timestamp column can hold both.
	{"ts_gap_la", pgtype.TimestampOID, "2024-03-10 02:30:00"},
	{"ts_gap_nz", pgtype.TimestampOID, "2024-09-29 02:30:00"},
	{"tstz", pgtype.TimestamptzOID, "2024-01-02 03:04:05.123456+00"},
	{"tstz_offset", pgtype.TimestamptzOID, "2024-01-02 03:04:05.5+05:30"},
	{"date", pgtype.DateOID, "2024-01-02"},
	{"tsvector", pgtype.TSVectorOID, "'a':1 'cat':3A,4B 'it''s' 'x\\\\y'"},
}

var goldenArrays = []goldenColumn{
	{"ts_arr", pgtype.TimestampArrayOID, "{\"2024-01-02 03:04:05\",NULL,\"2024-06-07 08:09:10.5\"}"},
	{"tstz_arr", pgtype.TimestamptzArrayOID, "{\"2024-01-02 03:04:05+00\"}"},
	{"date_arr", pgtype.DateArrayOID, "{2024-01-02,2024-12-31}"},
	{"tsvector_arr", pgtype.TSVectorArrayOID, "{\"'a':1 'b'\"}"},
}

func decodeText(t *testing.T, oid uint32, text string) any {
	t.Helper()
	m := pgtype.NewMap()
	dt, ok := m.TypeForOID(oid)
	if !ok {
		t.Fatalf("no pgx type for oid %d", oid)
	}
	v, err := dt.Codec.DecodeValue(m, oid, pgtype.TextFormatCode, []byte(text))
	if err != nil {
		t.Fatalf("decode %q: %v", text, err)
	}
	return v
}

func formatRow(t *testing.T, enc types.Encoding, cols []goldenColumn) opencdc.StructuredData {
	t.Helper()
	row := opencdc.StructuredData{}
	for _, c := range cols {
		v, err := types.FormatFor(enc, c.oid, decodeText(t, c.oid, c.text))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		row[c.name] = v
	}
	return row
}

// TestGoldenJSON pins the JSON a built-in pipeline without an Avro schema
// produces for the golden row, byte for byte, in any host zone.
func TestGoldenJSON(t *testing.T) {
	is := is.New(t)
	rec := opencdc.Record{Payload: opencdc.Change{After: formatRow(t, types.EncodingNative, append(goldenRow, goldenArrays...))}}
	is.Equal(string(rec.Payload.After.Bytes()), goldenJSON)
}

// TestGoldenAvro pins the Avro bytes the SDK produces from the schema in
// source/schema and the values from FormatFor, as the SDK's source encoding
// middleware does, in any host zone.
func TestGoldenAvro(t *testing.T) {
	is := is.New(t)

	var fields []pgconn.FieldDescription
	for _, c := range goldenRow {
		fields = append(fields, pgconn.FieldDescription{Name: c.name, DataTypeOID: c.oid, TypeModifier: -1})
	}
	sch, err := schema.Avro.Extract("golden_payload", fields)
	is.NoErr(err)
	serde, err := cavro.Parse([]byte(sch.String()))
	is.NoErr(err)

	row := formatRow(t, types.EncodingAvro, goldenRow)
	got, err := serde.Marshal(row)
	is.NoErr(err)
	is.Equal(hex.EncodeToString(got), goldenAvroHex)

	// The Avro bytes decode back to the wall clock that was inserted. Only
	// ts is checked: the Avro library decodes into time.Local, which cannot
	// hold the nonexistent wall clocks of ts_gap_*; the bytes above are what
	// pins those.
	var back map[string]any
	is.NoErr(serde.Unmarshal(got, &back))
	tm, ok := back["ts"].(time.Time)
	is.True(ok)
	is.Equal(tm.Format("2006-01-02T15:04:05.000000"), "2024-01-02T03:04:05.123456")
}

// TestGoldenTimestampOnly is the narrow form of the host-zone tests: it only
// involves types that never had a representation problem other than the host
// zone, so on a connector without the normalization it passes in UTC and fails
// anywhere else (#302).
func TestGoldenTimestampOnly(t *testing.T) {
	is := is.New(t)
	cols := goldenRow[:6:6] // ts, ts_gap_la, ts_gap_nz, tstz, tstz_offset, date

	var fields []pgconn.FieldDescription
	for _, c := range cols {
		fields = append(fields, pgconn.FieldDescription{Name: c.name, DataTypeOID: c.oid, TypeModifier: -1})
	}
	sch, err := schema.Avro.Extract("ts_payload", fields)
	is.NoErr(err)
	serde, err := cavro.Parse([]byte(sch.String()))
	is.NoErr(err)

	got, err := serde.Marshal(formatRow(t, types.EncodingAvro, cols))
	is.NoErr(err)
	is.Equal(hex.EncodeToString(got), goldenTimestampAvroHex)

	rec := opencdc.Record{Payload: opencdc.Change{After: formatRow(t, types.EncodingNative, cols)}}
	is.Equal(string(rec.Payload.After.Bytes()), goldenTimestampJSON)
}

// TestGoldenAcrossHostZones replays the golden tests in child processes whose
// host time zone is set through TZ. time.Local is initialized once per
// process, so this is the only way to run them under a zone the machine is not
// in without touching the global, and it makes the tests fail on a UTC CI
// machine if the output depends on the host zone.
func TestGoldenAcrossHostZones(t *testing.T) {
	if os.Getenv(goldenChildEnv) != "" {
		t.Skip("running as the child of the host-zone matrix")
	}
	for _, zone := range goldenZones {
		t.Run(strings.ReplaceAll(zone, "/", "_"), func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run", "^TestGolden(JSON|Avro|Format|TimestampOnly|Ranges)$", "-test.count=1", "-test.v") //nolint:gosec // re-executes this test binary
			cmd.Env = append(os.Environ(), "TZ="+zone, goldenChildEnv+"=1")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("TZ=%s: %v\n%s", zone, err, out)
			}
			if !strings.Contains(string(out), "--- PASS: TestGoldenAvro") {
				t.Fatalf("TZ=%s: golden tests did not run\n%s", zone, out)
			}
		})
	}
}

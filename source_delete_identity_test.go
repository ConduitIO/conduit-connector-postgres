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
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
)

// TestSource_DeleteAndKeyChange_ReplicaIdentity is the regression matrix for
// conduit-connector-postgres#379 and #380. It drives NewSource(), so every
// record goes through the SDK's default middleware, including
// SourceWithEncoding, which Avro-encodes Key, Before and After when
// logrepl.withAvroSchema is on (the default). The pre-fix tests stopped at
// the iterator and never encoded a delete, which is how #379 shipped.
//
// Each case runs its change and then an INSERT, and expects both records in
// order: a halt on the first record would also hold back the second.
func TestSource_DeleteAndKeyChange_ReplicaIdentity(t *testing.T) {
	type row = map[string]string // values compared as fmt.Sprint, Avro or not

	fullOld := row{"id": "1", "i": "10", "t": "a"}
	cases := []struct {
		name     string
		ddl      []string // %[1]s is the table, %[2]s an index name
		change   string
		openErr  string // non-empty: Open must fail with this code
		wantOp   opencdc.Operation
		wantKey  row
		wantBef  row // nil: Before must be absent
		wantAftr row
	}{{
		name:    "default_nullable_delete",
		ddl:     []string{`CREATE TABLE %[1]s (id int PRIMARY KEY, i int, t text)`},
		change:  `DELETE FROM %[1]s WHERE id = 1`,
		wantOp:  opencdc.OperationDelete,
		wantKey: row{"id": "1"},
	}, {
		name:    "default_notnull_delete",
		ddl:     []string{`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`},
		change:  `DELETE FROM %[1]s WHERE id = 1`,
		wantOp:  opencdc.OperationDelete,
		wantKey: row{"id": "1"},
	}, {
		name:    "default_pk_only_delete",
		ddl:     []string{`CREATE TABLE %[1]s (id int PRIMARY KEY)`},
		change:  `DELETE FROM %[1]s WHERE id = 1`,
		wantOp:  opencdc.OperationDelete,
		wantKey: row{"id": "1"},
	}, {
		name: "full_delete",
		ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY FULL`,
		},
		change:  `DELETE FROM %[1]s WHERE id = 1`,
		wantOp:  opencdc.OperationDelete,
		wantKey: row{"id": "1"},
		wantBef: fullOld,
	}, {
		name: "using_index_with_pk_delete",
		ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`,
			`CREATE UNIQUE INDEX %[2]s ON %[1]s (i, id)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY USING INDEX %[2]s`,
		},
		change:  `DELETE FROM %[1]s WHERE id = 1`,
		wantOp:  opencdc.OperationDelete,
		wantKey: row{"id": "1"},
	}, {
		name: "using_index_without_pk_refused",
		ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`,
			`CREATE UNIQUE INDEX %[2]s ON %[1]s (i)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY USING INDEX %[2]s`,
		},
		openErr: logrepl.ErrorCodeReplicaIdentityExcludesKey,
	}, {
		name:     "default_key_change_update",
		ddl:      []string{`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`},
		change:   `UPDATE %[1]s SET id = 2 WHERE id = 1`,
		wantOp:   opencdc.OperationUpdate,
		wantKey:  row{"id": "2"},
		wantAftr: row{"id": "2", "i": "10", "t": "a"},
	}, {
		name: "full_key_change_update",
		ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY FULL`,
		},
		change:   `UPDATE %[1]s SET id = 2 WHERE id = 1`,
		wantOp:   opencdc.OperationUpdate,
		wantKey:  row{"id": "2"},
		wantBef:  fullOld,
		wantAftr: row{"id": "2", "i": "10", "t": "a"},
	}}

	for _, tc := range cases {
		for _, withAvro := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/avro=%v", tc.name, withAvro), func(t *testing.T) {
				is := is.New(t)
				ctx := test.Context(t)
				conn := test.ConnectSimple(ctx, t, test.RegularConnString)

				// Table names from RandomIdentifier use the full 63 bytes, so
				// "<table>_ux" would truncate onto the table's own name.
				table := test.RandomIdentifier(t)
				index := fmt.Sprintf("ux_%d", time.Now().UnixNano())
				for _, q := range tc.ddl {
					_, err := conn.Exec(ctx, fmt.Sprintf(q, table, index))
					is.NoErr(err)
				}
				// The one-column table has no i/t; insert only what exists.
				ins := `INSERT INTO %[1]s VALUES (1, 10, 'a')`
				next := `INSERT INTO %[1]s VALUES (9, 90, 'z')`
				if tc.name == "default_pk_only_delete" {
					ins, next = `INSERT INTO %[1]s VALUES (1)`, `INSERT INTO %[1]s VALUES (9)`
				}
				_, err := conn.Exec(ctx, fmt.Sprintf(ins, table))
				is.NoErr(err)

				slot, pub := "s_"+table, "p_"+table
				s := NewSource()
				err = sdk.Util.ParseConfig(ctx, map[string]string{
					"url":                     test.RepmgrConnString,
					"tables":                  table,
					"snapshotMode":            "never",
					"cdcMode":                 "logrepl",
					"logrepl.slotName":        slot,
					"logrepl.publicationName": pub,
					"logrepl.withAvroSchema":  fmt.Sprint(withAvro),
				}, s.Config(), Connector.NewSpecification().SourceParams)
				is.NoErr(err)
				t.Cleanup(func() {
					is.NoErr(s.Teardown(context.Background()))
					_ = logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
						URL: test.RepmgrConnString, SlotName: slot, PublicationName: pub,
					})
				})

				err = s.Open(ctx, nil)
				if tc.openErr != "" {
					is.True(err != nil)
					is.True(strings.HasPrefix(err.Error(), tc.openErr+": "))
					is.True(strings.Contains(err.Error(), "REPLICA IDENTITY FULL")) // the fix is in the message
					return
				}
				is.NoErr(err)

				_, err = conn.Exec(ctx, fmt.Sprintf(tc.change, table))
				is.NoErr(err)
				_, err = conn.Exec(ctx, fmt.Sprintf(next, table))
				is.NoErr(err)

				recs := readNThroughMiddleware(ctx, t, s, 2)
				is.Equal(recs[1].Operation, opencdc.OperationCreate) // the source moved past the change

				got := recs[0]
				is.Equal(got.Operation, tc.wantOp)
				if withAvro {
					// Proof that SourceWithEncoding ran: Avro-on records leave
					// the middleware as bytes.
					_, isRaw := got.Key.(opencdc.RawData)
					is.True(isRaw)
				}
				is.Equal(decodeRow(ctx, t, got, got.Key, true), tc.wantKey)
				is.Equal(decodeRow(ctx, t, got, got.Payload.Before, false), tc.wantBef)
				if tc.wantOp == opencdc.OperationUpdate {
					is.Equal(decodeRow(ctx, t, got, got.Payload.After, false), tc.wantAftr)
				}
			})
		}
	}
}

// readNThroughMiddleware reads n records from s, failing the test on the
// first non-retry error (the #379 halt) or after 20s.
func readNThroughMiddleware(ctx context.Context, t *testing.T, s sdk.Source, n int) []opencdc.Record {
	t.Helper()
	var got []opencdc.Record
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d of %d records", len(got), n)
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		recs, err := s.ReadN(rctx, n-len(got))
		cancel()
		if errors.Is(err, sdk.ErrBackoffRetry) || errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			t.Fatalf("ReadN failed after %d records: %v", len(got), err)
		}
		got = append(got, recs...)
	}
	return got
}

// decodeRow turns key or payload data into a map of fmt.Sprint values,
// decoding it with the record's attached schema when it is Avro bytes. nil
// data decodes to nil.
func decodeRow(ctx context.Context, t *testing.T, rec opencdc.Record, d opencdc.Data, isKey bool) map[string]string {
	t.Helper()
	if d == nil {
		return nil
	}
	var m map[string]any
	switch v := d.(type) {
	case opencdc.StructuredData:
		m = v
	case opencdc.RawData:
		subject, version := rec.Metadata[opencdc.MetadataPayloadSchemaSubject], rec.Metadata[opencdc.MetadataPayloadSchemaVersion]
		if isKey {
			subject, version = rec.Metadata[opencdc.MetadataKeySchemaSubject], rec.Metadata[opencdc.MetadataKeySchemaVersion]
		}
		var ver int
		if _, err := fmt.Sscan(version, &ver); err != nil {
			t.Fatalf("schema version %q: %v", version, err)
		}
		sch, err := sdkschema.Get(ctx, subject, ver)
		if err != nil {
			t.Fatalf("get schema %s v%d: %v", subject, ver, err)
		}
		if err := sch.Unmarshal(v, &m); err != nil {
			t.Fatalf("decode with %s v%d: %v", subject, ver, err)
		}
	default:
		t.Fatalf("unexpected data type %T", d)
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprint(v)
	}
	return out
}

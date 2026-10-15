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
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"go.uber.org/goleak"
)

var pgSeq atomic.Int64

// pgDriver seeds and reads the table directly with SQL, so that the source and
// the destination are tested separately.
type pgDriver struct {
	sdk.ConfigurableAcceptanceTestDriver

	table string
	slot  string
	pub   string
	idSeq atomic.Int64
	cols  map[string]bool
}

func (d *pgDriver) BeforeTest(t *testing.T) {
	d.ConfigurableAcceptanceTestDriver.BeforeTest(t) // honors Skip
	ctx := context.Background()
	n := pgSeq.Add(1)
	d.table = fmt.Sprintf("acc_v2_%d_%d", time.Now().UnixNano()%1000000000, n)
	d.slot = d.table
	d.pub = d.table
	d.cols = map[string]bool{"id": true, "name": true, "score": true, "note": true}
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint primary key, name text, score double precision, note text)`, d.table))
	if err != nil {
		t.Fatal(err)
	}
	// The table is created by the replication user (the source needs it to own
	// the publication), the destination connects as the regular user.
	_, err = conn.Exec(ctx, fmt.Sprintf(`GRANT ALL ON TABLE %s TO meroxauser`, d.table))
	if err != nil {
		t.Fatal(err)
	}
}

func (d *pgDriver) AfterTest(t *testing.T) {
	ctx := context.Background()
	_ = logrepl.Cleanup(ctx, logrepl.CleanupConfig{URL: test.RepmgrConnString, SlotName: d.slot, PublicationName: d.pub})
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+d.table)
}

func (d *pgDriver) SourceConfig(*testing.T) config.Config {
	return config.Config{
		"url":                                test.RepmgrConnString,
		"tables":                             d.table,
		"cdcMode":                            "logrepl",
		"logrepl.slotName":                   d.slot,
		"logrepl.publicationName":            d.pub,
		"logrepl.withAvroSchema":             "false",
		"logrepl.schemaDrift.policy":         "evolve",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
}

func (d *pgDriver) DestinationConfig(*testing.T) config.Config {
	return config.Config{
		"url":                                test.RegularConnString,
		"table":                              d.table,
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
}

func (d *pgDriver) GenerateRecord(_ *testing.T, op opencdc.Operation) opencdc.Record {
	id := d.idSeq.Add(1)
	return opencdc.Record{
		Position:  opencdc.Position(fmt.Sprintf("p-%d-%d", time.Now().UnixNano(), id)),
		Operation: op,
		Metadata:  map[string]string{opencdc.MetadataCollection: d.table},
		Key:       opencdc.StructuredData{"id": id},
		Payload: opencdc.Change{After: opencdc.StructuredData{
			"id":    id,
			"name":  fmt.Sprintf("name-%d", rand.Int63()), //nolint:gosec // test data, not security-sensitive
			"score": rand.Float64(),                       //nolint:gosec // test data, not security-sensitive
			"note":  fmt.Sprintf("note-%d", rand.Int63()), //nolint:gosec // test data, not security-sensitive
		}},
	}
}

func (d *pgDriver) GenerateLargeRecord(t *testing.T, op opencdc.Operation, size int) opencdc.Record {
	r := d.GenerateRecord(t, op)
	r.Payload.After.(opencdc.StructuredData)["note"] = strings.Repeat("x", size)
	return r
}

func (d *pgDriver) WriteToSource(t *testing.T, recs []opencdc.Record) []opencdc.Record {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	// evolve the table for fields it doesn't have yet
	for _, r := range recs {
		for k := range r.Payload.After.(opencdc.StructuredData) {
			if !d.cols[k] {
				_, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %q text`, d.table, k))
				if err != nil {
					t.Fatal(err)
				}
				d.cols[k] = true
			}
		}
	}
	b := &pgx.Batch{}
	for _, r := range recs {
		sd := r.Payload.After.(opencdc.StructuredData)
		var names, ph []string
		var args []any
		for k, v := range sd {
			names = append(names, fmt.Sprintf("%q", k))
			ph = append(ph, fmt.Sprintf("$%d", len(args)+1))
			args = append(args, v)
		}
		b.Queue(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", d.table, strings.Join(names, ","), strings.Join(ph, ",")), args...)
	}
	br := conn.SendBatch(ctx, b)
	for range recs {
		if _, err := br.Exec(); err != nil {
			t.Fatal(err)
		}
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	return recs
}

func (d *pgDriver) ReadAllFromDestination(t *testing.T) []opencdc.Record {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY id", d.table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []opencdc.Record
	fds := rows.FieldDescriptions()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		sd := opencdc.StructuredData{}
		for i, fd := range fds {
			if vals[i] != nil {
				sd[fd.Name] = vals[i]
			}
		}
		out = append(out, opencdc.Record{Payload: opencdc.Change{After: sd}})
	}
	return out
}

func (d *pgDriver) ReadFromDestination(t *testing.T, _ []opencdc.Record) []opencdc.Record {
	return d.ReadAllFromDestination(t)
}

func TestAcceptanceV2(t *testing.T) {
	d := &pgDriver{ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector:        Connector,
			GenerateDataType: sdk.GenerateStructuredData,
			GoleakOptions: []goleak.Option{
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgconn.(*PgConn).asyncClose.func1"),
			},
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 5 * time.Second,
			Capabilities: sdk.AcceptanceCapabilities{
				Destination: sdk.DestinationCapabilities{
					Idempotence:     sdk.IdempotenceYes, // upsert on the primary key
					SchemaEvolution: sdk.SchemaEvolutionReject,
				},
				Source: sdk.SourceCapabilities{
					ExactResume:     true,
					SchemaEvolution: sdk.SchemaEvolutionEvolve, // logrepl.schemaDrift.policy=evolve
				},
				MaxBatchSize: 10000,
			},
		},
	}}
	sdk.AcceptanceTestV2(t, d)
}

// KilledSessionID names the replication slot the source holds.
func (d *pgDriver) KilledSessionID(*testing.T) string { return d.slot }

// ReleaseSession does what the death of the connector process does on the
// server: the walsender holding the slot ends when its connection closes. It
// gets nothing but the slot name and touches nothing else.
func (d *pgDriver) ReleaseSession(ctx context.Context, slot string) error {
	conn, err := pgx.Connect(ctx, test.RepmgrConnString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active", slot)
	return err
}

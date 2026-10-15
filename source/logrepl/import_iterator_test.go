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

package logrepl

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// These call NewCDCIterator directly, without the connector's preflight, so
// they pin the iterator's own guarantees in import mode (#341): it never
// creates a slot or a publication, and it refuses a slot past Lc even when
// the slot moved after every earlier check.

func importFor(lc pglogrepl.LSN) *dbzoffset.Import {
	o := dbzoffset.Offset{LSNCommit: lc, LSN: lc, MessageType: dbzoffset.MessageCommit}
	return &dbzoffset.Import{Offset: o, Plan: dbzoffset.NewPlan(o)}
}

func importConfig(table, slot, pub string, imp *dbzoffset.Import) CDCConfig {
	return CDCConfig{
		SlotName: slot, PublicationName: pub, Tables: []string{table},
		TableKeys: map[string]string{table: "id"}, BatchSize: 2, SchemaDriftPolicy: SchemaDriftPolicyHalt,
		Import: imp,
	}
}

func slotAndPubExist(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot, pub string) (bool, bool) {
	t.Helper()
	var s, p bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1),
		EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $2)`, slot, pub).Scan(&s, &p); err != nil {
		t.Fatal(err)
	}
	return s, p
}

func TestCDCIterator_ImportNeverCreatesSlotOrPublication(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	slot, pub := test.RandomIdentifier(t), test.RandomIdentifier(t)

	_, err := NewCDCIterator(ctx, pool, importConfig(table, slot, pub, importFor(1<<40)))
	is.True(err != nil)
	is.True(errors.Is(err, internal.ErrMissingSlot))
	s, p := slotAndPubExist(ctx, t, pool, slot, pub)
	is.True(!s) // CREATE_REPLICATION_SLOT never ran
	is.True(!p) // CREATE PUBLICATION never ran
}

// A slot that is past Lc when the iterator reads it is refused.
func TestCDCIterator_ImportRefusesSlotPastLc(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	slot := test.RandomIdentifier(t)
	test.CreatePublication(t, pool, slot, []string{table})
	test.CreateReplicationSlot(t, pool, slot)
	var f pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&f))

	_, err := NewCDCIterator(ctx, pool, importConfig(table, slot, slot, importFor(f-1)))
	is.Equal(dbzoffset.CodeOf(err), dbzoffset.CodeSlotAhead)
}

// The slot moves after the iterator read it, before streaming starts: the
// re-check once START_REPLICATION holds the slot refuses it.
func TestCDCIterator_ImportRefusesSlotMovedBeforeStreaming(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	slot := test.RandomIdentifier(t)
	test.CreatePublication(t, pool, slot, []string{table})
	test.CreateReplicationSlot(t, pool, slot)
	var f pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&f))

	it, err := NewCDCIterator(ctx, pool, importConfig(table, slot, slot, importFor(f)))
	is.NoErr(err)
	defer func() { _ = it.Teardown(context.Background()) }()

	// Move the slot past Lc while nothing holds it.
	_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %q (column1) VALUES ('x')", table))
	is.NoErr(err)
	var moved pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT end_lsn FROM pg_replication_slot_advance($1, pg_current_wal_lsn())", slot).Scan(&moved))
	is.True(moved > f)

	startCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	err = it.StartSubscriber(startCtx)
	is.Equal(dbzoffset.CodeOf(err), dbzoffset.CodeSlotAhead)
}

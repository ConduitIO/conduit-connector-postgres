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

//go:build conduitchaos

package chaos

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	postgres "github.com/conduitio/conduit-connector-postgres"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// runRealChild is the actual chaos child: a real *postgres.Source
// (github.com/conduitio/conduit-connector-postgres, this repo's own root
// package), constructed and driven exactly the way
// source_integration_test.go's TestSource_Open/TestSource_Read already
// prove works outside sdk.Serve - sdk.Util.ParseConfig, then Open, then a
// ReadN(ctx, 1) loop - extended (for the first time in this repo, outside a
// real `conduit run`) to also call Ack and Teardown. It never returns:
// every exit path is an explicit os.Exit.
//
// Contract with the parent (harness plan §3.3):
//  1. durable before ack: every record is appended (and fsynced - see
//     Ledger.AppendSync) to the shared ledger strictly before this child
//     acks it upstream via Source.Ack.
//  2. single-writer ack loop: one goroutine, reading and acking one record
//     at a time, so "N ACKED lines observed" is an unambiguous progress
//     signal for the parent.
//  3. markers on stdout as single, unbuffered-in-practice writes (fmt.Printf
//     builds the whole line before issuing one Write) - logs never go to
//     stdout, only to stderr, so the parent's marker-line scanner never has
//     to filter noise.
func runRealChild() {
	ctx := context.Background()

	total, err := strconv.ParseUint(os.Getenv(envTotal), 10, 64)
	if err != nil {
		childFatalf("bad %s=%q: %v", envTotal, os.Getenv(envTotal), err)
	}
	batchSize, err := strconv.Atoi(os.Getenv(envBatchSize))
	if err != nil {
		childFatalf("bad %s=%q: %v", envBatchSize, os.Getenv(envBatchSize), err)
	}
	run, err := strconv.Atoi(os.Getenv(envRun))
	if err != nil {
		childFatalf("bad %s=%q: %v", envRun, os.Getenv(envRun), err)
	}

	table := requireEnv(envTable)
	url := requireEnv(envURL)
	slot := requireEnv(envSlot)
	pub := requireEnv(envPub)
	ledgerPath := requireEnv(envLedger)

	ledger, err := OpenLedger(ledgerPath)
	if err != nil {
		childFatalf("open ledger: %v", err)
	}

	src := postgres.NewSource()
	cfg := config.Config{
		"url":                     url,
		"tables":                  table,
		"snapshotMode":            "initial",
		"cdcMode":                 "logrepl",
		"logrepl.slotName":        slot,
		"logrepl.publicationName": pub,
		"logrepl.autoCleanup":     "false", // this harness owns slot/publication cleanup (reaper.go)
		// The connector's own avro encoding (independent of, and not what
		// F-1 flagged - see doc.go) chokes on a NULL value in a non-nullable
		// numeric Avro field, which the seeded test table
		// (test.SetupTestTableWithName) deliberately includes to exercise
		// elsewhere. Avro schema attachment isn't what this scenario is
		// proving; disabling it keeps the smoke test focused on Ack/batch/
		// handoff instead of an unrelated pre-existing encoding edge case.
		//
		// TODO(#326): this is broader than "a NULL numeric column" - Avro
		// schema extraction emits a non-nullable field type for ANY
		// nullable, bytes-backed-logical-type Postgres column, and
		// WithAvroSchema defaults to true (source/config.go), so this is
		// the connector's shipped default failing against its own standard
		// test table. B0-3/B0-4 must make an explicit decision about
		// whether to keep withAvroSchema=false here or fix #326 first,
		// rather than silently inheriting this workaround - DBZ-3 Area 2 is
		// specifically about schema behaviour across a restart, and
		// proving crash-safety with schema attachment off is a narrower
		// claim than it reads as.
		"logrepl.withAvroSchema": "false",
		"sdk.batch.size":         strconv.Itoa(batchSize),
	}
	if batchSize > 0 {
		// Required whenever sdk.batch.size > 0 - see
		// SourceWithBatch's own warning in conduit-connector-sdk - and
		// exercises exactly the read-ahead goroutine finding F-1 flagged
		// as unverified outside sdk.Serve.
		cfg["sdk.batch.delay"] = "20ms"
	}

	if err := sdk.Util.ParseConfig(ctx, cfg, src.Config(), postgres.Connector.NewSpecification().SourceParams); err != nil {
		childFatalf("parse config: %v", err)
	}

	if err := src.Open(ctx, nil); err != nil {
		childFatalf("open: %v", err)
	}
	printMarker("OPENED")

	var acked uint64
	for acked < total {
		recs, err := src.ReadN(ctx, 1)
		if err != nil {
			childFatalf("readn (after %d acked): %v", acked, err)
		}

		for _, rec := range recs {
			entry, err := buildLedgerEntry(run, table, rec)
			if err != nil {
				childFatalf("build ledger entry: %v", err)
			}

			// Invariant 1 / F-6 model (Ledger.AppendSync's doc comment):
			// durable-before-ack. This append (and its fsync, inside
			// AppendSync) MUST complete before Ack is called below.
			if _, err := ledger.AppendSync(entry); err != nil {
				childFatalf("ledger append: %v", err)
			}

			if err := src.Ack(ctx, rec.Position); err != nil {
				childFatalf("ack: %v", err)
			}

			acked++
			printMarker("ACKED %d", acked)

			if acked >= total {
				break
			}
		}
	}

	printMarker("DONE")

	if err := src.Teardown(ctx); err != nil {
		childFatalf("teardown: %v", err)
	}
	if err := ledger.Close(); err != nil {
		childFatalf("close ledger: %v", err)
	}

	os.Exit(0)
}

// buildLedgerEntry decodes rec's position to classify it as a snapshot or
// CDC delivery and derive a DeliveryKey. It deliberately reads the DECODED
// position - never rec.Key or rec.Metadata's collection name - for the
// delivery identity: a snapshot delivery's key is its cursor's LastRead
// (the row-ordinal the snapshot iterator itself is authoritative about) and
// a CDC delivery's key is its LSN, both of which come from the exact same
// position bytes source_integration_test.go and every production position
// round-trip already rely on, so the ledger analyzer never needs its own
// understanding of table schema.
func buildLedgerEntry(run int, table string, rec opencdc.Record) (LedgerEntry, error) {
	pos, err := position.ParseSDKPosition(rec.Position)
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("parse position: %w", err)
	}

	entry := LedgerEntry{
		Run:         run,
		Table:       table,
		RawPosition: base64.StdEncoding.EncodeToString(rec.Position),
		Resumed:     rec.Metadata[snapshot.MetadataSnapshotResumed] == "true",
	}
	// rec.Key, not the decoded position's snapshot/CDC identity: this is the
	// ROW's own business key (the connector's default "id" column), captured
	// so a scenario can assert the exact set of rows delivered - see
	// LedgerEntry.Key's doc comment for why that is a different property
	// than DeliveryKey proves. Both the snapshot and CDC iterators always
	// set rec.Key (source/snapshot/fetch_worker.go, source/logrepl), but
	// guard the nil case anyway rather than let a future connector change
	// that stops doing so panic here instead of failing the test loudly.
	if rec.Key != nil {
		entry.Key = string(rec.Key.Bytes())
	}

	switch pos.Type {
	case position.TypeSnapshot:
		sp, ok := pos.Snapshots[table]
		if !ok {
			return LedgerEntry{}, fmt.Errorf("snapshot position has no entry for table %q: %+v", table, pos.Snapshots)
		}
		entry.Op = "snapshot"
		entry.DeliveryKey = fmt.Sprintf("snapshot:%d", sp.LastRead)
	case position.TypeCDC:
		entry.Op = "cdc"
		entry.DeliveryKey = fmt.Sprintf("cdc:%s", pos.LastLSN)
	default:
		return LedgerEntry{}, fmt.Errorf("unexpected position type %q (raw position %q)", pos.Type, rec.Position)
	}

	return entry, nil
}

// runEchoChild is a trivial, Postgres-free child mode used only by
// harness_test.go to prove the spawn/wait/marker/sigkill plumbing itself
// (harness.go) independently of whether the real Postgres-backed child
// works - so a harness bug and a connector bug never masquerade as each
// other. It calls chaospoint.Reach envEchoReaches times (default 5),
// printing a REACHED marker after each non-parking reach; if PGCHAOS_PARK
// targets SnapshotFetchRow's Nth reach, chaospoint.Reach itself prints
// PARKED and blocks forever, exactly as it would for the real child (see
// internal/chaospoint).
func runEchoChild() {
	n, err := strconv.Atoi(os.Getenv(envEchoReaches))
	if err != nil || n <= 0 {
		n = 5
	}

	printMarker("OPENED")
	for i := 1; i <= n; i++ {
		chaospoint.Reach(chaospoint.SnapshotFetchRow)
		printMarker("REACHED %d", i)
	}
	printMarker("DONE")

	os.Exit(0)
}

// printMarker writes one line to stdout via a single fmt.Printf call
// (fmt.Printf builds the complete formatted string before issuing one
// Write), so the parent's line-scanner (harness.go) never observes a torn
// partial marker.
func printMarker(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

// childFatalf reports a child-side failure on stderr (never stdout - the
// parent's marker scanner only reads stdout) with a load-bearing prefix
// that makes a broken child unmistakable in CI logs, and exits nonzero.
func childFatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "CHILD_FATAL: "+format+"\n", args...)
	os.Exit(1)
}

// requireEnv reads name or childFatalf's loudly - every string-valued env
// var runRealChild depends on goes through this, so a missing value fails
// immediately with a clear "missing PGCHAOS_X" message instead of flowing
// silently into sdk.Util.ParseConfig/Source.Open as an empty string and
// surfacing later as a confusing, indirect connector error.
func requireEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		childFatalf("missing %s", name)
	}
	return v
}

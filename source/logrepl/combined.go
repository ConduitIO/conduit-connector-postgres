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

package logrepl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
)

type iterator interface {
	NextN(context.Context, int) ([]opencdc.Record, error)
	Ack(context.Context, opencdc.Position) error
	Teardown(context.Context) error
}

type CombinedIterator struct {
	conf Config
	pool *pgxpool.Pool

	// runCtx is the context the iterator was opened with. The CDC subscription
	// outlives any single NextN call, so it must run on this context and never
	// on a NextN context: when the snapshot handoff happens inside NextN, using
	// that call's context tied the subscription's lifetime to one ReadN
	// deadline, and after it expired the subscription spun and could not be
	// stopped (#347).
	runCtx context.Context

	// beforeHandoff is a test seam, nil in production. It runs at the top of
	// useCDCIterator, before the snapshot iterator is taken out.
	beforeHandoff func()

	cdcIterator *CDCIterator

	// iterMu guards snapshotIterator and activeIterator. NextN switches from
	// the snapshot to the CDC iterator (useCDCIterator) while Teardown and Ack
	// run on other goroutines (#352). The snapshot iterator is torn down by
	// whoever takes it out under the lock, so exactly once.
	iterMu           sync.Mutex
	tornDown         bool // Teardown ran: a snapshot iterator is never put back
	snapshotIterator *snapshot.Iterator
	activeIterator   iterator

	// snapshotLowWatermarkLSN is the effective snapshot low watermark for this
	// run (DBZ-3 Area 1): the persisted watermark on a resume, or the slot's
	// consistent point captured at fresh slot creation on a first run. It is
	// stored here so useCDCIterator can re-seed the CDC handler with it at the
	// snapshot->CDC handoff, ensuring every CDC-mode position carries the
	// watermark forward even on a first-run same-run handoff. Empty when this run
	// does not snapshot (WithSnapshot=false or resuming directly in CDC mode).
	snapshotLowWatermarkLSN string
}

type Config struct {
	Position          opencdc.Position
	SlotName          string
	PublicationName   string
	Tables            []string
	TableKeys         map[string]string
	WithSnapshot      bool
	WithAvroSchema    bool
	BatchSize         int
	SchemaDriftPolicy SchemaDriftPolicy
	// AllowWALGap accepts the data loss of resuming from a saved position on a
	// replication slot that no longer exists (see checkSlotCoversPosition).
	AllowWALGap bool
	// IdentityCheckTimeout and BeaconFailureTimeout tune the slot identity
	// (#442); zero means the default. See CDCConfig.
	IdentityCheckTimeout time.Duration
	BeaconFailureTimeout time.Duration
	// Import is the Debezium offset import (#341), set only for a pipeline
	// with no position, after dbzoffset.Preflight passed. WithSnapshot is
	// false with it.
	Import *dbzoffset.Import
}

// defaultCDCBatchSize is the CDC streaming batch size the connector falls back
// to when sdk.batch.size is unset (0). The SDK's SourceWithBatch default is 0,
// which the SDK documents as "read 1 record at a time" — its source run loop
// literally calls ReadN with n=1 (see conduit-connector-sdk source.go runRead)
// — and with a 0 the CDCHandler flushes after every single record. A
// record-at-a-time source forces the engine into a record-at-a-time pass
// (pkg/lifecycle-poc/funnel BenchmarkEnginePass measures ~790ns/record at
// batch 1 vs ~295ns/record at batch 1000) and, because the source batch size
// is also the destination's write granularity when the destination has no
// batching of its own, a record-at-a-time destination round-trip.
//
// 1000 matches the v0.21 CDC-parity benchmark's CONDUIT_BATCH_SIZE and is the
// point where the engine's per-pass cost has flattened. Memory bound: a
// full batch is 1000 opencdc.Records (each with a JSON position, a key and a
// 2-column payload, ~2KB including the position) ≈ 2MB per pipeline, plus the
// handler's single buffered output channel. Latency bound: a partial batch is
// flushed by the handler's 1s flush timer (CDCHandler.flushInterval), so a
// sparse stream is delayed at most ~1s — the same bound the benchmark's
// explicit sdk.batch.size: 1000 already has.
const defaultCDCBatchSize = 1000

// effectiveCDCBatchSize returns the CDC streaming batch size, substituting the
// connector default when sdk.batch.size is unset (0). It is applied only to the
// CDC handler; the snapshot fetch size is a separate knob (snapshot.fetchSize,
// default 50000) and keeps its own 0-means-default handling.
func effectiveCDCBatchSize(n int) int {
	if n <= 0 {
		return defaultCDCBatchSize
	}
	return n
}

// Validate performs validation tasks on the config.
func (c Config) Validate() error {
	var errs []error
	// make sure we have all table keys
	for _, tableName := range c.Tables {
		if c.TableKeys[tableName] == "" {
			errs = append(errs, fmt.Errorf("missing key for table %q", tableName))
		}
	}

	if _, err := ParseSchemaDriftPolicy(string(c.SchemaDriftPolicy)); err != nil {
		errs = append(errs, err)
	}
	if c.Import != nil && c.WithSnapshot {
		errs = append(errs, errors.New("an offset import cannot be combined with a snapshot"))
	}

	return errors.Join(errs...)
}

// NewCombinedIterator will initialize and start the Snapshot and CDC iterators.
// Failure to parse the position or validate the config will return an error.
func NewCombinedIterator(ctx context.Context, pool *pgxpool.Pool, conf Config) (*CombinedIterator, error) {
	pos, err := position.ParseSDKPosition(conf.Position)
	if err != nil {
		sdk.Logger(ctx).Debug().
			Err(err).
			Int("position_bytes", len(conf.Position)).
			Msg("failed to parse position") // never the position itself: it holds the slot identity key (#442)

		return nil, fmt.Errorf("failed to create logrepl iterator: %w", err)
	}

	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate logrepl config: %w", err)
	}

	c := &CombinedIterator{
		conf:   conf,
		pool:   pool,
		runCtx: ctx,
	}

	// Initialize the CDC iterator. This creates (or, on a resume, reuses) the
	// replication slot, so the slot's consistent point is only known afterwards.
	if err := c.initCDCIterator(ctx, pos); err != nil {
		return nil, err
	}

	// DBZ-3 Area 1: determine the effective snapshot low watermark before starting
	// the snapshot. On a fresh slot creation (TXSnapshotID present) the slot's
	// restart_lsn is the consistent point the exported snapshot is correlated with,
	// so it becomes the low watermark. On a resume the watermark is already carried
	// in the persisted position and must NOT be overwritten (the slot's current
	// restart_lsn may have advanced past the original snapshot point). Only capture
	// it when this run will actually snapshot; a pure CDC start has no snapshot to
	// reconcile against.
	// #442: snapshot records carry the run's slot identity too, so a restart
	// mid-snapshot can recognise its slot.
	pos.SlotIdentity = c.cdcIterator.SlotIdentity()
	pos.SystemIdentifier, pos.Timeline = c.cdcIterator.walHistory.SystemID, c.cdcIterator.walHistory.Timeline

	willSnapshot := c.conf.WithSnapshot && pos.Type != position.TypeCDC
	if willSnapshot && pos.SnapshotLowWatermarkLSN == "" && c.cdcIterator.TXSnapshotID() != "" {
		pos.SnapshotLowWatermarkLSN = c.cdcIterator.LowWatermarkLSN().String()
	}
	c.snapshotLowWatermarkLSN = pos.SnapshotLowWatermarkLSN

	// Initialize the snapshot iterator when snapshotting is enabled and not completed.
	// The CDC iterator must be initialized first when snapshotting is requested.
	if err := c.initSnapshotIterator(ctx, pos); err != nil {
		c.abortOpen(err)
		return nil, err
	}

	switch {
	case c.snapshotIterator != nil:
		c.setActive(c.snapshotIterator)
	default:
		if err := c.cdcIterator.StartSubscriber(ctx); err != nil {
			err = fmt.Errorf("failed to start CDC iterator: %w", err)
			c.abortOpen(err)
			return nil, err
		}

		c.setActive(c.cdcIterator)
	}

	return c, nil
}

// abortOpen releases everything NewCombinedIterator created when it fails after
// initCDCIterator. The caller never gets the iterator, so nobody else would
// ever call Teardown: without this the subscription keeps its pooled
// replication connection (a walsender on the server) for the life of the
// process, and pool.Close blocks on it. The caller's context may already be
// done, so teardown gets its own bounded context.
func (c *CombinedIterator) abortOpen(cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.runCtx), abortOpenTimeout)
	defer cancel()
	if err := c.Teardown(ctx); err != nil {
		sdk.Logger(ctx).Warn().Err(err).AnErr("cause", cause).Msg("failed to clean up after a failed open")
	}
}

// abortOpenTimeout bounds the cleanup in abortOpen.
const abortOpenTimeout = 10 * time.Second

// NextN retrieves up to n records from the active iterator.
// If the end of the snapshot is reached during this call, it will switch to the CDC iterator
// and continue retrieving records from there.
func (c *CombinedIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	records, err := c.active().NextN(ctx, n)
	if err != nil {
		if !errors.Is(err, snapshot.ErrIteratorDone) {
			return nil, fmt.Errorf("failed to fetch records in batch: %w", err)
		}

		// Snapshot iterator is done, handover to CDC iterator
		if err := c.useCDCIterator(ctx); err != nil {
			return nil, err
		}

		sdk.Logger(ctx).Debug().Msg("Snapshot completed, switching to CDC mode")
		return c.NextN(ctx, n)
	}

	return records, nil
}

func (c *CombinedIterator) Ack(ctx context.Context, p opencdc.Position) error {
	return c.active().Ack(ctx, p)
}

// Teardown will stop and teardown the CDC and Snapshot iterators.
func (c *CombinedIterator) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)

	c.iterMu.Lock()
	c.tornDown = true
	c.iterMu.Unlock()

	var errs []error

	if c.cdcIterator != nil {
		if err := c.cdcIterator.Teardown(ctx); err != nil {
			logger.Warn().Err(err).Msg("Failed to tear down cdc iterator")
			errs = append(errs, fmt.Errorf("failed to teardown cdc iterator: %w", err))
		}
	}

	if snap := c.takeSnapshotIterator(); snap != nil {
		if err := snap.Teardown(ctx); err != nil {
			logger.Warn().Err(err).Msg("Failed to tear down snapshot iterator")
			errs = append(errs, fmt.Errorf("failed to teardown snapshot iterator: %w", err))
		}
	}

	return errors.Join(errs...)
}

// initCDCIterator initializes the CDC iterator, which will create the replication slot.
// When snapshotting is disabled or the last known position is of CDC type, the iterator
// will start to consume CDC events from the created slot.
// Returns error when:
// * LSN position cannot be parsed,
// * The CDC iterator fails to initalize or fail to start.
func (c *CombinedIterator) initCDCIterator(ctx context.Context, pos position.Position) error {
	lsn, err := pos.LSN()
	if err != nil {
		return fmt.Errorf("failed to parse LSN in position: %w", err)
	}
	commitLSN, err := pos.TxCommit()
	if err != nil {
		return fmt.Errorf("failed to parse commit LSN in position: %w", err)
	}

	cdcIterator, err := NewCDCIterator(ctx, c.pool, CDCConfig{
		LSN:             lsn,
		TxCommitLSN:     commitLSN,
		TxSeq:           pos.TxSeq,
		SlotName:        c.conf.SlotName,
		PublicationName: c.conf.PublicationName,
		Tables:          c.conf.Tables,
		TableKeys:       c.conf.TableKeys,
		WithAvroSchema:  c.conf.WithAvroSchema,
		BatchSize:       effectiveCDCBatchSize(c.conf.BatchSize),
		// Seed the handler with the start position so DBZ-3 carry-forward fields
		// (e.g. SnapshotLowWatermarkLSN) survive across the snapshot->CDC handoff
		// and every subsequent CDC restart.
		StartPosition:        pos,
		SchemaDriftPolicy:    c.conf.SchemaDriftPolicy,
		Import:               c.conf.Import,
		AllowWALGap:          c.conf.AllowWALGap,
		IdentityCheckTimeout: c.conf.IdentityCheckTimeout,
		BeaconFailureTimeout: c.conf.BeaconFailureTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create CDC iterator: %w", err)
	}

	c.cdcIterator = cdcIterator

	return nil
}

// initSnapshotIterator initializes the Snapshot iterator. The CDC iterator must be initalized.
func (c *CombinedIterator) initSnapshotIterator(ctx context.Context, pos position.Position) error {
	if !c.conf.WithSnapshot || pos.Type == position.TypeCDC {
		return nil
	}

	if c.cdcIterator == nil {
		return fmt.Errorf("CDC iterator needs to be initialized before snapshot")
	}

	snapshotIterator, err := snapshot.NewIterator(ctx, c.pool, snapshot.Config{
		// Pass the enriched position (carrying the captured/persisted low
		// watermark) rather than c.conf.Position, so every snapshot record
		// carries SnapshotLowWatermarkLSN forward (DBZ-3 Area 1).
		Position:       pos.ToSDKPosition(),
		Tables:         c.conf.Tables,
		TableKeys:      c.conf.TableKeys,
		TXSnapshotID:   c.cdcIterator.TXSnapshotID(),
		FetchSize:      c.conf.BatchSize,
		WithAvroSchema: c.conf.WithAvroSchema,
		// A snapshot-typed start position means a prior run already persisted
		// snapshot progress: this is a resume, so tag emitted records
		// accordingly (DBZ-3 Area 1). A first run starts from an initial/empty
		// position and is therefore not tagged.
		SnapshotResumed: pos.Type == position.TypeSnapshot,
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("Initial snapshot requested, starting..")

	c.snapshotIterator = snapshotIterator

	return nil
}

// useCDCIterator will activate and start the CDC iterator. The snapshot iterator
// will be torn down if initialized.
func (c *CombinedIterator) useCDCIterator(ctx context.Context) error {
	// Invariant 2 / DBZ-3 B0 kill point (chaospoint.PreStartSubscriber): the
	// FIRST statement here, before the snapshot iterator is torn down,
	// before the low watermark is re-seeded, and before StartSubscriber. A
	// kill landing exactly here proves the snapshot->CDC handoff boundary:
	// every table's snapshot progress is already persisted (this method is
	// only reached after NextN observed ErrIteratorDone), but CDC has not
	// yet consumed or acked anything, so recovery must resume the CDC slot
	// at RestartLSN, never mid-handoff. No-op outside the conduitchaos
	// build (see internal/chaospoint).
	chaospoint.Reach(chaospoint.PreStartSubscriber)
	if c.beforeHandoff != nil {
		c.beforeHandoff()
	}

	if snap := c.takeSnapshotIterator(); snap != nil {
		if err := snap.Teardown(ctx); err != nil {
			// Keep the iterator so the next NextN retries the handoff instead
			// of losing both the error and the iterator.
			if !c.restoreSnapshotIterator(snap) {
				// Teardown already ran and will not see this iterator again.
				_ = snap.Teardown(ctx)
			}
			return fmt.Errorf("failed to teardown snapshot iterator during switch: %w", err)
		}
	}

	c.setActive(c.cdcIterator)

	// DBZ-3 Area 1 (load-bearing): re-seed the CDC handler's low watermark at the
	// snapshot->CDC handoff, BEFORE StartSubscriber launches the subscription
	// goroutine. On a first run the watermark was captured only after the CDC
	// iterator (and its handler) were constructed, so the handler's base position
	// does not yet carry it; without this re-seed the watermark would ride
	// snapshot records but be dropped the instant CDC took over — criterion 11
	// would hold for the resumed case but silently regress on the first-run
	// same-run handoff (the "intermittent regression" the design doc warns about).
	// Re-seeding here is race-free: Handle (the only reader of the base position)
	// does not run until StartSubscriber below.
	//
	// Invariant 1 (no early ack / WAL not pruned past unpersisted data): starting
	// the subscriber here is the FIRST point CDC consumes or acks anything, so the
	// slot's confirmed_flush_lsn cannot have advanced past the low watermark during
	// the snapshot — it advances only via CDCIterator.Ack after the engine durably
	// persists a record, all of which happens strictly after this handoff. This
	// re-seed reads/writes only in-process position state and does not touch the
	// ack or SendStandbyStatusUpdate path, so it cannot advance the slot early.
	c.cdcIterator.SetSnapshotLowWatermarkLSN(c.snapshotLowWatermarkLSN)

	if err := c.cdcIterator.StartSubscriber(c.runCtx); err != nil {
		return fmt.Errorf("failed to start CDC iterator: %w", err)
	}

	return nil
}

func (c *CombinedIterator) active() iterator {
	c.iterMu.Lock()
	defer c.iterMu.Unlock()
	return c.activeIterator
}

func (c *CombinedIterator) setActive(it iterator) {
	c.iterMu.Lock()
	defer c.iterMu.Unlock()
	c.activeIterator = it
}

// takeSnapshotIterator removes and returns the snapshot iterator (nil if it
// is already gone), so only one caller ever tears it down.
func (c *CombinedIterator) takeSnapshotIterator() *snapshot.Iterator {
	c.iterMu.Lock()
	defer c.iterMu.Unlock()
	snap := c.snapshotIterator
	c.snapshotIterator = nil
	return snap
}

// restoreSnapshotIterator puts snap back so the next handoff attempt retries
// its teardown. It reports false, and does not store snap, if Teardown already
// ran (nobody would ever tear the restored iterator down) or the slot was
// refilled; the caller then tears snap down itself.
func (c *CombinedIterator) restoreSnapshotIterator(snap *snapshot.Iterator) bool {
	c.iterMu.Lock()
	defer c.iterMu.Unlock()
	if c.tornDown || c.snapshotIterator != nil {
		return false
	}
	c.snapshotIterator = snap
	return true
}

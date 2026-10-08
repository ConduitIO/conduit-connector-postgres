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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
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

	cdcIterator      *CDCIterator
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

	return errors.Join(errs...)
}

// NewCombinedIterator will initialize and start the Snapshot and CDC iterators.
// Failure to parse the position or validate the config will return an error.
func NewCombinedIterator(ctx context.Context, pool *pgxpool.Pool, conf Config) (*CombinedIterator, error) {
	pos, err := position.ParseSDKPosition(conf.Position)
	if err != nil {
		sdk.Logger(ctx).Debug().
			Err(err).
			Msgf("failed to parse position: %s", string(conf.Position))

		return nil, fmt.Errorf("failed to create logrepl iterator: %w", err)
	}

	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate logrepl config: %w", err)
	}

	c := &CombinedIterator{
		conf: conf,
		pool: pool,
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
	willSnapshot := c.conf.WithSnapshot && pos.Type != position.TypeCDC
	if willSnapshot && pos.SnapshotLowWatermarkLSN == "" && c.cdcIterator.TXSnapshotID() != "" {
		pos.SnapshotLowWatermarkLSN = c.cdcIterator.LowWatermarkLSN().String()
	}
	c.snapshotLowWatermarkLSN = pos.SnapshotLowWatermarkLSN

	// Initialize the snapshot iterator when snapshotting is enabled and not completed.
	// The CDC iterator must be initialized first when snapshotting is requested.
	if err := c.initSnapshotIterator(ctx, pos); err != nil {
		return nil, err
	}

	switch {
	case c.snapshotIterator != nil:
		c.activeIterator = c.snapshotIterator
	default:
		if err := c.cdcIterator.StartSubscriber(ctx); err != nil {
			return nil, fmt.Errorf("failed to start CDC iterator: %w", err)
		}

		c.activeIterator = c.cdcIterator
	}

	return c, nil
}

// NextN retrieves up to n records from the active iterator.
// If the end of the snapshot is reached during this call, it will switch to the CDC iterator
// and continue retrieving records from there.
func (c *CombinedIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	records, err := c.activeIterator.NextN(ctx, n)
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
	return c.activeIterator.Ack(ctx, p)
}

// Teardown will stop and teardown the CDC and Snapshot iterators.
func (c *CombinedIterator) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)

	var errs []error

	if c.cdcIterator != nil {
		if err := c.cdcIterator.Teardown(ctx); err != nil {
			logger.Warn().Err(err).Msg("Failed to tear down cdc iterator")
			errs = append(errs, fmt.Errorf("failed to teardown cdc iterator: %w", err))
		}
	}

	if c.snapshotIterator != nil {
		if err := c.snapshotIterator.Teardown(ctx); err != nil {
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

	cdcIterator, err := NewCDCIterator(ctx, c.pool, CDCConfig{
		LSN:             lsn,
		SlotName:        c.conf.SlotName,
		PublicationName: c.conf.PublicationName,
		Tables:          c.conf.Tables,
		TableKeys:       c.conf.TableKeys,
		WithAvroSchema:  c.conf.WithAvroSchema,
		BatchSize:       c.conf.BatchSize,
		// Seed the handler with the start position so DBZ-3 carry-forward fields
		// (e.g. SnapshotLowWatermarkLSN) survive across the snapshot->CDC handoff
		// and every subsequent CDC restart.
		StartPosition:     pos,
		SchemaDriftPolicy: c.conf.SchemaDriftPolicy,
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

	if c.snapshotIterator != nil {
		if err := c.snapshotIterator.Teardown(ctx); err != nil {
			return fmt.Errorf("failed to teardown snapshot iterator during switch: %w", err)
		}
	}

	c.activeIterator, c.snapshotIterator = c.cdcIterator, nil

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

	if err := c.cdcIterator.StartSubscriber(ctx); err != nil {
		return fmt.Errorf("failed to start CDC iterator: %w", err)
	}

	return nil
}

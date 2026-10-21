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
}

type Config struct {
	Position        opencdc.Position
	SlotName        string
	PublicationName string
	Tables          []string
	TableKeys       map[string]string
	WithSnapshot    bool
	WithAvroSchema  bool
	BatchSize       int
	// AllowWALGap accepts the data loss of resuming from a saved position on a
	// replication slot that no longer exists (see checkSlot).
	AllowWALGap bool
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
		conf:   conf,
		pool:   pool,
		runCtx: ctx,
	}

	// Initialize the CDC iterator.
	if err := c.initCDCIterator(ctx, pos); err != nil {
		return nil, err
	}

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
		BatchSize:       c.conf.BatchSize,
		// The saved position decides whether a missing slot is a gap (see
		// checkSlot).
		StartPosition: pos,
		AllowWALGap:   c.conf.AllowWALGap,
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
		Position:       c.conf.Position,
		Tables:         c.conf.Tables,
		TableKeys:      c.conf.TableKeys,
		TXSnapshotID:   c.cdcIterator.TXSnapshotID(),
		FetchSize:      c.conf.BatchSize,
		WithAvroSchema: c.conf.WithAvroSchema,
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

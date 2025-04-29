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
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
)

type iterator interface {
	Next(context.Context) (opencdc.Record, error)
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
}

type Config struct {
	Position          opencdc.Position
	SlotName          string
	PublicationName   string
	Tables            []string
	TableKeys         map[string]string
	WithSnapshot      bool
	WithAvroSchema    bool
	SnapshotFetchSize int
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
		conf: conf,
		pool: pool,
	}

	// Initialize the CDC iterator.
	if err := c.initCDCIterator(ctx, pos); err != nil {
		return nil, err
	}

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

// Next provides the next available record from the snapshot or CDC stream.
// If the end of the snapshot is reached, next will switch to the CDC iterator and retrive
// the next available record. Failure to switch the iterator will return an error.
func (c *CombinedIterator) Next(ctx context.Context) (opencdc.Record, error) {
	r, err := c.activeIterator.Next(ctx)
	if err != nil {
		// Snapshot iterator is done, handover to CDC iterator
		if !errors.Is(err, snapshot.ErrIteratorDone) {
			return opencdc.Record{}, fmt.Errorf("failed to fetch next record: %w", err)
		}

		if err := c.useCDCIterator(ctx); err != nil {
			return opencdc.Record{}, err
		}
		sdk.Logger(ctx).Debug().Msg("Snapshot completed, switching to CDC mode")

		// retry with new iterator
		return c.activeIterator.Next(ctx)
	}

	return r, nil
}

// NextN retrieves up to n records from the active iterator.
// If the end of the snapshot is reached during this call, it will switch to the CDC iterator
// and continue retrieving records from there.
func (c *CombinedIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	// Check if the active iterator already implements NextN (like the CDCIterator)
	if nextN, ok := c.activeIterator.(interface {
		NextN(context.Context, int) ([]opencdc.Record, error)
	}); ok {
		records, err := nextN.NextN(ctx, n)
		if err != nil {
			// Snapshot iterator is done, handover to CDC iterator
			if errors.Is(err, snapshot.ErrIteratorDone) {
				if err := c.useCDCIterator(ctx); err != nil {
					return nil, err
				}
				sdk.Logger(ctx).Debug().Msg("Snapshot completed, switching to CDC mode")

				// Retry with new iterator
				return c.NextN(ctx, n)
			}
			return nil, fmt.Errorf("failed to fetch records in batch: %w", err)
		}
		return records, nil
	}

	// Fall back to using Next() if NextN is not implemented
	var records []opencdc.Record

	// Get the first record (blocking)
	r, err := c.activeIterator.Next(ctx)
	if err != nil {
		// Snapshot iterator is done, handover to CDC iterator
		if errors.Is(err, snapshot.ErrIteratorDone) {
			if err := c.useCDCIterator(ctx); err != nil {
				return nil, err
			}
			sdk.Logger(ctx).Debug().Msg("Snapshot completed, switching to CDC mode")

			// Retry with new iterator
			return c.NextN(ctx, n)
		}
		return nil, fmt.Errorf("failed to fetch next record: %w", err)
	}
	records = append(records, r)

	// Try to get the remaining (n-1) records without blocking
	for len(records) < n {
		select {
		case <-ctx.Done():
			return records, ctx.Err()
		default:
			r, err := c.activeIterator.Next(ctx)
			if err != nil {
				if errors.Is(err, snapshot.ErrIteratorDone) {
					// Return what we have before switching iterators
					return records, nil
				}
				// Return any records we got along with the error
				return records, err
			}
			records = append(records, r)
		}
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
		FetchSize:      c.conf.SnapshotFetchSize,
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
	if c.snapshotIterator != nil {
		if err := c.snapshotIterator.Teardown(ctx); err != nil {
			return fmt.Errorf("failed to teardown snapshot iterator during switch: %w", err)
		}
	}

	c.activeIterator, c.snapshotIterator = c.cdcIterator, nil

	if err := c.cdcIterator.StartSubscriber(ctx); err != nil {
		return fmt.Errorf("failed to start CDC iterator: %w", err)
	}

	return nil
}

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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	internal2 "github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CDCConfig holds configuration values for CDCIterator.
type CDCConfig struct {
	LSN             pglogrepl.LSN
	SlotName        string
	PublicationName string
	Tables          []string
	TableKeys       map[string]string
	WithAvroSchema  bool
	// BatchSize is the maximum size of a batch that will be read from the DB
	// in one go and processed by the CDCHandler.
	BatchSize int
}

// CDCIterator asynchronously listens for events from the logical replication
// slot and returns them to the caller through NextN.
type CDCIterator struct {
	config CDCConfig
	sub    *internal.Subscription

	// batchesCh is a channel shared between this iterator and a CDCHandler,
	// to which the CDCHandler is sending batches of records.
	// Using a shared queue here would be the fastest option. However,
	// we also need to watch for a context that can get cancelled,
	// and for the subscription that can end, so using a channel is
	// the best option at the moment.
	batchesCh chan []opencdc.Record

	// recordsForNextRead contains records from the previous batch (returned by the CDCHandler),
	// that weren't return by this iterator's ReadN method.
	recordsForNextRead []opencdc.Record
}

// NewCDCIterator initializes logical replication by creating the publication and subscription manager.
func NewCDCIterator(ctx context.Context, pool *pgxpool.Pool, c CDCConfig) (*CDCIterator, error) {
	if err := internal.CreatePublication(
		ctx,
		pool,
		c.PublicationName,
		internal.CreatePublicationOptions{Tables: c.Tables},
	); err != nil {
		// If creating the publication fails with code 42710, this means
		// the publication already exists.
		if !internal.IsPgDuplicateErr(err) {
			return nil, err
		}

		sdk.Logger(ctx).Warn().
			Msgf("Publication %q already exists.", c.PublicationName)
	}

	// Using a buffered channel here so that the handler can send a batch
	// to the channel and start building a new batch.
	// This is useful when the first batch in the channel didn't reach BatchSize (which is sdk.batch.size).
	// The handler can prepare the next batch, and the CDCIterator can use them
	// to return the maximum number of records.
	batchesCh := make(chan []opencdc.Record, 1)
	handler := NewCDCHandler(
		ctx,
		internal.NewRelationSet(),
		internal2.NewTableInfoFetcher(pool),
		c.TableKeys,
		batchesCh,
		c.WithAvroSchema,
		c.BatchSize,
		// todo make configurable
		time.Second,
	)

	sub, err := internal.CreateSubscription(
		ctx,
		pool,
		c.SlotName,
		c.PublicationName,
		c.Tables,
		c.LSN,
		handler.Handle,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize subscription: %w", err)
	}

	return &CDCIterator{
		config:    c,
		batchesCh: batchesCh,
		sub:       sub,
	}, nil
}

// StartSubscriber starts the logical replication service in the background.
// Blocks until the subscription becomes ready.
func (i *CDCIterator) StartSubscriber(ctx context.Context) error {
	sdk.Logger(ctx).Info().
		Str("slot", i.config.SlotName).
		Str("publication", i.config.PublicationName).
		Msgf("Starting logical replication at %s", i.sub.StartLSN)

	go func() {
		if err := i.sub.Run(ctx); err != nil {
			sdk.Logger(ctx).Error().
				Err(err).
				Msg("replication exited with an error")
		}
	}()

	<-i.sub.Ready()

	sdk.Logger(ctx).Info().
		Str("slot", i.config.SlotName).
		Str("publication", i.config.PublicationName).
		Msg("Logical replication started")

	return nil
}

// NextN takes and returns up to n records from the queue. NextN is allowed to
// block until either at least one record is available or the context gets canceled.
func (i *CDCIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if !i.subscriberReady() {
		return nil, errors.New("logical replication has not been started")
	}

	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	// First, we check if there are any records from the previous batch
	// that we can start with.
	recs := make([]opencdc.Record, len(i.recordsForNextRead), n)
	copy(recs, i.recordsForNextRead)
	i.recordsForNextRead = nil

	// NextN needs to wait until at least 1 record is available.
	if len(recs) == 0 {
		batch, err := i.nextRecordsBatchBlocking(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch next batch of records (blocking): %w", err)
		}
		recs = batch
	}

	// We add any already available batches (i.e., we're not blocking waiting for any new batches to arrive)
	// to return at most n records.
	for len(recs) < n {
		batch, err := i.nextRecordsBatch(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch next batch of records: %w", err)
		}
		if batch == nil {
			break
		}
		recs = i.appendRecordsWithLimit(recs, batch, n)
	}

	sdk.Logger(ctx).Trace().
		Int("records", len(recs)).
		Int("records_for_next_read", len(i.recordsForNextRead)).
		Msg("CDCIterator.NextN returning records")
	return recs, nil
}

// nextRecordsBatchBlocking waits for the next batch of records to arrive,
// or for the context to be done, or for the subscription to be done,
// whichever comes first.
func (i *CDCIterator) nextRecordsBatchBlocking(ctx context.Context) ([]opencdc.Record, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-i.sub.Done():
		if err := i.sub.Err(); err != nil {
			return nil, fmt.Errorf("logical replication error: %w", err)
		}
		if err := ctx.Err(); err != nil {
			// subscription is done because the context is canceled, we went
			// into the wrong case by chance
			return nil, err
		}
		// subscription stopped without an error and the context is still
		// open, this is a strange case, shouldn't actually happen
		return nil, fmt.Errorf("subscription stopped, no more data to fetch (this smells like a bug)")
	case batch := <-i.batchesCh:
		sdk.Logger(ctx).Trace().
			Int("records", len(batch)).
			Msg("CDCIterator.NextN received batch of records (blocking)")
		return batch, nil
	}
}

func (i *CDCIterator) nextRecordsBatch(ctx context.Context) ([]opencdc.Record, error) {
	select {
	case <-ctx.Done():
		// Return what we have with the error
		return nil, ctx.Err()
	case <-i.sub.Done():
		if err := i.sub.Err(); err != nil {
			return nil, fmt.Errorf("logical replication error: %w", err)
		}
		if err := ctx.Err(); err != nil {
			// Return what we have with the context error
			return nil, err
		}
		// Return what we have with subscription stopped error
		return nil, fmt.Errorf("subscription stopped, no more data to fetch (this smells like a bug)")
	case batch := <-i.batchesCh:
		sdk.Logger(ctx).Trace().
			Int("records", len(batch)).
			Msg("CDCIterator.NextN received batch of records")

		return batch, nil
	default:
		// No more records currently available
		return nil, nil
	}
}

// appendRecordsWithLimit appends records to dst from src, until the given limit is reached,
// or all records from src have been moved.
// If some records from src are not moved (probably because they lack emotions),
// they are saved to recordsForNextRead.
func (i *CDCIterator) appendRecordsWithLimit(dst []opencdc.Record, src []opencdc.Record, limit int) []opencdc.Record {
	if len(src) == 0 || len(dst) > limit {
		return src
	}

	needed := limit - len(dst)
	if needed > len(src) {
		needed = len(src)
	}

	dst = append(dst, src[:needed]...)
	i.recordsForNextRead = src[needed:]

	return dst
}

// Ack forwards the acknowledgment to the subscription.
func (i *CDCIterator) Ack(_ context.Context, sdkPos opencdc.Position) error {
	pos, err := position.ParseSDKPosition(sdkPos)
	if err != nil {
		return err
	}

	if pos.Type != position.TypeCDC {
		return fmt.Errorf("invalid type %q for CDC position", pos.Type.String())
	}

	lsn, err := pos.LSN()
	if err != nil {
		return err
	}

	if lsn == 0 {
		return fmt.Errorf("cannot ack zero position")
	}

	i.sub.Ack(lsn)
	return nil
}

// Teardown stops the CDC subscription and blocks until the subscription is done
// or the context gets canceled. If the subscription stopped with an unexpected
// error, the error is returned.
func (i *CDCIterator) Teardown(ctx context.Context) error {
	if i.sub != nil {
		return i.sub.Teardown(ctx)
	}

	return nil
}

// subscriberReady returns true when the subscriber is running.
func (i *CDCIterator) subscriberReady() bool {
	select {
	case <-i.sub.Ready():
		return true
	default:
		return false
	}
}

// TXSnapshotID returns the transaction snapshot which is received
// when the replication slot is created. The value can be empty, when the
// iterator is resuming.
func (i *CDCIterator) TXSnapshotID() string {
	return i.sub.TXSnapshotID
}

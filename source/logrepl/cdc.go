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
	// StartPosition is the position the connector was started/resumed with. Its
	// DBZ-3 carry-forward fields (e.g. SnapshotLowWatermarkLSN) are threaded into
	// every CDC-mode position the handler emits so they survive the snapshot->CDC
	// handoff and every subsequent CDC restart. See CDCHandler.buildPosition.
	StartPosition position.Position
	// SchemaDriftPolicy is what the connector does on schema drift (halt by
	// default). Validated via ParseSchemaDriftPolicy in Config.Validate.
	SchemaDriftPolicy SchemaDriftPolicy
}

// CDCIterator asynchronously listens for events from the logical replication
// slot and returns them to the caller through NextN.
type CDCIterator struct {
	config  CDCConfig
	sub     *internal.Subscription
	handler *CDCHandler

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
		c.TableKeys,
		batchesCh,
		c.WithAvroSchema,
		c.BatchSize,
		// todo make configurable
		time.Second,
		c.StartPosition,
		c.SchemaDriftPolicy,
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
		handler:   handler,
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

// NextN returns up to n records from the internal channel with records.
// NextN is allowed to block until either at least one record is available
// or the context gets canceled.
func (i *CDCIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if !i.subscriberReady() {
		return nil, errors.New("logical replication has not been started")
	}

	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	// D3 step 4: once the marker's ack has armed the halt, NextN surfaces the
	// terminal drift error instead of records — including any records from a
	// previous batch, which cannot exist past the marker (the marker is the
	// last record emitted, D4). The check is on entry so the error is
	// deterministic regardless of how many records the caller asks for.
	if err := i.handler.driftHaltError(); err != nil {
		return nil, err
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
	if i.handler.driftMarkerPending() {
		// F6 / AC4: while a drift marker is pending, prefer the marker batch
		// over a racing sub.Done (the select would otherwise choose randomly
		// among ready cases) so the subscription dying can never strand the
		// marker — the halt is acked-gated (D3), and a stranded marker means
		// the approval checkpoint is never persisted. Also select the halt
		// channel: if the marker's ack arms the halt while this call is
		// blocked, the terminal error must surface here rather than leaving
		// the caller blocked forever.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case batch := <-i.batchesCh:
			sdk.Logger(ctx).Trace().
				Int("records", len(batch)).
				Msg("CDCIterator.NextN received batch of records (blocking)")
			return batch, nil
		case <-i.handler.driftHaltCh:
			if err := i.handler.driftHaltError(); err != nil {
				return nil, err
			}
			return nil, errors.New("drift halt channel closed without an armed halt (this smells like a bug)")
		}
	}

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
	if i.handler.driftMarkerPending() {
		// F6 / AC4, non-blocking variant: while a marker is pending, a dead
		// subscription must not surface an error that would discard the
		// marker the caller already holds (the NextN top-up loop returns an
		// error immediately, dropping recs). Treat it as no-more-records so
		// the caller returns the marker batch it got.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case batch := <-i.batchesCh:
			sdk.Logger(ctx).Trace().
				Int("records", len(batch)).
				Msg("CDCIterator.NextN received batch of records")
			return batch, nil
		default:
			return nil, nil
		}
	}

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

	// D3 step 3: arming is acked-gated, never sighting-gated — the halt
	// surfaces only once the engine acked the marker (or anything past it),
	// proving the checkpoint the marker carries is durable. This is the
	// boundary the escape hatch depends on: the ack moves the slot's
	// confirmed_flush_lsn to exactly the point the connector has seen and no
	// further, and the engine's persisted position is the operator's approval.
	i.handler.maybeArmDriftHalt(lsn)

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

// LowWatermarkLSN returns the replication slot's restart_lsn captured when the
// slot was read at subscription creation (DBZ-3 Area 1). It is only the snapshot
// low watermark — the consistent point the initial snapshot is correlated with —
// when this run freshly created the slot, i.e. when TXSnapshotID() is non-empty.
// On a resume it reflects the slot's current restart_lsn and must not be used as
// the watermark; the persisted position's SnapshotLowWatermarkLSN is authoritative
// in that case. See CombinedIterator for the gating.
func (i *CDCIterator) LowWatermarkLSN() pglogrepl.LSN {
	return i.sub.RestartLSN
}

// SetSnapshotLowWatermarkLSN re-seeds the SnapshotLowWatermarkLSN that the
// handler carries forward onto every CDC-mode position it builds (DBZ-3 Area 1,
// acceptance criterion 11).
//
// It exists to fix the in-run snapshot->CDC handoff: NewCDCIterator seeds the
// handler's base position once, at construction, from the position the connector
// started with. On a first run that start position has no watermark (it is
// captured only when the slot is created, which happens inside NewCDCIterator
// itself), so without this call the watermark would ride snapshot records but be
// silently dropped the instant CDC took over — criterion 11 would hold only for
// the resumed case, not the first-run same-run handoff. CombinedIterator calls
// this at the handoff (useCDCIterator) with the effective watermark.
//
// Concurrency: this MUST be called before StartSubscriber. The handler's base
// position is only read on the single subscription goroutine (via Handle), which
// does not run until StartSubscriber launches it, so re-seeding beforehand needs
// no locking — the same single-writer-before-start discipline the base position
// relied on at construction.
func (i *CDCIterator) SetSnapshotLowWatermarkLSN(lsn string) {
	i.handler.setBasePositionLowWatermark(lsn)
}

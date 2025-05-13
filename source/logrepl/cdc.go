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
	BatchSize       int
}

// CDCIterator asynchronously listens for events from the logical replication
// slot and returns them to the caller through NextN.
type CDCIterator struct {
	config    CDCConfig
	batchesCh *internal.Blocking[opencdc.Record]

	sub *internal.Subscription
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

	var elems []opencdc.Record
	batchesCh := internal.NewBlocking(elems, internal.WithCapacity(c.BatchSize))
	handler := NewCDCHandler(
		ctx,
		internal.NewRelationSet(),
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

	// Block until at least one record is received or context is canceled
	// todo block until context done, or subscription done
	// recs := make([]opencdc.Record, 0, n)

	recs := i.batchesCh.GetAllWait()

	// recs = append(recs, first)
	//
	// for len(recs) < n {
	// 	rec, err := i.batchesCh.Get()
	// 	if errors.Is(err, internal.ErrNoElementsAvailable) {
	// 		break
	// 	}
	// 	recs = append(recs, rec)
	// }

	sdk.Logger(ctx).Trace().
		Int("records", len(recs)).
		Msg("CDCIterator.NextN returning records")
	return recs, nil
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

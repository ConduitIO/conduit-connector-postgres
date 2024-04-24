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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Config holds configuration values for CDCIterator.
type Config struct {
	Position        sdk.Position
	SlotName        string
	PublicationName string
	Tables          []string
	TableKeys       map[string]string
}

// CDCIterator asynchronously listens for events from the logical replication
// slot and returns them to the caller through Next.
type CDCIterator struct {
	config  Config
	records chan sdk.Record

	sub *internal.Subscription
}

// NewCDCIterator sets up the subscription to a logical replication slot and
// starts a goroutine that listens to events. The goroutine will keep running
// until either the context is canceled or Teardown is called.
func NewCDCIterator(ctx context.Context, connPool *pgxpool.Pool, config Config) (*CDCIterator, error) {
	i := &CDCIterator{
		config:  config,
		records: make(chan sdk.Record),
	}

	conn, err := connPool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	err = i.attachSubscription(ctx, conn.Conn())
	if err != nil {
		return nil, fmt.Errorf("failed to setup subscription: %w", err)
	}

	go i.listen(ctx)

	return i, nil
}

// listen should be called in a goroutine. It starts the subscription and keeps
// it running until the subscription is stopped or the context is canceled.
func (i *CDCIterator) listen(ctx context.Context) {
	sdk.Logger(ctx).Info().
		Str("slot", i.config.SlotName).
		Str("publication", i.config.PublicationName).
		Msg("starting logical replication")

	err := i.sub.Start(ctx)
	if err != nil {
		// log it to be safe we don't miss the error, but use warn level
		// because the error will most probably be still propagated to Conduit
		// and might be recovered from
		sdk.Logger(ctx).Warn().Err(err).Msg("subscription returned an error")
	}
}

// Next returns the next record retrieved from the subscription. This call will
// block until either a record is returned from the subscription, the
// subscription stops because of an error or the context gets canceled.
func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	for {
		select {
		case <-ctx.Done():
			return sdk.Record{}, ctx.Err()
		case <-i.sub.Done():
			if err := i.sub.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("logical replication error: %w", err)
			}
			if err := ctx.Err(); err != nil {
				// subscription is done because the context is canceled, we went
				// into the wrong case by chance
				return sdk.Record{}, err
			}
			// subscription stopped without an error and the context is still
			// open, this is a strange case, shouldn't actually happen
			return sdk.Record{}, fmt.Errorf("subscription stopped, no more data to fetch (this smells like a bug)")
		case r := <-i.records:
			return r, nil
		}
	}
}

// Ack forwards the acknowledgment to the subscription.
func (i *CDCIterator) Ack(_ context.Context, pos sdk.Position) error {
	lsn, err := PositionToLSN(pos)
	if err != nil {
		return fmt.Errorf("failed to parse position: %w", err)
	}
	i.sub.Ack(lsn)
	return nil
}

// Teardown stops the CDC subscription and blocks until the subscription is done
// or the context gets canceled. If the subscription stopped with an unexpected
// error, the error is returned.
func (i *CDCIterator) Teardown(ctx context.Context) error {
	i.sub.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.sub.Done():
		err := i.sub.Err()
		if errors.Is(err, context.Canceled) {
			// this was a controlled stop
			return nil
		} else if err != nil {
			return fmt.Errorf("logical replication error: %w", err)
		}
		return nil
	}
}

// attachSubscription determines the starting LSN and key column of the source
// table and prepares a subscription.
func (i *CDCIterator) attachSubscription(ctx context.Context, conn *pgx.Conn) error {
	var lsn pglogrepl.LSN
	if i.config.Position != nil && string(i.config.Position) != "" {
		var err error
		lsn, err = PositionToLSN(i.config.Position)
		if err != nil {
			return err
		}
	}

	var err error
	if i.config.TableKeys == nil {
		i.config.TableKeys = make(map[string]string, len(i.config.Tables))
	}
	for _, tableName := range i.config.Tables {
		// get unprovided table keys
		if _, ok := i.config.TableKeys[tableName]; ok {
			continue // key was provided manually
		}
		i.config.TableKeys[tableName], err = i.getTableKeys(ctx, conn, tableName)
		if err != nil {
			return fmt.Errorf("failed to find key for table %s (try specifying it manually): %w", tableName, err)
		}
	}

	sub := internal.NewSubscription(
		conn.Config().Config,
		i.config.SlotName,
		i.config.PublicationName,
		i.config.Tables,
		lsn,
		NewCDCHandler(
			internal.NewRelationSet(conn.TypeMap()),
			i.config.TableKeys,
			i.records,
		).Handle,
	)

	i.sub = sub
	return nil
}

// getTableKeys queries the db for the name of the primary key column for a
// table if one exists and returns it.
func (i *CDCIterator) getTableKeys(ctx context.Context, conn *pgx.Conn, tableName string) (string, error) {
	query := `SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name = $1 AND constraint_name LIKE '%_pkey'
		LIMIT 1;`
	row := conn.QueryRow(ctx, query, tableName)

	var colName string
	err := row.Scan(&colName)
	if err != nil {
		return "", fmt.Errorf("getTableKeys query failed: %w", err)
	}

	return colName, nil
}

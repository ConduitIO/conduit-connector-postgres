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
	"strings"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
)

// Config holds configuration values for CDCIterator.
type Config struct {
	SnapshotMode    string
	Position        sdk.Position
	SlotName        string
	PublicationName string
	TableName       string
	KeyColumnName   string
	Columns         []string
}

// Iterator asynchronously listens for events from the logical replication
// slot and returns them to the caller through Next.
type Iterator struct {
	config  Config
	records chan sdk.Record

	snap *SnapshotIterator
	sub  *internal.Subscription
}

// NewIterator sets up the subscription to a logical replication slot and
// starts a goroutine that listens to events. The goroutine will keep running
// until either the context is canceled or Teardown is called.
// * If SnapshotMode is set to `initial`, it waits for a snapshot of the
// configured table to finish before starting its replication subscription.
// * If SnapshotMode is set to `never`, it will skip the snapshot and
// immediately start listening for replication events.
func NewIterator(ctx context.Context, conn *pgx.Conn, config Config) (*Iterator, error) {
	i := &Iterator{
		config:  config,
		records: make(chan sdk.Record),
	}

	err := i.attachSnapshot(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to setup snapshot: %w", err)
	}

	err = i.attachSubscription(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to setup subscription: %w", err)
	}

	go i.listen(ctx)

	return i, nil
}

// listen should be called in a goroutine. It starts the subscription and keeps
// it running until the subscription is stopped or the context is canceled.
// if snapshot mode is set to `initial`, this waits for the snapshot to complete
// before starting the subscription.
func (i *Iterator) listen(ctx context.Context) {
	if i.config.SnapshotMode == "initial" {
		<-i.snap.Done()
	}

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

// Next checks if its snapshot is finished and returns a record from the
// snapshot if it's not. If it is finished, it returns the next record retrieved
// from the subscription. Once it's finished with the snapshot, it blocks until
// either a record is returned from subscription, the subscription stops because
// of an error, or the context is canceled.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	if !i.snap.Finished() {
		r, err := i.snap.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrSnapshotComplete) {
				err := i.snap.Teardown(ctx)
				if err != nil {
					return sdk.Record{}, fmt.Errorf("failed to teardown snapshot connector: %w", err)
				}
				return i.Next(ctx)
			}
			return sdk.Record{}, fmt.Errorf("snapshot iterator failed: %w", err)
		}
		return r, nil
	}

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
func (i *Iterator) Ack(ctx context.Context, pos sdk.Position) error {
	// TODO: Handle position with our own type for distinct CDC and snapshot positions
	if strings.HasPrefix(string(pos), "s:") {
		return i.snap.Ack(ctx, pos)
	}
	lsn, err := PositionToLSN(pos)
	if err != nil {
		return fmt.Errorf("failed to ack: %w", err)
	}
	i.sub.Ack(lsn)
	return nil
}

// Teardown stops the CDC subscription and blocks until the subscription is done
// or the context gets canceled. If the subscription stopped with an unexpected
// error, the error is returned.
func (i *Iterator) Teardown(ctx context.Context) error {
	i.sub.Stop()

	if i.snap != nil {
		sdk.Logger(ctx).Info().Msg("tearing down snapshotter")
		snapErr := i.snap.Teardown(ctx)
		if snapErr != nil {
			sdk.Logger(ctx).
				Info().
				Msgf("failed to teardown snapshot iterator: %v", snapErr)
		}
	}

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
func (i *Iterator) attachSubscription(ctx context.Context, conn *pgx.Conn) error {
	var lsn pglogrepl.LSN
	if i.config.Position != nil && string(i.config.Position) != "" {
		var err error
		lsn, err = PositionToLSN(i.config.Position)
		if err != nil {
			return err
		}
	}

	keyColumn, err := i.getKeyColumn(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to find key for table %s (try specifying it manually): %w", i.config.TableName, err)
	}

	sub := internal.NewSubscription(
		conn.Config().Config,
		i.config.SlotName,
		i.config.PublicationName,
		[]string{i.config.TableName},
		lsn,
		NewCDCHandler(
			internal.NewRelationSet(conn.ConnInfo()),
			keyColumn,
			i.config.Columns,
			i.records,
		).Handle,
	)

	i.sub = sub
	return nil
}

// attachSnapshot checks if a snapshot should be taken and attaches
// a snapshot iterator if it should. If Position is not set in the config, it
// sets the current subscription's position to the snapshot's anchored LSN.
func (i *Iterator) attachSnapshot(ctx context.Context, conn *pgx.Conn) error {
	if i.config.SnapshotMode == "never" {
		return nil
	}
	snap, err := NewSnapshotIterator(ctx, conn, Config{
		TableName:     i.config.TableName,
		Columns:       i.config.Columns,
		KeyColumnName: i.config.KeyColumnName,
	})
	if err != nil {
		return fmt.Errorf("failed to setup snapshot iterator: %w", err)
	}
	if i.config.Position == nil {
		i.config.Position = sdk.Position(snap.LSN())
	}
	i.snap = snap
	return nil
}

// getKeyColumn queries the db for the name of the primary key column for a
// table if one exists and returns it.
func (i *Iterator) getKeyColumn(ctx context.Context, conn *pgx.Conn) (string, error) {
	if i.config.KeyColumnName != "" {
		return i.config.KeyColumnName, nil
	}

	query := `SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name = $1 AND constraint_name LIKE '%_pkey'
		LIMIT 1;`
	row := conn.QueryRow(ctx, query, i.config.TableName)

	var colName string
	err := row.Scan(&colName)
	if err != nil {
		return "", fmt.Errorf("getKeyColumn query failed: %w", err)
	}

	return colName, nil
}

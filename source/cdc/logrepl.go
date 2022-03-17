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

package cdc

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/logrepl"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
)

// Config holds configuration values for our V1 Source. It is parsed from the
// map[string]string passed to the Connector at Configure time.
type Config struct {
	Position        sdk.Position
	SlotName        string
	PublicationName string
	TableName       string
	KeyColumnName   string
	Columns         []string
}

// LogreplIterator listens for events from the WAL and pushes them into its buffer.
// It iterates through that Buffer so that we have a controlled way to get 1
// record from our CDC buffer without having to expose a loop to the main Read.
type LogreplIterator struct {
	config   Config
	conn     *pgx.Conn
	messages chan sdk.Record

	sub *logrepl.Subscription
}

// NewCDCIterator takes a config and returns up a new CDCIterator or returns an
// error.
func NewCDCIterator(ctx context.Context, conn *pgx.Conn, config Config) (*LogreplIterator, error) {
	i := &LogreplIterator{
		config:   config,
		conn:     conn,
		messages: make(chan sdk.Record),
	}

	err := i.configureColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find table columns: %w", err)
	}

	err = i.configureKeyColumn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find key: %w", err)
	}

	err = i.attachSubscription()
	if err != nil {
		return nil, fmt.Errorf("failed to setup subscription: %w", err)
	}

	go i.listen(ctx)

	return i, nil
}

// listen is meant to be used in a goroutine. It starts the subscription
// passed to it and handles the subscription flush
func (i *LogreplIterator) listen(ctx context.Context) {
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

// Next returns the next record in the buffer. This is a blocking operation
// so it should only be called if we've checked that HasNext is true or else
// it will block until a record is inserted into the queue.
func (i *LogreplIterator) Next(ctx context.Context) (sdk.Record, error) {
	for {
		select {
		case <-ctx.Done():
			return sdk.Record{}, ctx.Err()
		case <-i.sub.Done():
			if err := i.sub.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("logical replication error: %w", err)
			}
			if err := ctx.Err(); err != nil {
				// subscription is done because the context is cancelled, we went
				// into the wrong case by chance
				return sdk.Record{}, err
			}
			// subscription stopped without an error and the context is still
			// open, this is a strange case, shouldn't actually happen
			return sdk.Record{}, fmt.Errorf("subscription stopped, no more data to fetch (this smells like a bug)")
		case r := <-i.messages:
			return r, nil
		}
	}
}

func (i *LogreplIterator) Ack(ctx context.Context, pos sdk.Position) error {
	lsn, err := PositionToLSN(pos)
	if err != nil {
		return fmt.Errorf("failed to parse position: %w", err)
	}
	i.sub.Ack(lsn)
	return nil
}

// Teardown kills the CDC subscription and waits for it to be done, closes its
// connection to the database, then cleans up its slot and publication.
func (i *LogreplIterator) Teardown(ctx context.Context) error {
	i.sub.Stop()
	err := i.sub.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error while waiting for subscription to stop: %w", err)
	}
	err = i.sub.Err()
	if err != nil {
		return fmt.Errorf("logical replication error: %w", err)
	}
	return nil
}

// attachSubscription builds a subscription with its own dedicated replication
// connection. It prepares a replication slot and publication for the connector
// if they don't exist yet.
func (i *LogreplIterator) attachSubscription() error {
	var lsn pglogrepl.LSN
	if i.config.Position != nil && string(i.config.Position) != "" {
		var err error
		lsn, err = PositionToLSN(i.config.Position)
		if err != nil {
			return err
		}
	}

	sub := logrepl.NewSubscription(
		i.conn.Config().Config,
		i.config.SlotName,
		i.config.PublicationName,
		[]string{i.config.TableName},
		lsn,
		NewLogreplHandler(
			logrepl.NewRelationSet(i.conn.ConnInfo()),
			i.config.KeyColumnName,
			i.messages,
		).Handle,
	)

	i.sub = sub
	return nil
}

// configureKeyColumn queries the db for the name of the primary key column
// for a table if one exists and sets it to the internal list.
// * TODO: Determine if tables must have keys
func (i *LogreplIterator) configureKeyColumn(ctx context.Context) error {
	if i.config.KeyColumnName != "" {
		return nil
	}

	query := `SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name = $1 AND constraint_name LIKE '%_pkey'
		LIMIT 1;`
	row := i.conn.QueryRow(ctx, query, i.config.TableName)

	var colName string
	err := row.Scan(&colName)
	if err != nil {
		return fmt.Errorf("failed to scan row: %w", err)
	}

	if colName == "" {
		return fmt.Errorf("got empty key column")
	}
	i.config.KeyColumnName = colName

	return nil
}

// configureColumns sets the default config to include all of the table's columns
// unless otherwise specified.
// * If other columns are specified, it uses them instead.
func (i *LogreplIterator) configureColumns(ctx context.Context) error {
	if len(i.config.Columns) > 0 {
		return nil
	}

	query := `SELECT column_name 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE table_name = $1`
	rows, err := i.conn.Query(ctx, query, i.config.TableName)
	if err != nil {
		return fmt.Errorf("configureColumns query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var val string
		err = rows.Scan(&val)
		if err != nil {
			return fmt.Errorf("failed to get column names from values: %w", err)
		}
		i.config.Columns = append(i.config.Columns, val)
	}

	return nil
}

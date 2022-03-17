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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// Config holds configuration values for our V1 Source. It is parsed from the
// map[string]string passed to the Connector at Configure time.
type Config struct {
	Position        sdk.Position
	URL             string
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
	wg *sync.WaitGroup

	config Config

	lsn        pglogrepl.LSN
	messages   chan sdk.Record
	killswitch context.CancelFunc

	sub  *Subscription
	conn *pgx.Conn
}

// NewCDCIterator takes a config and returns up a new CDCIterator or returns an
// error.
func NewCDCIterator(ctx context.Context, conn *pgx.Conn, config Config) (*LogreplIterator, error) {
	wctx, cancel := context.WithCancel(ctx)
	i := &LogreplIterator{
		wg:         &sync.WaitGroup{},
		config:     config,
		messages:   make(chan sdk.Record),
		killswitch: cancel,
		conn:       conn,
	}

	err := i.setPosition(config.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to set starting position: %w", err)
	}

	err = i.attachSubscription(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to setup subscription: %w", err)
	}

	i.wg.Add(1)
	go i.listen(wctx)

	return i, nil
}

func (i *LogreplIterator) setPosition(pos sdk.Position) error {
	if pos == nil || string(pos) == "" {
		i.lsn = 0
		return nil
	}

	lsn, err := i.parsePosition(pos)
	if err != nil {
		return err
	}
	i.lsn = lsn
	return nil
}

// listen is meant to be used in a goroutine. It starts the subscription
// passed to it and handles the the subscription flush
func (i *LogreplIterator) listen(ctx context.Context) {
	defer func() {
		err := i.sub.Flush(context.Background())
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("failed to flush subscription")
		}

		i.sub.Stop()

		wctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		err = i.sub.Wait(wctx)
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("could not stop postgres subscription in time")
		}

		i.wg.Done()
	}()

	sdk.Logger(ctx).Info().
		Str("lsn", i.lsn.String()).
		Str("slot", i.config.SlotName).
		Str("publication", i.config.PublicationName).
		Msg("starting logical replication")

	err := i.sub.Start(ctx, i.lsn, i.handler(ctx))
	if err != nil {
		// TODO return error
		sdk.Logger(ctx).Err(err).Msg("subscription failed")
	}
}

// Next returns the next record in the buffer. This is a blocking operation
// so it should only be called if we've checked that HasNext is true or else
// it will block until a record is inserted into the queue.
func (i *LogreplIterator) Next(ctx context.Context) (sdk.Record, error) {
	for {
		select {
		case r := <-i.messages:
			return r, nil
		case <-ctx.Done():
			return sdk.Record{}, ctx.Err()
		}
	}
}

func (i *LogreplIterator) Ack(ctx context.Context, pos sdk.Position) error {
	n, err := i.parsePosition(pos)
	if err != nil {
		return fmt.Errorf("failed to parse position")
	}
	return i.sub.AdvanceLSN(ctx, n)
}

// push pushes a record into the buffer.
func (i *LogreplIterator) push(r sdk.Record) {
	i.messages <- r
}

// Teardown kills the CDC subscription and waits for it to be done, closes its
// connection to the database, then cleans up its slot and publication.
func (i *LogreplIterator) Teardown(ctx context.Context) error {
	i.killswitch()
	i.wg.Wait()

	return i.dropPublication(ctx)
}

// attachSubscription builds a subscription with its own dedicated replication
// connection. It prepares a replication slot and publication for the connector
// if they're not yet setup with sane defaults if they're not configured.
func (i *LogreplIterator) attachSubscription(ctx context.Context) error {
	err := i.configureColumns(ctx)
	if err != nil {
		return fmt.Errorf("failed to find table columns: %w", err)
	}

	err = i.configureKeyColumn(ctx)
	if err != nil {
		return fmt.Errorf("failed to find key: %w", err)
	}

	err = i.createPublication(ctx)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	sub := NewSubscription(
		i.conn.Config().Config,
		i.config.SlotName,
		i.config.PublicationName,
		0,
		false,
	)

	i.sub = sub
	return nil
}

func (i *LogreplIterator) handler(ctx context.Context) Handler {
	set := NewRelationSet(i.conn.ConnInfo())

	return func(m pglogrepl.Message, lsn pglogrepl.LSN) error {
		sdk.Logger(ctx).Trace().
			Str("lsn", lsn.String()).
			Str("messageType", m.Type().String()).
			Msg("handler received pglogrepl.Message")

		switch v := m.(type) {
		case *pglogrepl.RelationMessage:
			// We have to add the Relations to our Set so that we can
			// decode our own output
			set.Add(v)
		case *pglogrepl.InsertMessage:
			oid := pgtype.OID(v.RelationID)
			values, err := set.Values(oid, v.Tuple)
			if err != nil {
				return fmt.Errorf("handleInsert failed: %w", err)
			}
			return i.handleInsert(oid, values, lsn)
		case *pglogrepl.UpdateMessage:
			oid := pgtype.OID(v.RelationID)
			values, err := set.Values(oid, v.NewTuple)
			if err != nil {
				return fmt.Errorf("handleUpdate failed: %w", err)
			}
			return i.handleUpdate(oid, values, lsn)
		case *pglogrepl.DeleteMessage:
			oid := pgtype.OID(v.RelationID)
			values, err := set.Values(oid, v.OldTuple)
			if err != nil {
				return fmt.Errorf("handleDelete failed: %w", err)
			}
			return i.handleDelete(oid, values, lsn)
		}

		return nil
	}
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

func (i *LogreplIterator) parsePosition(pos sdk.Position) (pglogrepl.LSN, error) {
	n, err := strconv.ParseUint(string(pos), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid position: %w", err)
	}
	return pglogrepl.LSN(n), nil
}

func (i *LogreplIterator) createPublication(ctx context.Context) error {
	query := "CREATE PUBLICATION %s FOR TABLE %s"
	query = fmt.Sprintf(query, i.config.PublicationName, i.config.TableName)
	_, err := i.conn.Exec(ctx, query)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code != pgDuplicateObjectErrorCode {
			return fmt.Errorf("failed to create publication %s: %w",
				i.config.SlotName, err)
		}
	}
	return nil
}

func (i *LogreplIterator) dropPublication(ctx context.Context) error {
	query := "DROP PUBLICATION IF EXISTS %s"
	query = fmt.Sprintf(query, i.config.PublicationName)
	if _, err := i.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}
	return nil
}

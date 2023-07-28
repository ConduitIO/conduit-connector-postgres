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

package trigger

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/multierr"
)

const (
	colID                = "conduit_id"
	colOperation         = "conduit_operation"
	conduitOperationType = "conduit_operation_type"

	referencingNew = "NEW."
	referencingOld = "OLD."

	orderingASC  = " ASC"
	orderingDESC = " DESC"

	actionInsert = "INSERT"
	actionUpdate = "UPDATE"
	actionDelete = "DELETE"

	queryIfTableExists                         = "SELECT 1 FROM information_schema.tables WHERE table_name=$1;"
	queryIfTypeExists                          = "SELECT 1 FROM pg_type WHERE typname=$1;"
	queryEnumTypeCreate                        = "CREATE TYPE %s AS ENUM ('%s', '%s', '%s');"
	queryTableCopy                             = "CREATE TABLE %s (LIKE %s);"
	queryTriggerInsertPart                     = "INSERT INTO %s (%s, %s) VALUES (%s, TG_OP::conduit_operation_type);"
	queryTrackingTableExtendWithConduitColumns = `
		ALTER TABLE %s 
		ADD COLUMN %s SERIAL,
		ADD COLUMN %s conduit_operation_type NOT NULL;`
	queryFunctionCreate = `
		CREATE OR REPLACE
		FUNCTION %s()
			RETURNS trigger
			LANGUAGE plpgsql
		AS $$
		BEGIN
			IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN %s END IF;
			IF TG_OP = 'DELETE' THEN %s END IF;
			RETURN NEW;
		END;
		$$`
	queryTriggerCreate = `
		CREATE OR REPLACE
		TRIGGER %s
		AFTER INSERT OR UPDATE OR DELETE ON %s
		FOR EACH ROW EXECUTE PROCEDURE %s();`
	queryColumnNamesSelect = "SELECT column_name FROM information_schema.columns WHERE table_name = $1;"
	queryPrimaryKeySelect  = `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name = $1 AND constraint_name LIKE '%_pkey'
		LIMIT 1;`
)

// declare Postgres $ placeholder format
var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// Iterator is an implementation of an iterator in a trigger CDC mode.
type Iterator struct {
	conn     *pgxpool.Pool
	position *Position
	snapshot *Snapshot
	cdc      *CDC

	// table is a table's name.
	table string
	// orderingColumn is a name of column what iterator use for sorting data.
	orderingColumn string
	// key is a column's name that iterator will use for setting key in record.
	key string
	// columns is a list of table's columns for record payload.
	// if empty - will get all columns.
	columns []string
	// batchSize is a size of batch.
	batchSize uint64
	// conduit is names of conduit items in a DB (like tracking table, trigger, and function).
	conduit string
}

// Params is an incoming iterator params for the New function.
type Params struct {
	Pos            sdk.Position
	Conn           *pgxpool.Pool
	Table          string
	OrderingColumn string
	Key            string
	Snapshot       bool
	Columns        []string
	BatchSize      uint64
}

// New creates a new instance of the iterator.
func New(ctx context.Context, params Params) (*Iterator, error) {
	pos, err := ParseSDKPosition(params.Pos)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	iterator := &Iterator{
		conn:           params.Conn,
		position:       pos,
		table:          params.Table,
		orderingColumn: params.OrderingColumn,
		key:            params.Key,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
		conduit:        fmt.Sprintf("conduit_%s_%s", params.Table, pos.CreatedAt),
	}

	err = iterator.populateKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("populate key: %w", err)
	}

	err = iterator.populateColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("populate names of the table columns: %w", err)
	}

	err = iterator.setupCDC(ctx)
	if err != nil {
		return nil, fmt.Errorf("setup cdc: %w", err)
	}

	if params.Snapshot && pos.Mode == ModeSnapshot {
		iterator.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			Conn:           iterator.conn,
			Position:       iterator.position,
			Table:          iterator.table,
			OrderingColumn: iterator.orderingColumn,
			Key:            iterator.key,
			Columns:        iterator.columns,
			BatchSize:      iterator.batchSize,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}

		return iterator, nil
	}

	iterator.cdc, err = NewCDC(ctx, CDCParams{
		Conn:           iterator.conn,
		Position:       iterator.position,
		Table:          iterator.table,
		OrderingColumn: iterator.orderingColumn,
		Key:            iterator.key,
		Columns:        iterator.columns,
		BatchSize:      iterator.batchSize,
		Conduit:        iterator.conduit,
	})
	if err != nil {
		return nil, fmt.Errorf("init cdc iterator: %w", err)
	}

	return iterator, nil
}

// Next returns the next record.
func (iter *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case iter.snapshot != nil:
		hasNext, err := iter.snapshot.HasNext(ctx)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("snapshot has next: %w", err)
		}

		if hasNext {
			return iter.snapshot.Next(ctx)
		}

		if err = iter.switchToCDCIterator(ctx); err != nil {
			return sdk.Record{}, fmt.Errorf("switch to cdc iterator: %w", err)
		}

		fallthrough

	case iter.cdc != nil:
		hasNext, err := iter.cdc.HasNext(ctx)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("cdc has next: %w", err)
		}

		if hasNext {
			return iter.cdc.Next(ctx)
		}

		return sdk.Record{}, sdk.ErrBackoffRetry

	default:
		return sdk.Record{}, errors.New("not initialized iterator")
	}
}

// Ack appends the last processed value to the slice to clear the tracking table in the future.
func (iter *Iterator) Ack(_ context.Context, position sdk.Position) error {
	pos, err := ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	if pos.Mode == ModeCDC {
		return iter.cdc.pushValueToDelete(pos.LastProcessedVal)
	}

	return nil
}

func (iter *Iterator) Teardown(ctx context.Context) error {
	var err error

	if iter.snapshot != nil {
		err = iter.snapshot.Close()
	}

	if iter.cdc != nil {
		err = multierr.Append(err, iter.cdc.Close())
	}

	if err != nil {
		return fmt.Errorf("teardown iterator: %w", err)
	}

	return nil
}

// populateKey queries the DB for the name of the primary key column for a table
// and, if it exists, populates a key field in the iterator or returns an error.
func (iter *Iterator) populateKey(ctx context.Context) error {
	if iter.key != "" {
		return nil
	}

	err := iter.conn.QueryRow(ctx, queryPrimaryKeySelect, iter.table).Scan(&iter.key)
	if err != nil {
		return fmt.Errorf("scan primary key: %w", err)
	}

	return nil
}

// populateColumns populates the iterator column with names of the table columns.
func (iter *Iterator) populateColumns(ctx context.Context) error {
	if len(iter.columns) > 0 {
		return nil
	}

	rows, err := iter.conn.Query(ctx, queryColumnNamesSelect, iter.table)
	if err != nil {
		return fmt.Errorf("select names of the table columns: %w", err)
	}
	defer rows.Close()

	colName := ""
	for rows.Next() {
		err = rows.Scan(&colName)
		if err != nil {
			return fmt.Errorf("scan column types rows: %w", err)
		}

		iter.columns = append(iter.columns, colName)
	}

	return nil
}

// switchToCDCIterator stops Snapshot and initializes CDC iterator.
func (iter *Iterator) switchToCDCIterator(ctx context.Context) error {
	err := iter.snapshot.Close()
	if err != nil {
		return fmt.Errorf("stop snaphot iterator: %w", err)
	}

	iter.snapshot = nil
	iter.position.LastProcessedVal = nil

	iter.cdc, err = NewCDC(ctx, CDCParams{
		Conn:           iter.conn,
		Position:       iter.position,
		Table:          iter.table,
		OrderingColumn: iter.orderingColumn,
		Key:            iter.key,
		Columns:        iter.columns,
		BatchSize:      iter.batchSize,
		Conduit:        iter.conduit,
	})
	if err != nil {
		return fmt.Errorf("new cdc iterator: %w", err)
	}

	return nil
}

// setupCDC creates a type, a tracking table, a function and a trigger.
func (iter *Iterator) setupCDC(ctx context.Context) error {
	tx, err := iter.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback(ctx) // nolint:errcheck,nolintlint

	err = iter.createEnumType(ctx, tx)
	if err != nil {
		return fmt.Errorf("create enum type: %w", err)
	}

	err = iter.createTrackingTable(ctx, tx)
	if err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	err = iter.createFunction(ctx, tx)
	if err != nil {
		return fmt.Errorf("create function: %w", err)
	}

	err = iter.createTrigger(ctx, tx)
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	return nil
}

// createEnumType creates an enum type if it does not exist.
func (iter *Iterator) createEnumType(ctx context.Context, tx pgx.Tx) error {
	rows, err := tx.Query(ctx, queryIfTypeExists, conduitOperationType)
	if err != nil {
		return fmt.Errorf("check if the %q enum type already exists: %w", conduitOperationType, err)
	}
	defer rows.Close()

	// return if the type already exists
	if rows.Next() {
		return nil
	}

	// create an enum type
	_, err = tx.Exec(ctx, fmt.Sprintf(queryEnumTypeCreate, conduitOperationType, actionInsert, actionUpdate, actionDelete))
	if err != nil {
		return fmt.Errorf("create the %q enum type: %w", conduitOperationType, err)
	}

	return nil
}

// createTrackingTable creates a new tracking table, if it does not exist.
func (iter *Iterator) createTrackingTable(ctx context.Context, tx pgx.Tx) error {
	rows, err := tx.Query(ctx, queryIfTableExists, iter.conduit)
	if err != nil {
		return fmt.Errorf("check if the tracking table %q already exists: %w", iter.conduit, err)
	}
	defer rows.Close()

	// return if the tracking table already exists
	if rows.Next() {
		return nil
	}

	// create a copy of table
	_, err = tx.Exec(ctx, fmt.Sprintf(queryTableCopy, iter.conduit, iter.table))
	if err != nil {
		return fmt.Errorf("create a copy of table: %w", err)
	}

	// extend a tracking table with conduit columns
	_, err = tx.Exec(ctx, fmt.Sprintf(queryTrackingTableExtendWithConduitColumns, iter.conduit, colID, colOperation))
	if err != nil {
		return fmt.Errorf("expand a tracking table with conduit columns: %w", err)
	}

	return nil
}

// createFunction creates a function for a trigger to track a data.
func (iter *Iterator) createFunction(ctx context.Context, tx pgx.Tx) error {
	newValues := make([]string, len(iter.columns))
	oldValues := make([]string, len(iter.columns))
	for i := range iter.columns {
		newValues[i] = fmt.Sprintf("%s%s", referencingNew, iter.columns[i])
		oldValues[i] = fmt.Sprintf("%s%s", referencingOld, iter.columns[i])
	}

	insertOnInsertingOrUpdating := fmt.Sprintf(queryTriggerInsertPart, iter.conduit,
		strings.Join(iter.columns, ","), colOperation, strings.Join(newValues, ","))
	insertOnDeleting := fmt.Sprintf(queryTriggerInsertPart, iter.conduit,
		strings.Join(iter.columns, ","), colOperation, strings.Join(oldValues, ","))

	_, err := tx.Exec(ctx, fmt.Sprintf(queryFunctionCreate, iter.conduit, insertOnInsertingOrUpdating, insertOnDeleting))
	if err != nil {
		return fmt.Errorf("create function: %w", err)
	}

	return nil
}

// createTrigger creates a trigger to track the data.
func (iter *Iterator) createTrigger(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, fmt.Sprintf(queryTriggerCreate, iter.conduit, iter.table, iter.conduit))
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	return nil
}

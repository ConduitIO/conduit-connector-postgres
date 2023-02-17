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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-connector-postgres/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/multierr"
)

const (
	timeoutBeforeCloseDB        = 20 * time.Second
	timeoutToClearTrackingTable = 5 * time.Second
)

var errWrongTrackingOperationType = errors.New("wrong tracking operation type")

// CDC is an implementation of a CDC iterator in a trigger CDC mode.
type CDC struct {
	conn     *pgxpool.Pool
	position *Position
	rows     pgx.Rows

	// tableSrv service for clearing tracking table.
	tableSrv *trackingTableService

	// table is a table name.
	table string
	// orderingColumn is a name of column what iterator use for sorting data.
	orderingColumn string
	// key is a column's name that iterator will use for setting key in record.
	key string
	// columns is a list of table's columns for record payload.
	// If empty - will get all columns.
	columns []string
	// batchSize is a size of batch.
	batchSize uint64
	// conduit is names of conduit items in DB like tracking table, trigger, and procedure.
	conduit string
}

// CDCParams is an incoming params for the NewCDC function.
type CDCParams struct {
	Conn           *pgxpool.Pool
	Position       *Position
	Table          string
	OrderingColumn string
	Key            string
	Columns        []string
	BatchSize      uint64
	Conduit        string
}

type trackingTableService struct {
	m sync.Mutex

	// channel for the stop signal
	stopCh chan struct{}
	// channel to notify that all queries are completed and the database connection can be closed
	canCloseCh chan struct{}
	// error channel
	errCh chan error
	// slice of identifiers to delete
	idsToDelete []any
}

// NewCDC creates a new instance of the CDC iterator.
func NewCDC(ctx context.Context, params CDCParams) (*CDC, error) {
	iterator := &CDC{
		conn:           params.Conn,
		position:       params.Position,
		table:          params.Table,
		orderingColumn: params.OrderingColumn,
		conduit:        params.Conduit,
		key:            params.Key,
		columns:        append(params.Columns, colID, colOperation),
		batchSize:      params.BatchSize,
		tableSrv:       newTrackingTableService(),
	}

	err := iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load cdc rows: %w", err)
	}

	// run clearing tracking table.
	go iterator.clearTrackingTable(ctx)

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (iter *CDC) HasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return iter.rows.Next(), nil
}

// Next returns the next record.
func (iter *CDC) Next(_ context.Context) (sdk.Record, error) {
	values, err := iter.rows.Values()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("scan row values: %w", err)
	}

	row := make(map[string]any, len(values))
	for i := range values {
		row[iter.columns[i]] = values[i]
	}

	operationType, ok := row[colOperation].(string)
	if !ok {
		return sdk.Record{}, errWrongTrackingOperationType
	}

	key := make(sdk.StructuredData)
	if iter.key != "" {
		val, ok := row[iter.key]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", iter.key)
		}

		key[iter.key] = val
	}

	iter.position.Mode = ModeCDC
	iter.position.LastProcessedVal = row[colID]

	convertedPosition, err := iter.position.marshal()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	// delete tracking columns
	delete(row, colOperation)
	delete(row, colID)

	rowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	metadata := sdk.Metadata{
		common.MetadataPostgresTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now())

	switch operationType {
	case actionInsert:
		return sdk.Util.Source.NewRecordCreate(
			convertedPosition, metadata, key, sdk.RawData(rowBytes),
		), nil
	case actionUpdate:
		return sdk.Util.Source.NewRecordUpdate(
			convertedPosition, metadata, key, nil, sdk.RawData(rowBytes),
		), nil
	case actionDelete:
		return sdk.Util.Source.NewRecordDelete(
			convertedPosition, metadata, key,
		), nil
	default:
		return sdk.Record{}, errWrongTrackingOperationType
	}
}

// Close closes database rows of CDC iterator.
func (iter *CDC) Close() (err error) {
	// send a signal to stop clearing the tracking table
	iter.tableSrv.stopCh <- struct{}{}

	if iter.rows != nil {
		iter.rows.Close()
	}

	select {
	// wait until clearing tracking table will be finished
	case <-iter.tableSrv.canCloseCh:
	// or until time out
	case <-time.After(timeoutBeforeCloseDB):
	}

	iter.tableSrv.close()

	return
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *CDC) loadRows(ctx context.Context) error {
	sb := psql.Select(iter.columns...).
		From(iter.conduit).
		OrderBy(colID + orderingASC).
		Limit(iter.batchSize)

	if iter.position.LastProcessedVal != nil {
		sb = sb.Where(sq.Gt{colID: iter.position.LastProcessedVal})
	}

	query, args, err := sb.ToSql()
	if err != nil {
		return fmt.Errorf("create select query from tracking table: %w", err)
	}

	iter.rows, err = iter.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query from tracking table: %w", err)
	}

	return nil
}

// newTrackingTableService is a service to delete processed rows.
func newTrackingTableService() *trackingTableService {
	return &trackingTableService{
		stopCh:      make(chan struct{}, 1),
		canCloseCh:  make(chan struct{}, 1),
		errCh:       make(chan error, 1),
		idsToDelete: make([]any, 0),
	}
}

func (t *trackingTableService) close() {
	close(t.canCloseCh)
	close(t.errCh)
	close(t.stopCh)
}

// pushValueToDelete appends the last processed value to the slice to clear the tracking table in the future.
func (iter *CDC) pushValueToDelete(lastProcessedVal any) (err error) {
	if len(iter.tableSrv.errCh) > 0 {
		for e := range iter.tableSrv.errCh {
			err = multierr.Append(err, e)
		}

		if err != nil {
			return fmt.Errorf("clear tracking table: %w", err)
		}
	}

	iter.tableSrv.m.Lock()
	defer iter.tableSrv.m.Unlock()

	if iter.tableSrv.idsToDelete == nil {
		iter.tableSrv.idsToDelete = make([]any, 0)
	}

	iter.tableSrv.idsToDelete = append(iter.tableSrv.idsToDelete, lastProcessedVal)

	return nil
}

// clearTrackingTable removes already processed rows from a tracking table.
func (iter *CDC) clearTrackingTable(ctx context.Context) {
	for {
		select {
		// connector is stopping, clear table last time
		case <-iter.tableSrv.stopCh:
			err := iter.deleteTrackingTableRows(ctx)
			if err != nil {
				iter.tableSrv.errCh <- err
			}

			// query finished, db can be closed.
			iter.tableSrv.canCloseCh <- struct{}{}

			return

		case <-time.After(timeoutToClearTrackingTable):
			err := iter.deleteTrackingTableRows(ctx)
			if err != nil {
				iter.tableSrv.errCh <- err

				return
			}
		}
	}
}

// deleteTrackingTableRows deletes processed rows from tracking table.
func (iter *CDC) deleteTrackingTableRows(ctx context.Context) error {
	iter.tableSrv.m.Lock()
	defer iter.tableSrv.m.Unlock()

	if len(iter.tableSrv.idsToDelete) == 0 {
		return nil
	}

	sql, args, err := psql.Delete(iter.conduit).Where(sq.Eq{colID: iter.tableSrv.idsToDelete}).ToSql()
	if err != nil {
		return fmt.Errorf("create delete sql query: %w", err)
	}

	_, err = iter.conn.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute delete sql query: %w", err)
	}

	iter.tableSrv.idsToDelete = nil

	return nil
}

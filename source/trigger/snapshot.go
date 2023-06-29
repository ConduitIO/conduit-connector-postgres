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
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-connector-postgres/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// ErrSnapshotInterrupt is returned when Teardown or any other signal
// cancels an in-progress snapshot.
var ErrSnapshotInterrupt = fmt.Errorf("interrupted snapshot")

// Snapshot is an implementation of a Snapshot iterator in a trigger CDC mode.
type Snapshot struct {
	conn     *pgxpool.Pool
	position *Position
	rows     pgx.Rows

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
}

// SnapshotParams is an incoming params for the NewSnapshot function.
type SnapshotParams struct {
	Conn           *pgxpool.Pool
	Position       *Position
	Table          string
	OrderingColumn string
	Key            string
	Columns        []string
	BatchSize      uint64
}

// NewSnapshot creates a new instance of the Snapshot iterator.
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	iterator := &Snapshot{
		conn:           params.Conn,
		position:       params.Position,
		table:          params.Table,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		key:            params.Key,
		batchSize:      params.BatchSize,
	}

	err := iterator.populateLatestSnapshotValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("populate latest snapshot value: %w", err)
	}

	err = iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load snapshot rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (iter *Snapshot) HasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return iter.rows.Next(), nil
}

// Next returns the next record.
func (iter *Snapshot) Next(_ context.Context) (sdk.Record, error) {
	values, err := iter.rows.Values()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("scan row values: %w", err)
	}

	row := make(map[string]any, len(values))
	for i := range values {
		row[iter.columns[i]] = values[i]
	}

	key := make(sdk.StructuredData)
	if iter.key != "" {
		val, ok := row[iter.key]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", iter.key)
		}

		key[iter.key] = val
	}

	rowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	iter.position.LastProcessedVal = row[iter.orderingColumn]

	convertedPosition, err := iter.position.marshal()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	metadata := sdk.Metadata{
		common.MetadataPostgresTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordSnapshot(
		convertedPosition,
		metadata,
		key,
		sdk.RawData(rowBytes),
	), nil
}

// Close closes database rows of Snapshot iterator.
func (iter *Snapshot) Close() error {
	if iter.rows != nil {
		iter.rows.Close()
	}

	return nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *Snapshot) loadRows(ctx context.Context) error {
	sb := psql.Select(iter.columns...).
		From(iter.table).
		Where(sq.LtOrEq{iter.orderingColumn: iter.position.LatestSnapshotValue}).
		OrderBy(iter.orderingColumn + orderingASC).
		Limit(iter.batchSize)

	if iter.position.LastProcessedVal != nil {
		sb = sb.Where(sq.Gt{iter.orderingColumn: iter.position.LastProcessedVal})
	}

	query, args, err := sb.ToSql()
	if err != nil {
		return fmt.Errorf("create select query: %w", err)
	}

	iter.rows, err = iter.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query: %w", err)
	}

	return nil
}

// populateLatestSnapshotValue populates the position with the latests snapshot value.
func (iter *Snapshot) populateLatestSnapshotValue(ctx context.Context) error {
	if iter.position.LatestSnapshotValue != nil {
		return nil
	}

	sb := psql.Select(iter.orderingColumn).
		From(iter.table).
		OrderBy(iter.orderingColumn + orderingDESC).
		Limit(1)

	query, _, err := sb.ToSql()
	if err != nil {
		return fmt.Errorf("create select query: %w", err)
	}

	err = iter.conn.QueryRow(ctx, query).Scan(&iter.position.LatestSnapshotValue)
	if err != nil {
		return fmt.Errorf("scan row: %w", err)
	}

	return nil
}

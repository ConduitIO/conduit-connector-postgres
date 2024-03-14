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

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-connector-postgres/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

const (
	// TODO same constant is defined in packages longpoll, logrepl and destination
	//  use same constant everywhere
	MetadataPostgresTable = "postgres.table"
)

type Destination struct {
	sdk.UnimplementedDestination

	conn        *pgx.Conn
	config      destination.Config
	stmtBuilder sq.StatementBuilderType
}

func NewDestination() sdk.Destination {
	d := &Destination{
		stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}
	return sdk.DestinationWithMiddleware(d, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

func (d *Destination) Configure(_ context.Context, cfg map[string]string) error {
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return err
	}
	// try parsing the url
	_, err = pgx.ParseConfig(d.config.URL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, d.config.URL)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	d.conn = conn
	return nil
}

// Write routes incoming records to their appropriate handler based on the
// operation.
func (d *Destination) Write(ctx context.Context, recs []sdk.Record) (int, error) {
	b := &pgx.Batch{}
	for _, rec := range recs {
		var err error
		switch rec.Operation {
		case sdk.OperationCreate:
			err = d.handleInsert(rec, b)
		case sdk.OperationUpdate:
			err = d.handleUpdate(rec, b)
		case sdk.OperationDelete:
			err = d.handleDelete(rec, b)
		case sdk.OperationSnapshot:
			err = d.handleInsert(rec, b)
		default:
			return 0, fmt.Errorf("invalid operation %q", rec.Operation)
		}
		if err != nil {
			return 0, err
		}
	}

	br := d.conn.SendBatch(ctx, b)
	defer br.Close()

	for i := range recs {
		// fetch error for each statement
		_, err := br.Exec()
		if err != nil {
			// the batch is executed in a transaction, if one failed all failed
			return 0, fmt.Errorf("failed to execute query for record %d: %w", i, err)
		}
	}
	return len(recs), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close(ctx)
	}
	return nil
}

// handleInsert adds a query to the batch that stores the record in the target
// table. It checks for the existence of a key. If no key is present or a key
// exists and no key column name is configured, it will plainly insert the data.
// Otherwise it upserts the record.
func (d *Destination) handleInsert(r sdk.Record, b *pgx.Batch) error {
	if !d.hasKey(r) || d.config.Key == "" {
		return d.insert(r, b)
	}
	return d.upsert(r, b)
}

// handleUpdate adds a query to the batch that updates the record in the target
// table. It assumes the record has a key and fails if one is not present.
func (d *Destination) handleUpdate(r sdk.Record, b *pgx.Batch) error {
	if !d.hasKey(r) {
		return fmt.Errorf("key must be provided on update actions")
	}
	// TODO handle case if the key was updated
	return d.upsert(r, b)
}

// handleDelete adds a query to the batch that deletes the record from the
// target table. It assumes the record has a key and fails if one is not present.
func (d *Destination) handleDelete(r sdk.Record, b *pgx.Batch) error {
	if !d.hasKey(r) {
		return fmt.Errorf("key must be provided on delete actions")
	}
	return d.remove(r, b)
}

func (d *Destination) upsert(r sdk.Record, b *pgx.Batch) error {
	payload, err := d.getPayload(r)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	key, err := d.getKey(r)
	if err != nil {
		return fmt.Errorf("failed to get key: %w", err)
	}

	keyColumnName := d.getKeyColumnName(key, d.config.Key)

	tableName, err := d.getTableName(r.Metadata)
	if err != nil {
		return fmt.Errorf("failed to get table name for write: %w", err)
	}

	query, args, err := d.formatUpsertQuery(key, payload, keyColumnName, tableName)
	if err != nil {
		return fmt.Errorf("error formatting query: %w", err)
	}

	b.Queue(query, args...)
	return nil
}

func (d *Destination) remove(r sdk.Record, b *pgx.Batch) error {
	key, err := d.getKey(r)
	if err != nil {
		return err
	}
	keyColumnName := d.getKeyColumnName(key, d.config.Key)
	tableName, err := d.getTableName(r.Metadata)
	if err != nil {
		return fmt.Errorf("failed to get table name for write: %w", err)
	}
	query, args, err := d.stmtBuilder.
		Delete(tableName).
		Where(sq.Eq{keyColumnName: key[keyColumnName]}).
		ToSql()
	if err != nil {
		return fmt.Errorf("error formatting delete query: %w", err)
	}

	b.Queue(query, args...)
	return nil
}

// insert is an append-only operation that doesn't care about keys, but
// can error on constraints violations so should only be used when no table
// key or unique constraints are otherwise present.
func (d *Destination) insert(r sdk.Record, b *pgx.Batch) error {
	tableName, err := d.getTableName(r.Metadata)
	if err != nil {
		return err
	}
	key, err := d.getKey(r)
	if err != nil {
		return err
	}
	payload, err := d.getPayload(r)
	if err != nil {
		return err
	}
	colArgs, valArgs := d.formatColumnsAndValues(key, payload)
	query, args, err := d.stmtBuilder.
		Insert(tableName).
		Columns(colArgs...).
		Values(valArgs...).
		ToSql()
	if err != nil {
		return fmt.Errorf("error formatting insert query: %w", err)
	}

	b.Queue(query, args...)
	return nil
}

func (d *Destination) getPayload(r sdk.Record) (sdk.StructuredData, error) {
	if r.Payload.After == nil {
		return sdk.StructuredData{}, nil
	}
	return d.structuredDataFormatter(r.Payload.After)
}

func (d *Destination) getKey(r sdk.Record) (sdk.StructuredData, error) {
	if r.Key == nil {
		return sdk.StructuredData{}, nil
	}
	return d.structuredDataFormatter(r.Key)
}

func (d *Destination) structuredDataFormatter(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil {
		return sdk.StructuredData{}, nil
	}
	if sdata, ok := data.(sdk.StructuredData); ok {
		return sdata, nil
	}
	raw := data.Bytes()
	if len(raw) == 0 {
		return sdk.StructuredData{}, nil
	}

	m := make(map[string]interface{})
	err := json.Unmarshal(raw, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// formatUpsertQuery manually formats the UPSERT and ON CONFLICT query statements.
// The `ON CONFLICT` portion of this query needs to specify the constraint
// name.
// * In our case, we can only rely on the record.Key's parsed key value.
// * If other schema constraints prevent a write, this won't upsert on
// that conflict.
func (d *Destination) formatUpsertQuery(
	key sdk.StructuredData,
	payload sdk.StructuredData,
	keyColumnName string,
	tableName string,
) (string, []interface{}, error) {
	upsertQuery := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET", keyColumnName)
	for column := range payload {
		// tuples form a comma separated list, so they need a comma at the end.
		// `EXCLUDED` references the new record's values. This will overwrite
		// every column's value except for the key column.
		tuple := fmt.Sprintf("%s=EXCLUDED.%s,", column, column)
		// TODO: Consider removing this space.
		upsertQuery += " "
		// add the tuple to the query string
		upsertQuery += tuple
	}

	// remove the last comma from the list of tuples
	upsertQuery = strings.TrimSuffix(upsertQuery, ",")

	// we have to manually append a semi colon to the upsert sql;
	upsertQuery += ";"

	colArgs, valArgs := d.formatColumnsAndValues(key, payload)

	return d.stmtBuilder.
		Insert(tableName).
		Columns(colArgs...).
		Values(valArgs...).
		SuffixExpr(sq.Expr(upsertQuery)).
		ToSql()
}

// formatColumnsAndValues turns the key and payload into a slice of ordered
// columns and values for upserting into Postgres.
func (d *Destination) formatColumnsAndValues(key, payload sdk.StructuredData) ([]string, []interface{}) {
	var colArgs []string
	var valArgs []interface{}

	// range over both the key and payload values in order to format the
	// query for args and values in proper order
	for key, val := range key {
		colArgs = append(colArgs, key)
		valArgs = append(valArgs, val)
		delete(payload, key) // NB: Delete Key from payload arguments
	}

	for field, value := range payload {
		colArgs = append(colArgs, field)
		valArgs = append(valArgs, value)
	}

	return colArgs, valArgs
}

// return either the record's metadata value for table or the default configured
// value for table. Otherwise it will error since we require some table to be
// set to write into.
func (d *Destination) getTableName(metadata map[string]string) (string, error) {
	tableName, ok := metadata[MetadataPostgresTable]
	if !ok {
		if d.config.Table == "" {
			return "", fmt.Errorf("no table provided for default writes")
		}
		return d.config.Table, nil
	}
	return tableName, nil
}

// getKeyColumnName will return the name of the first item in the key or the
// connector-configured default name of the key column name.
func (d *Destination) getKeyColumnName(key sdk.StructuredData, defaultKeyName string) string {
	if len(key) > 1 {
		// Go maps aren't order preserving, so anything over len 1 will have
		// non deterministic results until we handle composite keys.
		panic("composite keys not yet supported")
	}
	for k := range key {
		return k
	}
	return defaultKeyName
}

func (d *Destination) hasKey(e sdk.Record) bool {
	return e.Key != nil && len(e.Key.Bytes()) > 0
}

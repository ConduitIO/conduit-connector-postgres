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
	"math/big"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/destination"
	"github.com/conduitio/conduit-connector-postgres/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

type Destination struct {
	sdk.UnimplementedDestination

	config       destination.Config
	getTableName destination.TableFn

	conn        *pgx.Conn
	dbInfo      *internal.DbInfo
	stmtBuilder sq.StatementBuilderType
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func NewDestination() sdk.Destination {
	d := &Destination{
		stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}
	return sdk.DestinationWithMiddleware(d)
}

func (d *Destination) Open(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, d.config.URL)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	d.conn = conn

	d.getTableName, err = d.config.TableFunction()
	if err != nil {
		return fmt.Errorf("invalid table name or table name function: %w", err)
	}

	d.dbInfo = internal.NewDbInfo(conn)
	return nil
}

// Write routes incoming records to their appropriate handler based on the
// operation.
func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	b := &pgx.Batch{}
	for _, rec := range recs {
		var err error
		switch rec.Operation {
		case opencdc.OperationCreate:
			err = d.handleInsert(ctx, rec, b)
		case opencdc.OperationUpdate:
			err = d.handleUpdate(ctx, rec, b)
		case opencdc.OperationDelete:
			err = d.handleDelete(ctx, rec, b)
		case opencdc.OperationSnapshot:
			err = d.handleInsert(ctx, rec, b)
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
func (d *Destination) handleInsert(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	if !d.hasKey(r) || d.config.Key == "" {
		return d.insert(ctx, r, b)
	}
	return d.upsert(ctx, r, b)
}

// handleUpdate adds a query to the batch that updates the record in the target
// table. It assumes the record has a key and fails if one is not present.
func (d *Destination) handleUpdate(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	if !d.hasKey(r) {
		return fmt.Errorf("key must be provided on update actions")
	}
	// TODO handle case if the key was updated
	return d.upsert(ctx, r, b)
}

// handleDelete adds a query to the batch that deletes the record from the
// target table. It assumes the record has a key and fails if one is not present.
func (d *Destination) handleDelete(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	if !d.hasKey(r) {
		return fmt.Errorf("key must be provided on delete actions")
	}
	return d.remove(ctx, r, b)
}

func (d *Destination) upsert(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	payload, err := d.getPayload(r)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	key, err := d.getKey(r)
	if err != nil {
		return fmt.Errorf("failed to get key: %w", err)
	}

	keyColumnName := d.getKeyColumnName(key, d.config.Key)

	tableName, err := d.getTableName(r)
	if err != nil {
		return fmt.Errorf("failed to get table name for write: %w", err)
	}

	query, args, err := d.formatUpsertQuery(ctx, key, payload, keyColumnName, tableName)
	if err != nil {
		return fmt.Errorf("error formatting query: %w", err)
	}
	sdk.Logger(ctx).Trace().
		Str("table_name", tableName).
		Any("key", map[string]interface{}{keyColumnName: key[keyColumnName]}).
		Msg("upserting record")

	b.Queue(query, args...)
	return nil
}

func (d *Destination) remove(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	key, err := d.getKey(r)
	if err != nil {
		return err
	}
	keyColumnName := d.getKeyColumnName(key, d.config.Key)
	tableName, err := d.getTableName(r)
	if err != nil {
		return fmt.Errorf("failed to get table name for write: %w", err)
	}

	sdk.Logger(ctx).Trace().
		Str("table_name", tableName).
		Any("key", map[string]interface{}{keyColumnName: key[keyColumnName]}).
		Msg("deleting record")
	query, args, err := d.stmtBuilder.
		Delete(internal.WrapSQLIdent(tableName)).
		Where(sq.Eq{internal.WrapSQLIdent(keyColumnName): key[keyColumnName]}).
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
func (d *Destination) insert(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	tableName, err := d.getTableName(r)
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

	colArgs, valArgs, err := d.formatColumnsAndValues(ctx, tableName, key, payload)
	if err != nil {
		return fmt.Errorf("error formatting columns and values: %w", err)
	}

	sdk.Logger(ctx).Trace().
		Str("table_name", tableName).
		Msg("inserting record")
	query, args, err := d.stmtBuilder.
		Insert(internal.WrapSQLIdent(tableName)).
		Columns(colArgs...).
		Values(valArgs...).
		ToSql()
	if err != nil {
		return fmt.Errorf("error formatting insert query: %w", err)
	}

	b.Queue(query, args...)
	return nil
}

func (d *Destination) getPayload(r opencdc.Record) (opencdc.StructuredData, error) {
	if r.Payload.After == nil {
		return opencdc.StructuredData{}, nil
	}
	return d.structuredDataFormatter(r.Payload.After)
}

func (d *Destination) getKey(r opencdc.Record) (opencdc.StructuredData, error) {
	if r.Key == nil {
		return opencdc.StructuredData{}, nil
	}
	return d.structuredDataFormatter(r.Key)
}

func (d *Destination) structuredDataFormatter(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil {
		return opencdc.StructuredData{}, nil
	}
	if sdata, ok := data.(opencdc.StructuredData); ok {
		return sdata, nil
	}
	raw := data.Bytes()
	if len(raw) == 0 {
		return opencdc.StructuredData{}, nil
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
func (d *Destination) formatUpsertQuery(ctx context.Context, key opencdc.StructuredData, payload opencdc.StructuredData, keyColumnName string, tableName string) (string, []interface{}, error) {
	upsertQuery := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET", internal.WrapSQLIdent(keyColumnName))
	for column := range payload {
		// tuples form a comma separated list, so they need a comma at the end.
		// `EXCLUDED` references the new record's values. This will overwrite
		// every column's value except for the key column.
		wrappedCol := internal.WrapSQLIdent(column)
		tuple := fmt.Sprintf("%s=EXCLUDED.%s,", wrappedCol, wrappedCol)
		// TODO: Consider removing this space.
		upsertQuery += " "
		// add the tuple to the query string
		upsertQuery += tuple
	}

	// remove the last comma from the list of tuples
	upsertQuery = strings.TrimSuffix(upsertQuery, ",")

	// we have to manually append a semicolon to the upsert sql;
	upsertQuery += ";"

	colArgs, valArgs, err := d.formatColumnsAndValues(ctx, tableName, key, payload)
	if err != nil {
		return "", nil, fmt.Errorf("error formatting columns and values: %w", err)
	}

	return d.stmtBuilder.
		Insert(internal.WrapSQLIdent(tableName)).
		Columns(colArgs...).
		Values(valArgs...).
		SuffixExpr(sq.Expr(upsertQuery)).
		ToSql()
}

// formatColumnsAndValues turns the key and payload into a slice of ordered
// columns and values for upserting into Postgres.
func (d *Destination) formatColumnsAndValues(ctx context.Context, table string, key, payload opencdc.StructuredData) ([]string, []interface{}, error) {
	var colArgs []string
	var valArgs []interface{}

	// range over both the key and payload values in order to format the
	// query for args and values in proper order
	for key, val := range key {
		colArgs = append(colArgs, internal.WrapSQLIdent(key))
		formatted, err := d.formatValue(ctx, table, key, val)
		if err != nil {
			return nil, nil, fmt.Errorf("error formatting value: %w", err)
		}
		valArgs = append(valArgs, formatted)
		delete(payload, key) // NB: Delete Key from payload arguments
	}

	for field, val := range payload {
		colArgs = append(colArgs, internal.WrapSQLIdent(field))
		formatted, err := d.formatValue(ctx, table, field, val)
		if err != nil {
			return nil, nil, fmt.Errorf("error formatting value: %w", err)
		}
		valArgs = append(valArgs, formatted)
	}

	return colArgs, valArgs, nil
}

// getKeyColumnName will return the name of the first item in the key or the
// connector-configured default name of the key column name.
func (d *Destination) getKeyColumnName(key opencdc.StructuredData, defaultKeyName string) string {
	if len(key) > 1 {
		// Go maps aren't order preserving, so anything over len 1 will have
		// non-deterministic results until we handle composite keys.
		panic("composite keys not yet supported")
	}
	for k := range key {
		return k
	}
	return defaultKeyName
}

func (d *Destination) hasKey(e opencdc.Record) bool {
	return e.Key != nil && len(e.Key.Bytes()) > 0
}

func (d *Destination) formatValue(ctx context.Context, table string, column string, val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case *big.Rat:
		return d.formatBigRat(ctx, table, column, v)
	case big.Rat:
		return d.formatBigRat(ctx, table, column, &v)
	default:
		return val, nil
	}
}

// formatBigRat formats a big.Rat into a string that can be written into a NUMERIC/DECIMAL column.
func (d *Destination) formatBigRat(ctx context.Context, table string, column string, v *big.Rat) (string, error) {
	if v == nil {
		return "", nil
	}

	// we need to get the scale of the column so we that we can properly
	// round the result of dividing the input big.Rat's numerator and denominator.
	scale, err := d.dbInfo.GetNumericColumnScale(ctx, table, column)
	if err != nil {
		return "", fmt.Errorf("failed getting scale of numeric column: %w", err)
	}

	//nolint:gosec // no risk of overflow, because the scale in Pg is always <= 16383
	return decimal.NewFromBigRat(v, int32(scale)).String(), nil
}

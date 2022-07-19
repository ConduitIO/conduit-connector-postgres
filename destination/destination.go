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

package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
)

const (
	ConfigURL   = "url"
	ConfigTable = "table"
	ConfigKey   = "key"

	MetadataTable = "postgres.table"
)

type Destination struct {
	sdk.UnimplementedDestination

	conn        *pgx.Conn
	config      config
	stmtBuilder sq.StatementBuilderType
}

type config struct {
	url           string
	tableName     string
	keyColumnName string
}

func NewDestination() sdk.Destination {
	return &Destination{
		stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	d.config = config{
		url:           cfg[ConfigURL],
		tableName:     cfg[ConfigTable],
		keyColumnName: cfg[ConfigKey],
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, d.config.url)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	d.conn = conn
	return nil
}

// Write routes incoming records to their appropriate handler based on the
// operation.
func (d *Destination) Write(ctx context.Context, r sdk.Record) error {
	switch r.Operation {
	case sdk.OperationCreate, sdk.OperationSnapshot:
		return d.handleInsert(ctx, r)
	case sdk.OperationUpdate:
		return d.handleUpdate(ctx, r)
	case sdk.OperationDelete:
		return d.handleDelete(ctx, r)
	default:
		return fmt.Errorf("invalid record operation %q", r.Operation)
	}
}

func (d *Destination) Flush(context.Context) error {
	return nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close(ctx)
	}
	return nil
}

// handleInsert checks for the existence of a key. If no key is present it will
// plainly insert the data. If a key exists, but no key column name is
// configured, it attempts a plain insert to that database.
func (d *Destination) handleInsert(ctx context.Context, r sdk.Record) error {
	if !d.hasKey(r.After) || d.config.keyColumnName == "" {
		return d.insert(ctx, r)
	}
	return d.upsert(ctx, r)
}

// handleUpdate assumes the record has a key and will fail if one is not present
func (d *Destination) handleUpdate(ctx context.Context, r sdk.Record) error {
	if !d.hasKey(r.Before) {
		return fmt.Errorf("key must be provided on update actions")
	}
	// TODO handle case if the key was updated
	return d.upsert(ctx, r)
}

func (d *Destination) handleDelete(ctx context.Context, r sdk.Record) error {
	if !d.hasKey(r.Before) {
		return fmt.Errorf("key must be provided on delete actions")
	}
	return d.remove(ctx, r)
}

func (d *Destination) upsert(ctx context.Context, r sdk.Record) error {
	payload, err := d.getPayload(r.After)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	key, err := d.getKey(r.After)
	if err != nil {
		return fmt.Errorf("failed to get key: %w", err)
	}

	keyColumnName := d.getKeyColumnName(key, d.config.keyColumnName)

	tableName, err := d.getTableName(r.Metadata)
	if err != nil {
		return fmt.Errorf("failed to get table name for write: %w", err)
	}

	query, args, err := d.formatUpsertQuery(key, payload, keyColumnName, tableName)
	if err != nil {
		return fmt.Errorf("error formatting query: %w", err)
	}

	_, err = d.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert exec failed: %w", err)
	}

	return nil
}

func (d *Destination) remove(ctx context.Context, r sdk.Record) error {
	key, err := d.getKey(r.Before)
	if err != nil {
		return err
	}
	keyColumnName := d.getKeyColumnName(key, d.config.keyColumnName)
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
	_, err = d.conn.Exec(ctx, query, args...)
	return err
}

// insert is an append-only operation that doesn't care about keys, but
// can error on constraints violations so should only be used when no table
// key or unique constraints are otherwise present.
func (d *Destination) insert(ctx context.Context, r sdk.Record) error {
	tableName, err := d.getTableName(r.Metadata)
	if err != nil {
		return err
	}
	key, err := d.getKey(r.After)
	if err != nil {
		return err
	}
	payload, err := d.getPayload(r.After)
	if err != nil {
		return err
	}
	colArgs, valArgs := formatColumnsAndValues(key, payload)
	query, args, err := d.stmtBuilder.
		Insert(tableName).
		Columns(colArgs...).
		Values(valArgs...).
		ToSql()
	if err != nil {
		return fmt.Errorf("error formatting insert query: %w", err)
	}
	_, err = d.conn.Exec(ctx, query, args...)
	return err
}

func (d *Destination) getPayload(e sdk.Entity) (sdk.StructuredData, error) {
	if e.Payload == nil {
		return sdk.StructuredData{}, nil
	}
	return d.structuredDataFormatter(e.Payload.Bytes())
}

func (d *Destination) getKey(e sdk.Entity) (sdk.StructuredData, error) {
	if e.Key == nil {
		return sdk.StructuredData{}, nil
	}
	return d.structuredDataFormatter(e.Key.Bytes())
}

func (d *Destination) structuredDataFormatter(raw []byte) (sdk.StructuredData, error) {
	if len(raw) == 0 {
		return sdk.StructuredData{}, nil
	}
	data := make(map[string]interface{})
	err := json.Unmarshal(raw, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
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

	colArgs, valArgs := formatColumnsAndValues(key, payload)

	query, args, err := d.stmtBuilder.
		Insert(tableName).
		Columns(colArgs...).
		Values(valArgs...).
		SuffixExpr(sq.Expr(upsertQuery)).
		ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("error formatting query: %w", err)
	}

	return query, args, nil
}

// formatColumnsAndValues turns the key and payload into a slice of ordered
// columns and values for upserting into Postgres.
func formatColumnsAndValues(key, payload sdk.StructuredData) ([]string, []interface{}) {
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
	tableName, ok := metadata[MetadataTable]
	if !ok {
		if d.config.tableName == "" {
			return "", fmt.Errorf("no table provided for default writes")
		}
		return d.config.tableName, nil
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

func (d *Destination) hasKey(e sdk.Entity) bool {
	return e.Key != nil && len(e.Key.Bytes()) > 0
}

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
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

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

	// connSem (capacity 1, created lazily by sem) serializes use of conn: a
	// pgx connection must not be used from two goroutines. It is a channel
	// rather than a sync.Mutex so that waiting for it can honor a context.
	//
	// Teardown can be called while a Write is still running (the SDK calls it
	// without a prior Stop when a pipeline stops on an error,
	// conduit-connector-sdk#400). Teardown must neither close the connection
	// under that Write nor wait for it without bound (a Write blocked on a row
	// lock can take arbitrarily long), so it cancels the in-flight Write first
	// and then waits for the semaphore for as long as its own context allows.
	connSem chan struct{}
	semOnce sync.Once
	conn    *pgx.Conn
	// stateMu guards the fields below. It is only ever held for a few
	// instructions, never across I/O, so taking it can't block Teardown.
	stateMu     sync.Mutex
	cancelWrite context.CancelFunc // cancels the in-flight Write, if any
	closing     bool               // Teardown started: refuse and cancel Writes
	abandoned   bool               // Teardown gave up waiting: the last Write closes conn
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
	if err := d.acquire(ctx); err != nil {
		return 0, fmt.Errorf("waiting for the connection: %w", err)
	}
	defer d.release()
	if d.conn == nil {
		// Torn down: nothing was written, so nothing may be acked.
		return 0, errors.New("destination is closed")
	}

	// Teardown cancels this context to stop a Write that would otherwise
	// hold the connection (and Teardown) for as long as the database keeps
	// it waiting. Canceling rolls the batch back and Write returns (0, err),
	// so nothing is acked and the records are redelivered after a restart
	// (invariant 3: at-least-once).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer d.endWrite()
	if !d.beginWrite(cancel) {
		return 0, errors.New("destination is closing")
	}

	b := &pgx.Batch{}
	for _, rec := range recs {
		var err error
		rec, err = d.ensureStructuredData(rec)
		if err != nil {
			return 0, fmt.Errorf("failed to clean record: %w", err)
		}
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

// Teardown stops an in-flight Write (which then returns an error and acks
// nothing), waits for it to let go of the connection and closes the
// connection. The wait is bounded by ctx: if ctx ends first, Teardown returns
// ctx.Err() and leaves the connection to the Write, which closes it on its
// way out, so the connection is never closed under a running Write.
func (d *Destination) Teardown(ctx context.Context) error {
	d.stateMu.Lock()
	d.closing = true
	if d.cancelWrite != nil {
		d.cancelWrite()
	}
	d.stateMu.Unlock()

	if err := d.acquire(ctx); err != nil {
		d.stateMu.Lock()
		d.abandoned = true
		d.stateMu.Unlock()
		return fmt.Errorf("teardown gave up waiting for an in-flight write: %w", err)
	}
	defer d.release()
	return d.closeConn(ctx)
}

// closeConn closes and forgets the connection. The caller holds the semaphore.
func (d *Destination) closeConn(ctx context.Context) error {
	if d.conn == nil {
		return nil
	}
	conn := d.conn
	d.conn = nil
	return conn.Close(ctx)
}

func (d *Destination) sem() chan struct{} {
	d.semOnce.Do(func() { d.connSem = make(chan struct{}, 1) })
	return d.connSem
}

// acquire takes the connection semaphore, giving up when ctx ends.
func (d *Destination) acquire(ctx context.Context) error {
	select {
	case d.sem() <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Destination) release() {
	<-d.sem()
}

// beginWrite registers cancel so Teardown can stop the Write. It reports false
// if Teardown already started, in which case the Write must not touch conn.
func (d *Destination) beginWrite(cancel context.CancelFunc) bool {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	if d.closing {
		return false
	}
	d.cancelWrite = cancel
	return true
}

// endWrite deregisters the Write. If Teardown gave up waiting for it, the
// Write is the last user of the connection and closes it. The caller still
// holds the semaphore.
func (d *Destination) endWrite() {
	d.stateMu.Lock()
	d.cancelWrite = nil
	abandoned := d.abandoned
	d.stateMu.Unlock()
	if abandoned {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.closeConn(closeCtx)
	}
}

// handleInsert adds a query to the batch that stores the record in the target
// table. It checks for the existence of a key. If no key is present or a key
// exists and no key column name is configured, it will plainly insert the data.
// Otherwise it upserts the record.
func (d *Destination) handleInsert(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
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
	payload := r.Payload.After.(opencdc.StructuredData)
	key := r.Key.(opencdc.StructuredData)
	tableName, err := d.getTableName(r)
	if err != nil {
		return fmt.Errorf("failed to get table name for upsert: %w", err)
	}

	query, args, err := d.formatUpsertQuery(ctx, key, payload, tableName)
	if err != nil {
		return fmt.Errorf("error formatting query: %w", err)
	}
	sdk.Logger(ctx).Trace().
		Str("table", tableName).
		Str("query", query).
		Any("key", key).
		Msg("upserting record")

	b.Queue(query, args...)
	return nil
}

func (d *Destination) remove(ctx context.Context, r opencdc.Record, b *pgx.Batch) error {
	key := r.Key.(opencdc.StructuredData)
	tableName, err := d.getTableName(r)
	if err != nil {
		return fmt.Errorf("failed to get table name for delete: %w", err)
	}

	where := make(sq.Eq)
	for col, val := range key {
		where[internal.WrapSQLIdent(col)] = val
	}

	query, args, err := d.stmtBuilder.
		Delete(internal.WrapSQLIdent(tableName)).
		Where(where).
		ToSql()
	if err != nil {
		return fmt.Errorf("error formatting delete query: %w", err)
	}

	sdk.Logger(ctx).Trace().
		Str("table", tableName).
		Str("query", query).
		Any("key", key).
		Msg("deleting record")

	b.Queue(query, args...)
	return nil
}

// formatUpsertQuery manually formats the UPSERT and ON CONFLICT query statements.
// The `ON CONFLICT` portion of this query needs to specify the constraint
// name.
// * In our case, we can only rely on the record.Key's parsed key value.
// * If other schema constraints prevent a write, this won't upsert on
// that conflict.
func (d *Destination) formatUpsertQuery(
	ctx context.Context,
	key, payload opencdc.StructuredData,
	tableName string,
) (string, []interface{}, error) {
	colArgs, valArgs, err := d.formatColumnsAndValues(ctx, key, payload, tableName)
	if err != nil {
		return "", nil, fmt.Errorf("error formatting columns and values: %w", err)
	}

	stmt := d.stmtBuilder.
		Insert(internal.WrapSQLIdent(tableName)).
		Columns(colArgs...).
		Values(valArgs...)

	if len(key) > 0 {
		keyColumns := slices.Collect(maps.Keys(key))
		for i := range keyColumns {
			keyColumns[i] = internal.WrapSQLIdent(keyColumns[i])
		}

		var setOnConflict []string
		for column := range payload {
			// tuples form a comma separated list, so they need a comma at the end.
			// `EXCLUDED` references the new record's values. This will overwrite
			// every column's value except for the key columns.
			wrappedCol := internal.WrapSQLIdent(column)
			tuple := fmt.Sprintf("%s=EXCLUDED.%s", wrappedCol, wrappedCol)
			// add the tuple to the query string
			setOnConflict = append(setOnConflict, tuple)
		}

		upsertQuery := fmt.Sprintf(
			"ON CONFLICT (%s) DO UPDATE SET %s",
			strings.Join(keyColumns, ","),
			strings.Join(setOnConflict, ","),
		)

		stmt = stmt.Suffix(upsertQuery)
	}

	return stmt.ToSql()
}

// formatColumnsAndValues turns the key and payload into a slice of ordered
// columns and values for upserting into Postgres.
func (d *Destination) formatColumnsAndValues(
	ctx context.Context,
	key, payload opencdc.StructuredData,
	table string,
) ([]string, []interface{}, error) {
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

func (d *Destination) hasKey(e opencdc.Record) bool {
	structuredKey, ok := e.Key.(opencdc.StructuredData)
	if !ok {
		return false
	}
	return len(structuredKey) > 0
}

// ensureStructuredData makes sure the record key and payload are structured data.
func (d *Destination) ensureStructuredData(r opencdc.Record) (opencdc.Record, error) {
	payloadAfter, err := d.structuredDataFormatter(r.Payload.After)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to get structured data for .Payload.After: %w", err)
	}
	key, err := d.structuredDataFormatter(r.Key)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to get structured data for .Key: %w", err)
	}

	r.Key = key
	r.Payload.After = payloadAfter
	return r, nil
}

func (d *Destination) structuredDataFormatter(data opencdc.Data) (opencdc.StructuredData, error) {
	switch data := data.(type) {
	case opencdc.StructuredData:
		// already structured data, no need to convert
		return data, nil
	case opencdc.RawData:
		raw := data.Bytes()
		if len(raw) == 0 {
			return opencdc.StructuredData{}, nil
		}
		m := make(map[string]interface{})
		err := json.Unmarshal(raw, &m)
		if err != nil {
			return nil, fmt.Errorf("failed to JSON unmarshal raw data: %w", err)
		}
		return m, nil
	case nil:
		return opencdc.StructuredData{}, nil
	default:
		return nil, fmt.Errorf("unexpected data type %T, expected StructuredData or RawData", data)
	}
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

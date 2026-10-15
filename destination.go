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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/destination"
	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
	connConfig, err := pgx.ParseConfig(d.config.URL)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	// main keeps pgx 5.7's SCRAM behaviour; channel_binding=require stays
	// opt-in. See cpool.DefaultSCRAMCompat.
	cpool.DefaultSCRAMCompat(&connConfig.Config)

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	d.conn = conn

	d.getTableName, err = d.config.TableFunction()
	if err != nil {
		return fmt.Errorf("invalid table name or table name function: %w", err)
	}

	return nil
}

// Write routes incoming records to their appropriate handler based on the
// operation.
func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	// Fail fast once Teardown has begun instead of queueing behind it.
	if d.isClosing() {
		return 0, errors.New("destination is closing")
	}
	if err := d.acquire(ctx); err != nil {
		return 0, fmt.Errorf("waiting for the connection: %w", err)
	}
	// finishWrite releases the semaphore (and closes the connection if
	// Teardown gave up waiting), on every path below.
	defer d.finishWrite()
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
	if !d.beginWrite(cancel) {
		return 0, errors.New("destination is closing")
	}
	// pgx must not watch this context. When a watched context ends in the
	// middle of a batch, pgx leaves its context-watcher goroutine parked
	// forever (it waits for an Unwatch that the failed batch never reaches),
	// pinning the connection: one leaked goroutine per interrupted
	// connection. So the database calls get a context that cannot be
	// canceled, and the interruption is done here instead: when ctx ends,
	// the connection's deadline is set to now, which fails the blocked read
	// the same way pgx does. stopWatch always runs before Write returns.
	stopWatch := interruptOnDone(ctx, d.conn)
	defer func() {
		// If the watcher fired, the connection carries a deadline in the
		// past and is unusable: close it before Write returns so a later
		// caller never sees a half-dead connection.
		if stopWatch() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = d.closeConn(closeCtx)
		}
	}()
	ctx = context.WithoutCancel(ctx)

	b := &pgx.Batch{}
	var skipped int
	for _, rec := range recs {
		// Schema-drift markers (postgres.schema.drift, source/logrepl) are
		// keyless, payload-less OperationCreate records that carry the approval
		// checkpoint for the escape hatch. Writing them would insert an empty
		// DEFAULT VALUES row (or error on NOT NULL columns) — FM6 in
		// docs/design-documents/20260829-dbz3-b1-schema-drift-escape-hatch.md
		// pins an explicit no-op: never a silent wrong row. The record is still
		// acked (Write returns the full count), so the marker's position
		// checkpoint flows end to end.
		if rec.Metadata[logrepl.MetadataSchemaDrift] == "true" {
			sdk.Logger(ctx).Debug().
				Str("table", rec.Metadata[logrepl.MetadataSchemaDriftTable]).
				Msg("skipping schema-drift marker record at the destination (FM6 no-op)")
			skipped++
			continue
		}
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

	for i := 0; i < len(recs)-skipped; i++ {
		// fetch error for each statement
		if _, err := br.Exec(); err != nil {
			// the batch is executed in a transaction, if one failed all failed
			_ = br.Close()
			return 0, fmt.Errorf("failed to execute query for record %d: %w", i, err)
		}
	}
	// Invariant 1: ack only after the destination confirms a durable write.
	// The batch runs in one implicit transaction, and the COMMIT outcome (e.g.
	// deferred constraints, serialization failures) is reported only by Close,
	// after every statement result has been read successfully. If Close fails
	// the transaction was rolled back, so none of the records are stored and
	// none may be acked.
	if err := br.Close(); err != nil {
		return 0, fmt.Errorf("failed to commit batch: %w", err)
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
		// A writer releases the semaphore under stateMu (finishWrite), so
		// trying once more here, under the same mutex, cannot miss a writer
		// that just finished: either the semaphore is free and nobody can
		// use the connection, or the writer is still inside and will see
		// abandoned and close the connection itself.
		select {
		case d.sem() <- struct{}{}:
			d.stateMu.Unlock()
			defer d.release()
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			return d.closeConn(closeCtx)
		default:
		}
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

// acquire takes the connection semaphore, giving up when ctx ends. A free
// semaphore is always taken, even with an already-ended ctx (select alone
// would pick at random between the two ready cases).
func (d *Destination) acquire(ctx context.Context) error {
	select {
	case d.sem() <- struct{}{}:
		return nil
	default:
	}
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

func (d *Destination) isClosing() bool {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	return d.closing
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

// finishWrite ends a Write: it deregisters the cancel function and releases
// the semaphore. If Teardown gave up waiting for this Write, the Write is the
// last user of the connection and closes it first. The caller holds the
// semaphore. The semaphore is released under stateMu, after reading abandoned,
// so Teardown's last-chance acquire (also under stateMu) and this read cannot
// interleave in a way that leaves the connection open.
func (d *Destination) finishWrite() {
	d.stateMu.Lock()
	d.cancelWrite = nil
	if !d.abandoned {
		d.release()
		d.stateMu.Unlock()
		return
	}
	d.stateMu.Unlock()
	// abandoned is only ever set while a writer holds the semaphore, so no
	// one else is using the connection.
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.closeConn(closeCtx)
	d.release()
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
	payload, _ := r.Payload.After.(opencdc.StructuredData) // ensureStructuredData made it structured
	key, _ := r.Key.(opencdc.StructuredData)
	tableName, err := d.getTableName(r)
	if err != nil {
		return fmt.Errorf("failed to get table name for upsert: %w", err)
	}
	if len(payload) == 0 {
		// An empty row would be an INSERT of only the key (or of nothing),
		// acked as if the record had been applied (invariant 6).
		return fmt.Errorf("%s: %s record for table %q has no fields in .Payload.After. "+
			"Fix: make sure create, update and snapshot records carry the row in .Payload.After",
			ErrorCodeDestinationPayloadEmpty, r.Operation, tableName)
	}

	if err := checkIdentifiers(tableName, key, payload); err != nil {
		return err
	}
	if err := checkKeyValues(r.Operation, tableName, key); err != nil {
		return err
	}
	query, args, err := d.formatUpsertQuery(key, payload, tableName)
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
	key, _ := r.Key.(opencdc.StructuredData) // hasKey checked it
	tableName, err := d.getTableName(r)
	if err != nil {
		return fmt.Errorf("failed to get table name for delete: %w", err)
	}

	if err := checkIdentifiers(tableName, key, nil); err != nil {
		return err
	}
	if err := checkKeyValues(r.Operation, tableName, key); err != nil {
		return err
	}
	where := make(sq.Eq)
	for col, val := range key {
		where[quoteIdent(col)] = val
	}

	query, args, err := d.stmtBuilder.
		Delete(quoteIdent(tableName)).
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

// formatUpsertQuery builds the INSERT for one record. With a key it is an
// upsert on the key columns: ON CONFLICT (key) DO UPDATE SET every other column
// of the record, or DO NOTHING when the record has no other column (a table
// whose columns are all key columns; an empty SET list is a syntax error).
//
// The key columns come from the record key, the other columns from the
// payload, and a payload field that is also a key column is written once, with
// the key's value. Neither map is modified: the record belongs to the caller
// (#343). Columns are sorted so that a record shape always gives the same SQL
// text.
func (d *Destination) formatUpsertQuery(
	key, payload opencdc.StructuredData,
	tableName string,
) (string, []interface{}, error) {
	keyColumns := slices.Sorted(maps.Keys(key))
	var otherColumns []string
	for field := range payload {
		if _, isKey := key[field]; !isKey {
			otherColumns = append(otherColumns, field)
		}
	}
	slices.Sort(otherColumns)

	colArgs := make([]string, 0, len(keyColumns)+len(otherColumns))
	valArgs := make([]interface{}, 0, len(keyColumns)+len(otherColumns))
	for _, col := range keyColumns {
		colArgs = append(colArgs, quoteIdent(col))
		valArgs = append(valArgs, d.formatValue(key[col]))
	}
	for _, col := range otherColumns {
		colArgs = append(colArgs, quoteIdent(col))
		valArgs = append(valArgs, d.formatValue(payload[col]))
	}

	stmt := d.stmtBuilder.
		Insert(quoteIdent(tableName)).
		Columns(colArgs...).
		Values(valArgs...)

	if len(keyColumns) > 0 {
		quotedKeys := make([]string, len(keyColumns))
		for i, col := range keyColumns {
			quotedKeys[i] = quoteIdent(col)
		}
		action := "DO NOTHING"
		if len(otherColumns) > 0 {
			setOnConflict := make([]string, len(otherColumns))
			for i, col := range otherColumns {
				// EXCLUDED is the row proposed for insertion: every column but
				// the key columns is overwritten with the record's value.
				setOnConflict[i] = fmt.Sprintf("%s=EXCLUDED.%s", quoteIdent(col), quoteIdent(col))
			}
			action = "DO UPDATE SET " + strings.Join(setOnConflict, ",")
		}
		stmt = stmt.Suffix(fmt.Sprintf("ON CONFLICT (%s) %s", strings.Join(quotedKeys, ","), action))
	}

	return stmt.ToSql()
}

// checkKeyValues fails unless every key value is a single non-null scalar.
//
// Invariant 6: a key identifies one row. In a delete, squirrel turns a nil
// value into IS NULL (every row with a NULL there) and a slice into IN (...)
// (several rows; an empty slice into 1=0, a silent no-op). In an upsert, a
// null key value never conflicts, so every redelivery of the record appends
// another row on a nullable unique column. []byte is a scalar (bytea).
func checkKeyValues(op opencdc.Operation, table string, key opencdc.StructuredData) error {
	for col, val := range key {
		if val == nil {
			return fmt.Errorf("%s: the %s record's key has a null value for column %q of table %q, "+
				"so it cannot identify one row. Fix: send records with a non-null value for every key column",
				ErrorCodeDestinationKeyColumnMissing, op, col, table)
		}
		if _, isBytes := val.([]byte); isBytes {
			continue
		}
		switch reflect.ValueOf(val).Kind() {
		case reflect.Slice, reflect.Array, reflect.Map:
			return fmt.Errorf("%s: the %s record's key has a %T value for column %q of table %q, "+
				"which would match several rows or none. Fix: send one scalar value per key column",
				ErrorCodeDestinationKeyValueNotScalar, op, val, col, table)
		default:
		}
	}
	return nil
}

// maxIdentifierBytes is NAMEDATALEN-1 of a default Postgres build. Postgres
// truncates longer identifiers to this length with only a NOTICE, so a longer
// field name would write into whichever column has the truncated name.
const maxIdentifierBytes = 63

// checkIdentifiers fails for a table or column name that Postgres would not
// take literally: one with a NUL byte (pgx.Identifier.Sanitize strips it, so
// "v\x00" would write into column v) or one longer than maxIdentifierBytes
// (Postgres truncates it). Both would silently alias another column or table.
func checkIdentifiers(table string, key, payload opencdc.StructuredData) error {
	check := func(kind, name string) error {
		switch {
		case strings.IndexByte(name, 0) >= 0:
			return fmt.Errorf("%s: %s name %q contains a NUL byte, which Postgres identifiers cannot hold. "+
				"Fix: rename the field (or the table template result) before the destination",
				ErrorCodeDestinationInvalidIdentifier, kind, name)
		case len(name) > maxIdentifierBytes:
			return fmt.Errorf("%s: %s name %q is %d bytes long, Postgres would truncate it to %d bytes and "+
				"address a different column or table. Fix: rename it to at most %d bytes before the destination",
				ErrorCodeDestinationInvalidIdentifier, kind, name, len(name), maxIdentifierBytes, maxIdentifierBytes)
		case name == "":
			return fmt.Errorf("%s: empty %s name. Fix: set the table template or the field name",
				ErrorCodeDestinationInvalidIdentifier, kind)
		}
		return nil
	}
	if err := check("table", table); err != nil {
		return err
	}
	for name := range key {
		if err := check("column", name); err != nil {
			return err
		}
	}
	for name := range payload {
		if err := check("column", name); err != nil {
			return err
		}
	}
	return nil
}

// quoteIdent quotes a table or column name as a Postgres identifier: double
// quotes around it, and every double quote inside it doubled. The names come
// from records (payload field names, the record key, the table template), so
// they must never change the SQL text. internal.WrapSQLIdent (strconv.Quote)
// escapes a double quote as \", which Postgres does not understand, so a name
// with a double quote broke the statement.
//
// Callers check the names with checkIdentifiers first: Sanitize strips NUL
// bytes, which would alias names.
func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
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
		// Invariant 6: numbers are decoded exactly. Through float64, int8 max
		// was stored as int8 min on amd64 and every integer above 2^53 was
		// rounded (#402), and a delete key matched a different row.
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var m map[string]interface{}
		if err := dec.Decode(&m); err != nil {
			return nil, fmt.Errorf("failed to JSON unmarshal raw data: %w", err)
		}
		if dec.More() {
			return nil, fmt.Errorf("failed to JSON unmarshal raw data: more than one JSON value")
		}
		return exactNumbers(m).(map[string]interface{}), nil
	case nil:
		return opencdc.StructuredData{}, nil
	default:
		return nil, fmt.Errorf("unexpected data type %T, expected StructuredData or RawData", data)
	}
}

func (d *Destination) formatValue(val interface{}) interface{} {
	switch v := val.(type) {
	case *big.Rat:
		return d.formatBigRat(v)
	case big.Rat:
		return d.formatBigRat(&v)
	default:
		return val
	}
}

// formatBigRat formats a big.Rat into a string that can be written into a
// NUMERIC/DECIMAL column. The column's scale is intentionally not applied
// here, Postgres applies the live column definition on assignment (see
// internal.RatToDecimalString).
func (d *Destination) formatBigRat(v *big.Rat) string {
	if v == nil {
		return ""
	}
	return internal.RatToDecimalString(v)
}

// interruptOnDone makes a blocked read or write on conn fail once ctx is done,
// by moving the connection's deadline to now. The connection is unusable
// afterwards (pgx closes it on the resulting error), as it is after pgx's own
// context cancellation. The returned stop function ends the watcher and waits
// for it, and reports whether the deadline was moved (the connection is then
// unusable and the caller closes it). Once it has returned, ctx ending no
// longer touches conn, so a healthy connection never gets a stray deadline.
func interruptOnDone(ctx context.Context, conn *pgx.Conn) (stop func() (fired bool)) {
	var (
		mu       sync.Mutex
		finished bool
		fired    bool
		done     = make(chan struct{})
		exited   = make(chan struct{})
	)
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()
			if !finished {
				fired = true
				_ = conn.PgConn().Conn().SetDeadline(time.Now())
			}
		case <-done:
		}
	}()
	return func() bool {
		mu.Lock()
		finished = true
		mu.Unlock()
		close(done)
		<-exited
		return fired // exited happens-before this read
	}
}

// ErrorCodeDestinationPayloadEmpty prefixes the error for a create, update or
// snapshot record without fields.
const ErrorCodeDestinationPayloadEmpty = "postgres.destination.payload_empty"

// exactNumbers replaces the json.Number values in v (a decoded JSON value) with
// an int64 or uint64 when the number is an integer literal in range, and with a
// jsonNumber otherwise, which keeps the literal so that numeric, text and jsonb
// columns store it exactly.
func exactNumbers(v interface{}) interface{} {
	switch v := v.(type) {
	case map[string]interface{}:
		for k, e := range v {
			v[k] = exactNumbers(e)
		}
		return v
	case []interface{}:
		for i, e := range v {
			v[i] = exactNumbers(e)
		}
		return v
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if u, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return u
		}
		return jsonNumber(v)
	default:
		return v
	}
}

// jsonNumber is a JSON number literal that is not an int64 or uint64 literal
// (a fraction, an exponent, or an integer beyond uint64). It implements the
// pgx valuer interfaces so that each column type gets it exactly or fails:
//
//   - numeric: the exact decimal value (12345678901234567.89 stays exact);
//   - int2/int4/int8: the integer if the value is whole and in range ("1e3",
//     "1.0"), an error otherwise;
//   - float4/float8: the nearest float, as before;
//   - text and jsonb: the literal itself.
//
// Before, every such number became a float64, which rounded numeric values with
// more than about 15 significant digits.
type jsonNumber string

var (
	_ pgtype.NumericValuer = jsonNumber("")
	_ pgtype.Int64Valuer   = jsonNumber("")
	_ pgtype.Float64Valuer = jsonNumber("")
	_ pgtype.TextValuer    = jsonNumber("")
	_ json.Marshaler       = jsonNumber("")
)

func (n jsonNumber) rat() (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(string(n))
	if !ok {
		return nil, fmt.Errorf("invalid JSON number %q", string(n))
	}
	return r, nil
}

// NumericValue returns the exact decimal value.
func (n jsonNumber) NumericValue() (pgtype.Numeric, error) {
	var num pgtype.Numeric
	if err := num.Scan(string(n)); err != nil {
		return pgtype.Numeric{}, fmt.Errorf("JSON number %q as numeric: %w", string(n), err)
	}
	return num, nil
}

// Int64Value returns the value if it is a whole number in the int64 range.
func (n jsonNumber) Int64Value() (pgtype.Int8, error) {
	r, err := n.rat()
	if err != nil {
		return pgtype.Int8{}, err
	}
	if !r.IsInt() || !r.Num().IsInt64() {
		return pgtype.Int8{}, fmt.Errorf("JSON number %q is not a whole number in the int64 range", string(n))
	}
	return pgtype.Int8{Int64: r.Num().Int64(), Valid: true}, nil
}

// Float64Value returns the nearest float64.
func (n jsonNumber) Float64Value() (pgtype.Float8, error) {
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return pgtype.Float8{}, fmt.Errorf("JSON number %q as float: %w", string(n), err)
	}
	return pgtype.Float8{Float64: f, Valid: true}, nil
}

// TextValue returns the literal.
func (n jsonNumber) TextValue() (pgtype.Text, error) {
	return pgtype.Text{String: string(n), Valid: true}, nil
}

// MarshalJSON writes the literal as a JSON number, for json and jsonb columns.
func (n jsonNumber) MarshalJSON() ([]byte, error) {
	return []byte(n), nil
}

// ErrorCodeDestinationInvalidIdentifier prefixes the error for a table or
// column name that Postgres would not take literally.
const ErrorCodeDestinationInvalidIdentifier = "postgres.destination.invalid_identifier"

// ErrorCodeDestinationKeyColumnMissing prefixes the error for a delete whose key
// has no usable value for a key column.
const ErrorCodeDestinationKeyColumnMissing = "postgres.destination.key_column_missing"

// ErrorCodeDestinationKeyValueNotScalar prefixes the error for a key value that
// is a list or an object.
const ErrorCodeDestinationKeyValueNotScalar = "postgres.destination.key_value_not_scalar"

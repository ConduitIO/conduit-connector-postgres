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
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/destination"
	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
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
	// upsertSQLCache and deleteSQLCache memoize the SQL text of a record shape.
	// The text depends only on the column names (every value is a single
	// placeholder), so records that share a shape reuse one string instead of
	// rebuilding it with squirrel for every record. They are read and written
	// only inside Write, which connSem serializes, so they need no lock of
	// their own, and they stay bounded by the number of distinct (table, column
	// set) shapes, not by the number of records.
	upsertSQLCache map[sqlShape]string
	deleteSQLCache map[sqlShape]string
	// guardSchema is set in Open: it is true when the connection's query exec
	// mode caches statement parameter types (cache_statement, cache_describe),
	// so a schema change must invalidate that cache. describe_exec, exec and
	// simple_protocol learn the parameter types fresh or never cache them, so
	// they need no guard.
	guardSchema bool
	// schemaFingerprint maps a table name (as getTableName returns it) to the
	// md5 of its live (column name, type OID, typmod) list, learned on first
	// write and re-checked by the schema guard on every batch. It is read and
	// written only inside Write, which connSem serializes, so it needs no lock
	// of its own; it stays bounded by the number of distinct tables written.
	schemaFingerprint map[string]string
	// connConfig is the parsed configuration Open connected with, kept so a
	// Write can replace a connection that pgx closed after an error with a
	// fresh one (and so a fresh, empty statement cache).
	connConfig *pgx.ConnConfig
	// stats counts the write path's recoveries. The SDK has no metrics API
	// for connectors, so each recovery is logged at Info with its running
	// total; the counters are atomic so they can be read outside Write.
	stats destinationStats
	// recoveryLogs rate-limits the Info lines of those recoveries, so a
	// pooler that makes most batches retry cannot flood the log.
	recoveryLogs recoveryLogThrottle
}

// recoveryLogInterval is the least time between two Info lines about the same
// kind of recovery. The lines in between are counted, not dropped silently:
// the next line carries the count, and the running totals in destinationStats
// count every recovery.
const recoveryLogInterval = 30 * time.Second

// recoveryLogThrottle remembers when each kind of recovery was last logged.
type recoveryLogThrottle struct {
	mu         sync.Mutex
	last       map[string]time.Time
	suppressed map[string]uint64
}

// recoveryLog returns an Info event for a recovery of the given kind, with
// the number of lines of that kind suppressed since the last one, or nil if
// one was logged less than recoveryLogInterval ago. A nil *zerolog.Event is a
// no-op, so callers chain on the result unconditionally.
func (d *Destination) recoveryLog(ctx context.Context, kind string) *zerolog.Event {
	t := &d.recoveryLogs
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if t.last == nil {
		t.last = make(map[string]time.Time)
		t.suppressed = make(map[string]uint64)
	}
	if last, ok := t.last[kind]; ok && now.Sub(last) < recoveryLogInterval {
		t.suppressed[kind]++
		return nil
	}
	n := t.suppressed[kind]
	t.suppressed[kind] = 0
	t.last[kind] = now
	return sdk.Logger(ctx).Info().Uint64("suppressed_since_last", n)
}

// destinationStats counts the write path's recoveries over a destination's
// lifetime: schema guards that fired, connections replaced after pgx closed
// them, and batches retried after a pooler lost or duplicated a prepared
// statement.
type destinationStats struct {
	schemaChanges   atomic.Uint64
	reconnects      atomic.Uint64
	staleStatements atomic.Uint64
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
	d.guardSchema = setDestinationQueryExecMode(connConfig, d.config.URL)
	d.connConfig = connConfig
	if d.guardSchema {
		sdk.Logger(ctx).Warn().
			Str("default_query_exec_mode", connConfig.DefaultQueryExecMode.String()).
			Msg("the url selects a query exec mode that caches prepared statements. It is safe only on a " +
				"direct connection or behind a pooler that does not share prepared statements between clients. " +
				"Behind PgBouncer (at any max_prepared_statements) or another statement-sharing pooler, a column " +
				"type change can make writes convert values silently (5/2 stored as 3 in an integer column); " +
				"use default_query_exec_mode=describe_exec there")
	}

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

// setDestinationQueryExecMode selects describe_exec as the default query exec
// mode unless the connection settings set default_query_exec_mode themselves,
// in which case the explicit value is kept. It reports whether the mode in use
// caches statement parameter types (cache_statement or cache_describe), so
// Open can enable the schema guard and warn.
//
// Invariant 6: describe_exec describes every statement on the connection that
// runs it, so parameter types follow ALTER COLUMN ... TYPE (#367) and nothing
// cached can go stale. A cached statement keeps the parameter types Postgres
// inferred when it was prepared. On a direct connection the schema guard
// catches that: it aborts the batch and clears the cache when the table's
// column types change. Behind a pooler that shares prepared statements across
// clients (PgBouncer with max_prepared_statements > 0) the guard cannot: a
// destination that connects after the ALTER learns a fresh fingerprint but is
// handed a statement another client prepared before it, and 5/2 into a column
// that had become integer is stored as 3 (#436). That is why cache_statement,
// the default from v0.17.0 to v0.17.1, is no longer the default.
func setDestinationQueryExecMode(cfg *pgx.ConnConfig, rawURL string) (cachesTypes bool) {
	if !explicitQueryExecMode(rawURL) {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
		return false
	}
	switch cfg.DefaultQueryExecMode {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe:
		return true
	default:
		return false
	}
}

// explicitQueryExecMode reports whether the connection settings set
// default_query_exec_mode anywhere pgx reads it from: the URL, the
// keyword/value form, or a service file selected with service= or PGSERVICE
// (PGSERVICEFILE). pgx's parsed config cannot tell an explicit cache_statement
// from its default, so this asks pgconn, which keeps every setting it does not
// know itself, default_query_exec_mode included, in RuntimeParams.
func explicitQueryExecMode(connString string) bool {
	cfg, err := pgconn.ParseConfig(connString)
	if err != nil {
		// pgx.ParseConfig parses the same string first and fails the same way,
		// so Open never gets here with an invalid string.
		return false
	}
	_, ok := cfg.RuntimeParams["default_query_exec_mode"]
	return ok
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

	// The records become statements once per Write; only the batch built
	// from them (with its schema guards) is rebuilt on a retry. So the
	// per-record checks and the drift warnings run once, not per attempt.
	p, err := d.planWrites(ctx, recs)
	if err != nil {
		return 0, err
	}

	// freshRetried records that the batch already got its one retry on a
	// clean statement cache after it could not be sent (see isUnsentBatch).
	freshRetried := false
	for attempt := 0; ; attempt++ {
		// A connection pgx closed after an error (a failed send, a network
		// error) would fail every later Write; replace it first. The new
		// connection has an empty statement cache.
		if err := d.ensureConn(ctx); err != nil {
			return 0, err
		}
		changed, err := d.attemptWrite(ctx, p)
		if err == nil && changed == "" {
			return len(recs), nil
		}
		// Teardown canceled the Write: the batch was rolled back (or never
		// sent), and nothing is retried or acked.
		if ctx.Err() != nil {
			if err == nil {
				err = ctx.Err()
			}
			return 0, err
		}
		if err := d.prepareRetry(ctx, p, attempt, changed, err, &freshRetried); err != nil {
			return 0, err
		}
	}
}

// prepareRetry decides what follows a failed attempt of a Write. It returns
// nil after readying the destination for another attempt, or the error the
// Write returns. changed is the table whose schema guard fired (err is nil
// then); freshRetried records that the one retry after an unsent batch was
// used.
func (d *Destination) prepareRetry(
	ctx context.Context, p *writePlan, attempt int, changed string, err error, freshRetried *bool,
) error {
	retryable := attempt < maxWriteRetries
	switch {
	case err == nil: // a schema guard fired and rolled the batch back
		total := d.stats.schemaChanges.Add(1)
		if !retryable {
			return fmt.Errorf("%s: the schema of table %q changed %d times while one batch was being "+
				"written; it is being altered faster than writes can re-describe it. Fix: retry once the "+
				"DDL is done", ErrorCodeDestinationSchemaChurn, changed, attempt+1)
		}
		d.recoveryLog(ctx, "schema_change").
			Str("table", changed).
			Uint64("schema_changes_total", total).
			Msg("destination table schema changed: rolled the batch back, clearing the statement cache and retrying")
		// Invariant 6: forget the stale fingerprints so the retry re-learns
		// them, and reset pgx's statement and description caches so the
		// retry re-prepares with the live column types, not the cached ones.
		d.schemaFingerprint = nil
		if err := d.resetStatementCache(ctx); err != nil {
			return fmt.Errorf("failed to clear the statement cache after a schema change: %w", err)
		}
	case d.guardSchema && retryable && isStaleStatement(err):
		// Only a pooler that does not track prepared statements (PgBouncer
		// in transaction mode with max_prepared_statements=0) produces
		// these with a cached statement: the server that runs the batch is
		// not the one the statement was prepared on (26000), or already has
		// one of that name from another client (42P05). Either error aborts
		// the batch's implicit transaction, or happens while preparing,
		// before anything ran, so nothing was committed and a retry cannot
		// apply a record twice.
		total := d.stats.staleStatements.Add(1)
		d.recoveryLog(ctx, "stale_statement").
			Err(err).
			Uint64("stale_statement_retries_total", total).
			Msg("a prepared statement was missing or already existed on the server (a transaction-mode " +
				"connection pooler?): clearing the statement cache and retrying the batch. Behind such a " +
				"pooler set default_query_exec_mode=describe_exec or exec in the url")
		if err := d.clearStatementCache(ctx); err != nil {
			return err
		}
	case d.guardSchema && retryable && !*freshRetried && isUnsentBatch(err):
		// The batch failed before its Sync was sent, so the server never
		// committed any of it. In a caching mode the likely cause is a
		// statement cached before ALTER widened a column (a value that
		// overflows the old type cannot be encoded), and on any record but
		// the first pgx closes the connection on this path (it cannot
		// resynchronize a half-sent pipeline). Retry once on a clean cache;
		// if the batch fails again, the value itself does not fit.
		*freshRetried = true
		d.recoveryLog(ctx, "unsent_batch").
			Err(err).
			Msg("could not send the batch: retrying it once with an empty statement cache")
		if err := d.clearStatementCache(ctx); err != nil {
			return err
		}
	default:
		return d.failWrite(ctx, p, err)
	}
	return nil
}

// maxWriteRetries bounds the retries of one Write, whatever their cause: a
// schema guard that fired, a missing or duplicate prepared statement, or a
// batch that could not be sent. One retry succeeds unless the cause keeps
// recurring; beyond this bound the destination gives up rather than spin.
const maxWriteRetries = 3

// ErrorCodeDestinationSchemaChurn prefixes the error for a batch whose target
// table changed schema on every retry.
const ErrorCodeDestinationSchemaChurn = "postgres.destination.schema_churn"

// ErrorCodeDestinationTableNotFound prefixes the error for a write to a table
// that does not exist (or is not visible through the connection's
// search_path).
const ErrorCodeDestinationTableNotFound = "postgres.destination.table_not_found"

// attemptWrite builds the batch for p and runs it once. It returns the table
// whose schema guard fired (the batch was rolled back and must be retried), or
// the error that failed the batch.
func (d *Destination) attemptWrite(ctx context.Context, p *writePlan) (changed string, err error) {
	err = d.withConn(ctx, func(ctx context.Context) error {
		b, err := d.buildBatch(ctx, p)
		if err != nil {
			return err
		}
		changed, err = d.execBatch(ctx, b, p)
		return err
	})
	return changed, err
}

// withConn runs fn against d.conn with a context pgx cannot watch. When a
// watched context ends in the middle of a batch, pgx leaves its
// context-watcher goroutine parked forever (it waits for an Unwatch that the
// failed batch never reaches), pinning the connection: one leaked goroutine
// per interrupted connection. So fn gets a context that cannot be canceled,
// and the interruption is done here instead: when ctx ends, the connection's
// deadline is set to now, which fails the blocked read the same way pgx does.
// If that happened, the connection is unusable and is closed (and forgotten)
// before withConn returns, so a later caller never sees a half-dead
// connection.
func (d *Destination) withConn(ctx context.Context, fn func(context.Context) error) error {
	stop := interruptOnDone(ctx, d.conn)
	err := fn(context.WithoutCancel(ctx))
	if stop() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.closeConn(closeCtx)
	}
	return err
}

// ensureConn replaces a connection pgx has closed with a new one. pgx closes
// a connection when it cannot resynchronize it: a network error, or a batch
// that failed after some of its statements were sent (pgconn's
// Pipeline.Close on unsynced requests). Without this every later Write would
// fail on the closed connection, and the stale statement cache that caused
// the failure would never be cleared. The new connection starts with empty
// statement and description caches, and the schema fingerprints are
// re-learned. ctx bounds the connect, so Teardown can interrupt it.
func (d *Destination) ensureConn(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("destination is closed")
	}
	if !d.conn.IsClosed() {
		return nil
	}
	conn, err := pgx.ConnectConfig(ctx, d.connConfig)
	if err != nil {
		// d.conn stays the closed connection, so the next Write tries again.
		return fmt.Errorf("the connection was closed after an error and reconnecting failed: %w", err)
	}
	d.conn = conn
	d.schemaFingerprint = nil
	total := d.stats.reconnects.Add(1)
	d.recoveryLog(ctx, "reconnect").
		Uint64("reconnects_total", total).
		Msg("the connection was closed after an error: reconnected with an empty statement cache")
	return nil
}

// resetStatementCache empties pgx's statement and description caches and
// deallocates the connection's prepared statements, so the next batch
// re-prepares every statement with the live column types.
func (d *Destination) resetStatementCache(ctx context.Context) error {
	return d.withConn(ctx, func(ctx context.Context) error {
		return d.conn.DeallocateAll(ctx)
	})
}

// clearStatementCache makes the next attempt start from an empty statement
// cache and re-learned schema fingerprints: on an open connection by
// resetting its cache, on one pgx closed by leaving it to ensureConn, which
// replaces it with a new connection.
func (d *Destination) clearStatementCache(ctx context.Context) error {
	d.schemaFingerprint = nil
	if d.conn == nil || d.conn.IsClosed() {
		return nil
	}
	if err := d.resetStatementCache(ctx); err != nil {
		return fmt.Errorf("failed to clear the statement cache: %w", err)
	}
	return nil
}

// failWrite is the error path of Write. A failed batch can leave cached
// parameter types behind that no longer match the table (a value that does
// not encode into a cached type fails before the schema guard runs, and pgx
// does not invalidate the statement cache on that path), so the cache is
// cleared if the connection is still open: a redelivery re-prepares with the
// live column types (invariant 6, and at-least-once liveness). For ordinary
// errors this is a cheap no-op next to the redelivery. A batch that could
// not be sent gets the index of the record whose value could not be encoded.
func (d *Destination) failWrite(ctx context.Context, p *writePlan, err error) error {
	if isUnsentBatch(err) && d.conn != nil && d.encodesWithDescribedTypes() && d.ensureConn(ctx) == nil {
		var idx int
		var encErr error
		_ = d.withConn(ctx, func(ctx context.Context) error {
			idx, encErr = d.locateEncodeFailure(ctx, p)
			return nil
		})
		if encErr != nil {
			err = fmt.Errorf("failed to encode record %d for table %q: %w", p.writes[idx].record,
				p.writes[idx].table, withholdEncodeError(encErr))
		}
	}
	if table, ok := undefinedTable(p, err); ok {
		err = tableNotFoundError(table, err)
	}
	if d.conn != nil && !d.conn.IsClosed() {
		_ = d.withConn(ctx, func(ctx context.Context) error {
			return d.conn.DeallocateAll(ctx)
		})
	}
	return err
}

// undefinedTable reports whether err is Postgres's undefined_table (42P01),
// raised where describe_exec and exec describe or run a statement for a table
// that does not exist, and which of p's tables it is about, if that can be
// told: the statement pgx names, or the only table of the batch.
func undefinedTable(p *writePlan, err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
		return "", false
	}
	var pre pgx.ErrPreprocessingBatch
	if errors.As(err, &pre) {
		for _, w := range p.writes {
			if w.sql == pre.SQL() {
				return w.table, true
			}
		}
	}
	if len(p.tables) == 1 {
		return p.tables[0], true
	}
	return "", true
}

// tableNotFoundError is the error for a write to a table that does not exist,
// in every query exec mode. cause is the server error, if there is one.
func tableNotFoundError(table string, cause error) error {
	msg := fmt.Sprintf("%s: table %q does not exist or is not visible through the connection's "+
		"search_path. Fix: create the table, or set the destination's table (or the record's "+
		"opencdc.collection metadata) to an existing one", ErrorCodeDestinationTableNotFound, table)
	if cause == nil {
		return errors.New(msg)
	}
	return fmt.Errorf("%s: %w", msg, cause)
}

// encodesWithDescribedTypes reports whether the connection's query exec mode
// encodes arguments into the parameter types the server describes (every mode
// but exec and simple_protocol), so locateEncodeFailure reproduces its
// encoding.
func (d *Destination) encodesWithDescribedTypes() bool {
	switch d.conn.Config().DefaultQueryExecMode {
	case pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol:
		return false
	default:
		return true
	}
}

// locateEncodeFailure finds the first write in p whose arguments pgx cannot
// encode into the parameter types the server describes for its statement now,
// and returns its index in p.writes and the encoding error. It describes each
// distinct statement once (an unnamed statement: nothing is left on the
// server). It returns a nil error if every write encodes, for example because
// the schema changed again since the batch failed. It only runs on the error
// path, so the extra round trips do not matter.
func (d *Destination) locateEncodeFailure(ctx context.Context, p *writePlan) (int, error) {
	descs := make(map[string]*pgconn.StatementDescription)
	var eqb pgx.ExtendedQueryBuilder
	for i, w := range p.writes {
		sd, ok := descs[w.sql]
		if !ok {
			var err error
			sd, err = d.conn.PgConn().Prepare(ctx, "", w.sql, nil)
			if err != nil {
				// (0, nil) means "not found": the lookup is best effort, and
				// the caller then keeps the batch's original error instead of
				// replacing it with one about the lookup. The index is
				// meaningless without an error.
				return 0, nil
			}
			descs[w.sql] = sd
		}
		if err := eqb.Build(d.conn.TypeMap(), sd, w.args); err != nil {
			return i, err
		}
	}
	// Every write encodes now (the schema changed again since the batch
	// failed): not found, as above.
	return 0, nil
}

// isStaleStatement reports whether err is a server error about a prepared
// statement the client believes is cached: 26000 (invalid_sql_statement_name,
// the statement does not exist on this server) or 42P05
// (duplicate_prepared_statement, this server already has one of that name).
func isStaleStatement(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "26000" || pgErr.Code == "42P05")
}

// isUnsentBatch reports whether err failed a batch before pgx sent its Sync:
// pgx could not encode an argument (the "build" step), or could not prepare a
// statement for a reason the server did not report (a network error in the
// "prepare" step). The server commits a batch's implicit transaction only at
// its Sync, so none of the batch was committed. A server error in the prepare
// step (a missing table, a syntax error) is not included: it would fail the
// same way on a retry.
func isUnsentBatch(err error) bool {
	var pre pgx.ErrPreprocessingBatch
	var pgErr *pgconn.PgError
	return errors.As(err, &pre) && !errors.As(err, &pgErr)
}

// schemaFingerprintSQL computes the schema fingerprint of the table named by
// $1 (a quoted identifier, the same form the write's table reference uses): the
// md5 of its live (column name, type OID, typmod) list. It changes on
// ALTER COLUMN ... TYPE and on ADD/DROP/RENAME COLUMN, and resolves the name
// exactly as the INSERT does (through search_path, case-sensitively).
const schemaFingerprintSQL = `
SELECT pg_catalog.md5(pg_catalog.string_agg(
		a.attname || ':' || a.atttypid::text || ':' || a.atttypmod::text,
		',' ORDER BY a.attnum))
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = pg_catalog.to_regclass($1)
	AND a.attnum > 0 AND NOT a.attisdropped`

// schemaGuardSQL is queued once per distinct table, after a batch's writes. It
// evaluates to 1 when the live fingerprint of $1 matches the remembered $2, and
// otherwise divides by zero (the boolean match casts to 0), raising SQLSTATE
// 22012 and aborting the batch's implicit transaction so the stale writes are
// rolled back. The guard is itself a cached prepared statement, so it costs no
// extra round trip: it is one more statement in the already-pipelined batch,
// and the server-side work is an indexed pg_attribute lookup over a handful of
// columns.
const schemaGuardSQL = `
SELECT 1 / ((` + schemaFingerprintSQL + `) IS NOT DISTINCT FROM $2::text)::int`

// isSchemaChanged reports whether err is the guard's division-by-zero
// (SQLSTATE 22012) that signals a schema change. The destination's own
// statements never divide, so 22012 can only come from a guard.
func isSchemaChanged(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22012"
}

// learnFingerprint reads and returns the current schema fingerprint of table.
// The name is passed to the query as a quoted identifier so the catalog lookup
// resolves the same relation the write's table reference does.
func (d *Destination) learnFingerprint(ctx context.Context, table string) (string, error) {
	var fp pgtype.Text
	if err := d.conn.QueryRow(ctx, schemaFingerprintSQL, quoteIdent(table)).Scan(&fp); err != nil {
		return "", fmt.Errorf("failed to read the schema of table %q: %w", table, err)
	}
	if !fp.Valid || fp.String == "" {
		return "", tableNotFoundError(table, nil)
	}
	return fp.String, nil
}

// writePlan holds the statements of one Write: one per record that is not a
// skipped drift marker, in record order, and the distinct tables they write
// to, in first-use order. It is built once per Write; every attempt turns it
// into a new pgx.Batch (buildBatch), because the schema guards appended to the
// batch depend on the fingerprints known at that attempt.
type writePlan struct {
	writes []plannedWrite
	tables []string
	seen   map[string]struct{}
}

// plannedWrite is one record's statement.
type plannedWrite struct {
	sql    string
	args   []any
	table  string
	record int // the record's index in the slice Write was given
}

func (p *writePlan) add(record int, table, sql string, args []any) {
	p.writes = append(p.writes, plannedWrite{sql: sql, args: args, table: table, record: record})
	if p.seen == nil {
		p.seen = make(map[string]struct{})
	}
	if _, ok := p.seen[table]; !ok {
		p.seen[table] = struct{}{}
		p.tables = append(p.tables, table)
	}
}

// planWrites turns the records into statements. It skips drift markers that
// carry the source's proof, and refuses invalid records before anything is
// sent. Its warnings about drift markers are logged once per Write, not once
// per attempt.
func (d *Destination) planWrites(ctx context.Context, recs []opencdc.Record) (*writePlan, error) {
	p := &writePlan{}
	var strayDrift int
	var modifiedMarkers []string
	for i, rec := range recs {
		// Schema-drift markers (postgres.schema.drift, source/logrepl) are
		// keyless, payload-less OperationCreate records that carry the approval
		// checkpoint for the escape hatch. Writing them would insert an empty
		// DEFAULT VALUES row (or error on NOT NULL columns) — FM6 in
		// docs/design-documents/20260829-dbz3-b1-schema-drift-escape-hatch.md
		// pins an explicit no-op: never a silent wrong row. The record is still
		// acked (Write returns the full count), so the marker's position
		// checkpoint flows end to end.
		//
		// Invariant 3: only a marker this connector's source produced may be
		// skipped. The metadata key is plain, user-reachable metadata, so it
		// is not proof; the proof is the position, which only the source
		// writes. The classic engine refuses a processor that changes a
		// record's position; the arch-v2 engine lets a processor rewrite it,
		// so there a processor could forge one, as it could already drop or
		// rewrite any record (docs/design-documents/20261015-drift-marker-source-signal.md).
		skip, added, err := driftMarkerDisposition(rec)
		if err != nil {
			return nil, err
		}
		if skip {
			if added != "" {
				modifiedMarkers = append(modifiedMarkers, added)
			}
			sdk.Logger(ctx).Debug().
				Str("table", rec.Metadata[logrepl.MetadataSchemaDriftTable]).
				Msg("skipping schema-drift marker record at the destination (FM6 no-op)")
			continue
		}
		if hasDriftMetadata(rec) {
			strayDrift++
		}
		rec, err = d.ensureStructuredData(rec)
		if err != nil {
			return nil, fmt.Errorf("failed to clean record: %w", err)
		}
		switch rec.Operation {
		case opencdc.OperationCreate:
			err = d.handleInsert(ctx, i, rec, p)
		case opencdc.OperationUpdate:
			err = d.handleUpdate(ctx, i, rec, p)
		case opencdc.OperationDelete:
			err = d.handleDelete(ctx, i, rec, p)
		case opencdc.OperationSnapshot:
			err = d.handleInsert(ctx, i, rec, p)
		default:
			return nil, fmt.Errorf("invalid operation %q", rec.Operation)
		}
		if err != nil {
			return nil, err
		}
	}

	logDriftWarnings(ctx, modifiedMarkers, strayDrift)
	return p, nil
}

// buildBatch queues p's statements into a new batch and, when the query exec
// mode caches parameter types, one schema guard per distinct table after the
// writes. It learns a table's schema fingerprint on first use.
func (d *Destination) buildBatch(ctx context.Context, p *writePlan) (*pgx.Batch, error) {
	b := &pgx.Batch{}
	for _, w := range p.writes {
		b.Queue(w.sql, w.args...)
	}
	if d.guardSchema {
		for _, table := range p.tables {
			if d.schemaFingerprint == nil {
				d.schemaFingerprint = make(map[string]string)
			}
			if _, ok := d.schemaFingerprint[table]; !ok {
				fp, err := d.learnFingerprint(ctx, table)
				if err != nil {
					return nil, err
				}
				d.schemaFingerprint[table] = fp
			}
			b.Queue(schemaGuardSQL, quoteIdent(table), d.schemaFingerprint[table])
		}
	}
	return b, nil
}

// execBatch sends b and reads every result. It returns the table whose schema
// guard fired — a schema change rolled the batch back, so the caller must
// clear the statement cache and retry — or the error to surface.
//
// Invariant 1: the batch runs in one implicit transaction; a write error or a
// fired guard rolls it back, so nothing is acked on the error paths.
func (d *Destination) execBatch(ctx context.Context, b *pgx.Batch, p *writePlan) (changed string, err error) {
	br := d.conn.SendBatch(ctx, b)
	var firstErr error
	firstIdx := 0
	for i := 0; i < len(b.QueuedQueries); i++ {
		if _, e := br.Exec(); e != nil && firstErr == nil {
			firstErr, firstIdx = e, i
		}
	}
	// Close drains the remaining results, resynchronizes the connection and,
	// on an error, invalidates the statement cache for the batch.
	if closeErr := br.Close(); firstErr == nil && closeErr != nil {
		return "", fmt.Errorf("failed to commit batch: %w", scrubExecError(closeErr))
	}
	nWrites := len(p.writes)
	switch {
	case firstErr == nil:
		return "", nil
	case errors.As(firstErr, new(pgx.ErrPreprocessingBatch)):
		// pgx failed the batch before running it, and every result reports
		// the same error, so its index says nothing about which record caused
		// it. failWrite names the record when it can.
		return "", fmt.Errorf("failed to send the batch: %w", scrubExecError(firstErr))
	case firstIdx >= nWrites && isSchemaChanged(firstErr):
		return p.tables[firstIdx-nWrites], nil
	case firstIdx >= nWrites:
		return "", fmt.Errorf("failed to verify the schema of table %q: %w", p.tables[firstIdx-nWrites], scrubExecError(firstErr))
	default:
		w := p.writes[firstIdx]
		return "", fmt.Errorf("failed to execute query for record %d (table %q): %w", w.record, w.table, scrubExecError(firstErr))
	}
}

const labelTable = "table"

// scrubbedExecError is an execution error without the text that quotes row
// values. The original stays in the chain, so classification (SQLSTATE,
// pgx.ErrPreprocessingBatch) keeps working.
type scrubbedExecError struct {
	msg string
	err error
}

func (e *scrubbedExecError) Error() string { return e.msg }
func (e *scrubbedExecError) Unwrap() error { return e.err }

// withheldEncodeMessage replaces the text of a pgx encode error, which prints
// the value that did not encode.
const withheldEncodeMessage = "a statement argument could not be encoded for its column: details withheld, they quote the value"

// withholdEncodeError replaces any error of encoding a statement argument.
// pgx formats the value into it ("unable to encode %#v into binary format for
// int8 (OID 20)") and the text of other encode failures cannot be told apart
// from it, so none of it is kept. The original stays in the chain.
func withholdEncodeError(err error) error {
	return &scrubbedExecError{msg: withheldEncodeMessage, err: err}
}

// scrubExecError removes the row values from the error of a statement. Errors
// reach the DLQ, the API and CLI output. It withholds by default:
//
//   - Every server error (*pgconn.PgError) keeps its SQLSTATE and its schema,
//     table, column and constraint names; the message, detail, hint, where and
//     the rest are withheld. The server quotes values in them: "invalid input
//     syntax for type integer: \"x\"", "Key (id)=(5) already exists", a CHECK
//     failure's "Failing row contains (...)", and the text of a trigger's
//     RAISE (P0001) is written by whoever wrote the trigger.
//   - A pgx error from building the batch (pgx.ErrPreprocessingBatch) or
//     encoding an argument prints the value. It is withheld, except when its
//     cause is a network error, which has no row values.
//
// Any other error (a closed connection, a context error) is returned as it is.
func scrubExecError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		var where []string
		for _, kv := range [][2]string{{"schema", pgErr.SchemaName}, {labelTable, pgErr.TableName}, {"column", pgErr.ColumnName}, {"constraint", pgErr.ConstraintName}} {
			if kv[1] != "" {
				where = append(where, fmt.Sprintf("%s %q", kv[0], kv[1]))
			}
		}
		msg := "SQLSTATE " + pgErr.Code
		if len(where) > 0 {
			msg += " (" + strings.Join(where, ", ") + ")"
		}
		return &scrubbedExecError{msg: msg + ": message and detail withheld, they can quote row values", err: err}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return err
	}
	if errors.As(err, new(pgx.ErrPreprocessingBatch)) || strings.Contains(err.Error(), "unable to encode") {
		return withholdEncodeError(err)
	}
	return err
}

// logDriftWarnings reports, once per batch, proven markers a processor added
// data to and records that carry the drift metadata without being markers.
// Once per batch, not per record: a processor that stamps the key on every
// record would otherwise flood the log.
func logDriftWarnings(ctx context.Context, modifiedMarkers []string, strayDrift int) {
	if len(modifiedMarkers) > 0 {
		sdk.Logger(ctx).Warn().
			Strs("markers", modifiedMarkers).
			Msg("schema-drift markers proven by their position carried data a processor added; " +
				"they were skipped as markers, and the added fields were not written")
	}
	if strayDrift > 0 {
		// Once per batch, not per record: a processor that stamps the key on
		// every record would otherwise flood the log.
		sdk.Logger(ctx).Warn().
			Int("records", strayDrift).
			Str("metadata_key", logrepl.MetadataSchemaDrift).
			Msg("records carry schema-drift metadata but no drift-marker position, and have a key or payload; writing them as normal records")
	}
}

// driftMarkerDisposition decides what Write does with a record that may be a
// schema-drift marker: skip it (a marker proven by its position; added
// describes data a processor added to it, if any), refuse it (marker-shaped
// but unproven), or neither (write it as an ordinary record).
//
// Invariant 3: only a marker this connector's source produced may be
// skipped. The metadata key is plain, user-reachable metadata, so it is not
// proof; the proof is the position, which only the source writes. The
// classic engine refuses a processor that changes a record's position; the
// arch-v2 engine lets a processor rewrite it, so there a processor could
// forge one, as it could already filter any record
// (docs/design-documents/20261015-drift-marker-source-signal.md).
func driftMarkerDisposition(rec opencdc.Record) (skip bool, added string, err error) {
	if !hasDriftMetadata(rec) {
		return false, "", nil
	}
	if hasMarkerPosition(rec) {
		// A proven marker is never a row. If a processor added data to it
		// (a tenant column, a key), writing it would insert a wrong row; it
		// is still skipped, and the added fields are named.
		if !isDriftMarker(rec) {
			added = markerAdditions(rec)
		}
		return true, added, nil
	}
	if isDriftMarker(rec) {
		// Marker-shaped, but not provably a marker: it cannot be written as a
		// row (FM6) and must not be acked unwritten.
		return false, "", fmt.Errorf("%s: a record carries %s=true and the drift marker's shape (a create "+
			"with no key and no payload), but its position was not written by this connector's source "+
			"as a marker for table %q, so it is neither written nor acknowledged. Drift markers lose "+
			"their position when they pass through another system (Kafka, a file): drop them before "+
			"that hop with a filter processor on the %s metadata, or remove the metadata if it was "+
			"set by mistake", ErrorCodeDriftMarkerUnverified, logrepl.MetadataSchemaDrift,
			rec.Metadata[logrepl.MetadataSchemaDriftTable], logrepl.MetadataSchemaDrift)
	}
	return false, "", nil
}

// ErrorCodeDriftMarkerUnverified prefixes the error Write returns for a
// record that looks like a schema-drift marker but whose position does not
// prove this connector's source produced it.
const ErrorCodeDriftMarkerUnverified = "postgres.destination.drift_marker_unverified"

// hasMarkerPosition reports whether rec's position is a CDC position this
// connector's source wrote for a schema-drift marker of the table the
// metadata names: exact (case-sensitive) keys as the source writes them, a
// parseable last_lsn, a change key (tx_commit_lsn with tx_seq or tx_seq_m),
// and schema_drift_marker equal to postgres.schema.drift.table.
//
// encoding/json matches keys case-insensitively, so the raw keys are checked
// first: {"TYPE":2,...} or a key differing only in case is not this source's
// position.
func hasMarkerPosition(rec opencdc.Record) bool {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Position, &raw); err != nil {
		return false
	}
	known := []string{
		"version", "type", "last_lsn", "tx_commit_lsn", "tx_seq", "tx_seq_m", "schema_drift_marker",
		"snapshots", "snapshot_low_watermark_lsn", "schema_history",
	}
	for k := range raw {
		for _, want := range known {
			if k != want && strings.EqualFold(k, want) {
				return false
			}
		}
	}
	for _, required := range []string{"type", "last_lsn", "tx_commit_lsn", "schema_drift_marker"} {
		if _, ok := raw[required]; !ok {
			return false
		}
	}
	p, err := position.ParseSDKPosition(rec.Position)
	if err != nil || p.Type != position.TypeCDC || p.SchemaDriftMarker == "" {
		return false
	}
	if lsn, err := p.LSN(); err != nil || lsn == 0 {
		return false
	}
	if commit, err := p.TxCommit(); err != nil || commit == 0 {
		return false
	}
	_, hasSeq := raw["tx_seq"]
	_, hasSeqM := raw["tx_seq_m"]
	if !hasSeq && !hasSeqM {
		return false
	}
	return p.SchemaDriftMarker == rec.Metadata[logrepl.MetadataSchemaDriftTable]
}

// markerAdditions describes what a processor added to a proven marker, for
// the WARN: its table, operation if not create, and the key and payload
// fields it carries (names only, never values).
func markerAdditions(rec opencdc.Record) string {
	var parts []string
	if rec.Operation != opencdc.OperationCreate {
		parts = append(parts, "operation="+rec.Operation.String())
	}
	for name, d := range map[string]opencdc.Data{"key": rec.Key, "before": rec.Payload.Before, "after": rec.Payload.After} {
		if isEmptyData(d) {
			continue
		}
		if sd, ok := d.(opencdc.StructuredData); ok {
			fields := make([]string, 0, len(sd))
			for f := range sd {
				fields = append(fields, f)
			}
			slices.Sort(fields)
			parts = append(parts, name+"{"+strings.Join(fields, ",")+"}")
		} else {
			parts = append(parts, name+"(raw)")
		}
	}
	slices.Sort(parts)
	return rec.Metadata[logrepl.MetadataSchemaDriftTable] + ": " + strings.Join(parts, " ")
}

// hasDriftMetadata reports whether rec carries postgres.schema.drift=true.
// It is necessary but not sufficient for a record to be a drift marker.
func hasDriftMetadata(rec opencdc.Record) bool {
	return rec.Metadata[logrepl.MetadataSchemaDrift] == strconv.FormatBool(true)
}

// isDriftMarker reports whether rec is exactly what source/logrepl's
// emitDriftMarker produces: an OperationCreate with the postgres.schema.drift
// metadata set to "true", no key, and no payload. Anything else carrying the
// metadata is a real record and must be written.
func isDriftMarker(rec opencdc.Record) bool {
	return hasDriftMetadata(rec) &&
		rec.Operation == opencdc.OperationCreate &&
		isEmptyData(rec.Key) &&
		isEmptyData(rec.Payload.Before) &&
		isEmptyData(rec.Payload.After)
}

// isEmptyData reports whether d carries no bytes. A nil interface, an empty
// RawData and an empty StructuredData all count: the marker is built with nil,
// but an intermediate hop may normalize nil to an empty value.
func isEmptyData(d opencdc.Data) bool {
	switch v := d.(type) {
	case nil:
		return true
	case opencdc.RawData:
		return len(v) == 0
	case opencdc.StructuredData:
		return len(v) == 0
	default:
		return false
	}
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
func (d *Destination) handleInsert(ctx context.Context, idx int, r opencdc.Record, p *writePlan) error {
	return d.upsert(ctx, idx, r, p)
}

// handleUpdate adds a query to the batch that updates the record in the target
// table. It assumes the record has a key and fails if one is not present.
func (d *Destination) handleUpdate(ctx context.Context, idx int, r opencdc.Record, p *writePlan) error {
	if !d.hasKey(r) {
		return fmt.Errorf("key must be provided on update actions")
	}
	// TODO handle case if the key was updated
	return d.upsert(ctx, idx, r, p)
}

// handleDelete adds a query to the batch that deletes the record from the
// target table. It assumes the record has a key and fails if one is not present.
func (d *Destination) handleDelete(ctx context.Context, idx int, r opencdc.Record, p *writePlan) error {
	if !d.hasKey(r) {
		return fmt.Errorf("key must be provided on delete actions")
	}
	return d.remove(ctx, idx, r, p)
}

func (d *Destination) upsert(ctx context.Context, idx int, r opencdc.Record, p *writePlan) error {
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
		Int("key_columns", len(key)).
		Str("key_hash", internal.KeyHash(key)).
		Msg("upserting record")

	p.add(idx, tableName, query, args)
	return nil
}

func (d *Destination) remove(ctx context.Context, idx int, r opencdc.Record, p *writePlan) error {
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
	// sq.Eq sorts the WHERE terms by the quoted column name, so sort the key
	// columns the same way to reproduce the argument order.
	type keyPair struct {
		quoted, name string
	}
	pairs := make([]keyPair, 0, len(key))
	for col := range key {
		pairs = append(pairs, keyPair{quoteIdent(col), col})
	}
	slices.SortFunc(pairs, func(a, b keyPair) int { return strings.Compare(a.quoted, b.quoted) })

	quotedCols := make([]string, 0, len(pairs))
	args := make([]interface{}, 0, len(pairs))
	for _, p := range pairs {
		quotedCols = append(quotedCols, p.quoted)
		args = append(args, key[p.name])
	}

	query, err := d.deleteSQL(tableName, quotedCols)
	if err != nil {
		return fmt.Errorf("error formatting delete query: %w", err)
	}

	sdk.Logger(ctx).Trace().
		Str("table", tableName).
		Str("query", query).
		Int("key_columns", len(key)).
		Str("key_hash", internal.KeyHash(key)).
		Msg("deleting record")

	p.add(idx, tableName, query, args)
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
//
// The SQL text is a pure function of the column names: every value becomes one
// placeholder, never inlined SQL. The only exception is a value that implements
// squirrel.Sqlizer, which the destination never produces (values are
// JSON-decoded scalars, big.Rat, time.Time and []byte); such a record bypasses
// the cache and is built per record, exactly as before.
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

	valArgs := make([]interface{}, 0, len(keyColumns)+len(otherColumns))
	var sqlizer bool
	for _, col := range keyColumns {
		v := d.formatValue(key[col])
		valArgs = append(valArgs, v)
		sqlizer = sqlizer || isSQLizer(v)
	}
	for _, col := range otherColumns {
		v := d.formatValue(payload[col])
		valArgs = append(valArgs, v)
		sqlizer = sqlizer || isSQLizer(v)
	}

	if sqlizer {
		// A Sqlizer value inlines SQL instead of a placeholder, so this
		// statement is not a pure function of the column names; it must not be
		// memoized. Build it with the real values, as before, and use the args
		// squirrel expanded from those values.
		query, args, err := d.buildUpsertSQL(tableName, keyColumns, otherColumns, valArgs)
		if err != nil {
			return "", nil, fmt.Errorf("error formatting query: %w", err)
		}
		return query, args, nil
	}

	query, err := d.upsertSQL(tableName, keyColumns, otherColumns)
	if err != nil {
		return "", nil, fmt.Errorf("error formatting query: %w", err)
	}
	return query, valArgs, nil
}

// isSQLizer reports whether a formatted value implements squirrel.Sqlizer. A
// Sqlizer value would inline SQL text into the statement instead of taking a
// single placeholder, so its presence makes the SQL depend on the value, not
// just the column names.
func isSQLizer(v interface{}) bool {
	_, ok := v.(sq.Sqlizer)
	return ok
}

// sqlShape identifies the SQL text of a statement: the target table plus the
// sorted key and non-key column names. Column names are joined with NUL, which
// no name can contain (checkIdentifiers rejects it), so the join is
// unambiguous.
type sqlShape struct {
	table        string
	keyColumns   string
	otherColumns string
}

func shapeKey(table string, keyColumns, otherColumns []string) sqlShape {
	return sqlShape{
		table:        table,
		keyColumns:   strings.Join(keyColumns, "\x00"),
		otherColumns: strings.Join(otherColumns, "\x00"),
	}
}

// upsertSQL returns the cached SQL text for an upsert of one record shape,
// building and caching it on the first use. The cached string is the same text
// buildUpsertSQL produces for the real values: the placeholder count is the
// column count, so a build with dummy values of that arity yields the identical
// string.
func (d *Destination) upsertSQL(tableName string, keyColumns, otherColumns []string) (string, error) {
	key := shapeKey(tableName, keyColumns, otherColumns)
	if query, ok := d.upsertSQLCache[key]; ok {
		return query, nil
	}
	dummy := make([]interface{}, len(keyColumns)+len(otherColumns))
	query, _, err := d.buildUpsertSQL(tableName, keyColumns, otherColumns, dummy)
	if err != nil {
		return "", err
	}
	if d.upsertSQLCache == nil {
		d.upsertSQLCache = make(map[sqlShape]string)
	}
	d.upsertSQLCache[key] = query
	return query, nil
}

// buildUpsertSQL renders the INSERT for the given columns and values, returning
// the SQL text and the arguments squirrel produced. The SQL text depends only
// on the column names; valArgs supplies one placeholder's worth of argument per
// column (real values, or dummy values when the text is being memoized).
func (d *Destination) buildUpsertSQL(
	tableName string,
	keyColumns, otherColumns []string,
	valArgs []interface{},
) (string, []interface{}, error) {
	colArgs := make([]string, 0, len(keyColumns)+len(otherColumns))
	for _, col := range keyColumns {
		colArgs = append(colArgs, quoteIdent(col))
	}
	for _, col := range otherColumns {
		colArgs = append(colArgs, quoteIdent(col))
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

// deleteSQL returns the cached SQL text for a delete of one record shape,
// building and caching it on the first use. quotedKeyCols are the quoted key
// column names, sorted the way sq.Eq sorts its WHERE terms; each is one "= ?"
// for a non-null scalar, which checkKeyValues guarantees for a key.
func (d *Destination) deleteSQL(tableName string, quotedKeyCols []string) (string, error) {
	key := shapeKey(tableName, quotedKeyCols, nil)
	if query, ok := d.deleteSQLCache[key]; ok {
		return query, nil
	}
	where := make(sq.Eq, len(quotedKeyCols))
	for _, col := range quotedKeyCols {
		// A non-nil scalar stands in for the real value: it yields one "= ?"
		// term. nil would yield "IS NULL", a different statement.
		where[col] = ""
	}
	query, _, err := d.stmtBuilder.Delete(quoteIdent(tableName)).Where(where).ToSql()
	if err != nil {
		return "", err
	}
	if d.deleteSQLCache == nil {
		d.deleteSQLCache = make(map[sqlShape]string)
	}
	d.deleteSQLCache[key] = query
	return query, nil
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

// numberCause returns the reason a number failed to parse without the number,
// which strconv.NumError repeats.
func numberCause(err error) error {
	var ne *strconv.NumError
	if errors.As(err, &ne) {
		return ne.Err
	}
	return errors.New("not a valid number")
}

func (n jsonNumber) rat() (*big.Rat, error) {
	// big.Rat builds a number with as many digits as the exponent (1e1000000
	// takes milliseconds and hundreds of kilobytes per value); no int64 needs
	// an exponent above 20.
	if i := strings.IndexAny(string(n), "eE"); i >= 0 {
		exp, err := strconv.Atoi(strings.TrimPrefix(string(n)[i+1:], "+"))
		if err != nil || exp < -400 || exp > 400 {
			return nil, fmt.Errorf("a %d byte JSON number has an exponent outside -400..400", len(n))
		}
	}
	r, ok := new(big.Rat).SetString(string(n))
	if !ok {
		return nil, fmt.Errorf("invalid JSON number (%d bytes)", len(n))
	}
	return r, nil
}

// NumericValue returns the exact decimal value.
func (n jsonNumber) NumericValue() (pgtype.Numeric, error) {
	var num pgtype.Numeric
	if err := num.Scan(string(n)); err != nil {
		return pgtype.Numeric{}, fmt.Errorf("a %d byte JSON number as numeric: %w", len(n), numberCause(err))
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
		return pgtype.Int8{}, fmt.Errorf("a %d byte JSON number is not a whole number in the int64 range", len(n))
	}
	return pgtype.Int8{Int64: r.Num().Int64(), Valid: true}, nil
}

// Float64Value returns the nearest float64.
func (n jsonNumber) Float64Value() (pgtype.Float8, error) {
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return pgtype.Float8{}, fmt.Errorf("a %d byte JSON number as float: %w", len(n), numberCause(err))
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

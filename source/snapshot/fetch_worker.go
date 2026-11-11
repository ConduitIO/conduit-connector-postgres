// Copyright © 2024 Meroxa, Inc.
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

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultFetchSize = 50000

var supportedKeyTypes = []string{
	"smallint",
	"integer",
	"bigint",
}

type FetchConfig struct {
	Table          string
	Key            string
	TXSnapshotID   string
	FetchSize      int
	Position       position.Position
	WithAvroSchema bool
	// JSON is what to do with json text that is not valid UTF-8, shared with
	// the other workers and the CDC handler of the source. nil is the
	// default, base64.
	JSON *types.JSONPolicy
}

var (
	errTableRequired  = errors.New("table name is required")
	errKeyRequired    = errors.New("table key required")
	errInvalidCDCType = errors.New("invalid position type CDC")
	// errInvalidFetchSize guards invariant 3: a negative FETCH count returns
	// zero rows without an error, which Run treats as the end of the cursor,
	// so the table would be marked snapshotted with no records emitted.
	errInvalidFetchSize = errors.New("fetch size must not be negative")
)

func (c FetchConfig) Validate() error {
	var errs []error

	if c.Table == "" {
		errs = append(errs, errTableRequired)
	}

	if c.Key == "" {
		errs = append(errs, errKeyRequired)
	}

	if c.FetchSize < 0 {
		errs = append(errs, errInvalidFetchSize)
	}

	switch c.Position.Type {
	case position.TypeSnapshot, position.TypeInitial:
	default:
		errs = append(errs, errInvalidCDCType)
	}

	return errors.Join(errs...)
}

type FetchData struct {
	Key           opencdc.StructuredData
	Payload       opencdc.StructuredData
	Position      position.SnapshotPosition
	Table         string
	PayloadSchema cschema.Schema
	KeySchema     cschema.Schema
	// RawJSON lists the json, jsonb, json[] and jsonb[] columns whose text
	// was not valid UTF-8 and is emitted as base64 (types.JSONPolicy). The
	// iterator puts it in the record metadata types.MetadataJSONRawBytes.
	RawJSON []string
}

// FetchWorker fetches snapshot data from a single table
type FetchWorker struct {
	conf FetchConfig
	db   *pgxpool.Pool
	out  chan<- []FetchData

	keySchema     *cschema.Schema
	payloadSchema *cschema.Schema
	// notNull is the catalog nullability of the table's columns and tableOID
	// the table it was read for, loaded once per worker in the snapshot
	// transaction when Avro schemas are on (#326). widenForNull marks a
	// column nullable in it when a row holds NULL in it.
	notNull  schema.NotNull
	tableOID uint32
	// createSchema registers a schema; nil means sdkschema.Create. A field so
	// a test can make the registry reject one.
	createSchema func(ctx context.Context, typ cschema.Type, subject string, bytes []byte) (cschema.Schema, error)

	snapshotEnd int64
	lastRead    int64
	// resumed is true when the position holds this table's progress, so
	// lastRead and snapshotEnd are real key values. Without it the zero
	// values of those fields cannot be told apart from a read key 0: a new
	// snapshot has no lower bound and computes its end (see passes).
	resumed bool
	// lowPass is true when the position's entry was written by a v0.14.0 to
	// v0.19.0 build (no SnapshotPosition.FromMin): its scan started at
	// "key > 0", so the rows keyed 0 or below are still owed. They are read
	// by a low pass before the main pass resumes (see passes).
	lowPass bool
	// lowPassLastRead is the last key the low pass read in an earlier run,
	// nil when it read none.
	lowPassLastRead *int64
	// pass is the scan being fetched; fetch builds row positions from it.
	pass       scanPass
	cursorName string
}

// scanPass is one ordered scan over a part of the table's key range.
type scanPass struct {
	// low marks the low pass, the catch-up scan of the keys at or below zero
	// that a legacy entry still owes.
	low bool
	// after is the exclusive lower bound, nil for none.
	after *int64
	// through is the inclusive upper bound.
	through int64
}

func NewFetchWorker(db *pgxpool.Pool, out chan<- []FetchData, c FetchConfig) *FetchWorker {
	f := &FetchWorker{
		conf:       c,
		db:         db,
		out:        out,
		cursorName: "fetcher_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
	}

	if f.conf.FetchSize == 0 {
		f.conf.FetchSize = defaultFetchSize
	}

	if c.Position.Type == position.TypeInitial || c.Position.Snapshots == nil {
		return f
	}

	if t, ok := c.Position.Snapshots[c.Table]; ok {
		// A table's entry exists only once one of its rows was read (the
		// iterator writes it from FetchData.Position), so LastRead is the key
		// of a row, which may be 0 or negative.
		f.snapshotEnd = t.SnapshotEnd
		f.lastRead = t.LastRead
		f.resumed = true
		// Invariant 3: an entry without FromMin was written by a build whose
		// scan skipped every key at or below zero; those rows are read now.
		f.lowPass = !t.FromMin
		f.lowPassLastRead = t.LowPassLastRead
	}

	return f
}

// Validate will ensure the config is correct.
// * Table and keys exist
// * Key is a primary key
func (f *FetchWorker) Validate(ctx context.Context) error {
	if err := f.conf.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}

	tx, err := f.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start tx for validation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			sdk.Logger(ctx).Warn().
				Err(err).
				Msgf("error on validation tx rollback for %q", f.cursorName)
		}
	}()

	if err := f.validateTable(ctx, f.conf.Table, tx); err != nil {
		return fmt.Errorf("failed to validate table: %w", err)
	}

	if err := f.validateKey(ctx, f.conf.Table, f.conf.Key, tx); err != nil {
		return fmt.Errorf("failed to validate key: %w", err)
	}

	return nil
}

func (f *FetchWorker) Run(ctx context.Context) error {
	start := time.Now().UTC()

	tx, err := f.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("failed to start tx: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			sdk.Logger(ctx).Warn().
				Err(err).
				Msgf("error run tx rollback for %q", f.cursorName)
		}
	}()

	if err := f.withSnapshot(ctx, tx); err != nil {
		return err
	}

	if err := f.updateSnapshotEnd(ctx, tx); err != nil {
		return fmt.Errorf("failed to update fetch limit: %w", err)
	}

	f.logStart(ctx)

	var nfetched int
	for _, pass := range f.passes() {
		n, err := f.runPass(ctx, tx, pass, nfetched, start)
		if err != nil {
			return err
		}
		nfetched += n
	}

	sdk.Logger(ctx).Info().
		Int("rows", nfetched).
		Dur("elapsed", time.Since(start)).
		Str("rate_per_min", fmt.Sprintf("%.0f", float64(nfetched)/time.Since(start).Minutes())).
		Str("table", f.conf.Table).
		Msgf("%q snapshot completed", f.conf.Table)

	return nil
}

// passes returns the scans this worker runs, in order. Each reads its keys
// in ascending order, so the position of a delivered row says how far its
// pass got.
//
//   - A new snapshot has one pass with no lower bound, through the end.
//   - A resumed snapshot whose entry has FromMin has one pass after
//     LastRead, through the stored end.
//   - A resumed legacy entry (no FromMin) first runs the low pass over the
//     keys at or below zero, after LowPassLastRead when an earlier run read
//     some, then the main pass after LastRead. The low pass leaves LastRead
//     unchanged in its positions, so a crash in it resumes it.
func (f *FetchWorker) passes() []scanPass {
	// Invariant 3: a new snapshot has no lower bound. It used to start at
	// "key > 0", the zero value of lastRead, which silently skipped the rows
	// keyed 0 or below; nothing delivers them later, since the slot streams
	// only changes made after the snapshot.
	if !f.resumed {
		return []scanPass{{through: f.snapshotEnd}}
	}
	lastRead := f.lastRead
	main := scanPass{after: &lastRead, through: f.snapshotEnd}
	if !f.lowPass {
		return []scanPass{main}
	}
	// A legacy entry always has LastRead >= 1, so its snapshot end is at
	// least 1 and the low pass ends at 0; min keeps the pass inside the
	// snapshot's range for any entry.
	return []scanPass{{low: true, after: f.lowPassLastRead, through: min(0, f.snapshotEnd)}, main}
}

// runPass declares the cursor of one pass and fetches it to the end. It
// returns the number of rows read.
func (f *FetchWorker) runPass(ctx context.Context, tx pgx.Tx, pass scanPass, before int, start time.Time) (int, error) {
	f.pass = pass
	closeCursor, err := f.createCursor(ctx, tx)
	if err != nil {
		return 0, fmt.Errorf("failed to create cursor: %w", err)
	}
	defer closeCursor()

	var nfetched int
	for {
		n, err := f.fetch(ctx, tx)
		if err != nil {
			return nfetched, fmt.Errorf("failed to fetch results: %w", err)
		}
		if n == 0 { // end of cursor
			break
		}
		nfetched += n
		f.logProgress(ctx, before+nfetched, start)
	}
	if pass.low {
		sdk.Logger(ctx).Info().
			Int("rows", nfetched).
			Str("table", f.conf.Table).
			Msg("low pass completed: read the rows keyed 0 or below that the snapshot of an earlier version skipped")
	}
	return nfetched, nil
}

// logProgress logs how many rows were fetched. It does not log a percentage
// or anything else computed from the end of the range: the end is the largest
// key value, which is record data, and rows with a percentage gives it away
// (rows * 100 / percentage).
func (f *FetchWorker) logProgress(ctx context.Context, nfetched int, start time.Time) {
	sdk.Logger(ctx).Info().
		Int("rows", nfetched).
		Str("table", f.conf.Table).
		Dur("elapsed", time.Since(start)).
		Str("rate_per_min", fmt.Sprintf("%.0f", float64(nfetched)/time.Since(start).Minutes())).
		Msg("fetching rows")
}

// createCursor declares the cursor of the current pass (f.pass).
func (f *FetchWorker) createCursor(ctx context.Context, tx pgx.Tx) (func(), error) {
	lower := ""
	if f.pass.after != nil {
		lower = fmt.Sprintf("%s > %d AND ", internal.WrapSQLIdent(f.conf.Key), *f.pass.after)
	}
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s%s <= %d ORDER BY %s",
		internal.WrapSQLIdent(f.conf.Table),
		lower,                                             // range start
		internal.WrapSQLIdent(f.conf.Key), f.pass.through, // range end
		internal.WrapSQLIdent(f.conf.Key), // order by
	)

	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR(%s)", f.cursorName, selectQuery)

	if _, err := tx.Exec(ctx, cursorQuery); err != nil {
		return nil, err
	}

	return func() {
		// N.B. The cursor will automatically close when the TX is done.
		if _, err := tx.Exec(ctx, "CLOSE "+f.cursorName); err != nil {
			sdk.Logger(ctx).Warn().
				Err(err).
				Msgf("unexpected error when closing cursor %q", f.cursorName)
		}
	}, nil
}

func (f *FetchWorker) updateSnapshotEnd(ctx context.Context, tx pgx.Tx) error {
	// A resumed snapshot keeps the end it started with, even one at or below
	// zero; recomputing it would widen the range to rows inserted since. A
	// new one has no end yet: snapshotEnd is set only from a position entry.
	if f.resumed {
		return nil
	}

	query := fmt.Sprintf("SELECT COALESCE(max(%s), 0) FROM %s",
		internal.WrapSQLIdent(f.conf.Key), internal.WrapSQLIdent(f.conf.Table),
	)
	if err := tx.QueryRow(ctx, query).Scan(&f.snapshotEnd); err != nil {
		return fmt.Errorf("failed to get snapshot end with query %q: %w", query, err)
	}

	return nil
}

// logStart logs the start of the fetcher. The range bounds are values of the
// key column, which is record data, so only salted hashes of them are logged.
// A new snapshot has no lower bound, so it logs no start hash.
func (f *FetchWorker) logStart(ctx context.Context) {
	ev := sdk.Logger(ctx).Info().
		Int("fetchSize", f.conf.FetchSize).
		Str("tx.snapshot", f.conf.TXSnapshotID).
		Bool("resumed", f.resumed).
		Bool("low_pass", f.lowPass)
	if f.resumed {
		ev = ev.Str("startAtHash", internal.KeyHash(map[string]any{f.conf.Key: f.lastRead}))
	}
	ev.Str("snapshotEndHash", internal.KeyHash(map[string]any{f.conf.Key: f.snapshotEnd})).
		Msgf("starting fetcher %s", f.cursorName)
}

func (f *FetchWorker) fetch(ctx context.Context, tx pgx.Tx) (int, error) {
	start := time.Now().UTC()

	if err := f.loadNotNull(ctx, tx); err != nil {
		return 0, fmt.Errorf("failed to read column nullability: %w", err)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", f.conf.FetchSize, f.cursorName))
	if err != nil {
		return 0, fmt.Errorf("failed to fetch rows: %w", err)
	}
	defer rows.Close()

	sdk.Logger(ctx).Info().
		Dur("fetch_elapsed", time.Since(start)).
		Msg("cursor fetched data")

	fields := rows.FieldDescriptions()
	var nread int

	var toBeSent []FetchData

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("failed to get values: %w", err)
		}

		if err := f.widenForNull(ctx, fields, values); err != nil {
			return 0, err
		}
		err = f.extractSchemas(ctx, fields)
		if err != nil {
			return 0, fmt.Errorf("failed to extract schemas: %w", err)
		}

		data, err := f.buildFetchData(fields, values)
		if err != nil {
			return nread, fmt.Errorf("failed to build fetch data: %w", err)
		}
		if len(data.RawJSON) > 0 {
			f.conf.JSON.WarnRawBytes(sdk.Logger(ctx), "snapshot", f.conf.Table, f.rowID(fields, values), data.RawJSON)
		}

		toBeSent = append(toBeSent, data)

		// Invariant 2 / DBZ-3 B0 kill point (chaospoint.SnapshotFetchRow): a
		// kill landing exactly here proves a mid-batch snapshot crash, where
		// this row is only in the in-memory toBeSent slice — not yet sent
		// (f.send, below) and therefore not yet durably acked downstream.
		// Recovery must resume at f.lastRead (the last row a prior fetch
		// actually completed sending), never at this row, or the NO GAP /
		// DUP BOUND properties in the chaos harness would be unprovable.
		// No-op outside the conduitchaos build (see internal/chaospoint).
		chaospoint.Reach(chaospoint.SnapshotFetchRow)

		nread++
	}

	if nread > 0 {
		err := f.send(ctx, toBeSent)
		if err != nil {
			return nread, fmt.Errorf("failed to send record: %w", err)
		}
	}

	if rows.Err() != nil {
		return 0, fmt.Errorf("failed to read rows: %w", rows.Err())
	}

	return nread, nil
}

func (f *FetchWorker) send(ctx context.Context, d []FetchData) error {
	start := time.Now().UTC()
	defer func() {
		sdk.Logger(ctx).Trace().
			Dur("send_elapsed", time.Since(start)).
			Msg("sending data to chan")
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.out <- d:
		return nil
	}
}

func (f *FetchWorker) buildFetchData(fields []pgconn.FieldDescription, values []any) (FetchData, error) {
	pos, err := f.buildSnapshotPosition(fields, values)
	if err != nil {
		return FetchData{}, fmt.Errorf("failed to build snapshot position: %w", err)
	}

	key, payload, rawJSON, err := f.buildRecordData(fields, values)
	if err != nil {
		return FetchData{}, fmt.Errorf("failed to encode record data: %w", err)
	}

	fd := FetchData{
		Key:      key,
		Payload:  payload,
		Position: pos,
		Table:    f.conf.Table,
		RawJSON:  rawJSON,
	}
	if f.conf.WithAvroSchema {
		fd.PayloadSchema = *f.payloadSchema
		fd.KeySchema = *f.keySchema
	}
	return fd, nil
}

func (f *FetchWorker) buildSnapshotPosition(fields []pgconn.FieldDescription, values []any) (position.SnapshotPosition, error) {
	for i, fd := range fields {
		if fd.Name == f.conf.Key {
			// Always coerce snapshot position to bigint, pk may be any type of integer.
			lastRead, err := keyInt64(values[i])
			if err != nil {
				return position.SnapshotPosition{}, fmt.Errorf("failed to parse key: %w", err)
			}
			if f.pass.low {
				// Invariant 2: the low pass moves only LowPassLastRead.
				// LastRead keeps the legacy value, which the main pass
				// resumes after, and FromMin stays unset until the main
				// pass delivers a row, so a crash resumes the low pass.
				return position.SnapshotPosition{
					LastRead:        f.lastRead,
					SnapshotEnd:     f.snapshotEnd,
					LowPassLastRead: &lastRead,
				}, nil
			}
			// Every row at or below this key was read: by this pass, which
			// started at the lowest key or after a row of an entry that
			// already had FromMin, or by the low pass and the earlier build.
			return position.SnapshotPosition{
				LastRead:    lastRead,
				SnapshotEnd: f.snapshotEnd,
				FromMin:     true,
			}, nil
		}
	}
	return position.SnapshotPosition{}, fmt.Errorf("key %q not found in fields", f.conf.Key)
}

// rowID names the row being formatted by its key column, for error messages.
func (f *FetchWorker) rowID(fields []pgconn.FieldDescription, values []any) string {
	for i, fd := range fields {
		if fd.Name == f.conf.Key {
			return internal.RowRef(map[string]any{fd.Name: values[i]})
		}
	}
	return ""
}

// buildRecordData formats the key and the payload of a row. rawJSON lists the
// columns whose json text was not valid UTF-8 and holds the base64 of its
// bytes instead (types.JSONPolicy.FormatColumn); the key column, when it is
// one of them, is in it as a payload column.
func (f *FetchWorker) buildRecordData(fields []pgconn.FieldDescription, values []any) (key, payload opencdc.StructuredData, rawJSON []string, err error) {
	key = make(opencdc.StructuredData)
	payload = make(opencdc.StructuredData)
	enc := types.EncodingFor(f.conf.WithAvroSchema)

	for i, fd := range fields {
		if fd.Name == f.conf.Key {
			k, _, err := f.conf.JSON.FormatColumn(enc, fd.DataTypeOID, fd.TypeModifier, values[i])
			if err != nil {
				if located, ok := types.LocateErrorSnapshot(err, f.conf.Table, fd.Name); ok {
					return key, payload, nil, types.WithRow(located, f.rowID(fields, values))
				}
				return key, payload, nil, fmt.Errorf("failed to format key %q: %w", f.conf.Key, err)
			}

			// Invariant 6: the key owns its value; FormatColumn can hand back
			// the scanned value itself, which the payload gets too (#425).
			key[f.conf.Key] = f.avroShape(fd, types.CloneValue(k))
		}

		v, raw, err := f.conf.JSON.FormatColumn(enc, fd.DataTypeOID, fd.TypeModifier, values[i])
		if err != nil {
			if located, ok := types.LocateErrorSnapshot(err, f.conf.Table, fd.Name); ok {
				return key, payload, nil, types.WithRow(located, f.rowID(fields, values))
			}
			return key, payload, nil, fmt.Errorf("failed to format payload field %q: %w", fd.Name, err)
		}
		if raw {
			// Invariant 6: a base64 value is never emitted without its
			// column in the metadata that says so; the record is delivered,
			// never dropped (invariant 3).
			rawJSON = append(rawJSON, fd.Name)
		}
		payload[fd.Name] = f.avroShape(fd, v)
	}

	return key, payload, rawJSON, nil
}

// avroShape gives the value of a nullable column the shape its ["null", T]
// field needs (types.NullableAvroValue). Without Avro schemas, and for NOT
// NULL columns, it is v.
func (f *FetchWorker) avroShape(fd pgconn.FieldDescription, v any) any {
	if !f.conf.WithAvroSchema || f.notNull[fd.Name] {
		return v
	}
	return types.NullableAvroValue(fd.DataTypeOID, v)
}

func (f *FetchWorker) withSnapshot(ctx context.Context, tx pgx.Tx) error {
	if f.conf.TXSnapshotID == "" {
		sdk.Logger(ctx).Warn().
			Msgf("fetcher %q starting without transaction snapshot", f.cursorName)
		return nil
	}

	if _, err := tx.Exec(
		ctx,
		fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", f.conf.TXSnapshotID),
	); err != nil {
		return fmt.Errorf("failed to set tx snapshot %q: %w", f.conf.TXSnapshotID, err)
	}

	return nil
}

func (*FetchWorker) validateKey(ctx context.Context, table, key string, tx pgx.Tx) error {
	var dataType string

	if err := tx.QueryRow(
		ctx,
		`SELECT a.atttypid::regtype AS type FROM pg_class c JOIN pg_attribute a ON c.oid = a.attrelid
			WHERE c.relkind = 'r' AND a.attname = $1 AND c.relname = $2`,
		key, table,
	).Scan(&dataType); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("key %q not present on table %q", key, table)
		}
		return fmt.Errorf("unable to check key %q on table %q: %w", key, table, err)
	}

	if !slices.Contains(supportedKeyTypes, dataType) {
		return fmt.Errorf("key %q of type %q is unsupported", key, dataType)
	}

	var isPK bool

	// As per https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
	if err := tx.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT a.attname FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = $1::regclass AND a.attname = $2 AND i.indisprimary)`,
		internal.WrapSQLIdent(table), key,
	).Scan(&isPK); err != nil {
		return fmt.Errorf("unable to determine key %q constraints: %w", key, err)
	}

	if !isPK {
		sdk.Logger(ctx).Warn().
			Err(fmt.Errorf("column %q is not a primary key", key)).
			Msg("this may cause unexpected behavior if the key is not unique")
	}

	return nil
}

func (*FetchWorker) validateTable(ctx context.Context, table string, tx pgx.Tx) error {
	var tableExists bool

	if err := tx.QueryRow(
		ctx,
		"SELECT EXISTS(SELECT tablename FROM pg_tables WHERE tablename=$1)",
		table,
	).Scan(&tableExists); err != nil {
		return fmt.Errorf("unable to check table %q: %w", table, err)
	}

	if !tableExists {
		return fmt.Errorf("table %q does not exist", table)
	}

	return nil
}

func (f *FetchWorker) extractSchemas(ctx context.Context, fields []pgconn.FieldDescription) error {
	if !f.conf.WithAvroSchema {
		return nil
	}
	if f.payloadSchema != nil && f.keySchema != nil {
		return nil
	}
	if err := f.checkFieldsFromTable(fields); err != nil {
		return err
	}
	notNull := f.notNull

	if f.payloadSchema == nil {
		sdk.Logger(ctx).Debug().
			Msgf("extracting payload schema for %v fields in %v", len(fields), f.conf.Table)

		avroPayloadSch, err := schema.Avro.Extract(f.conf.Table+"_payload", fields, notNull)
		if err != nil {
			return fmt.Errorf("failed to extract payload schema for table %v: %w", f.conf.Table, err)
		}
		text, err := schema.Text(avroPayloadSch)
		if err != nil {
			return fmt.Errorf("failed to extract payload schema for table %v: %w", f.conf.Table, err)
		}
		ps, err := f.create(
			ctx,
			cschema.TypeAvro,
			avroPayloadSch.Name(),
			text,
		)
		if err != nil {
			return fmt.Errorf("failed creating payload schema for table %v: %w",
				f.conf.Table, &schema.SubjectError{Subject: avroPayloadSch.Name(), Err: err})
		}
		f.payloadSchema = &ps
	}

	if f.keySchema == nil {
		sdk.Logger(ctx).Debug().
			Msgf("extracting schema for key %v in %v", f.conf.Key, f.conf.Table)

		avroKeySch, err := schema.Avro.Extract(f.conf.Table+"_key", fields, notNull, f.conf.Key)
		if err != nil {
			return fmt.Errorf("failed to extract key schema for table %v: %w", f.conf.Table, err)
		}
		text, err := schema.Text(avroKeySch)
		if err != nil {
			return fmt.Errorf("failed to extract key schema for table %v: %w", f.conf.Table, err)
		}
		ks, err := f.create(
			ctx,
			cschema.TypeAvro,
			avroKeySch.Name(),
			text,
		)
		if err != nil {
			return fmt.Errorf("failed creating key schema for table %v: %w",
				f.conf.Table, &schema.SubjectError{Subject: avroKeySch.Name(), Err: err})
		}
		f.keySchema = &ks
	}

	return nil
}

func (f *FetchWorker) create(ctx context.Context, typ cschema.Type, subject string, b []byte) (cschema.Schema, error) {
	if f.createSchema != nil {
		return f.createSchema(ctx, typ, subject, b)
	}
	return sdkschema.Create(ctx, typ, subject, b)
}

// widenForNull handles a row that holds NULL in a column the catalog
// declares NOT NULL. It is a defensive guard, not a handled race: Postgres
// does not produce such a row here. loadNotNull is a plain SELECT on
// pg_attribute in the snapshot's REPEATABLE READ transaction, so it reads the
// catalog as of the same snapshot as the rows (checked on Postgres 17.5 and
// 18.6: after a concurrent UPDATE and SET NOT NULL commit, the transaction
// still reads attnotnull false and still sees the NULL row), and it runs after
// the cursor is declared, so the transaction holds ACCESS SHARE on the table
// and a SET NOT NULL waits for the snapshot to end. The guard costs one map
// lookup per NULL value and keeps the row from being encoded under a schema
// that forbids its NULL if the catalog and the rows ever disagree. The
// column's field becomes ["null", T] for the rest of the worker, the widened
// schemas are registered for this row and the next ones, and a warning coded
// schema.ErrorCodeAvroNullInNotNullColumn is logged once per column; it names
// the table and the column, never a value or a key. If the registry rejects
// the widened schema, the fetch fails with an error with that code and the
// row is not sent. Without Avro schemas it does nothing.
func (f *FetchWorker) widenForNull(ctx context.Context, fields []pgconn.FieldDescription, values []any) error {
	if !f.conf.WithAvroSchema {
		return nil
	}
	var cols []string
	for i, fd := range fields {
		if values[i] == nil && f.notNull[fd.Name] {
			cols = append(cols, fd.Name)
		}
	}
	if len(cols) == 0 {
		return nil
	}
	// Invariant 6: the NULL is delivered as null under a schema that admits
	// it, never coerced to a zero value and never dropped.
	notNull := maps.Clone(f.notNull)
	for _, c := range cols {
		notNull[c] = false
	}
	prevNotNull, prevPayload, prevKey := f.notNull, f.payloadSchema, f.keySchema
	f.notNull, f.payloadSchema, f.keySchema = notNull, nil, nil
	if err := f.extractSchemas(ctx, fields); err != nil {
		f.notNull, f.payloadSchema, f.keySchema = prevNotNull, prevPayload, prevKey
		// The key is widened too when the NULL column is the configured
		// key, so the rejected subject can be either one.
		subject := schema.RejectedSubject(err, f.conf.Table+"_payload")
		return fmt.Errorf(
			"%s: a row of table %q holds NULL in the column(s) %q, which the catalog declares NOT NULL "+
				"(the catalog and the snapshot's rows disagree), and the schema of the subject %q with the column(s) made "+
				"nullable could not be registered: %w. "+
				"Fix: allow the change on the subject %q, and on the key subject too if the column is the record key "+
				"(T -> [\"null\", T] is BACKWARD compatible; FORWARD and FULL reject it), "+
				"for example set its compatibility to BACKWARD or NONE, and restart; or set logrepl.withAvroSchema to false and restart",
			schema.ErrorCodeAvroNullInNotNullColumn, f.conf.Table, cols, subject, err, subject)
	}
	// Once per column: f.notNull now says it is nullable, so its NULLs are
	// not detected again.
	for _, c := range cols {
		sdk.Logger(ctx).Warn().
			Str("code", schema.ErrorCodeAvroNullInNotNullColumn).
			Str("table", f.conf.Table).
			Str("column", c).
			Msg("a snapshot row holds NULL in a column the catalog declares NOT NULL (the catalog and the snapshot's rows disagree); " +
				"its Avro field is now nullable and a new payload schema version is registered")
	}
	return nil
}

// loadNotNull reads the nullability of the table's columns from the catalog,
// in the snapshot's transaction, so it describes the table the rows are read
// from (#326). It runs before the first FETCH: the transaction's connection
// cannot run a query while a FETCH result is being read.
func (f *FetchWorker) loadNotNull(ctx context.Context, tx pgx.Tx) error {
	if !f.conf.WithAvroSchema || f.notNull != nil {
		return nil
	}
	var oid uint32
	if err := tx.QueryRow(ctx, "SELECT $1::regclass::oid", internal.WrapSQLIdent(f.conf.Table)).Scan(&oid); err != nil {
		return fmt.Errorf("table %v: failed to resolve the table: %w", f.conf.Table, err)
	}
	notNull, err := schema.LoadNotNull(ctx, tx, oid)
	if err != nil {
		return fmt.Errorf("table %v: %w", f.conf.Table, err)
	}
	f.tableOID, f.notNull = oid, notNull
	return nil
}

// checkFieldsFromTable fails unless every field comes from the table whose
// nullability loadNotNull read. The cursor is SELECT * on that table, so this
// holds by construction; it is checked so a schema can never pair the rows of
// one relation with the nullability of another.
func (f *FetchWorker) checkFieldsFromTable(fields []pgconn.FieldDescription) error {
	for _, fd := range fields {
		if fd.TableOID != f.tableOID {
			return fmt.Errorf("table %v: field %q comes from relation OID %d, not the table (OID %d)", f.conf.Table, fd.Name, fd.TableOID, f.tableOID)
		}
	}
	return nil
}

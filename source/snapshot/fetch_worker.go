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
	"slices"
	"strings"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultFetchSize = 50000

const (
	pgUUID     = "uuid"
	pgSmallint = "smallint"
	pgInteger  = "integer"
	pgBigint   = "bigint"
)

var supportedKeyTypes = []string{
	pgSmallint,
	pgInteger,
	pgBigint,
	pgUUID,
}

type FetchConfig struct {
	Table          string
	Key            string
	TXSnapshotID   string
	FetchSize      int
	Position       position.Position
	WithAvroSchema bool
}

var (
	errTableRequired  = errors.New("table name is required")
	errKeyRequired    = errors.New("table key required")
	errInvalidCDCType = errors.New("invalid position type CDC")
)

func (c FetchConfig) Validate() error {
	var errs []error

	if c.Table == "" {
		errs = append(errs, errTableRequired)
	}

	if c.Key == "" {
		errs = append(errs, errKeyRequired)
	}

	switch c.Position.Type {
	case position.TypeSnapshot, position.TypeInitial:
	default:
		errs = append(errs, errInvalidCDCType)
	}

	return errors.Join(errs...)
}

type FetchData struct {
	Key        sdk.StructuredData
	Payload    sdk.StructuredData
	Position   position.SnapshotPosition
	Table      string
	AvroSchema avro.Schema
}

type FetchWorker struct {
	conf       FetchConfig
	db         *pgxpool.Pool
	out        chan<- FetchData
	avroSchema avro.Schema

	keyType     string
	snapshotEnd string
	lastRead    string
	cursorName  string
}

func NewFetchWorker(db *pgxpool.Pool, out chan<- FetchData, c FetchConfig) *FetchWorker {
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
		f.snapshotEnd = t.SnapshotEnd
		f.lastRead = t.LastRead
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

	sdk.Logger(ctx).Info().
		Int("fetchSize", f.conf.FetchSize).
		Str("tx.snapshot", f.conf.TXSnapshotID).
		Str("startAt", f.lastRead).
		Str("snapshotEnd", f.snapshotEnd).
		Msgf("starting fetcher %s", f.cursorName)

	closeCursor, err := f.createCursor(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to create cursor: %w", err)
	}
	defer closeCursor()

	var nfetched int

	for {
		n, err := f.fetch(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to fetch results: %w", err)
		}

		if n == 0 { // end of cursor
			break
		}

		nfetched += n

		sdk.Logger(ctx).Info().
			Int("rows", nfetched).
			Str("table", f.conf.Table).
			Dur("elapsed", time.Since(start)).
			// need to adjust this to fetch the size of the snapshot
			// Str("completed_perc", fmt.Sprintf("%.2f", (float64(nfetched)/float64(f.snapshotEnd))*100)).
			Str("rate_per_min", fmt.Sprintf("%.0f", float64(nfetched)/time.Since(start).Minutes())).
			Msg("fetching rows")
	}

	sdk.Logger(ctx).Info().
		Dur("elapsed", time.Since(start)).
		Str("rate_per_min", fmt.Sprintf("%.0f", float64(nfetched)/time.Since(start).Minutes())).
		Str("table", f.conf.Table).
		Msgf("%q snapshot completed", f.conf.Table)

	return nil
}

func (f *FetchWorker) createCursor(ctx context.Context, tx pgx.Tx) (func(), error) {
	// set zero values
	lastReadStr := f.lastRead
	snapshotEndstr := f.snapshotEnd
	if lastReadStr == "" {
		switch f.keyType {
		case pgUUID:
			lastReadStr = "'00000000-0000-0000-0000-000000000000'"
			snapshotEndstr = fmt.Sprintf("'%s'", f.snapshotEnd)
		case pgSmallint, pgInteger, pgBigint:
			lastReadStr = "0"
			f.lastRead = "0"
		}
	}

	// This query will scan the table for rows based on the conditions.
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s >= %s AND %s <= %s ORDER BY %s",
		f.conf.Table,
		f.conf.Key, lastReadStr, // range start
		f.conf.Key, snapshotEndstr, // range end,
		f.conf.Key, // order by
	)

	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR(%s)", f.cursorName, selectQuery)

	sdk.Logger(ctx).Debug().Msgf("cursor query: %s", cursorQuery)

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
	if f.snapshotEnd > "" {
		return nil
	}

	var (
		snapshotEndStr string
		snapshotEndInt int64
		err            error
	)
	row := tx.QueryRow(
		ctx,
		fmt.Sprintf("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", f.conf.Key, f.conf.Table, f.conf.Key),
	)
	if err != nil {
		return fmt.Errorf("failed to query max on %q.%q: %w", f.conf.Table, f.conf.Key, err)
	}

	switch f.keyType {
	case pgUUID:
		if err = row.Scan(&snapshotEndStr); err != nil {
			return fmt.Errorf("failed to query max on %q.%q: %w", f.conf.Table, f.conf.Key, err)
		}
		f.snapshotEnd = snapshotEndStr
	case pgSmallint, pgInteger, pgBigint:
		if err = row.Scan(&snapshotEndInt); err != nil {
			return fmt.Errorf("failed to query max on %q.%q: %w", f.conf.Table, f.conf.Key, err)
		}
		f.snapshotEnd = fmt.Sprint(snapshotEndInt)
	}

	sdk.Logger(ctx).Debug().Msgf("snapshot end after conv: %s", f.snapshotEnd)

	return nil
}

func (f *FetchWorker) fetch(ctx context.Context, tx pgx.Tx) (int, error) {
	start := time.Now().UTC()

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

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("failed to get values: %w", err)
		}

		if f.conf.WithAvroSchema && f.avroSchema == nil {
			sch, err := schema.Avro.Extract(f.conf.Table, fields, values)
			if err != nil {
				return 0, fmt.Errorf("failed to extract schema: %w", err)
			}

			f.avroSchema = sch
		}

		data, err := f.buildFetchData(fields, values)
		if err != nil {
			return nread, fmt.Errorf("failed to build fetch data: %w", err)
		}

		if err := f.send(ctx, data); err != nil {
			return nread, fmt.Errorf("failed to send record: %w", err)
		}

		nread++
	}
	if rows.Err() != nil {
		return 0, fmt.Errorf("failed to read rows: %w", rows.Err())
	}

	return nread, nil
}

func (f *FetchWorker) send(ctx context.Context, d FetchData) error {
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

	key, payload, err := f.buildRecordData(fields, values)
	if err != nil {
		return FetchData{}, fmt.Errorf("failed to encode record data: %w", err)
	}

	return FetchData{
		Key:        key,
		Payload:    payload,
		Position:   pos,
		Table:      f.conf.Table,
		AvroSchema: f.avroSchema,
	}, nil
}

func (f *FetchWorker) buildSnapshotPosition(fields []pgconn.FieldDescription, values []any) (position.SnapshotPosition, error) {
	for i, fd := range fields {
		if fd.Name == f.conf.Key {
			return position.SnapshotPosition{
				SnapshotEnd: f.snapshotEnd,
				LastRead:    fmt.Sprint(values[i]),
			}, nil
		}
	}
	return position.SnapshotPosition{}, fmt.Errorf("key %q not found in fields", f.conf.Key)
}

func (f *FetchWorker) buildRecordData(fields []pgconn.FieldDescription, values []any) (sdk.StructuredData, sdk.StructuredData, error) {
	var (
		key     = make(sdk.StructuredData)
		payload = make(sdk.StructuredData)
	)

	for i, fd := range fields {
		if fd.Name == f.conf.Key {
			k, err := types.Format(fd.DataTypeOID, values[i])
			if err != nil {
				return key, payload, fmt.Errorf("failed to format key %q: %w", f.conf.Key, err)
			}

			key[f.conf.Key] = k
		}

		v, err := types.Format(fd.DataTypeOID, values[i])
		if err != nil {
			return key, payload, fmt.Errorf("failed to format payload field %q: %w", fd.Name, err)
		}
		payload[fd.Name] = v
	}

	return key, payload, nil
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

func (f *FetchWorker) validateKey(ctx context.Context, table, key string, tx pgx.Tx) error {
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

	f.keyType = dataType

	var isPK bool

	// As per https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
	if err := tx.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT a.attname FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = $1::regclass AND a.attname = $2 AND i.indisprimary)`,
		table, key,
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

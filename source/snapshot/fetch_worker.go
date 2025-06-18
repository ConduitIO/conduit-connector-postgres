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

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/internal"
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
	Key           opencdc.StructuredData
	Payload       opencdc.StructuredData
	Position      position.SnapshotPosition
	Table         string
	PayloadSchema cschema.Schema
	KeySchema     cschema.Schema
}

// FetchWorker fetches snapshot data from a single table
type FetchWorker struct {
	conf FetchConfig
	db   *pgxpool.Pool
	out  chan<- []FetchData

	// notNullMap maps column names to if the column is NOT NULL.
	tableInfoFetcher *internal.TableInfoFetcher
	keySchema        *cschema.Schema

	payloadSchema *cschema.Schema
	snapshotEnd   int64
	lastRead      int64
	cursorName    string
}

func NewFetchWorker(db *pgxpool.Pool, out chan<- []FetchData, c FetchConfig) *FetchWorker {
	f := &FetchWorker{
		conf:             c,
		db:               db,
		out:              out,
		tableInfoFetcher: internal.NewTableInfoFetcher(db),
		cursorName:       "fetcher_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
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

// Init will ensure the config is correct.
// * Table and keys exist
// * Key is a primary key
func (f *FetchWorker) Init(ctx context.Context) error {
	err := f.Validate(ctx)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	err = f.tableInfoFetcher.Refresh(ctx, f.conf.Table)
	if err != nil {
		return fmt.Errorf("failed to refresh table info: %w", err)
	}

	return nil
}

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
		Int64("startAt", f.lastRead).
		Int64("snapshotEnd", f.snapshotEnd).
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
			Str("completed_perc", fmt.Sprintf("%.2f", (float64(nfetched)/float64(f.snapshotEnd))*100)).
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
	// This query will scan the table for rows based on the conditions.
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s > %d AND %s <= %d ORDER BY %q",
		internal.WrapSQLIdent(f.conf.Table),
		internal.WrapSQLIdent(f.conf.Key), f.lastRead, // range start
		internal.WrapSQLIdent(f.conf.Key), f.snapshotEnd, // range end,
		f.conf.Key, // order by
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
	if f.snapshotEnd > 0 {
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

	var toBeSent []FetchData

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("failed to get values: %w", err)
		}

		err = f.extractSchemas(ctx, fields)
		if err != nil {
			return 0, fmt.Errorf("failed to extract schemas: %w", err)
		}

		data, err := f.buildFetchData(fields, values)
		if err != nil {
			return nread, fmt.Errorf("failed to build fetch data: %w", err)
		}

		toBeSent = append(toBeSent, data)
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

	key, payload, err := f.buildRecordData(fields, values)
	if err != nil {
		return FetchData{}, fmt.Errorf("failed to encode record data: %w", err)
	}

	fd := FetchData{
		Key:      key,
		Payload:  payload,
		Position: pos,
		Table:    f.conf.Table,
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
			return position.SnapshotPosition{
				LastRead:    lastRead,
				SnapshotEnd: f.snapshotEnd,
			}, nil
		}
	}
	return position.SnapshotPosition{}, fmt.Errorf("key %q not found in fields", f.conf.Key)
}

func (f *FetchWorker) buildRecordData(fields []pgconn.FieldDescription, values []any) (opencdc.StructuredData, opencdc.StructuredData, error) {
	var (
		key     = make(opencdc.StructuredData)
		payload = make(opencdc.StructuredData)
	)

	tableInfo := f.getTableInfo()
	for i, fd := range fields {
		isNotNull := tableInfo.Columns[fd.Name].IsNotNull

		if fd.Name == f.conf.Key {
			k, err := types.Format(fd.DataTypeOID, values[i], isNotNull)
			if err != nil {
				return key, payload, fmt.Errorf("failed to format key %q: %w", f.conf.Key, err)
			}

			key[f.conf.Key] = k
		}

		v, err := types.Format(fd.DataTypeOID, values[i], isNotNull)
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

	if f.payloadSchema == nil {
		sdk.Logger(ctx).Debug().
			Msgf("extracting payload schema for %v fields in %v", len(fields), f.conf.Table)

		avroPayloadSch, err := schema.Avro.Extract(f.conf.Table+"_payload", f.tableInfoFetcher.GetTable(f.conf.Table), fields)
		if err != nil {
			return fmt.Errorf("failed to extract payload schema for table %v: %w", f.conf.Table, err)
		}
		ps, err := sdkschema.Create(
			ctx,
			cschema.TypeAvro,
			avroPayloadSch.Name(),
			[]byte(avroPayloadSch.String()),
		)
		if err != nil {
			return fmt.Errorf("failed creating payload schema for table %v: %w", f.conf.Table, err)
		}
		f.payloadSchema = &ps
	}

	if f.keySchema == nil {
		sdk.Logger(ctx).Debug().
			Msgf("extracting schema for key %v in %v", f.conf.Key, f.conf.Table)

		avroKeySch, err := schema.Avro.Extract(f.conf.Table+"_key", f.getTableInfo(), fields, f.conf.Key)
		if err != nil {
			return fmt.Errorf("failed to extract key schema for table %v: %w", f.conf.Table, err)
		}
		ks, err := sdkschema.Create(
			ctx,
			cschema.TypeAvro,
			avroKeySch.Name(),
			[]byte(avroKeySch.String()),
		)
		if err != nil {
			return fmt.Errorf("failed creating key schema for table %v: %w", f.conf.Table, err)
		}
		f.keySchema = &ks
	}

	return nil
}

func (f *FetchWorker) getTableInfo() *internal.TableInfo {
	return f.tableInfoFetcher.GetTable(f.conf.Table)
}

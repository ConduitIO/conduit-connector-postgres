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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultFetchSize = 50000

var supportedKeyTypes = []string{
	"smallint",
	"integer",
	"bigint",
}

type FetchConfig struct {
	Table        string
	Key          string
	TXSnapshotID string
	FetchSize    int
	Position     position.Position
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
	Key      sdk.StructuredData
	Payload  sdk.StructuredData
	Position position.SnapshotPosition
	Table    string
}

type FetchWorker struct {
	conf FetchConfig
	db   *pgxpool.Pool
	out  chan<- FetchData

	snapshotEnd int64
	lastRead    int64
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
			Msg("fetching rows")
	}

	sdk.Logger(ctx).Info().
		Dur("elapsed", time.Since(start)).
		Str("table", f.conf.Table).
		Msgf("%q snapshot completed", f.conf.Table)

	return nil
}

func (f *FetchWorker) createCursor(ctx context.Context, tx pgx.Tx) (func(), error) {
	// N.B. Prepare as much as possible when the cursor is created.
	//      Table and columns cannot be prepared.
	//      This query will scan the table for rows based on the conditions.
	selectQuery := "SELECT * FROM " + f.conf.Table + " WHERE " + f.conf.Key + " > $1 AND " + f.conf.Key + " <= $2 ORDER BY $3"

	if _, err := tx.Exec(
		ctx,
		"DECLARE "+f.cursorName+" CURSOR FOR("+selectQuery+")",
		f.lastRead,    // range start
		f.snapshotEnd, // range end
		f.conf.Key,    // order by this
	); err != nil {
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

	if err := tx.QueryRow(
		ctx,
		fmt.Sprintf("SELECT max(%s) FROM %s", f.conf.Key, f.conf.Table),
	).Scan(&f.snapshotEnd); err != nil {
		return fmt.Errorf("failed to query max on %q.%q: %w", f.conf.Table, f.conf.Key, err)
	}

	return nil
}

func (f *FetchWorker) fetch(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", f.conf.FetchSize, f.cursorName))
	if err != nil {
		return 0, fmt.Errorf("failed to fetch rows: %w", err)
	}
	defer rows.Close()

	var fields []string
	for _, f := range rows.FieldDescriptions() {
		fields = append(fields, f.Name)
	}

	var nread int

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("failed to get values: %w", err)
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.out <- d:
		return nil
	}
}

func (f *FetchWorker) buildFetchData(fields []string, values []any) (FetchData, error) {
	pos, err := f.buildSnapshotPosition(fields, values)
	if err != nil {
		return FetchData{}, fmt.Errorf("failed to build snapshot position: %w", err)
	}
	key, payload := f.buildRecordData(fields, values)
	return FetchData{
		Key:      key,
		Payload:  payload,
		Position: pos,
		Table:    f.conf.Table,
	}, nil
}

func (f *FetchWorker) buildSnapshotPosition(fields []string, values []any) (position.SnapshotPosition, error) {
	for i, name := range fields {
		if name == f.conf.Key {
			// Always coerce snapshot position to bigint, pk may be any type of integer.
			lastRead, err := keyInt64(values[i])
			if err != nil {
				return position.SnapshotPosition{}, fmt.Errorf("failed to parse key: %w", err)
			}
			return position.SnapshotPosition{
				LastRead:    lastRead,
				SnapshotEnd: f.snapshotEnd,
				Done:        f.snapshotEnd == lastRead,
			}, nil
		}
	}
	return position.SnapshotPosition{}, fmt.Errorf("key %q not found in fields", f.conf.Key)
}

func (f *FetchWorker) buildRecordData(fields []string, values []any) (key sdk.StructuredData, payload sdk.StructuredData) {
	payload = make(sdk.StructuredData)

	for i, name := range fields {
		switch t := values[i].(type) {
		case time.Time: // type not supported in sdk.Record
			payload[name] = t.UTC().String()
		default:
			payload[name] = t
		}
	}

	key = sdk.StructuredData{
		f.conf.Key: payload[f.conf.Key],
	}

	return key, payload
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
		"SELECT data_type FROM information_schema.columns WHERE table_name=$1 AND column_name=$2",
		table, key,
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

	if err := tx.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT tc.constraint_type
			FROM information_schema.constraint_column_usage cu JOIN information_schema.table_constraints tc
			ON tc.constraint_name = cu.constraint_name
			WHERE cu.table_name=$1 AND cu.column_name=$2)`,
		table, key,
	).Scan(&isPK); err != nil {
		return fmt.Errorf("unable to determine key %q constraints: %w", key, err)
	}

	if !isPK {
		return fmt.Errorf("invalid key %q, not a primary key", key)
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

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
	"strings"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultFetchSize = 50000

type FetcherConfig struct {
	Table     string
	Key       string
	Snapshot  string
	FetchSize int
	Position  position.Position
}

func (c *FetcherConfig) Validate() error {
	var errs []error

	if c.Table == "" {
		errs = append(errs, fmt.Errorf("invalid table %q", c.Table))
	}

	if c.Key == "" {
		errs = append(errs, fmt.Errorf("invalid table key %q", c.Key))
	}

	if c.FetchSize == 0 {
		c.FetchSize = defaultFetchSize
	}

	switch c.Position.Type {
	case position.TypeSnapshot, position.TypeInitial:
	default:
		errs = append(errs, fmt.Errorf("invalid position type %q", c.Position.Type.String()))
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}

	return nil
}

type FetcherWorker struct {
	conf FetcherConfig
	db   *pgxpool.Pool
	out  chan<- sdk.Record

	snapshotEnd int64
	lastRead    int64
	cursorName  string
}

func NewFetcherWorker(db *pgxpool.Pool, out chan<- sdk.Record, c FetcherConfig) *FetcherWorker {
	f := &FetcherWorker{
		conf:       c,
		db:         db,
		out:        out,
		cursorName: fmt.Sprint("fetcher_", strings.ReplaceAll(uuid.NewString(), "-", "")),
	}

	if c.Position.Type == position.TypeInitial || c.Position.Snapshot == nil {
		return f
	}

	if t, ok := c.Position.Snapshot[c.Table]; ok {
		f.snapshotEnd = t.SnapshotEnd
		f.lastRead = t.LastRead
	}

	return f
}

// Validate will ensure the config is correctt.
// * Table and keys exist
// * Key is a primary key
func (f *FetcherWorker) Validate(ctx context.Context) error {
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

	if err := validateTable(ctx, f.conf.Table, tx); err != nil {
		return fmt.Errorf("failed to validate table: %w", err)
	}

	if err := validateKey(ctx, f.conf.Table, f.conf.Key, tx); err != nil {
		return fmt.Errorf("failed to validate key: %w", err)
	}

	return nil
}

func (f *FetcherWorker) Run(ctx context.Context) error {
	start := time.Now().UTC()

	tx, err := f.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return err
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

	if err := f.updateFetchLimit(ctx, tx); err != nil {
		return fmt.Errorf("failed to update fetch limit: %w", err)
	}

	closeCursor, err := f.createCursor(ctx, tx)
	if err != nil {
		return fmt.Errorf("fail to create cursor: %w", err)
	}
	defer closeCursor()

	var nfetched int64

	for {
		n, err := f.fetch(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to fetch results: %w", err)
		}

		if n == 0 { // end of cursor
			break
		}

		nfetched += int64(n)

		sdk.Logger(ctx).Info().
			Int64("rows", nfetched).
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

func (f *FetcherWorker) createCursor(ctx context.Context, tx pgx.Tx) (func(), error) {
	cursorSQL := fmt.Sprintf("DECLARE %s CURSOR FOR (SELECT * FROM %s WHERE %s > %d AND %s <= %d ORDER BY %s)",
		f.cursorName,
		f.conf.Table,
		f.conf.Key,
		f.lastRead,
		f.conf.Key,
		f.snapshotEnd,
		f.conf.Key,
	)

	if _, err := tx.Exec(ctx, cursorSQL); err != nil {
		return nil, err
	}

	return func() {
		// N.B. The cursor will automatically close when the TX is done.
		if _, err := tx.Exec(ctx, fmt.Sprint("CLOSE ", f.cursorName)); err != nil {
			sdk.Logger(ctx).Warn().
				Err(err).
				Msgf("unexpected error when closing cursor %q", f.cursorName)
		}
	}, nil
}

func (f *FetcherWorker) updateFetchLimit(ctx context.Context, tx pgx.Tx) error {
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

func (f *FetcherWorker) fetch(ctx context.Context, tx pgx.Tx) (int, error) {
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
			return 0, fmt.Errorf("failed to get values")
		}

		if err := f.send(
			ctx,
			f.buildRecord(fields, values),
		); err != nil {
			return nread, fmt.Errorf("failed to send record")
		}

		nread++
	}

	return nread, nil
}

func (f *FetcherWorker) send(ctx context.Context, r sdk.Record) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("send context done: %w", ctx.Err())
	case f.out <- r:
		return nil
	}
}

func (f *FetcherWorker) buildRecord(fields []string, values []any) sdk.Record {
	payload := make(sdk.StructuredData)

	for i, name := range fields {
		switch t := values[i].(type) {
		case time.Time: // type not supported in sdk.Record
			payload[name] = t.UTC().String()
		default:
			payload[name] = t
		}
	}

	pos := position.Position{
		Type: position.TypeSnapshot,
		Snapshot: position.SnapshotPositions{
			f.conf.Table: {
				LastRead:    payload[f.conf.Key].(int64),
				SnapshotEnd: f.snapshotEnd,
				Done:        f.snapshotEnd == payload[f.conf.Key].(int64),
			},
		},
	}.ToSDKPosition()

	meta := map[string]string{
		"table": f.conf.Table,
	}

	key := sdk.StructuredData{
		f.conf.Key: payload[f.conf.Key],
	}

	return sdk.Util.Source.NewRecordSnapshot(pos, meta, key, payload)
}

func (f *FetcherWorker) withSnapshot(ctx context.Context, tx pgx.Tx) error {
	if f.conf.Snapshot == "" {
		// log snapshot not provided
		return nil
	}

	if _, err := tx.Exec(
		ctx,
		fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", f.conf.Snapshot),
	); err != nil {
		return fmt.Errorf("failed to set tx snapshot %q: %w", f.conf.Snapshot, err)
	}

	return nil
}

func validateKey(ctx context.Context, table, key string, tx pgx.Tx) error {
	var keyExists, isPK bool

	if err := tx.QueryRow(
		ctx,
		"SELECT EXISTS(SELECT column_name FROM information_schema.columns WHERE table_name=$1 AND column_name=$2)",
		table, key,
	).Scan(&keyExists); err != nil {
		return fmt.Errorf("unable to check key %q on table %q: %w", key, table, err)
	}

	if !keyExists {
		return fmt.Errorf("key %q not present on table %q", key, table)
	}

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

func validateTable(ctx context.Context, table string, tx pgx.Tx) error {
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

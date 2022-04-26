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

package logrepl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/conduitio/conduit-connector-postgres/pgutil"
	sdk "github.com/conduitio/conduit-connector-sdk"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// ErrSnapshotComplete is returned by Next when a snapshot is finished
var ErrSnapshotComplete = errors.New("snapshot complete")

// ErrSnapshotInterrupt is returned by Teardown when a snapshot is interrupted
var ErrSnapshotInterrupt = errors.New("snapshot interrupted")

// snapshotPrefix prefixes all Positions that originate from a snapshot
const snapshotPrefix = "s"

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

const actionSnapshot string = "snapshot"

type SnapshotIterator struct {
	config      Config
	currentLSN  string
	finished    bool
	internalPos int64

	rows pgx.Rows
	done chan struct{}
}

func NewSnapshotIterator(ctx context.Context, conn *pgx.Conn, cfg Config) (*SnapshotIterator, error) {
	s := &SnapshotIterator{
		config: cfg,
		done:   make(chan struct{}),
	}

	poolcfg, err := pgxpool.ParseConfig(conn.Config().ConnString())
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.ConnectConfig(ctx, poolcfg)
	if err != nil {
		return nil, err
	}

	err = s.withSnapshot(ctx, pool, func(snapshotName string) error {
		err = s.setCurrentLSN(ctx, pool)
		if err != nil {
			return fmt.Errorf("failed to set current LSN: %w", err)
		}

		err = s.setSnapshotTx(ctx, pool, snapshotName)
		if err != nil {
			return fmt.Errorf("failed to set snapshot transaction id: %w", err)
		}

		err = s.setRows(ctx, pool)
		if err != nil {
			return fmt.Errorf("failed to get rows: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to export snapshot: %w", err)
	}

	return s, nil
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Record{}, fmt.Errorf("context err: %w", err)
	}

	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return sdk.Record{}, fmt.Errorf("rows error: %w", err)
		}
		s.finished = true
		close(s.done)
		return sdk.Record{}, ErrSnapshotComplete
	}

	return s.buildRecord(ctx)
}

// Ack is a noop for snapshots
func (s *SnapshotIterator) Ack(ctx context.Context, pos sdk.Position) error {
	return nil // noop for snapshots
}

// Teardown attempts to gracefully teardown the iterator.
func (s *SnapshotIterator) Teardown(ctx context.Context) error {
	defer s.rows.Close()

	var err error
	rowsErr := s.rows.Err()
	if rowsErr != nil {
		err = logOrReturnError(ctx, err, rowsErr, "rows returned an error")
	}

	if !s.finished {
		err = logOrReturnError(ctx, rowsErr, ErrSnapshotInterrupt, "snapshot interrupted")
	}

	return err
}

// LSN returns the transactions's current LSN for anchoring replication
func (s *SnapshotIterator) LSN() string {
	return s.currentLSN
}

// Finished returns whether or not the snapshot iterator has returned all of its
// rows or not.
func (s *SnapshotIterator) Finished() bool {
	return s.finished
}

// Done signals when the snapshot is finished.
func (s *SnapshotIterator) Done() <-chan struct{} {
	return s.done
}

func (s *SnapshotIterator) withSnapshot(
	ctx context.Context,
	pool *pgxpool.Pool,
	fn func(snapshotName string) error,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			sdk.Logger(ctx).Err(err).Msg("transaction failed to commit")
		}
	}()
	var name string
	row := tx.QueryRow(ctx, "select pg_export_snapshot();")
	if err := row.Scan(&name); err != nil {
		return err
	}
	return fn(name)
}

func (s *SnapshotIterator) setRows(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	query, args, err := psql.
		Select(s.config.Columns...).
		From(s.config.TableName).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to create read query: %w", err)
	}
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	s.rows = rows
	go func() {
		<-s.Done()
		s.rows.Close()
	}()
	return nil
}

func (s *SnapshotIterator) setSnapshotTx(ctx context.Context, pool *pgxpool.Pool, snapshotName string) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return err
	}
	snapshotTx := fmt.Sprintf(`SET TRANSACTION SNAPSHOT '%s'`, snapshotName)
	_, err = tx.Exec(ctx, snapshotTx)
	if err != nil {
		if rollErr := tx.Rollback(ctx); rollErr != nil {
			sdk.Logger(ctx).Err(rollErr).Msg("set transaction rollback failed")
		}
		return fmt.Errorf("failed to set transaction snapshot id: %w", err)
	}
	go func() {
		<-s.Done()
		if err := tx.Commit(ctx); err != nil {
			sdk.Logger(ctx).Err(err).Msg("failed to commit snapshot tx")
		}
	}()

	return nil
}

func (s *SnapshotIterator) setCurrentLSN(ctx context.Context, pool *pgxpool.Pool) error {
	var lsn string
	row := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn();")
	if err := row.Scan(&lsn); err != nil {
		return fmt.Errorf("failed to retrieve current lsn: %w", err)
	}
	sdk.Logger(ctx).Info().Msgf("")
	s.currentLSN = lsn
	return nil
}

func (s *SnapshotIterator) buildRecord(ctx context.Context) (sdk.Record, error) {
	r, err := withPayloadAndKey(sdk.Record{}, s.rows, s.config.Columns, s.config.KeyColumnName)
	if err != nil {
		return sdk.Record{}, err
	}
	r.CreatedAt = time.Now()
	r.Metadata = map[string]string{
		"action": actionSnapshot,
		"table":  s.config.TableName,
	}
	r.Position = s.snapshotPosition()
	return r, nil
}

func (s *SnapshotIterator) snapshotPosition() sdk.Position {
	pos := SnapshotPosition(s.config.TableName, s.internalPos)
	s.internalPos++
	return pos
}

// withPayloadAndKey builds a record's payload from *sql.Rows. It calls
// Scan so it assumes that Next has been checked previously.
func withPayloadAndKey(
	rec sdk.Record,
	rows pgx.Rows,
	columns []string,
	key string,
) (sdk.Record, error) {
	colTypes := rows.FieldDescriptions()

	vals := make([]interface{}, len(columns))
	for i := range columns {
		vals[i] = oidToScannerValue(pgtype.OID(colTypes[i].DataTypeOID))
	}

	err := rows.Scan(vals...)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed to scan: %w", err)
	}

	payload := make(sdk.StructuredData)
	for i, col := range columns {
		val := vals[i].(pgtype.Value)

		// handle and assign the record a Key
		if key == col {
			// TODO: Handle composite keys
			rec.Key = sdk.StructuredData{
				col: val.Get(),
			}
			// continue without assigning so payload doesn't duplicate key data
			continue
		}

		payload[col] = val.Get()
	}

	rec.Payload = payload
	return rec, nil
}

type scannerValue interface {
	pgtype.Value
	sql.Scanner
}

func oidToScannerValue(oid pgtype.OID) scannerValue {
	t, ok := pgutil.OIDToPgType(oid).(scannerValue)
	if !ok {
		// not all pg types implement pgtype.Value and sql.Scanner
		return &pgtype.Unknown{}
	}
	return t
}

// logOrReturn
func logOrReturnError(ctx context.Context, oldErr, newErr error, msg string) error {
	if oldErr == nil {
		return fmt.Errorf(msg+": %w", newErr)
	}
	sdk.Logger(ctx).Err(newErr).Msg(msg)
	return oldErr
}

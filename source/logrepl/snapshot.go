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
	"strconv"
	"time"

	"github.com/conduitio/conduit-connector-postgres/pgutil"
	sdk "github.com/conduitio/conduit-connector-sdk"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// ErrSnapshotComplete is returned by Next when a snapshot is finished
var ErrSnapshotComplete = errors.New("ErrSnapshotComplete")

// ErrSnapshotInterrupt is returned by Teardown when a snapshot is interrupted
var ErrSnapshotInterrupt = errors.New("ErrSnapshotInterrupt")

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

const actionSnapshot string = "snapshot"

type SnapshotConfig struct {
	SnapshotName string
	Table        string
	Columns      []string
	KeyColumn    string
}

type SnapshotIterator struct {
	config SnapshotConfig

	tx   pgx.Tx
	rows pgx.Rows

	complete    bool
	internalPos int64
}

func NewSnapshotIterator(ctx context.Context, conn *pgx.Conn, cfg SnapshotConfig) (*SnapshotIterator, error) {
	s := &SnapshotIterator{
		config: cfg,
	}

	err := s.startSnapshotTx(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to start snapshot tx: %w", err)
	}

	err = s.loadRows(ctx)
	if err != nil {
		if rollErr := s.tx.Rollback(ctx); err != nil {
			sdk.Logger(ctx).Err(err).Msg("load rows failed")
			return nil, fmt.Errorf("rollback failed: %w", rollErr)
		}
		return nil, fmt.Errorf("failed to load rows: %w", err)
	}

	return s, nil
}

func (s *SnapshotIterator) loadRows(ctx context.Context) error {
	query, args, err := psql.
		Select(s.config.Columns...).
		From(s.config.Table).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to create read query: %w", err)
	}

	rows, err := s.tx.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	s.rows = rows

	return nil
}

func (s *SnapshotIterator) startSnapshotTx(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return err
	}

	s.tx = tx

	snapshotTx := fmt.Sprintf(`SET TRANSACTION SNAPSHOT '%s'`, s.config.SnapshotName)
	_, err = tx.Exec(ctx, snapshotTx)
	if err != nil {
		if rollErr := s.tx.Rollback(ctx); rollErr != nil {
			sdk.Logger(ctx).Err(rollErr).Msg("set transaction rollback failed")
		}
		return fmt.Errorf("failed to set transaction snapshot id: %w", err)
	}

	return nil
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Record{}, fmt.Errorf("context err: %w", err)
	}

	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return sdk.Record{}, fmt.Errorf("rows error: %w", err)
		}
		s.complete = true
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
	s.rows.Close()
	var err error
	if commitErr := s.tx.Commit(ctx); commitErr != nil {
		err = logOrReturnError(ctx, err, commitErr, "teardown commit failed")
	}
	if rowsErr := s.rows.Err(); rowsErr != nil {
		err = logOrReturnError(ctx, err, rowsErr, "rows returned an error")
	}

	if !s.complete {
		sdk.Logger(ctx).Warn().Msg("snapshot interrupted")
		return ErrSnapshotInterrupt
	}

	return err
}

func (s *SnapshotIterator) buildRecord(ctx context.Context) (sdk.Record, error) {
	r, err := withPayloadAndKey(sdk.Record{}, s.rows, s.config.Columns, s.config.KeyColumn)
	if err != nil {
		return sdk.Record{}, err
	}

	r.CreatedAt = time.Now()

	r.Metadata = map[string]string{
		"action": actionSnapshot,
		"table":  s.config.Table,
	}

	r.Position = s.formatPosition()

	return r, nil
}

// withPosition adds a position to a record that contains the table name and
// the record's position in the current snapshot, aka it's number.
func (s *SnapshotIterator) formatPosition() sdk.Position {
	position := fmt.Sprintf("%s:%s", s.config.Table, strconv.FormatInt(s.internalPos, 10))
	s.internalPos++
	return sdk.Position(position)
}

// withPayloadAndKey builds a record's payload from *sql.Rows. It calls
// Scan so it assumes that Next has been checked previously.
func withPayloadAndKey(rec sdk.Record, rows pgx.Rows, columns []string, key string) (sdk.Record, error) {
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

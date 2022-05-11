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
var ErrSnapshotComplete = errors.New("snapshot complete")

// ErrSnapshotInterrupt is returned by Teardown when a snapshot is interrupted
var ErrSnapshotInterrupt = errors.New("snapshot interrupted")

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

// TODO: remove conn from args here
func NewSnapshotIterator(ctx context.Context, conn *pgx.Conn, cfg SnapshotConfig) (*SnapshotIterator, error) {
	s := &SnapshotIterator{
		config: cfg,
	}
	return s, nil
}

func (s *SnapshotIterator) Start(ctx context.Context, conn *pgx.Conn) error {
	err := s.StartSnapshotTx(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to start snapshot tx: %w", err)
	}

	err = s.LoadRows(ctx)
	if err != nil {
		if rollErr := s.tx.Rollback(ctx); rollErr != nil {
			sdk.Logger(ctx).Err(rollErr).Msg("rollback failed")
		}
		return fmt.Errorf("failed to load rows: %w", err)
	}

	return nil
}

// LoadRowsConn allows a user to pass a custom postgres connection to the
// rows query for snapshots.
// TODO: This should be a SetRows function that both Start and an external
// user could use to set the snapshotter's rows manually.
func (s *SnapshotIterator) LoadRowsConn(ctx context.Context, conn *pgx.Conn) error {
	query, args, err := psql.
		Select(s.config.Columns...).
		From(s.config.Table).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to create read query: %w", err)
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	s.rows = rows

	return nil
}

// LoadRows uses the SnapshotIterator's own connection
func (s *SnapshotIterator) LoadRows(ctx context.Context) error {
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

// SetSnapshot sets the current connection's snapshot to the configured snapshot
func (s *SnapshotIterator) SetSnapshot(ctx context.Context, conn *pgx.Conn) error {
	snapshotTx := fmt.Sprintf(`SET TRANSACTION SNAPSHOT '%s'`, s.config.SnapshotName)
	_, err := conn.Exec(ctx, snapshotTx)
	if err != nil {
		if rollErr := s.tx.Rollback(ctx); rollErr != nil {
			sdk.Logger(ctx).Err(rollErr).Msg("set transaction rollback failed")
		}
		return fmt.Errorf("failed to set transaction snapshot id: %w", err)
	}
	return nil
}

func (s *SnapshotIterator) StartSnapshotTx(ctx context.Context, conn *pgx.Conn) error {
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

	// TODO: Handle this gracefully instead of just a nil check
	if s.tx != nil {
		if commitErr := s.tx.Commit(ctx); commitErr != nil {
			err = logOrReturnError(ctx, err, commitErr, "teardown commit failed")
		}
	}

	if rowsErr := s.rows.Err(); rowsErr != nil {
		err = logOrReturnError(ctx, err, rowsErr, "rows returned an error")
	}

	if !s.complete {
		err = logOrReturnError(ctx, err, ErrSnapshotInterrupt, "snapshot interrupted")
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

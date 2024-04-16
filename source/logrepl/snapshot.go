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
	"errors"
	"fmt"
	"strconv"

	sq "github.com/Masterminds/squirrel"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

// ErrSnapshotComplete is returned by Next when a snapshot is finished
var ErrSnapshotComplete = errors.New("snapshot complete")

// ErrSnapshotInterrupt is returned by Teardown when a snapshot is interrupted
var ErrSnapshotInterrupt = errors.New("snapshot interrupted")

const (
	// TODO same constant is defined in packages longpoll, logrepl and destination
	//  use same constant everywhere
	MetadataPostgresTable = "postgres.table"
)

type SnapshotConfig struct {
	SnapshotName string
	Table        string
	Columns      []string
	KeyColumn    string
}

type SnapshotIterator struct {
	config SnapshotConfig

	tx          pgx.Tx
	rows        pgx.Rows
	stmtBuilder sq.StatementBuilderType

	complete    bool
	internalPos int64

	keyColumnIndex int
}

func NewSnapshotIterator(ctx context.Context, conn *pgx.Conn, cfg SnapshotConfig) (*SnapshotIterator, error) {
	s := &SnapshotIterator{
		config:         cfg,
		keyColumnIndex: -1,
		stmtBuilder:    sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}

	err := s.startSnapshotTx(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to start snapshot tx: %w", err)
	}

	err = s.loadRows(ctx)
	if err != nil {
		if rollErr := s.tx.Rollback(ctx); rollErr != nil {
			sdk.Logger(ctx).Err(rollErr).Msg("rollback failed")
		}
		return nil, fmt.Errorf("failed to load rows: %w", err)
	}

	for i, col := range cfg.Columns {
		if col == cfg.KeyColumn {
			s.keyColumnIndex = i
		}
	}

	return s, nil
}

func (s *SnapshotIterator) loadRows(ctx context.Context) error {
	query, args, err := s.stmtBuilder.
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
		return sdk.Record{}, err
	}

	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return sdk.Record{}, fmt.Errorf("rows error: %w", err)
		}
		s.complete = true
		return sdk.Record{}, ErrSnapshotComplete
	}

	vals, err := s.rows.Values()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("could not scan row values: %w", err)
	}

	s.internalPos++ // increment internal position
	rec := sdk.Util.Source.NewRecordSnapshot(
		s.buildRecordPosition(),
		s.buildRecordMetadata(),
		s.buildRecordKey(vals),
		s.buildRecordPayload(vals),
	)

	return rec, nil
}

// Ack is a noop for snapshots
func (s *SnapshotIterator) Ack(context.Context, sdk.Position) error {
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
		err = logOrReturnError(ctx, err, ErrSnapshotInterrupt, "snapshot interrupted")
	}

	return err
}

// buildRecordPosition returns the current position used to identify the current
// record.
func (s *SnapshotIterator) buildRecordPosition() sdk.Position {
	position := fmt.Sprintf("%s:%s", s.config.Table, strconv.FormatInt(s.internalPos, 10))
	return sdk.Position(position)
}

func (s *SnapshotIterator) buildRecordMetadata() map[string]string {
	return map[string]string{
		MetadataPostgresTable: s.config.Table,
	}
}

// buildRecordKey returns the key for the record.
func (s *SnapshotIterator) buildRecordKey(values []interface{}) sdk.Data {
	if s.keyColumnIndex == -1 {
		return nil
	}
	return sdk.StructuredData{
		// TODO handle composite keys
		s.config.KeyColumn: values[s.keyColumnIndex],
	}
}

func (s *SnapshotIterator) buildRecordPayload(values []interface{}) sdk.Data {
	payload := make(sdk.StructuredData)
	for i, val := range values {
		payload[s.config.Columns[i]] = val
	}
	return payload
}

func logOrReturnError(ctx context.Context, oldErr, newErr error, msg string) error {
	if oldErr == nil {
		return fmt.Errorf(msg+": %w", newErr)
	}
	sdk.Logger(ctx).Err(newErr).Msg(msg)
	return oldErr
}

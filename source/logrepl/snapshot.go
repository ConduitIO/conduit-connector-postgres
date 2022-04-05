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
	"fmt"
	"strconv"
	"time"

	"github.com/conduitio/conduit-connector-postgres/pgutil"
	sdk "github.com/conduitio/conduit-connector-sdk"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

const actionSnapshot string = "snapshot"

type SnapshotConfig struct {
	URI          string
	SnapshotName string
	Table        string
	Columns      []string
	KeyColumn    string
}

type SnapshotIterator struct {
	config SnapshotConfig

	conn *pgx.Conn
	rows pgx.Rows

	internalPos int64
}

func NewSnapshotIterator(ctx context.Context, conn *pgx.Conn, cfg SnapshotConfig) (*SnapshotIterator, error) {
	s := &SnapshotIterator{
		config: cfg,
		conn:   conn,
	}

	err := s.startSnapshotTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start snapshot tx: %w", err)
	}

	err = s.loadRows(ctx)
	if err != nil {
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

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	s.rows = rows

	return nil
}

func (s *SnapshotIterator) startSnapshotTx(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ;`)
	if err != nil {
		return nil
	}

	snapshotTx := fmt.Sprintf(`SET TRANSACTION SNAPSHOT '%s'`, s.config.SnapshotName)
	_, err = s.conn.Exec(ctx, snapshotTx)
	if err != nil {
		return err
	}

	return nil
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return sdk.Record{}, fmt.Errorf("rows error: %w", err)
		}

		return sdk.Record{}, sdk.ErrBackoffRetry
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
	return s.commit(ctx)
}

func (s *SnapshotIterator) commit(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, `COMMIT;`)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	return nil
}

func (s *SnapshotIterator) buildRecord(ctx context.Context) (sdk.Record, error) {
	if err := s.rows.Err(); err != nil {
		return sdk.Record{}, fmt.Errorf("build record rows error: %w", err)
	}

	r, err := withPayloadAndKey(sdk.Record{}, s.rows, s.config.Columns, s.config.KeyColumn)
	if err != nil {
		return sdk.Record{}, err
	}

	r.CreatedAt = time.Now()

	r.Metadata = map[string]string{
		"action": actionSnapshot,
		"table":  s.config.Table,
	}

	r.Position = sdk.Position(strconv.FormatInt(s.internalPos, 10))
	s.internalPos++

	return r, nil
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

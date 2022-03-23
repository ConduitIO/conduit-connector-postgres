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

package longpoll

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-connector-postgres/pgutil"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// Declare Postgres $ placeholder format
var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

var (
	// ErrNoRows is returned when there are no rows to read.
	// * This can happen if the database is closed early, if there are no
	// rows in the result set, or if there are no results left to return.
	ErrNoRows = fmt.Errorf("no more rows")
	// ErrSnapshotInterrupt is returned when Teardown or any other signal
	// cancels an in-progress snapshot.
	ErrSnapshotInterrupt = fmt.Errorf("interrupted snapshot")
)

// SnapshotIterator implements the Iterator interface for capturing an initial table
// snapshot.
type SnapshotIterator struct {
	// table is the table to snapshot
	table string
	// key is the name of the key column for the table snapshot
	key string
	// list of columns that the iterator should record
	columns []string
	// conn handle to postgres
	conn *pgx.Conn
	// rows holds a reference to the postgres connection. this can be nil so
	// we must always call loadRows before HasNext or Next.
	rows pgx.Rows
	// ineternalPos is an internal integer Position for the SnapshotIterator to
	// to return at each Read call.
	internalPos int64
	// snapshotComplete keeps an internal record of whether the snapshot is
	// complete yet
	snapshotComplete bool
}

// NewSnapshotIterator returns a SnapshotIterator that is an Iterator.
// * NewSnapshotIterator attempts to load the sql rows into the SnapshotIterator and will
// immediately begin to return them to subsequent Read calls.
// * It acquires a read only transaction lock before reading the table.
// * If Teardown is called while a snpashot is in progress, it will return an
// ErrSnapshotInterrupt error.
func NewSnapshotIterator(ctx context.Context, conn *pgx.Conn, table string, columns []string, key string) (*SnapshotIterator, error) {
	s := &SnapshotIterator{
		conn:             conn,
		table:            table,
		columns:          columns,
		key:              key,
		internalPos:      0,
		snapshotComplete: false,
	}
	// load our initial set of rows into the iterator after we've set the db
	err := s.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows for snapshot: %w", err)
	}
	return s, nil
}

// Next returns the next row in the iterators rows.
// * If Next is called after HasNext has returned false, it will
// return an ErrNoRows error.
func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	if s.rows == nil {
		return sdk.Record{}, ErrNoRows
	}
	if !s.rows.Next() {
		s.snapshotComplete = true
		return sdk.Record{}, ErrNoRows
	}
	s.internalPos++

	rec := sdk.Record{}
	rec, err := withPayload(rec, s.rows, s.columns, s.key)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed to assign payload: %w",
			err)
	}
	rec = withMetadata(rec, s.table, s.key)
	rec = withTimestampNow(rec)
	rec = withPosition(rec, s.internalPos)
	return rec, nil
}

// Ack is here to implement the Iterator interface, it does nothing.
func (s *SnapshotIterator) Ack(context.Context, sdk.Position) error {
	return nil // acks not needed
}

// Teardown cleans up the database iterator by committing and closing the
// connection to sql.Rows
// * If the snapshot is not complete yet, it will return an ErrSnpashotInterrupt
// * Teardown must be called by the caller, it will not automatically be called
// when the snapshot is completed.
// * Teardown handles all of its manual cleanup first then calls cancel to
// stop any unhandled contexts that we've received.
func (s *SnapshotIterator) Teardown(ctx context.Context) error {
	// throw interrupt error if we're not finished with snapshot
	var interruptErr error
	if !s.snapshotComplete {
		interruptErr = ErrSnapshotInterrupt
	}
	s.rows.Close()
	rowsErr := s.rows.Err()
	if rowsErr != nil {
		return fmt.Errorf("rows error: %w", rowsErr)
	}
	return interruptErr
}

// loadRows loads the rows returned from the database onto the iterator
// or returns an error.
// * It returns nil if no error was detected.
// * rows.Close and rows.Err are called at Teardown.
func (s *SnapshotIterator) loadRows(ctx context.Context) error {
	query, args, err := psql.Select(s.columns...).From(s.table).ToSql()
	if err != nil {
		return fmt.Errorf("failed to create read query: %w", err)
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query context: %w", err)
	}
	s.rows = rows
	return nil
}

// sets an integer position to the correct stringed integer on
func withPosition(rec sdk.Record, i int64) sdk.Record {
	rec.Position = sdk.Position(strconv.FormatInt(i, 10))
	return rec
}

// withMetadata sets the Metadata field on a Record.
// Currently it adds the table name and key column of the Record.
func withMetadata(rec sdk.Record, table string, key string) sdk.Record {
	if rec.Metadata == nil {
		rec.Metadata = make(map[string]string)
	}
	rec.Metadata["table"] = table
	rec.Metadata["key"] = key
	return rec
}

// withTimestampNow is used when no column name for records' timestamp
// field is set.
func withTimestampNow(rec sdk.Record) sdk.Record {
	rec.CreatedAt = time.Now()
	return rec
}

// withPayload builds a record's payload from *sql.Rows.
func withPayload(rec sdk.Record, rows pgx.Rows, columns []string, key string) (sdk.Record, error) {
	// get the column types for those rows and record them as well
	colTypes := rows.FieldDescriptions()

	// make a new slice of correct pgtypes to scan into
	vals := make([]interface{}, len(columns))
	for i := range columns {
		vals[i] = oidToScannerValue(pgtype.OID(colTypes[i].DataTypeOID))
	}

	// build the payload from the row
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

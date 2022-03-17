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

package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
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

// Snapshotter implements the Iterator interface for capturing an initial table
// snapshot.
type Snapshotter struct {
	// table is the table to snapshot
	table string
	// key is the name of the key column for the table snapshot
	key string
	// list of columns that snapshotter should record
	columns []string
	// conn handle to postgres
	conn *pgx.Conn
	// rows holds a reference to the postgres connection. this can be nil so
	// we must always call loadRows before HasNext or Next.
	rows pgx.Rows
	// ineternalPos is an internal integer Position for the Snapshotter to
	// to return at each Read call.
	internalPos int64
	// snapshotComplete keeps an internal record of whether the snapshot is
	// complete yet
	snapshotComplete bool
}

// NewSnapshotter returns a Snapshotter that is an Iterator.
// * NewSnapshotter attempts to load the sql rows into the Snapshotter and will
// immediately begin to return them to subsequent Read calls.
// * It acquires a read only transaction lock before reading the table.
// * If Teardown is called while a snpashot is in progress, it will return an
// ErrSnapshotInterrupt error.
func NewSnapshotter(ctx context.Context, conn *pgx.Conn, table string, columns []string, key string) (*Snapshotter, error) {
	s := &Snapshotter{
		conn:             conn,
		table:            table,
		columns:          columns,
		key:              key,
		internalPos:      0,
		snapshotComplete: false,
	}
	// load our initial set of rows into the snapshotter after we've set the db
	err := s.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows for snapshot: %w", err)
	}
	return s, nil
}

// HasNext returns whether s.rows has another row.
// * It must be called before Snapshotter#Next is or else it will fail.
// * It increments the interal position if another row exists.
// * If HasNext is called and no rows are available, it will mark the snapshot
// as complete and then returns.
func (s *Snapshotter) HasNext() bool {
	next := s.rows.Next()
	if !next {
		s.snapshotComplete = true
		return next
	}
	s.internalPos++
	return next
}

// Next returns the next row in the snapshotter's rows.
// * If Next is called after HasNext has returned false, it will
// return an ErrNoRows error.
func (s *Snapshotter) Next(ctx context.Context) (sdk.Record, error) {
	if s.snapshotComplete {
		return sdk.Record{}, ErrNoRows
	}
	if s.rows == nil {
		return sdk.Record{}, ErrNoRows
	}
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

// Teardown cleans up the database snapshotter by committing and closing the
// connection to sql.Rows
// * If the snapshot is not complete yet, it will return an ErrSnpashotInterrupt
// * Teardown must be called by the caller, it will not automatically be called
// when the snapshot is completed.
// * Teardown handles all of its manual cleanup first then calls cancel to
// stop any unhandled contexts that we've received.
func (s *Snapshotter) Teardown(ctx context.Context) error {
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

// loadRows loads the rows returned from the database onto the snapshotter
// or returns an error.
// * It returns nil if no error was detected.
// * rows.Close and rows.Err are called at Teardown.
func (s *Snapshotter) loadRows(ctx context.Context) error {
	query, args, err := psql.Select(s.columns...).From(s.table).ToSql()
	if err != nil {
		return fmt.Errorf("failed to create read query: %w", err)
	}
	//nolint:sqlclosecheck,rowserrcheck //both are called at teardown
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
		vals[i] = scannerValue(pgtype.OID(colTypes[i].DataTypeOID))
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

type ScannerValue interface {
	pgtype.Value
	sql.Scanner
}

func scannerValue(oid pgtype.OID) ScannerValue {
	switch oid {
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.ByteaOID:
		return &pgtype.Bytea{}
	case pgtype.NameOID:
		return &pgtype.Name{}
	case pgtype.Int8OID:
		return &pgtype.Int8{}
	case pgtype.Int2OID:
		return &pgtype.Int2{}
	case pgtype.Int4OID:
		return &pgtype.Int4{}
	case pgtype.TextOID:
		return &pgtype.Text{}
	case pgtype.TIDOID:
		return &pgtype.TID{}
	case pgtype.XIDOID:
		return &pgtype.XID{}
	case pgtype.CIDOID:
		return &pgtype.CID{}
	case pgtype.JSONOID:
		return &pgtype.JSON{}
	case pgtype.PointOID:
		return &pgtype.Point{}
	case pgtype.LsegOID:
		return &pgtype.Lseg{}
	case pgtype.PathOID:
		return &pgtype.Path{}
	case pgtype.BoxOID:
		return &pgtype.Box{}
	case pgtype.PolygonOID:
		return &pgtype.Polygon{}
	case pgtype.LineOID:
		return &pgtype.Line{}
	case pgtype.CIDRArrayOID:
		return &pgtype.CIDRArray{}
	case pgtype.Float4OID:
		return &pgtype.Float4{}
	case pgtype.Float8OID:
		return &pgtype.Float8{}
	case pgtype.CircleOID:
		return &pgtype.Circle{}
	case pgtype.UnknownOID:
		return &pgtype.Unknown{}
	case pgtype.MacaddrOID:
		return &pgtype.Macaddr{}
	case pgtype.InetOID:
		return &pgtype.Inet{}
	case pgtype.BoolArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.Int2ArrayOID:
		return &pgtype.Int2Array{}
	case pgtype.Int4ArrayOID:
		return &pgtype.Int4Array{}
	case pgtype.TextArrayOID:
		return &pgtype.TextArray{}
	case pgtype.ByteaArrayOID:
		return &pgtype.ByteaArray{}
	case pgtype.BPCharArrayOID:
		return &pgtype.BPCharArray{}
	case pgtype.VarcharArrayOID:
		return &pgtype.VarcharArray{}
	case pgtype.Int8ArrayOID:
		return &pgtype.Int8Array{}
	case pgtype.Float4ArrayOID:
		return &pgtype.Float4Array{}
	case pgtype.Float8ArrayOID:
		return &pgtype.Float8Array{}
	case pgtype.ACLItemOID:
		return &pgtype.ACLItem{}
	case pgtype.ACLItemArrayOID:
		return &pgtype.ACLItemArray{}
	case pgtype.InetArrayOID:
		return &pgtype.InetArray{}
	case pgtype.BPCharOID:
		return &pgtype.BPChar{}
	case pgtype.VarcharOID:
		return &pgtype.Varchar{}
	case pgtype.DateOID:
		return &pgtype.Date{}
	case pgtype.TimeOID:
		return &pgtype.Time{}
	case pgtype.TimestampOID:
		return &pgtype.Timestamp{}
	case pgtype.TimestampArrayOID:
		return &pgtype.TimestampArray{}
	case pgtype.DateArrayOID:
		return &pgtype.DateArray{}
	case pgtype.TimestamptzOID:
		return &pgtype.Timestamptz{}
	case pgtype.TimestamptzArrayOID:
		return &pgtype.TimestamptzArray{}
	case pgtype.IntervalOID:
		return &pgtype.Interval{}
	case pgtype.NumericArrayOID:
		return &pgtype.NumericArray{}
	case pgtype.BitOID:
		return &pgtype.Bit{}
	case pgtype.VarbitOID:
		return &pgtype.Varbit{}
	case pgtype.NumericOID:
		return &pgtype.Numeric{}
	case pgtype.UUIDOID:
		return &pgtype.UUID{}
	case pgtype.UUIDArrayOID:
		return &pgtype.UUIDArray{}
	case pgtype.JSONBOID:
		return &pgtype.JSONB{}
	case pgtype.JSONBArrayOID:
		return &pgtype.JSONBArray{}
	case pgtype.DaterangeOID:
		return &pgtype.Daterange{}
	case pgtype.Int4rangeOID:
		return &pgtype.Int4range{}
	case pgtype.NumrangeOID:
		return &pgtype.Numrange{}
	case pgtype.TsrangeOID:
		return &pgtype.Tsrange{}
	case pgtype.TsrangeArrayOID:
		return &pgtype.TsrangeArray{}
	case pgtype.TstzrangeOID:
		return &pgtype.Tstzrange{}
	case pgtype.TstzrangeArrayOID:
		return &pgtype.TstzrangeArray{}
	case pgtype.Int8rangeOID:
		return &pgtype.Int8range{}
	case pgtype.CIDROID:
		// pgtype.CIDROID does not implement the Scanner interface
		return &pgtype.Unknown{}
	case pgtype.QCharOID:
		// Not all possible values of QChar are representable in the text format
		return &pgtype.Unknown{}
	case pgtype.OIDOID:
		// pgtype.OID does not implement the value interface
		return &pgtype.Unknown{}
	case pgtype.RecordOID:
		// The text format output format for Records does not include type
		// information and is therefore impossible to decode
		return &pgtype.Unknown{}
	default:
		return &pgtype.Unknown{}
	}
}

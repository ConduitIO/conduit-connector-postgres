// Copyright © 2026 Meroxa, Inc.
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

package types

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ErrorCodeTemporalNotRepresentable prefixes the error returned when a date or
// timestamp value (infinity, or a year beyond what Avro microseconds can hold)
// has to be emitted under an Avro schema, whose logical types cannot hold it.
// The code is at the start of the error text; callers that wrap the error with
// %w put their own prefix before it.
const ErrorCodeTemporalNotRepresentable = "postgres.source.temporal_not_representable"

// TemporalNotRepresentableError reports a date, timestamp or timestamptz value
// that the Avro schema cannot represent. Value is the Postgres text form.
// Table and Column are filled in by the caller that knows them, see
// LocateTemporal.
type TemporalNotRepresentableError struct {
	Table  string
	Column string
	Type   string
	Value  string
	// Snapshot is set for a value read by the snapshot, where changing the row
	// helps. A CDC change is already in the WAL and is read again after every
	// restart.
	Snapshot bool
}

func (e *TemporalNotRepresentableError) Error() string {
	where := ""
	switch {
	case e.Table != "" && e.Column != "":
		where = fmt.Sprintf(" in column %q of table %q", e.Column, e.Table)
	case e.Column != "":
		where = fmt.Sprintf(" in column %q", e.Column)
	}
	advice := "Removing the row does not help a change that is already in the " +
		"WAL: it is read again after every restart"
	if e.Snapshot {
		advice = "Changing or deleting the row also helps, because the snapshot reads the table"
	}
	return fmt.Sprintf("%s: %s value %q%s cannot be encoded in an Avro schema, "+
		"so the source halts instead of changing it. Fix: set logrepl.withAvroSchema to false "+
		"to receive it as text. %s",
		ErrorCodeTemporalNotRepresentable, e.Type, e.Value, where, advice)
}

// LocateTemporal returns a copy of err with its table and column set, and
// true, when err is or wraps a *TemporalNotRepresentableError. The copy is
// returned unwrapped so the error code stays at the start of the message. For
// any other error it returns err and false.
//
// The error is for a CDC change; LocateTemporalSnapshot is the snapshot form.
func LocateTemporal(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	var tne *TemporalNotRepresentableError
	if !errors.As(err, &tne) {
		return err, false
	}
	located := *tne
	located.Table, located.Column = table, column
	return &located, true
}

type temporalKind int

const (
	kindTimestamp temporalKind = iota
	kindTimestamptz
	kindDate
)

func (k temporalKind) String() string {
	switch k {
	case kindTimestamp:
		return "timestamp"
	case kindTimestamptz:
		return "timestamptz"
	default:
		return "date"
	}
}

var (
	temporalKinds = map[uint32]temporalKind{
		pgtype.TimestampOID:   kindTimestamp,
		pgtype.TimestamptzOID: kindTimestamptz,
		pgtype.DateOID:        kindDate,
	}
	temporalArrayKinds = map[uint32]temporalKind{
		pgtype.TimestampArrayOID:   kindTimestamp,
		pgtype.TimestamptzArrayOID: kindTimestamptz,
		pgtype.DateArrayOID:        kindDate,
	}
)

const (
	// Years time.Time.MarshalJSON accepts. Record.Bytes() in conduit-commons
	// panics outside of it, so values outside are emitted as text instead.
	minNativeYear = 0
	maxNativeYear = 9999

	infinityText         = "infinity"
	negativeInfinityText = "-infinity"
)

// formatTemporal normalizes one timestamp, timestamptz or date value.
func formatTemporal(enc Encoding, kind temporalKind, v any) (any, error) {
	var t time.Time
	switch x := v.(type) {
	case nil:
		return nil, nil
	case time.Time:
		t = x
	case pgtype.InfinityModifier:
		return formatInfinity(enc, kind, x)
	default:
		return nil, unexpectedType(kind.String(), v)
	}

	// Wall-clock types have no zone. Whatever location the decoder labeled the
	// value with (pgx has used both UTC and time.Local, depending on version,
	// format and type), its wall clock is the value; instants are not. Relabel
	// the wall clock as UTC instead of converting, which would shift it by the
	// host offset. timestamptz is an instant, so converting is right there.
	if kind == kindTimestamptz {
		t = t.UTC()
	} else {
		t = wallClockUTC(t)
	}

	if enc == EncodingAvro {
		return formatTemporalAvro(kind, t)
	}

	if y := t.Year(); y < minNativeYear || y > maxNativeYear {
		return pgText(kind, t), nil
	}
	return t, nil
}

func formatInfinity(enc Encoding, kind temporalKind, m pgtype.InfinityModifier) (any, error) {
	if enc == EncodingAvro {
		// Avro's date and timestamp logical types have no infinity. The Avro
		// encoder rejects pgx's InfinityModifier with an error that offers no
		// way out; fail here with one instead of inventing a sentinel value
		// (Invariant 6: no silent coercion). Before this, date infinity under
		// Avro was written as a day number (1970-01-02 / 1969-12-31).
		return nil, &TemporalNotRepresentableError{Type: kind.String(), Value: m.String()}
	}
	switch m {
	case pgtype.Infinity:
		return infinityText, nil
	case pgtype.NegativeInfinity:
		return negativeInfinityText, nil
	default:
		return nil, fmt.Errorf("unexpected %s infinity modifier %d", kind, int8(m))
	}
}

func wallClockUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

// formatTemporalAvro prepares a value for the SDK's Avro encoder.
//
// timestamp has the Avro type local-timestamp-micros. The Avro library encodes
// a time.Time for that type by converting it to time.Local first and writing
// the local wall clock, so a UTC-labeled wall clock is shifted by the host
// offset (#302), and a wall clock that does not exist in the host zone (a DST
// gap) cannot be written correctly at all. A plain int64 is written as is, so
// we compute the microseconds of the wall clock ourselves. The Avro-encoded
// record never leaves the connector process as a Go value, so the different Go
// type is not visible to anything else.
//
// timestamptz (timestamp-micros) and date are encoded from t.Unix(), which does
// not depend on the zone, so they stay time.Time.
func formatTemporalAvro(kind temporalKind, t time.Time) (any, error) {
	switch kind {
	case kindTimestamp:
		return wallClockMicros(t)
	case kindTimestamptz:
		if _, err := wallClockMicros(t); err != nil {
			return nil, err
		}
		return t, nil
	default:
		return t, nil
	}
}

// wallClockMicros returns the microseconds between 1970-01-01T00:00:00 and the
// wall clock of t, which must be labeled UTC.
func wallClockMicros(t time.Time) (int64, error) {
	// One second of headroom: sec*1e6 plus up to 999999 microseconds must fit.
	const maxSec = math.MaxInt64/1_000_000 - 1
	sec := t.Unix()
	if sec > maxSec || sec < -maxSec {
		return 0, &TemporalNotRepresentableError{Type: "timestamp", Value: pgText(kindTimestamp, t)}
	}
	return sec*1e6 + int64(t.Nanosecond())/1e3, nil
}

// pgText renders t the way Postgres prints it, including the BC suffix and
// years above 9999, which time.Time.Format cannot do.
func pgText(kind temporalKind, t time.Time) string {
	year := t.Year()
	bc := year <= 0
	if bc {
		year = 1 - year // Go counts 1 BC as year 0
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%04d-%02d-%02d", year, int(t.Month()), t.Day())
	if kind != kindDate {
		fmt.Fprintf(&b, " %02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
		if us := t.Nanosecond() / 1000; us != 0 {
			b.WriteByte('.')
			b.WriteString(strings.TrimRight(fmt.Sprintf("%06d", us), "0"))
		}
	}
	if kind == kindTimestamptz {
		b.WriteString("+00")
	}
	if bc {
		b.WriteString(" BC")
	}
	return b.String()
}

func formatTemporalArray(enc Encoding, kind temporalKind, v any) (any, error) {
	return mapArray(v, kind.String()+"[]", func(e any) (any, error) {
		return formatTemporal(enc, kind, e)
	})
}

// mapArray applies f to every element of an array decoded by pgx. A NULL
// element stays nil. pgx flattens multi-dimensional arrays into one slice (the
// dimensions are lost, #377), so there is no nesting to recurse into.
func mapArray(v any, name string, f func(any) (any, error)) (any, error) {
	switch a := v.(type) {
	case nil:
		return nil, nil
	case []any:
		out := make([]any, len(a))
		for i, e := range a {
			r, err := f(e)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out[i] = r
		}
		return out, nil
	default:
		return nil, unexpectedType(name, v)
	}
}

// Range types over the temporal types. pgx decodes a range to
// pgtype.Range[any] and a multirange to pgtype.Multirange[pgtype.Range[any]],
// whose bounds are the same time.Time or InfinityModifier values as the
// scalars, so they get the same host-zone dependence (tstzrange bounds in
// time.Local since pgx 5.11 over CDC).
var (
	rangeKinds = map[uint32]temporalKind{
		pgtype.TsrangeOID:        kindTimestamp,
		pgtype.TstzrangeOID:      kindTimestamptz,
		pgtype.DaterangeOID:      kindDate,
		pgtype.TsmultirangeOID:   kindTimestamp,
		pgtype.TstzmultirangeOID: kindTimestamptz,
		pgtype.DatemultirangeOID: kindDate,
	}
	// Arrays of multiranges (OIDs 6152, 6153, 6155) are left out on purpose: pgx
	// 5.11 does not register those types, so their values arrive as the
	// Postgres text and pass through unchanged, as they do on main.
	rangeArrayKinds = map[uint32]temporalKind{
		pgtype.TsrangeArrayOID:   kindTimestamp,
		pgtype.TstzrangeArrayOID: kindTimestamptz,
		pgtype.DaterangeArrayOID: kindDate,
	}
)

// formatRange normalizes the bounds of a range or multirange. Ranges cannot be
// Avro-encoded (the schema extractor has no range type), so the bounds always
// get the native form. Unbounded and empty ranges carry no bound value and are
// returned as they are.
func formatRange(kind temporalKind, v any) (any, error) {
	switch r := v.(type) {
	case nil:
		return nil, nil
	case pgtype.Range[any]:
		return formatRangeBounds(kind, r)
	case pgtype.Multirange[pgtype.Range[any]]:
		out := make(pgtype.Multirange[pgtype.Range[any]], len(r))
		for i := range r {
			b, err := formatRangeBounds(kind, r[i])
			if err != nil {
				return nil, fmt.Errorf("range %d: %w", i, err)
			}
			out[i] = b
		}
		return out, nil
	default:
		return nil, unexpectedType(kind.String()+" range", v)
	}
}

func formatRangeBounds(kind temporalKind, r pgtype.Range[any]) (pgtype.Range[any], error) {
	var err error
	if r.Lower != nil {
		if r.Lower, err = formatTemporal(EncodingNative, kind, r.Lower); err != nil {
			return r, fmt.Errorf("lower bound: %w", err)
		}
	}
	if r.Upper != nil {
		if r.Upper, err = formatTemporal(EncodingNative, kind, r.Upper); err != nil {
			return r, fmt.Errorf("upper bound: %w", err)
		}
	}
	return r, nil
}

func formatRangeArray(kind temporalKind, v any) (any, error) {
	return mapArray(v, kind.String()+" range[]", func(e any) (any, error) {
		return formatRange(kind, e)
	})
}

// LocateTemporalSnapshot is LocateTemporal for a value read by the snapshot, whose
// error advice differs: the row can be fixed.
func LocateTemporalSnapshot(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	located, ok := LocateTemporal(err, table, column)
	if ok {
		located.(*TemporalNotRepresentableError).Snapshot = true
	}
	return located, ok
}

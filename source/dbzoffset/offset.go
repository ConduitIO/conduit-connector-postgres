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

package dbzoffset

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
)

// Debezium offset keys (PostgresOffsetContext, SourceInfo).
const (
	keyLSN                = "lsn"
	keyLSNCommit          = "lsn_commit"
	keyLSNProc            = "lsn_proc"
	keyTxID               = "txId"
	keyTsUsec             = "ts_usec"
	keyMessageType        = "messageType"
	keyLSNEventsProcessed = "lsn_events_processed"
	keySnapshot           = "snapshot"
	keyLastSnapshotRecord = "last_snapshot_record"
	keySnapshotCompleted  = "snapshot_completed"
	// prefixIncrementalSnapshot prefixes the keys Debezium's incremental
	// snapshot context stores, and stores only while one runs.
	prefixIncrementalSnapshot = "incremental_snapshot_"
)

// MessageType is Debezium's `messageType` offset field: the kind of the last
// message Debezium processed (ReplicationMessage.Operation).
type MessageType string

// The message types the resume rule distinguishes. Debezium writes others
// (RELATION, NOOP, ...); every value not listed here is treated like an
// absent type: base rule only, the offset's transaction delivered in full.
const (
	MessageInsert   MessageType = "INSERT"
	MessageUpdate   MessageType = "UPDATE"
	MessageDelete   MessageType = "DELETE"
	MessageTruncate MessageType = "TRUNCATE"
	MessageMessage  MessageType = "MESSAGE"
	MessageBegin    MessageType = "BEGIN"
	MessageCommit   MessageType = "COMMIT"
)

// IsDML reports whether t is a row change (INSERT, UPDATE, DELETE,
// TRUNCATE), the only types that arm the partial rule (D4).
func (t MessageType) IsDML() bool {
	switch t {
	case MessageInsert, MessageUpdate, MessageDelete, MessageTruncate:
		return true
	default:
		return false
	}
}

// Offset is a parsed Debezium Postgres offset. LSN fields use zero for
// "absent": zero is InvalidXLogRecPtr in Postgres, never a real WAL position.
type Offset struct {
	// LSN is `lsn`: the LSN of the last message Debezium processed. For a
	// change it is the change's WAL start; for COMMIT it is the end of the
	// commit record. Zero when absent.
	LSN pglogrepl.LSN
	// LSNCommit is `lsn_commit` (Lc): the end of the last commit record
	// Debezium processed. ParseOffset guarantees it is non-zero.
	LSNCommit pglogrepl.LSN
	// LSNProc is `lsn_proc`, shown in the dry run only. Zero when absent.
	LSNProc pglogrepl.LSN
	// TxID is `txId` (X), normalised to 32 bits: older Debezium read the
	// decoderbufs xid as a signed int, so a negative value is taken as
	// txId + 2^32. Valid only when HasTxID.
	TxID    uint32
	HasTxID bool
	// TsUsec is `ts_usec`, the commit time of the offset's transaction in
	// microseconds since the Unix epoch. Valid only when HasTsUsec.
	TsUsec    int64
	HasTsUsec bool
	// MessageType is `messageType` (M). Empty when absent (older 1.x).
	MessageType MessageType
	// LSNEventsProcessed is `lsn_events_processed` (n). It is parsed and
	// shown, and never used to skip anything (B2). Zero when absent.
	LSNEventsProcessed uint64
}

// IsNonTransactionalMessage reports whether the offset was written for a
// non-transactional logical decoding message: Debezium's commitMessage sets
// lsn_commit to the message's own LSN, so M = MESSAGE with lsn == lsn_commit
// (S1). Such an offset is commit-class.
func (o Offset) IsNonTransactionalMessage() bool {
	return o.MessageType == MessageMessage && o.LSN == o.LSNCommit
}

// ParseOffset parses Debezium's offset map, as the JSON object Kafka Connect
// stores (the `offset` of `GET /connectors/{name}/offsets`, or a
// connect-offsets record value). It runs the offset-only checks in the order
// of S5:
//
//  1. Parse: an object with no duplicate keys; every LSN an unsigned 64-bit
//     integer; txId within [-2^31, 2^32), normalised to uint32; ts_usec a
//     signed 64-bit integer; messageType a string; the snapshot flags
//     booleans. A failure is CodeOffsetInvalid.
//  2. Any incremental_snapshot_* key: CodeIncrementalSnapshotInProgress.
//  3. A `snapshot` key (any value but JSON false or null), or
//     last_snapshot_record / snapshot_completed true with no lsn_commit:
//     CodeSnapshotInProgress.
//  4. No lsn_commit: CodeOffsetUnsupported. A zero lsn_commit is
//     CodeOffsetInvalid.
//
// Keys it does not know (xmin, transaction_*, keys a later Debezium adds)
// are ignored: none of them changes where the stream resumes.
//
// Numbers are decoded as json.Number and parsed as integers, never through
// float64, which cannot hold every LSN.
func ParseOffset(raw []byte) (Offset, error) {
	return parseOffset(raw, SettingPath+".offset")
}

func parseOffset(raw []byte, path string) (Offset, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return Offset{}, newError(CodeOffsetInvalid, path, fixReexport,
			"the Debezium offset is not a JSON object: %v", err)
	}
	// 1. Parse every field the rule reads, before any semantic check, so a
	// malformed offset is always reported as malformed.
	o, flags, err := parseFields(fields, path)
	if err != nil {
		return Offset{}, err
	}
	if err := checkState(fields, flags, o, path); err != nil {
		return Offset{}, err
	}
	return o, nil
}

// stateFlags are the parsed fields that only the state checks read.
type stateFlags struct {
	lsnCommitPresent   bool
	lastSnapshotRecord bool
	snapshotCompleted  bool
}

func parseFields(fields map[string]json.RawMessage, path string) (Offset, stateFlags, error) {
	var o Offset
	var flags stateFlags
	invalid := func(key, format string, args ...any) error {
		return newError(CodeOffsetInvalid, path+"."+key, fixReexport, format, args...)
	}
	present := func(key string) (json.RawMessage, bool) {
		v, ok := fields[key]
		return v, ok && !isNull(v)
	}

	for _, f := range []struct {
		key string
		dst *pglogrepl.LSN
	}{
		{keyLSN, &o.LSN},
		{keyLSNCommit, &o.LSNCommit},
		{keyLSNProc, &o.LSNProc},
	} {
		v, ok := present(f.key)
		if !ok {
			continue
		}
		u, err := parseUint64(v)
		if err != nil {
			return Offset{}, flags, invalid(f.key, "%q is not an LSN (an unsigned 64-bit integer): %v", f.key, err)
		}
		*f.dst = pglogrepl.LSN(u)
		if f.key == keyLSNCommit {
			flags.lsnCommitPresent = true
		}
	}

	if v, ok := present(keyTxID); ok {
		x, err := parseInt64(v)
		if err != nil {
			return Offset{}, flags, invalid(keyTxID, "%q is not an integer: %v", keyTxID, err)
		}
		xid, err := normaliseXid(x)
		if err != nil {
			return Offset{}, flags, invalid(keyTxID, "%v", err)
		}
		o.TxID, o.HasTxID = xid, true
	}

	if v, ok := present(keyTsUsec); ok {
		ts, err := parseInt64(v)
		if err != nil {
			return Offset{}, flags, invalid(keyTsUsec, "%q is not an integer: %v", keyTsUsec, err)
		}
		o.TsUsec, o.HasTsUsec = ts, true
	}

	if v, ok := present(keyMessageType); ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return Offset{}, flags, invalid(keyMessageType, "%q is not a string", keyMessageType)
		}
		o.MessageType = MessageType(s)
	}

	if v, ok := present(keyLSNEventsProcessed); ok {
		n, err := parseUint64(v)
		if err != nil {
			return Offset{}, flags, invalid(keyLSNEventsProcessed, "%q is not an unsigned integer: %v", keyLSNEventsProcessed, err)
		}
		o.LSNEventsProcessed = n
	}

	var err error
	if flags.lastSnapshotRecord, err = optionalBool(fields, keyLastSnapshotRecord); err != nil {
		return Offset{}, flags, invalid(keyLastSnapshotRecord, "%v", err)
	}
	if flags.snapshotCompleted, err = optionalBool(fields, keySnapshotCompleted); err != nil {
		return Offset{}, flags, invalid(keySnapshotCompleted, "%v", err)
	}
	return o, flags, nil
}

// checkState runs S5 checks 2 to 4 on a parsed offset.
func checkState(fields map[string]json.RawMessage, flags stateFlags, o Offset, path string) error {
	// 2. A running incremental snapshot gets its own code, even when the
	// offset also lacks lsn_commit (S5). Debezium stores these keys only
	// while the snapshot runs (AbstractIncrementalSnapshotContext.store).
	// Keys are checked in sorted order so the reported key is stable.
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.HasPrefix(key, prefixIncrementalSnapshot) && !isNull(fields[key]) {
			return newError(CodeIncrementalSnapshotInProgress, path+"."+key, fixIncrementalSnapshot,
				"the offset was committed while a Debezium incremental snapshot was running (%q is set)", key)
		}
	}

	// 3. Snapshot state (D7). Debezium writes `snapshot` only while a
	// snapshot runs (true in older versions, the snapshot type in newer
	// ones), so any value but false/null is a snapshot offset. The boundary
	// between Debezium's snapshot and its stream is Debezium-internal, so
	// such offsets are refused rather than interpreted.
	if v, ok := fields[keySnapshot]; ok && !isNull(v) && !isFalse(v) {
		return newError(CodeSnapshotInProgress, path+"."+keySnapshot, fixSnapshotRunning,
			"the offset was committed during a Debezium snapshot (%s = %s)", keySnapshot, compact(v))
	}
	if (flags.lastSnapshotRecord || flags.snapshotCompleted) && !flags.lsnCommitPresent {
		return newError(CodeSnapshotInProgress, path, fixSnapshotNoStream,
			"the Debezium snapshot completed, but no streaming offset with %q was committed after it", keyLSNCommit)
	}

	// 4. lsn_commit is what the base rule compares against.
	if !flags.lsnCommitPresent {
		return newError(CodeOffsetUnsupported, path+"."+keyLSNCommit, fixOffsetUnsupported,
			"the offset has no %q", keyLSNCommit)
	}
	if o.LSNCommit == 0 {
		return newError(CodeOffsetInvalid, path+"."+keyLSNCommit, fixReexport,
			"%q is 0, which is not a WAL position", keyLSNCommit)
	}
	return nil
}

// normaliseXid maps Debezium's txId to a 32-bit xid (S2, 3rd pass). Older
// Debezium read the decoderbufs xid as a signed int, so values in
// [-2^31, 0) stand for txId + 2^32. Anything outside [-2^31, 2^32) is not an
// xid.
func normaliseXid(x int64) (uint32, error) {
	switch {
	case x >= 0 && x <= math.MaxUint32:
		return uint32(x), nil
	case x < 0 && x >= math.MinInt32:
		return uint32(x + 1<<32), nil
	default:
		return 0, fmt.Errorf("%q = %d is outside [-2^31, 2^32), so it is not a Postgres xid", keyTxID, x)
	}
}

// decodeObject decodes a JSON object into its raw member values, refusing
// duplicate keys (encoding/json would silently keep the last one, and an
// offset with two different lsn_commit values cannot be interpreted) and
// trailing data.
func decodeObject(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("expected an object, got %v", tok)
	}
	fields := make(map[string]json.RawMessage)
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("expected a key, got %v", tok)
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("value of %q: %w", key, err)
		}
		if _, dup := fields[key]; dup {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		fields[key] = v
	}
	if _, err := dec.Token(); err != nil { // the closing '}'
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the object")
	}
	return fields, nil
}

func isNull(v json.RawMessage) bool  { return bytes.Equal(bytes.TrimSpace(v), []byte("null")) }
func isFalse(v json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(v), []byte("false")) }

func compact(v json.RawMessage) string {
	var b bytes.Buffer
	if err := json.Compact(&b, v); err != nil {
		return string(v)
	}
	return b.String()
}

// integerLiteral returns v's text when v is a JSON number written as an
// integer (no fraction, no exponent). Debezium writes every numeric offset
// field as a Java long, so anything else is malformed.
func integerLiteral(v json.RawMessage) (string, error) {
	s := string(bytes.TrimSpace(v))
	if s == "" || strings.ContainsAny(s, ".eE") {
		return "", fmt.Errorf("%s is not an integer", s)
	}
	if s[0] != '-' && (s[0] < '0' || s[0] > '9') {
		return "", fmt.Errorf("%s is not a number", s)
	}
	return s, nil
}

func parseUint64(v json.RawMessage) (uint64, error) {
	s, err := integerLiteral(v)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(s, 10, 64)
}

func parseInt64(v json.RawMessage) (int64, error) {
	s, err := integerLiteral(v)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func optionalBool(fields map[string]json.RawMessage, key string) (bool, error) {
	v, ok := fields[key]
	if !ok || isNull(v) {
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(v, &b); err != nil {
		return false, fmt.Errorf("%q is not a boolean: %s", key, compact(v))
	}
	return b, nil
}

// checkStrictJSON reports an error when raw is not exactly one JSON value
// with no duplicate object keys at any depth.
func checkStrictJSON(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := walkJSON(dec); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after the JSON value")
	}
	return nil
}

// walkJSON consumes one value from dec, checking every object for
// duplicate keys.
func walkJSON(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil // a scalar
	}
	switch d {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := kt.(string)
			if !ok {
				return fmt.Errorf("expected a key, got %v", kt)
			}
			if seen[key] {
				return fmt.Errorf("duplicate key %q", key)
			}
			seen[key] = true
			if err := walkJSON(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := walkJSON(dec); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected %v", d)
	}
	_, err = dec.Token() // the closing delimiter
	return err
}

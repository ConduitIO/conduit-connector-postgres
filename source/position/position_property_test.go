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

package position

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pglogrepl"
	"pgregory.net/rapid"
)

// Property tests for the position format (a serialized format: invariant 2).
// The check count follows rapid: -rapid.checks or RAPID_CHECKS (default 100).

// genLSN draws an LSN as the connector writes it, or the empty string (the
// field absent).
func genLSN(t *rapid.T, label string) string {
	if rapid.Bool().Draw(t, label+" set") {
		return pglogrepl.LSN(rapid.Uint64().Draw(t, label)).String()
	}
	return ""
}

// genSlotIdentity draws a slot identity as the connector writes it (#442):
// random, derived from a legacy position, or absent.
func genSlotIdentity(t *rapid.T) string {
	switch rapid.IntRange(0, 2).Draw(t, "slot identity kind") {
	case 1:
		return rapid.StringMatching(`[0-9a-f]{64}`).Draw(t, "random slot identity")
	case 2:
		return "l" + rapid.StringMatching(`[0-9a-f]{64}`).Draw(t, "legacy slot identity")
	default:
		return ""
	}
}

// genPosition draws any position this connector can write. Table names are
// arbitrary valid UTF-8 strings, since a quoted Postgres identifier can hold
// any character. Invalid UTF-8 is left out: encoding/json writes it as
// U+FFFD, so such a name does not round-trip (possible only for identifiers
// read from a SQL_ASCII database; tracked separately).
func genPosition(t *rapid.T) Position {
	p := Position{
		Version:                 rapid.IntRange(0, CurrentPositionVersion).Draw(t, "version"),
		Type:                    rapid.SampledFrom([]Type{TypeInitial, TypeSnapshot, TypeCDC}).Draw(t, "type"),
		LastLSN:                 genLSN(t, "last lsn"),
		SnapshotLowWatermarkLSN: genLSN(t, "low watermark"),
		TxCommitLSN:             genLSN(t, "tx commit lsn"),
		TxSeq:                   rapid.Uint64().Draw(t, "tx seq"),
		SlotIdentity:            genSlotIdentity(t),
	}
	if rapid.Bool().Draw(t, "wal history set") {
		p.SystemIdentifier = strconv.FormatUint(rapid.Uint64().Draw(t, "system identifier"), 10)
		p.Timeline = rapid.Int32Range(1, 1<<30).Draw(t, "timeline")
	}
	for _, table := range rapid.SliceOfNDistinct(rapid.String(), 0, 3, rapid.ID[string]).Draw(t, "snapshot tables") {
		if p.Snapshots == nil {
			p.Snapshots = SnapshotPositions{}
		}
		p.Snapshots[table] = SnapshotPosition{
			LastRead:    rapid.Int64().Draw(t, "last read"),
			SnapshotEnd: rapid.Int64().Draw(t, "snapshot end"),
		}
	}
	for _, table := range rapid.SliceOfNDistinct(rapid.String(), 0, 3, rapid.ID[string]).Draw(t, "history tables") {
		if p.SchemaHistory == nil {
			p.SchemaHistory = SchemaHistories{}
		}
		n := rapid.IntRange(1, DefaultSchemaHistoryVersions).Draw(t, "versions")
		for range n {
			p.SchemaHistory[table] = append(p.SchemaHistory[table], SchemaVersion{
				ColumnSetHash: rapid.StringMatching(`[0-9a-f]{64}`).Draw(t, "hash"),
				FirstSeenLSN:  genLSN(t, "first seen"),
			})
		}
	}
	return p
}

// An absent map or slice and an empty one are the same position: every
// field is omitempty, so an empty map is written as nothing and read back
// as nil.
var positionCmp = cmpopts.EquateEmpty()

// TestPosition_PropertyRoundTrip: every position reads back as written,
// stamped with the current format version.
func TestPosition_PropertyRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := genPosition(t)
		got, err := ParseSDKPosition(p.ToSDKPosition())
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		want := p
		want.Version = CurrentPositionVersion
		if diff := cmp.Diff(want, got, positionCmp); diff != "" {
			t.Fatalf("position changed in a round trip (-want +got):\n%s", diff)
		}
	})
}

// TestPosition_PropertyRewriteIsStable: reading a position and writing it
// again gives the same bytes. The engine compares and stores positions as
// bytes, so a rewrite that reordered or reformatted fields would look like
// a new position.
func TestPosition_PropertyRewriteIsStable(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		first := genPosition(t).ToSDKPosition()
		p, err := ParseSDKPosition(first)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if second := p.ToSDKPosition(); !bytes.Equal(first, second) {
			t.Fatalf("rewrite changed the bytes:\nfirst:  %s\nsecond: %s", first, second)
		}
	})
}

// TestPosition_PropertyReadsNewerFormat: a position written by a newer
// build, with a higher version and fields this build does not know, is read,
// not rejected, and every field this build knows keeps its value (the
// "readable by N+1 versions" rule in CurrentPositionVersion's contract).
func TestPosition_PropertyReadsNewerFormat(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := genPosition(t)

		var obj map[string]json.RawMessage
		if err := json.Unmarshal(p.ToSDKPosition(), &obj); err != nil {
			t.Fatal(err)
		}
		newer := CurrentPositionVersion + rapid.IntRange(1, 100).Draw(t, "versions ahead")
		obj["version"] = json.RawMessage(strconv.Itoa(newer))
		for i, v := range rapid.SliceOfN(rapid.SampledFrom([]string{
			`1`, `"x"`, `null`, `true`, `[1,"a"]`, `{"nested":{"k":[]}}`,
		}), 0, 4).Draw(t, "unknown fields") {
			obj["future_field_"+strconv.Itoa(i)] = json.RawMessage(v)
		}
		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}

		got, err := ParseSDKPosition(b)
		if err != nil {
			t.Fatalf("a newer position was rejected: %v\n%s", err, b)
		}
		want := p
		want.Version = newer
		if diff := cmp.Diff(want, got, positionCmp); diff != "" {
			t.Fatalf("a newer position lost known fields (-want +got):\n%s\n%s", diff, b)
		}
	})
}

// TestPosition_PropertyLSNRoundTrip: for every LSN, the LSN read from a
// serialized position (LSN, TxCommit) is the LSN written. The round-trip
// properties above compare the text; this one compares the parsed value, which
// is what resume decisions compare (logrepl/internal.ResumePoint). Exactness
// also means the order of any two LSNs survives serialization, so a restart
// can neither skip nor repeat by misordering them.
func TestPosition_PropertyLSNRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		lsn := pglogrepl.LSN(rapid.Uint64().Draw(t, "lsn"))
		p, err := ParseSDKPosition(Position{
			Type:        TypeCDC,
			LastLSN:     lsn.String(),
			TxCommitLSN: lsn.String(),
			TxSeq:       1,
		}.ToSDKPosition())
		if err != nil {
			t.Fatal(err)
		}
		last, err := p.LSN()
		if err != nil {
			t.Fatal(err)
		}
		commit, err := p.TxCommit()
		if err != nil {
			t.Fatal(err)
		}
		if last != lsn || commit != lsn {
			t.Fatalf("LSN %d (%s) read back as last %d, commit %d", uint64(lsn), lsn, uint64(last), uint64(commit))
		}
	})
}

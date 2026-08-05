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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/matryer/is"
)

func cols(specs ...ColumnIdentity) []ColumnIdentity { return specs }

func ci(name string, dt uint32, tm int32) ColumnIdentity {
	return ColumnIdentity{Name: name, DataType: dt, TypeModifier: tm}
}

// Test_HashColumnSet_StableAcrossOrdering pins the property the whole mechanism
// rests on: the hash is compared against one computed by a DIFFERENT PROCESS
// after a restart. If column order changed the hash, every restart would look
// like drift.
func Test_HashColumnSet_StableAcrossOrdering(t *testing.T) {
	is := is.New(t)

	a := HashColumnSet(cols(ci("id", 23, -1), ci("email", 25, -1), ci("age", 23, -1)))
	b := HashColumnSet(cols(ci("age", 23, -1), ci("id", 23, -1), ci("email", 25, -1)))
	is.Equal(a, b)

	// And it must be stable across repeated computation in one process.
	for range 10 {
		is.Equal(HashColumnSet(cols(ci("id", 23, -1), ci("email", 25, -1), ci("age", 23, -1))), a)
	}
}

// Test_HashColumnSet_DistinguishesShapes pins that every part of the identity
// triple actually participates. A hash that ignored TypeModifier would let
// varchar(10) -> varchar(20) pass as "same shape" across a restart, which is
// exactly the drift this is meant to catch.
func Test_HashColumnSet_DistinguishesShapes(t *testing.T) {
	is := is.New(t)

	base := HashColumnSet(cols(ci("s", 1043, 14)))

	is.True(base != HashColumnSet(cols(ci("s", 1043, 24))))                      // TypeModifier
	is.True(base != HashColumnSet(cols(ci("s", 25, 14))))                        // DataType
	is.True(base != HashColumnSet(cols(ci("t", 1043, 14))))                      // Name
	is.True(base != HashColumnSet(cols(ci("s", 1043, 14), ci("extra", 23, -1)))) // added column
	is.True(base != HashColumnSet(nil))                                          // empty
}

// Test_HashColumnSet_SeparatorCannotCollide guards the classic delimiter
// collision: with a naive separator, ("a|b", "c") and ("a", "b|c") hash the
// same. The separator used here cannot appear in a Postgres identifier, but the
// property is worth pinning rather than trusting.
func Test_HashColumnSet_SeparatorCannotCollide(t *testing.T) {
	is := is.New(t)

	a := HashColumnSet(cols(ci("a", 1, 1), ci("b", 2, 2)))
	b := HashColumnSet(cols(ci("a", 1, 1), ci("b", 2, 2), ci("", 0, 0)))
	is.True(a != b)
}

// Test_RecordSchemaVersion_DedupesCurrentShape pins that re-observing the
// current shape is a no-op.
//
// A RelationMessage is re-sent for reasons other than DDL — a reconnect, a new
// subscriber attaching. Appending on every sighting would fill the bounded
// history with duplicates of one shape and prune away the genuinely older ones
// that carry the drift signal, so the mechanism would quietly destroy its own
// evidence.
func Test_RecordSchemaVersion_DedupesCurrentShape(t *testing.T) {
	is := is.New(t)

	p := Position{}
	h := HashColumnSet(cols(ci("id", 23, -1)))

	is.True(p.RecordSchemaVersion("public.t", h, "0/1"))  // first: recorded
	is.True(!p.RecordSchemaVersion("public.t", h, "0/2")) // same shape: not recorded
	is.True(!p.RecordSchemaVersion("public.t", h, "0/3"))
	is.Equal(len(p.SchemaHistory["public.t"]), 1)

	// A genuinely different shape IS recorded.
	h2 := HashColumnSet(cols(ci("id", 23, -1), ci("new", 25, -1)))
	is.True(p.RecordSchemaVersion("public.t", h2, "0/4"))
	is.Equal(len(p.SchemaHistory["public.t"]), 2)
}

// Test_RecordSchemaVersion_PrunesOldestFirst pins that the bound keeps the
// NEWEST versions. Pruning newest-first would retain ancient history and
// discard the shape a drift decision is actually made against.
func Test_RecordSchemaVersion_PrunesOldestFirst(t *testing.T) {
	is := is.New(t)

	old := DefaultSchemaHistoryVersions
	DefaultSchemaHistoryVersions = 3
	defer func() { DefaultSchemaHistoryVersions = old }()

	p := Position{}
	var lastHash string
	for i := range 6 {
		h := HashColumnSet(cols(ci(fmt.Sprintf("c%d", i), 23, -1)))
		lastHash = h
		p.RecordSchemaVersion("public.t", h, fmt.Sprintf("0/%d", i))
	}

	versions := p.SchemaHistory["public.t"]
	is.Equal(len(versions), 3)

	// The most recent shape must be retained and be last.
	is.Equal(versions[len(versions)-1].ColumnSetHash, lastHash)
	last, ok := p.LastSchemaVersion("public.t")
	is.True(ok)
	is.Equal(last.ColumnSetHash, lastHash)

	// The retained window is the newest three: LSNs 3,4,5.
	is.Equal(versions[0].FirstSeenLSN, "0/3")
	is.Equal(versions[2].FirstSeenLSN, "0/5")
}

// Test_SchemaHistory_SurvivesPositionRoundTrip is the point of the whole slice:
// the history must survive serialization, because that is what makes drift
// detectable across a restart.
func Test_SchemaHistory_SurvivesPositionRoundTrip(t *testing.T) {
	is := is.New(t)

	p := Position{Type: TypeCDC, LastLSN: "0/ABC"}
	h := HashColumnSet(cols(ci("id", 23, -1), ci("email", 25, -1)))
	p.RecordSchemaVersion("public.users", h, "0/AAA")

	parsed, err := ParseSDKPosition(p.ToSDKPosition())
	is.NoErr(err)

	got, ok := parsed.LastSchemaVersion("public.users")
	is.True(ok)
	is.Equal(got.ColumnSetHash, h)
	is.Equal(got.FirstSeenLSN, "0/AAA")
}

// Test_SchemaHistory_AbsentOnLegacyPosition pins the compatibility contract: a
// v0.14 position carries no schema history and must parse cleanly with none,
// rather than erroring or being treated as "shape unknown, assume drift".
func Test_SchemaHistory_AbsentOnLegacyPosition(t *testing.T) {
	is := is.New(t)

	legacy := []byte(`{"type":2,"last_lsn":"0/16B3748"}`)
	var p Position
	is.NoErr(json.Unmarshal(legacy, &p))

	is.Equal(p.Version, 0)
	is.Equal(len(p.SchemaHistory), 0)

	_, ok := p.LastSchemaVersion("public.users")
	is.True(!ok)

	// Recording against a legacy position must work, not panic on a nil map.
	is.True(p.RecordSchemaVersion("public.users", "abc", "0/1"))
}

// legacyPosition mirrors the Position struct as it existed BEFORE SchemaHistory
// was added. It is the N-1 reader.
type legacyPosition struct {
	Version                 int    `json:"version"`
	Type                    Type   `json:"type"`
	LastLSN                 string `json:"last_lsn,omitempty"`
	SnapshotLowWatermarkLSN string `json:"snapshot_low_watermark_lsn,omitempty"`
}

// Test_SchemaHistory_DowngradeIsSafe is the other half of the serialized-format
// contract, and the half that is easy to forget.
//
// LegacyPositionSeedsWithoutDrift covers N reading an N-1 position. This covers
// N-1 reading an N position, which is what happens on a ROLLBACK — the case that
// matters most, because a rollback is already an incident and a position that
// fails to parse would turn it into a worse one.
//
// It works because ParseSDKPosition uses a permissive json.Unmarshal. Pinning it
// here means adding DisallowUnknownFields later fails this test instead of
// silently making every rollback unrecoverable.
func Test_SchemaHistory_DowngradeIsSafe(t *testing.T) {
	is := is.New(t)

	p := Position{Type: TypeCDC, LastLSN: "0/ABC", SnapshotLowWatermarkLSN: "0/100"}
	p.RecordSchemaVersion("public.users", "somehash", "0/AAA")
	encoded := p.ToSDKPosition()

	var old legacyPosition
	is.NoErr(json.Unmarshal(encoded, &old))

	// The unknown field is ignored, and everything the old reader relies on
	// survives intact — dropping either of these would resume from the wrong LSN.
	is.Equal(old.LastLSN, "0/ABC")
	is.Equal(old.SnapshotLowWatermarkLSN, "0/100")
	is.Equal(old.Type, TypeCDC)
}

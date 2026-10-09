// Copyright © 2024 Meroxa, Inc.
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
	"os"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func Test_ToSDKPosition(t *testing.T) {
	is := is.New(t)

	p := Position{
		Type: TypeSnapshot,
		Snapshots: SnapshotPositions{
			"orders": {LastRead: 1, SnapshotEnd: 2},
		},
		LastLSN: "4/137515E8",
	}

	sdkPos := p.ToSDKPosition()
	// ToSDKPosition stamps the current format version onto every serialized
	// position (leading "version" key).
	is.Equal(
		string(sdkPos),
		`{"version":2,"type":1,"snapshots":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`,
	)
}

// Test_ToSDKPosition_StampsVersion asserts ToSDKPosition upgrades a legacy
// (Version == 0) position to CurrentPositionVersion on first write, with no
// forced migration step (DBZ-3 upgrade path).
func Test_ToSDKPosition_StampsVersion(t *testing.T) {
	is := is.New(t)

	// A position value with no version set (as a legacy in-memory position would be).
	legacy := Position{Type: TypeCDC, LastLSN: "4/137515E8"}
	is.Equal(legacy.Version, 0)

	upgraded, err := ParseSDKPosition(legacy.ToSDKPosition())
	is.NoErr(err)
	is.Equal(upgraded.Version, CurrentPositionVersion)
	is.Equal(upgraded.Type, TypeCDC)
	is.Equal(upgraded.LastLSN, "4/137515E8")
}

// Test_ParseSDKPosition_LegacyV014 asserts DBZ-3 acceptance criterion 9: a
// v0.14-serialized position (no "version" key, none of the new fields)
// deserializes cleanly with Version == 0 and zero-value new fields, so the
// connector can treat it as legacy and behave exactly as v0.14 did.
func Test_ParseSDKPosition_LegacyV014(t *testing.T) {
	is := is.New(t)

	legacyCDC := opencdc.Position(
		[]byte(`{"type":2,"last_lsn":"4/137515E8"}`),
	)
	p, err := ParseSDKPosition(legacyCDC)
	is.NoErr(err)
	is.Equal(p.Version, 0) // absent "version" key => legacy
	is.Equal(p.Type, TypeCDC)
	is.Equal(p.LastLSN, "4/137515E8")
	is.Equal(p.SnapshotLowWatermarkLSN, "") // new field absent on legacy

	legacySnapshot := opencdc.Position(
		[]byte(`{"type":1,"snapshots":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`),
	)
	ps, err := ParseSDKPosition(legacySnapshot)
	is.NoErr(err)
	is.Equal(ps.Version, 0)
	is.Equal(ps.Snapshots["orders"], SnapshotPosition{LastRead: 1, SnapshotEnd: 2})
	is.Equal(ps.SnapshotLowWatermarkLSN, "")
}

// Test_Position_RoundTrip_NewFields asserts the new DBZ-3 fields survive a
// serialize/parse round trip and that a newer-than-current Version is NOT
// rejected (additive-only forward compatibility).
func Test_Position_RoundTrip_NewFields(t *testing.T) {
	is := is.New(t)

	p := Position{
		Type:                    TypeCDC,
		LastLSN:                 "4/137515E8",
		SnapshotLowWatermarkLSN: "4/13750000",
	}
	got, err := ParseSDKPosition(p.ToSDKPosition())
	is.NoErr(err)
	is.Equal(got.SnapshotLowWatermarkLSN, "4/13750000")
	is.Equal(got.Version, CurrentPositionVersion)

	// A position from a hypothetical newer connector (higher version, unknown
	// extra key) must still parse — the format is additive-only and must remain
	// readable by N+1 versions per the compatibility contract.
	newer := opencdc.Position(
		[]byte(`{"version":999,"type":2,"last_lsn":"4/137515E8","future_field":"x"}`),
	)
	pn, err := ParseSDKPosition(newer)
	is.NoErr(err)
	is.Equal(pn.Version, 999)
	is.Equal(pn.Type, TypeCDC)
	is.Equal(pn.LastLSN, "4/137515E8")
}

// Test_Position_ReadOnlyContract_LossyRewrite pins the actual limit of the
// "readable by N+1 versions" compatibility contract: it is READ-ONLY. When this
// build parses a position written by a newer connector (carrying a field it
// does not know) and then RE-SERIALIZES it, the unknown field is dropped and
// Version is re-stamped down to this build's CurrentPositionVersion. So a
// downgrade path that reads-then-rewrites a newer position permanently loses
// the newer state — a future field-adding slice (e.g. SchemaHistory) must not
// assume an older build preserves its state across a re-write. Regression guard
// for that assumption before it can be made.
func Test_Position_ReadOnlyContract_LossyRewrite(t *testing.T) {
	is := is.New(t)

	newer := opencdc.Position(
		[]byte(`{"version":999,"type":2,"last_lsn":"4/137515E8","future_field":"x"}`),
	)
	parsed, err := ParseSDKPosition(newer)
	is.NoErr(err)
	is.Equal(parsed.Version, 999) // read faithfully

	// A rewrite by THIS build is lossy: the unknown future_field is gone and the
	// version is stamped back down to what this build knows.
	rewritten := parsed.ToSDKPosition()
	is.True(!strings.Contains(string(rewritten), "future_field")) // unknown field dropped

	reparsed, err := ParseSDKPosition(rewritten)
	is.NoErr(err)
	is.Equal(reparsed.Version, CurrentPositionVersion) // version downgraded on rewrite
}

func Test_PositionLSN(t *testing.T) {
	is := is.New(t)

	invalid := Position{LastLSN: "invalid"}
	_, err := invalid.LSN()
	is.True(err != nil)
	is.Equal(err.Error(), "failed to parse LSN: expected integer")

	valid := Position{LastLSN: "4/137515E8"}
	lsn, noErr := valid.LSN()
	is.NoErr(noErr)
	is.Equal(uint64(lsn), uint64(17506309608))
}

func Test_ParseSDKPosition(t *testing.T) {
	is := is.New(t)

	valid := opencdc.Position(
		[]byte(
			`{"type":1,"snapshots":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`,
		),
	)

	p, validErr := ParseSDKPosition(valid)
	is.NoErr(validErr)

	is.Equal(p, Position{
		Type: TypeSnapshot,
		Snapshots: SnapshotPositions{
			"orders": {LastRead: 1, SnapshotEnd: 2},
		},
		LastLSN: "4/137515E8",
	})

	_, invalidErr := ParseSDKPosition(opencdc.Position("{"))
	is.True(invalidErr != nil)
	is.Equal(invalidErr.Error(), "invalid position: unexpected end of JSON input")
}

// Test_ParseGoldenPositions decodes positions written by every format
// version that can reach this build (testdata/, each generated by running the
// code that writes it, not by hand): v0.14.2 (version 0), DBZ-3 on main
// before #331 (version 1), and the v0.14.x hotfix (version 2, no DBZ-3
// fields). Fields are read by presence, never by version number.
func Test_ParseGoldenPositions(t *testing.T) {
	tests := []struct {
		file       string
		wantLSN    string
		wantCommit string
		wantSeq    uint64
		wantWM     string
		hasHistory bool
	}{
		{file: "v0.14.2-cdc.json", wantLSN: "0/3EA20140"},
		{file: "dbz3-v1-cdc.json", wantLSN: "0/3EA20140", wantWM: "0/3E000000", hasHistory: true},
		{file: "v0.14.x-hotfix-cdc.json", wantLSN: "0/3EA20050", wantCommit: "0/3EA203D8", wantSeq: 3},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			is := is.New(t)
			raw, err := os.ReadFile("testdata/" + tt.file)
			is.NoErr(err)
			p, err := ParseSDKPosition(raw)
			is.NoErr(err)
			is.Equal(p.Type, TypeCDC)
			is.Equal(p.LastLSN, tt.wantLSN)
			is.Equal(p.TxCommitLSN, tt.wantCommit)
			is.Equal(p.TxSeq, tt.wantSeq)
			is.Equal(p.SnapshotLowWatermarkLSN, tt.wantWM)
			_, ok := p.LastSchemaVersion("public.users")
			is.Equal(ok, tt.hasHistory)
			commit, err := p.TxCommit()
			is.NoErr(err)
			is.Equal(commit.String() == tt.wantCommit, tt.wantCommit != "")

			// Re-serializing keeps every field and stamps the current version.
			again, err := ParseSDKPosition(p.ToSDKPosition())
			is.NoErr(err)
			p.Version = CurrentPositionVersion
			is.Equal(again, p)
		})
	}

	raw, err := os.ReadFile("testdata/v0.14.2-snapshot.json")
	is.New(t).NoErr(err)
	p, err := ParseSDKPosition(raw)
	is.New(t).NoErr(err)
	is.New(t).Equal(p, Position{Type: TypeSnapshot, Snapshots: SnapshotPositions{"users": {LastRead: 3, SnapshotEnd: 4}}})
}

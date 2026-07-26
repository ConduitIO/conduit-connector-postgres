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
		`{"version":1,"type":1,"snapshots":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`,
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

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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jackc/pglogrepl"
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
	is.Equal(
		string(sdkPos),
		`{"version":2,"type":1,"snapshots":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`,
	)

	cdc := Position{Type: TypeCDC, LastLSN: "0/3EA20050", TxCommitLSN: "0/3EA203D8"}
	is.Equal(string(cdc.ToSDKPosition()), `{"version":2,"type":2,"last_lsn":"0/3EA20050","tx_commit_lsn":"0/3EA203D8"}`)
}

// Test_ParseV0142GoldenPositions decodes positions serialized by the
// v0.14.2 connector (testdata/, generated with source/position at tag
// v0.14.2). They must keep decoding: Version 0, no TxCommitLSN, same LSN.
// Re-serializing upgrades them to the current version without changing
// what they say.
func Test_ParseV0142GoldenPositions(t *testing.T) {
	is := is.New(t)

	raw, err := os.ReadFile("testdata/v0.14.2-cdc.json")
	is.NoErr(err)
	p, err := ParseSDKPosition(raw)
	is.NoErr(err)
	is.Equal(p, Position{Type: TypeCDC, LastLSN: "0/3EA20140"})
	commit, err := p.TxCommit()
	is.NoErr(err)
	is.Equal(commit, pglogrepl.LSN(0)) // legacy: resume uses the legacy rule
	lsn, err := p.LSN()
	is.NoErr(err)
	is.Equal(lsn.String(), "0/3EA20140")
	is.Equal(string(p.ToSDKPosition()), `{"version":2,"type":2,"last_lsn":"0/3EA20140"}`)

	raw, err = os.ReadFile("testdata/v0.14.2-snapshot.json")
	is.NoErr(err)
	p, err = ParseSDKPosition(raw)
	is.NoErr(err)
	is.Equal(p, Position{Type: TypeSnapshot, Snapshots: SnapshotPositions{"users": {LastRead: 3, SnapshotEnd: 4}}})
}

// Test_ParseFutureVersion: a position from a newer format (version 1 from
// main's DBZ-3 work, or anything higher) is read, not rejected; unknown
// fields are ignored.
func Test_ParseFutureVersion(t *testing.T) {
	is := is.New(t)
	p, err := ParseSDKPosition(opencdc.Position(`{"version":7,"type":2,"last_lsn":"0/10","tx_commit_lsn":"0/20","schema_history":{"public.t":[]}}`))
	is.NoErr(err)
	is.Equal(p, Position{Version: 7, Type: TypeCDC, LastLSN: "0/10", TxCommitLSN: "0/20"})

	_, err = Position{TxCommitLSN: "garble"}.TxCommit()
	is.True(err != nil)
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

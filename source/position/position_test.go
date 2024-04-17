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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func Test_ToSDKPosition(t *testing.T) {
	is := is.New(t)

	p := Position{
		Type: TypeSnapshot,
		Snapshot: SnapshotPositions{
			"orders": {LastRead: 1, SnapshotEnd: 2},
		},
		LastLSN: "4/137515E8",
	}

	sdkPos := p.ToSDKPosition()
	is.Equal(
		string(sdkPos),
		`{"type":1,"snapshot":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`,
	)
}

func Test_PositionLSN(t *testing.T) {
	is := is.New(t)

	invalid := Position{LastLSN: "invalid"}
	_, err := invalid.LSN()
	is.True(err != nil)
	is.Equal(err.Error(), "failed to parse LSN in position: failed to parse LSN: expected integer")

	valid := Position{LastLSN: "4/137515E8"}
	lsn, noErr := valid.LSN()
	is.NoErr(noErr)
	is.Equal(uint64(lsn), uint64(17506309608))
}

func Test_ParseSDKPosition(t *testing.T) {
	is := is.New(t)

	valid := sdk.Position(
		[]byte(
			`{"type":1,"snapshot":{"orders":{"last_read":1,"snapshot_end":2}},"last_lsn":"4/137515E8"}`,
		),
	)

	p, validErr := ParseSDKPosition(valid)
	is.NoErr(validErr)

	is.Equal(p, Position{
		Type: TypeSnapshot,
		Snapshot: SnapshotPositions{
			"orders": {LastRead: 1, SnapshotEnd: 2},
		},
		LastLSN: "4/137515E8",
	})

	_, invalidErr := ParseSDKPosition(sdk.Position([]byte("{")))
	is.True(invalidErr != nil)
	is.Equal(invalidErr.Error(), "invalid position: unexpected end of JSON input")
}

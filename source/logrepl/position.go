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

package logrepl

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
)

// LSNToPosition converts a Postgres LSN to a Conduit position.
func LSNToPosition(lsn pglogrepl.LSN) sdk.Position {
	return sdk.Position(lsn.String())
}

// PositionToLSN converts a Conduit position to a Postgres LSN.
func PositionToLSN(pos sdk.Position) (pglogrepl.LSN, error) {
	return pglogrepl.ParseLSN(string(pos))
}

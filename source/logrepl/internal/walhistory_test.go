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

package internal

import (
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestParseSwitchPoint(t *testing.T) {
	is := is.New(t)
	// The history file of timeline 3: 1 left at 0/3000158, 2 at 0/5000000.
	h := []byte("1\t0/3000158\tno recovery target specified\n\n2\t0/5000000\tbefore 2026-10-21 12:00:00+00\n")
	lsn, ok, err := ParseSwitchPoint(h, 1)
	is.NoErr(err)
	is.True(ok)
	is.Equal(lsn, pglogrepl.LSN(0x3000158))
	lsn, ok, err = ParseSwitchPoint(h, 2)
	is.NoErr(err)
	is.True(ok)
	is.Equal(lsn, pglogrepl.LSN(0x5000000))
	_, ok, err = ParseSwitchPoint(h, 4)
	is.NoErr(err)
	is.True(!ok) // not an ancestor
	_, _, err = ParseSwitchPoint([]byte("x\t0/1\n"), 1)
	is.True(err != nil)
	_, _, err = ParseSwitchPoint([]byte("1\tnope\n"), 1)
	is.True(err != nil)
}

func TestReadWALHistory(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	h, err := ReadWALHistory(ctx, pool)
	is.NoErr(err)
	is.True(h.SystemID != "")
	is.True(h.Timeline >= 1)
	var cur pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()").Scan(&cur))
	is.True(h.CurrentLSN <= cur)
	is.True(h.CurrentLSN > 0)
}

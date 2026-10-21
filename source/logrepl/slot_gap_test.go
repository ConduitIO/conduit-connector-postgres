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

package logrepl

import (
	"errors"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestSlotError_Messages(t *testing.T) {
	is := is.New(t)

	var gap error = newSlotGapError("myslot", slotGapPositions{saved: "last acked change at LSN 0/1000", newSlotStart: "0/2000"})
	is.True(strings.HasPrefix(gap.Error(), "postgres.source.slot_wal_gap: ")) // the code survives layers that keep only the string
	for _, want := range []string{`"myslot"`, "0/1000", "0/2000", "(logrepl.slotName)", "set it back", "logrepl.allowWALGap", "DATA LOSS", "re-snapshot", "failover or promotion"} {
		is.True(strings.Contains(gap.Error(), want))
	}
	is.Equal(CodeOf(errors.Join(errors.New("outer"), gap)), CodeSlotWALGap)

	var inv error = newSlotInvalidatedError("myslot", internal.SlotHealth{WALStatus: "lost", InvalidationReason: "wal_removed"})
	is.True(strings.HasPrefix(inv.Error(), "postgres.source.slot_invalidated: "))
	for _, want := range []string{`"myslot"`, "wal_status=lost", "invalidation_reason=wal_removed", "pg_drop_replication_slot('myslot')", "re-snapshot"} {
		is.True(strings.Contains(inv.Error(), want))
	}
	is.Equal(CodeOf(inv), CodeSlotInvalidated)
	is.Equal(CodeOf(errors.New("x")), Code(""))
}

func TestSlotHealth_Invalidated(t *testing.T) {
	is := is.New(t)
	is.True(!internal.SlotHealth{}.Invalidated())
	is.True(!internal.SlotHealth{WALStatus: "reserved"}.Invalidated())
	is.True(!internal.SlotHealth{WALStatus: "unreserved"}.Invalidated())
	is.True(internal.SlotHealth{WALStatus: "lost"}.Invalidated())
	is.True(internal.SlotHealth{InvalidationReason: "wal_removed"}.Invalidated())
}

func TestDescribeSavedPosition(t *testing.T) {
	for name, tc := range map[string]struct {
		c    CDCConfig
		want string
	}{
		"commit LSN wins": {CDCConfig{LSN: 1, TxCommitLSN: pglogrepl.LSN(0x10)}, "committed at LSN 0/10"},
		"change LSN":      {CDCConfig{LSN: pglogrepl.LSN(0x20)}, "change at LSN 0/20"},
		"snapshot":        {CDCConfig{StartPosition: position.Position{Type: position.TypeSnapshot}}, "snapshot in progress"},
	} {
		t.Run(name, func(t *testing.T) {
			is.New(t).True(strings.Contains(describeSavedPosition(tc.c), tc.want))
		})
	}
}

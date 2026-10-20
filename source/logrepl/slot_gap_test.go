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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestSlotGapError_Message(t *testing.T) {
	is := is.New(t)

	var err error = &SlotGapError{
		Code:          CodeSlotGap,
		Path:          slotNamePath,
		Slot:          "myslot",
		SavedPosition: "last acked change at LSN 0/1000",
		NewSlotStart:  "0/2000",
	}
	is.True(strings.HasPrefix(err.Error(), "postgres.slot.wal_gap: ")) // the code survives layers that keep only the string
	for _, want := range []string{`"myslot"`, "0/1000", "0/2000", "logrepl.slotName", "logrepl.allowWALGap", "DATA LOSS", "re-snapshot"} {
		is.True(strings.Contains(err.Error(), want))
	}

	var gap *SlotGapError
	is.True(errors.As(errors.Join(errors.New("outer"), err), &gap))
	is.Equal(gap.Code, "postgres.slot.wal_gap")
}

// A fresh start never touches the database: a new slot is what it wants.
func TestCheckSlotCoversPosition_FreshStart(t *testing.T) {
	is := is.New(t)
	// A nil pool would panic on any query.
	is.NoErr(checkSlotCoversPosition(context.Background(), nil, CDCConfig{SlotName: "s"}))
	is.NoErr(checkSlotCoversPosition(context.Background(), nil, CDCConfig{SlotName: "s", StartPosition: position.Position{Version: 3}}))
}

func TestDescribeSavedPosition(t *testing.T) {
	for name, tc := range map[string]struct {
		c    CDCConfig
		want string
	}{
		"commit LSN wins":    {CDCConfig{LSN: 1, TxCommitLSN: pglogrepl.LSN(0x10)}, "committed at LSN 0/10"},
		"change LSN":         {CDCConfig{LSN: pglogrepl.LSN(0x20)}, "change at LSN 0/20"},
		"snapshot watermark": {CDCConfig{StartPosition: position.Position{Type: position.TypeSnapshot, SnapshotLowWatermarkLSN: "0/30"}}, "started at LSN 0/30"},
		"snapshot":           {CDCConfig{StartPosition: position.Position{Type: position.TypeSnapshot}}, "snapshot in progress"},
	} {
		t.Run(name, func(t *testing.T) {
			is.New(t).True(strings.Contains(describeSavedPosition(tc.c), tc.want))
		})
	}
}

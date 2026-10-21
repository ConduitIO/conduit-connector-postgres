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
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CodeWALHistoryChanged: the server is not on the WAL history the saved
// position was written on (#447 B3), so the position's LSNs do not name the
// same changes here. Comparing them to the slot (the bound, the beacons)
// would skip changes silently.
const CodeWALHistoryChanged Code = "postgres.source.wal_history_changed"

// CodeWALHistoryUnverified is logged, not returned: the saved position
// predates WAL history tracking (written before v0.17.5), so this run cannot
// check that the server is on the history it was written on. Protection
// starts once the next position (which names the history) is saved.
const CodeWALHistoryUnverified Code = "postgres.source.wal_history_unverified"

// historyBound is the highest LSN of the saved position: its coverage bound,
// or for a snapshot position the slot position its snapshot was taken at.
func historyBound(c CDCConfig) pglogrepl.LSN {
	b := coveredThrough(c)
	if wm, err := pglogrepl.ParseLSN(c.StartPosition.SnapshotLowWatermarkLSN); err == nil && wm > b {
		b = wm
	}
	if c.LSN > b {
		b = c.LSN
	}
	return b
}

// checkWALHistory refuses to resume from a position written on another WAL
// history than the server's (#447 B3):
//
//   - another system identifier: another cluster (pg_upgrade, a different
//     server at the same address, a cluster restored as a new one);
//   - an earlier timeline than the position's: restored from before it;
//   - a later timeline that left the position's timeline before the
//     position (point-in-time recovery, a failover to a standby that was
//     behind): the changes after the switch point are not in this WAL;
//   - a server whose WAL is behind the position: restored from a backup,
//     snapshot or VM image taken before the position, on the same timeline.
//
// A position without a system identifier (written before this check) is not
// checked; the run writes the server's history into its positions.
//
// Invariant 3: in all of these the slot and its beacons can look fine (a
// restored slot is a genuine earlier state of this run's slot), and the
// position's change key would skip changes the new history has at the same
// LSNs.
func checkWALHistory(ctx context.Context, pool *pgxpool.Pool, c CDCConfig, h internal.WALHistory) error {
	p := c.StartPosition
	if p.SystemIdentifier == "" {
		// The same bootstrap as a legacy slot identity: nothing to compare
		// with on this run, so say so instead of passing silently.
		sdk.Logger(ctx).Warn().
			Str("code", string(CodeWALHistoryUnverified)).
			Str("slot", c.SlotName).
			Str("saved_position", describeSavedPosition(c)).
			Str("system_identifier_fp", internal.IdentityFingerprint(h.SystemID)).
			Int32("timeline", h.Timeline).
			Msg("the saved position predates WAL history tracking (written before v0.17.5), so this run cannot verify " +
				"that the server is on the WAL history the position was written on (a restore, point-in-time recovery or " +
				"another server at the same address would go unnoticed). Protection starts once the next position is " +
				"saved: it names this server's history. If the source database was restored or replaced since the " +
				"pipeline last delivered a record, re-snapshot the pipeline")
		return nil
	}
	bound := historyBound(c)
	var reason string
	switch {
	case p.SystemIdentifier != h.SystemID:
		reason = fmt.Sprintf("the server's system identifier is %s, but the saved position was written on %s: this is "+
			"another cluster (pg_upgrade, a different server at the same address, or a backup restored as a new cluster)",
			h.SystemID, p.SystemIdentifier)
	case h.Timeline < p.Timeline:
		reason = fmt.Sprintf("the server is on timeline %d, before the saved position's timeline %d: it was restored "+
			"from a backup taken before the position", h.Timeline, p.Timeline)
	case h.Timeline > p.Timeline:
		switchPoint, ok, err := internal.TimelineSwitchPoint(ctx, pool, h.Timeline, p.Timeline)
		if err != nil {
			return fmt.Errorf("failed to check the WAL history of the server: %w", err)
		}
		switch {
		case !ok:
			reason = fmt.Sprintf("the server's timeline %d does not descend from the saved position's timeline %d",
				h.Timeline, p.Timeline)
		case switchPoint < bound:
			reason = fmt.Sprintf("the server's history left timeline %d at %s, before the saved position (%s): the changes "+
				"after %s on the old timeline are not in this server's WAL (point-in-time recovery, or a failover to a "+
				"standby that was behind)", p.Timeline, switchPoint, bound, switchPoint)
		}
	}
	if reason == "" && bound > h.CurrentLSN {
		reason = fmt.Sprintf("the saved position (%s) is past the server's current WAL position %s: the server was "+
			"restored to an earlier point (a backup, snapshot or VM image) and its new WAL reuses those LSNs",
			bound, h.CurrentLSN)
	}
	if reason == "" {
		return nil
	}
	if c.AllowWALGap {
		sdk.Logger(ctx).Warn().
			Str("code", string(CodeWALHistoryChanged)).
			Str("slot", c.SlotName).
			Str("saved_position", describeSavedPosition(c)).
			Msg("WAL HISTORY CHANGE ACCEPTED (logrepl.allowWALGap): " + reason + ". After a pg_upgrade that migrated the " +
				"slot (which requires every slot to have decoded all WAL) nothing is lost if the pipeline had delivered " +
				"everything before the upgrade; otherwise changes may be missing")
		return nil
	}
	return &SlotError{
		Code:    CodeWALHistoryChanged,
		Path:    slotNamePath,
		Message: fmt.Sprintf("the pipeline's saved position (%s) was written on another WAL history: %s", describeSavedPosition(c), reason),
		Fix: "Check that `url` points at the server the pipeline has always read from. After a pg_upgrade: pg_upgrade migrates " +
			"logical slots only once they decoded all WAL, so if the pipeline had delivered every change before the upgrade, " +
			"start once with `logrepl.allowWALGap: true` and remove it after the first record was delivered. Otherwise " +
			"(a restore, point-in-time recovery, a failover to a lagging standby) re-snapshot: stop the pipeline, delete this " +
			"source connector and create it again so it has no position",
	}
}

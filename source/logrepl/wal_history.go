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
	"math"
	"time"

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
// A position without a system identifier (written before this check) gets
// only the last check, and a wal_history_unverified warning; the run writes
// the server's history into its positions.
//
// Invariant 3: in all of these the slot and its beacons can look fine (a
// restored slot is a genuine earlier state of this run's slot), and the
// position's change key would skip changes the new history has at the same
// LSNs.
func checkWALHistory(ctx context.Context, pool *pgxpool.Pool, c CDCConfig, h internal.WALHistory) error {
	p := c.StartPosition
	bound := historyBound(c)
	var reason string
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
		// #447 S3: the one check that needs no history in the position
		// still applies below (WAL behind the position).
	}
	switch {
	case p.SystemIdentifier == "":
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
		case leftTimelineBy(switchPoint, bound):
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

// leftTimelineBy reports whether a history that switched timelines at
// switchPoint left the position's timeline before its bound did: the switch
// point is the first LSN of the new timeline, so a bound at the switch point
// names a record of the new timeline, not the position's (#447 fresh-context
// review NIT: the comparison was strict).
func leftTimelineBy(switchPoint, bound pglogrepl.LSN) bool {
	return switchPoint <= bound
}

// verifyCoveredTransaction checks a resume that the saved position's bound
// covers (identityCovered) against the WAL itself (#447 re-review S2).
//
// Invariant 3: "confirmed_flush_lsn at or below the position's commit LSN
// loses nothing" holds only if that LSN still names the position's
// transaction. A server restored on the same timeline to a point before the
// position (a volume snapshot, a VM image) writes new transactions at the
// same LSNs; once its WAL has passed the bound, neither the system
// identifier, the timeline nor the WAL position tells it apart, and the
// restored slot is genuinely covered. The position's transaction tells: the
// slot decodes it again (its commit is at or after confirmed_flush_lsn), so
// the first transaction the slot decodes from its commit LSN on must be that
// one: the same commit LSN, xid and commit timestamp (a restored server
// reuses xids, so the xid alone proves nothing). It is read from the slot
// itself, never from pg_walinspect: pg_get_wal_record_info returns the next
// record for an LSN inside one, and its commit records carry no commit time
// in a column and xid 0 for COMMIT PREPARED (#447 round-4 B1, S1).
//
// The transaction is savedTransaction's: the key's commit LSN, or for a
// drift marker, whose key sits one below its change's commit LSN (#338), the
// commit LSN after it (#447 round-4 B2). A position that names no xid
// (written before this check) still names a commit LSN; at least a commit
// must be there.
//
// False positive: on Postgres 15+ pgoutput skips a transaction with no
// changes for the publication, so a publication (or row filter) changed
// since the position can hide the transaction; the start is then refused
// with wal_history_changed, and logrepl.allowWALGap accepts it.
func verifyCoveredTransaction(ctx context.Context, pool *pgxpool.Pool, c CDCConfig) error {
	want, ok := savedTransaction(c)
	if !ok {
		return nil // no transaction to name
	}
	timeout := c.IdentityCheckTimeout
	if timeout <= 0 {
		timeout = defaultIdentityCheckTimeout
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	got, found, err := internal.TxCommittingAt(pctx, pool, c.SlotName, c.PublicationName, want.FinalLSN)
	if err != nil && ctx.Err() != nil {
		return err // stopped, not a verdict
	}
	// Same rule as checkSlotIdentity: a transient failure (a query error, a
	// timeout, a connection drop) is not a verdict, so it fails loudly
	// instead of passing for the verified wal_history_changed case that
	// logrepl.allowWALGap accepts.
	// guard: logrepl-covered-transient-refused tests=^TestSlotIdentity_TransientCoveredCheckError_RefusedEvenWithAllowWALGap$
	if err != nil {
		return newSlotIdentityCheckFailedError(c.SlotName, "the saved position's transaction", timeout, err)
	}
	if found && sameTransaction(want, got) {
		return nil
	}
	var seen string
	if found {
		seen = fmt.Sprintf("the transaction the slot decodes there has xid %d and commit time %s",
			got.Xid, got.CommitTime.UTC().Format(time.RFC3339Nano))
	} else {
		seen = "the slot decodes no transaction committing there"
	}
	named := fmt.Sprintf("xid %d, committed at LSN %s at %s", want.Xid, want.FinalLSN, want.CommitTime.UTC().Format(time.RFC3339Nano))
	if want.Xid == 0 {
		named = fmt.Sprintf("committed at LSN %s", want.FinalLSN)
	}
	reason := fmt.Sprintf("the saved position's transaction (%s) is not in this server's WAL: %s. "+
		"The server was restored to an earlier point (a backup, snapshot or VM image) and its new WAL reuses those LSNs. "+
		"(On Postgres 15+ a publication or row filter changed since the position can also hide the transaction.)",
		named, seen)
	if c.AllowWALGap {
		sdk.Logger(ctx).Warn().
			Str("code", string(CodeWALHistoryChanged)).
			Str("slot", c.SlotName).
			Msg("WAL HISTORY CHANGE ACCEPTED (logrepl.allowWALGap): " + reason)
		return nil
	}
	return &SlotError{
		Code:    CodeWALHistoryChanged,
		Path:    slotNamePath,
		Message: fmt.Sprintf("the pipeline's saved position (%s) was written on another WAL history: %s", describeSavedPosition(c), reason),
		Fix: "Re-snapshot: stop the pipeline, delete this source connector and create it again so it has no position. " +
			"If the publication or its row filter was changed since the last delivered record and the database was not " +
			"restored, start once with `logrepl.allowWALGap: true` and remove it after a record was delivered",
	}
}

// savedTransaction is the transaction the saved position's change key names
// (verifyCoveredTransaction): its commit LSN, and the xid and commit time
// when the position carries them (Xid 0 when it does not). A drift marker's
// key is (C-1, MaxUint64), the predecessor of its change's first key
// (internal.ChangeKey.Predecessor), so the transaction it names commits at C.
// ok is false when the key names no transaction.
func savedTransaction(c CDCConfig) (internal.TxAt, bool) {
	if c.TxCommitLSN == 0 || c.TxSeq == 0 {
		return internal.TxAt{}, false
	}
	commit := c.TxCommitLSN
	if c.TxSeq == math.MaxUint64 {
		commit++
	}
	p := c.StartPosition
	t := internal.TxAt{FinalLSN: commit, Xid: p.TxXid}
	if p.TxXid != 0 {
		t.CommitTime = time.UnixMicro(p.TxCommitTime)
	}
	return t, true
}

// sameTransaction reports whether got, the first transaction the slot
// decodes from want's commit LSN, is want: the same commit LSN, and when want
// names one, the same xid and commit time.
func sameTransaction(want, got internal.TxAt) bool {
	if got.FinalLSN != want.FinalLSN {
		return false
	}
	if want.Xid == 0 {
		return true // a position written before the xid was recorded
	}
	return got.Xid == want.Xid && got.CommitTime.Equal(want.CommitTime)
}

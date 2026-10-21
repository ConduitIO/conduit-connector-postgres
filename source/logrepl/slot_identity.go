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
	"fmt"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Slot identity (#442, docs/design-documents/20261021-slot-identity.md).

const (
	// CodeSlotReplaced: the slot exists and is healthy, but it is not the
	// slot the saved position was taken from (or another client moved it past
	// the position), so changes after the position may be gone.
	CodeSlotReplaced Code = "postgres.source.slot_replaced"
	// CodeSlotBeaconFailed: the connector could not write the slot identity
	// beacon, usually because its role may not call pg_logical_emit_message.
	CodeSlotBeaconFailed Code = "postgres.source.slot_beacon_failed"
	// CodeSlotIdentityUnverified is logged, not returned: a position written
	// before format version 4 has no slot identity, and the slot could not be
	// proven to be its slot. The connector takes the slot over and stamps it.
	CodeSlotIdentityUnverified Code = "postgres.source.slot_identity_unverified"
	// CodeSlotBeaconPublic is logged, not returned: PUBLIC may call
	// pg_logical_emit_message, so any role that can connect can write logical
	// decoding messages. Beacons are authenticated, so this does not weaken
	// the check; revoking it is the recommended hardening.
	CodeSlotBeaconPublic Code = "postgres.source.slot_beacon_public"
	// CodeSlotIdentityUnsupported is logged, not returned: the server cannot
	// carry identity beacons (before Postgres 14, or a standby), so only the
	// saved-position bound is checked.
	CodeSlotIdentityUnsupported Code = "postgres.source.slot_identity_unsupported"
)

// identityOutcome is what the identity check found out about the slot.
type identityOutcome int

const (
	// identityNotChecked: no saved position, import mode, or the slot is
	// missing (checkSlot and OpenSubscription handle that).
	identityNotChecked identityOutcome = iota
	// identityCovered: confirmed_flush_lsn is at or below the saved
	// position's bound, so the slot decodes every change after the position,
	// whichever slot it is.
	identityCovered
	// identityBeacon: the slot decodes a beacon with the position's identity.
	identityBeacon
	// identityAdopted: a pre-version-4 position, and neither proof holds. The
	// slot is taken over with a warning.
	identityAdopted
	// identityUnsupported: no beacons on this server, and the bound does not
	// hold. Checked as before #442, with a warning.
	identityUnsupported
	// identityReplaced: the position has an identity, and the slot is past
	// the position and holds no beacon with it.
	identityReplaced
)

func (o identityOutcome) String() string {
	switch o {
	case identityCovered:
		return "covered by the saved position"
	case identityBeacon:
		return "identity beacon found"
	case identityAdopted:
		return "adopted without proof (legacy position)"
	case identityUnsupported:
		return "not checked (server cannot carry beacons)"
	case identityReplaced:
		return "replaced"
	default:
		return "not checked"
	}
}

// slotIdentityCheck is the result of checkSlotIdentity.
type slotIdentityCheck struct {
	// identity is the slot identity the run carries forward in its positions.
	identity string
	outcome  identityOutcome
	// legacy: the position had no identity; identity is a new random one.
	legacy bool
	// rotated: the run accepts (or would accept) a slot that is not the
	// saved position's, so identity is a new one, not the position's. Until
	// a position with the new identity is saved, a restart from the old
	// position finds no beacon of its identity in that slot and is refused
	// again. This is what keeps a half-finished attempt (it created or took
	// over the slot, then failed before saving) from passing for the
	// original slot.
	rotated bool
	// confirmed is the slot's confirmed_flush_lsn when it was checked, 0 when
	// the slot was not read.
	confirmed pglogrepl.LSN
	// bound is the saved position's CoveredThrough, 0 when it has none.
	bound pglogrepl.LSN
	// beaconLSN is the end LSN of the beacon found (identityBeacon).
	beaconLSN pglogrepl.LSN
}

// rotate gives the check result a new identity (see rotated).
func (r *slotIdentityCheck) rotate() {
	r.identity, r.rotated, r.legacy = internal.NewSlotIdentity(), true, false
}

// coveredThrough is position.CoveredThrough for the resume point in c, which
// callers may also set directly (LSN, TxCommitLSN, TxSeq).
func coveredThrough(c CDCConfig) pglogrepl.LSN {
	if c.StartPosition.Type == position.TypeSnapshot {
		return 0 // the slot was created after the snapshot started
	}
	if c.TxCommitLSN != 0 && c.TxSeq != 0 {
		return c.TxCommitLSN
	}
	return c.LSN
}

// findBeaconTimeout bounds the identity check's peek. A healthy slot holds
// its beacon right after confirmed_flush_lsn, so the peek is quick; it blocks
// only when Postgres makes decoding wait (synchronized_standby_slots).
const findBeaconTimeout = 2 * time.Minute

// checkSlotIdentity decides whether the existing slot is the slot the saved
// position was taken from. It only reads: it peeks the slot, it never
// consumes from it.
//
// Invariant 3 (#442): a slot whose confirmed_flush_lsn is past the saved
// position decodes from there on, so the changes in between are lost unless
// they were delivered before the stop. The connector advances its own idle
// slot past the position only up to an identity beacon (reportedPositions),
// so such a slot is accepted only if it still decodes a beacon with the
// position's identity. A slot created again under the same name starts after
// every beacon the pipeline wrote and decodes none.
func checkSlotIdentity(ctx context.Context, pool *pgxpool.Pool, c CDCConfig, server internal.ServerInfo) (slotIdentityCheck, error) {
	res := slotIdentityCheck{identity: c.StartPosition.SlotIdentity}
	if res.identity == "" {
		// A position from before v0.17.5: no key to prove the slot with. It
		// gets a new random key, stamped at once; a value derived from the
		// position would be guessable by anyone who knows its LSNs.
		res.identity, res.legacy = internal.NewSlotIdentity(), true
	}

	slot, err := readInactiveSlot(ctx, pool, c.SlotName)
	if errors.Is(err, internal.ErrMissingSlot) {
		return res, nil // a gap, refused (or accepted) when the subscription opens
	}
	if err != nil {
		return res, err
	}
	res.confirmed = slot.ConfirmedFlushLSN
	res.bound = coveredThrough(c)

	switch {
	case res.bound != 0 && res.confirmed <= res.bound:
		res.outcome = identityCovered
		return res, nil
	case !server.BeaconsSupported():
		res.outcome = identityUnsupported
		return res, nil
	case res.legacy:
		res.outcome = identityAdopted
		return res, nil
	}

	pctx, cancel := context.WithTimeout(ctx, findBeaconTimeout)
	defer cancel()
	found, lsn, err := internal.FindBeacon(pctx, pool, c.SlotName, c.PublicationName, res.identity, res.confirmed)
	if err != nil {
		if pctx.Err() != nil && ctx.Err() == nil {
			err = fmt.Errorf("%w (on Postgres 17+, decoding a failover slot waits for every physical slot in "+
				"synchronized_standby_slots to confirm the WAL; check that each one exists and its standby is connected)", err)
		}
		return res, fmt.Errorf("failed to check the identity of replication slot %q: %w", c.SlotName, err)
	}
	if found {
		res.outcome, res.beaconLSN = identityBeacon, lsn
	} else {
		res.outcome = identityReplaced
	}
	return res, nil
}

// slotInUseWait is how long the identity check waits for a slot that is
// still streamed from, typically by the previous run's walsender on its way
// out.
const slotInUseWait = 10 * time.Second

// readInactiveSlot reads the slot once no connection streams from it. A
// walsender still holding the slot can still move its confirmed_flush_lsn
// (its last status update), which the check must not race: the stream start
// would then see a higher position than the one checked and refuse an intact
// slot. Streaming cannot start on an active slot anyway.
func readInactiveSlot(ctx context.Context, pool *pgxpool.Pool, name string) (internal.ReadReplicationSlotResult, error) {
	deadline := time.Now().Add(slotInUseWait)
	for {
		slot, err := internal.ReadReplicationSlot(ctx, pool, name)
		if err != nil || !slot.Active {
			return slot, err
		}
		if time.Now().After(deadline) {
			return slot, fmt.Errorf("replication slot %q is in use by another connection; it must be free to resume from it", name)
		}
		select {
		case <-ctx.Done():
			return slot, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func newSlotReplacedError(slot string, saved string, confirmed pglogrepl.LSN) *SlotError {
	return &SlotError{
		Code: CodeSlotReplaced,
		Path: slotNamePath,
		Message: fmt.Sprintf("replication slot %q exists, but it is not the slot the pipeline's saved position (%s) was taken from: "+
			"its confirmed_flush_lsn %s is past the saved position, and it holds none of the identity beacons this pipeline "+
			"keeps ahead of its slot. Postgres would stream from %s, so every change between the saved position and there "+
			"would be skipped and lost. Common causes: the slot was dropped and created again under the same name (by hand, "+
			"by another pipeline or tool, or by pg_upgrade), another client advanced or consumed it "+
			"(pg_replication_slot_advance, pg_logical_slot_get_changes), the pipeline ran on a connector version before "+
			"v0.17.5 after this one, or a failover to a standby that was not listed in synchronized_standby_slots",
			slot, saved, confirmed, confirmed),
		Fix: "If you advanced the slot yourself on purpose (for example to skip a transaction, see the README), start once with " +
			"`logrepl.allowWALGap: true` and remove it after the pipeline has resumed. Otherwise either re-snapshot (stop the " +
			"pipeline, delete this source connector and create it again so it has no position; with snapshotMode initial every " +
			"table is read again and every row is delivered again), or accept the loss by setting `logrepl.allowWALGap: true` " +
			"(DATA LOSS: the skipped changes are never delivered, so repair the destination another way) and remove the " +
			"setting after the pipeline has resumed",
	}
}

func newSlotBeaconError(slot string, serverVersion int, err error) *SlotError {
	return &SlotError{
		Code: CodeSlotBeaconFailed,
		Path: slotNamePath,
		Message: fmt.Sprintf("could not write the identity beacon of replication slot %q (%v). The connector keeps a "+
			"logical decoding message with a random token in the WAL to recognise its slot after a restart; without it "+
			"a slot dropped and created again under the same name would go unnoticed", slot, err),
		Fix: fmt.Sprintf("Let the connector's role call it, in the pipeline's database: `GRANT EXECUTE ON FUNCTION %s TO <role>`. "+
			"Postgres grants it to PUBLIC by default", internal.EmitMessageSignature(serverVersion)),
	}
}

// logSlotIdentity reports the identity check once per start.
func logSlotIdentity(ctx context.Context, c CDCConfig, r slotIdentityCheck) {
	switch r.outcome {
	case identityNotChecked:
		return
	case identityCovered, identityBeacon:
		sdk.Logger(ctx).Info().
			Str("slot", c.SlotName).
			Str("slot_identity_fp", internal.IdentityFingerprint(r.identity)).
			Stringer("confirmed_flush_lsn", r.confirmed).
			Stringer("saved_position_bound", r.bound).
			Stringer("beacon_lsn", r.beaconLSN).
			Str("proof", r.outcome.String()).
			Msg("replication slot verified as the slot of the saved position")
	case identityAdopted:
		sdk.Logger(ctx).Warn().
			Str("code", string(CodeSlotIdentityUnverified)).
			Str("slot", c.SlotName).
			Str("slot_identity_fp", internal.IdentityFingerprint(r.identity)).
			Stringer("confirmed_flush_lsn", r.confirmed).
			Str("saved_position", describeSavedPosition(c)).
			Msg("the saved position predates slot identities (written before v0.17.5), and the replication slot is past it, " +
				"so it cannot be proven to be the same slot. It is taken over and stamped; from now on a slot dropped and " +
				"created again under the same name is refused. If the slot was replaced before this upgrade, changes after " +
				"the saved position may be missing")
	case identityUnsupported:
		sdk.Logger(ctx).Warn().
			Str("code", string(CodeSlotIdentityUnsupported)).
			Str("slot", c.SlotName).
			Stringer("confirmed_flush_lsn", r.confirmed).
			Str("saved_position", describeSavedPosition(c)).
			Msg("this server cannot carry slot identity beacons (they need Postgres 14 or later and a primary), so a " +
				"replication slot dropped and created again under the same name is not detected")
	case identityReplaced:
		// Refused, or accepted with warnReplacedAccepted.
	}
}

// warnEmitMessagePublic recommends revoking pg_logical_emit_message from
// PUBLIC when it is still granted. The check is a privilege lookup any role
// may make; if it fails, nothing is logged.
func warnEmitMessagePublic(ctx context.Context, pool *pgxpool.Pool, serverVersion int) {
	public, err := internal.EmitMessagePublic(ctx, pool, serverVersion)
	if err != nil || !public {
		return
	}
	sig := internal.EmitMessageSignature(serverVersion)
	sdk.Logger(ctx).Warn().
		Str("code", string(CodeSlotBeaconPublic)).
		Msgf("every role that can connect may call %s and write logical decoding messages into the WAL. "+
			"The connector's slot identity beacons are authenticated and cannot be forged that way, but as hardening "+
			"run `REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC` and `GRANT EXECUTE ON FUNCTION %s TO <connector role>`", sig, sig, sig)
}

// warnReplacedAccepted logs the accepted data loss of logrepl.allowWALGap on
// a slot that is not the position's.
func warnReplacedAccepted(ctx context.Context, c CDCConfig, confirmed pglogrepl.LSN) {
	sdk.Logger(ctx).Warn().
		Str("code", string(CodeSlotReplaced)).
		Str("slot", c.SlotName).
		Str("saved_position", describeSavedPosition(c)).
		Stringer("confirmed_flush_lsn", confirmed).
		Msg("DATA LOSS ACCEPTED (logrepl.allowWALGap): the replication slot is not the slot of the saved position; " +
			"every change between the saved position and the slot's confirmed_flush_lsn is skipped and will not be delivered")
}

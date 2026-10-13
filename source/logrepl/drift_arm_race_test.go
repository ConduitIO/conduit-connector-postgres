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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

// Test_EmitDriftMarker_AckInsideQueueWindowArmsHalt is the deterministic
// regression test for the arm race (#369). The marker is queued by
// emitDriftMarker, and from that instant the engine may read and ack it on
// its own goroutine. The afterMarkerQueued seam delivers exactly that ack at
// the earliest point it can happen: right after the marker is on the batch
// channel, on the subscription goroutine, before emitDriftMarker returns. No
// sleep and no second goroutine, so the interleaving is forced, not hoped for.
//
// Before the fix the pending state was published after the queue; the ack saw
// driftMarkerLSN == 0, maybeArmDriftHalt returned, and the halt never armed
// (D4 emits nothing after the marker, so no later ack could arm it either).
func Test_EmitDriftMarker_AckInsideQueueWindowArmsHalt(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	cur := internal.ChangeKey{CommitLSN: 0x500, Seq: 1}
	h.changeKey = func() internal.ChangeKey { return cur }

	var acked bool
	h.afterMarkerQueued = func() {
		// What the engine does: read the batch, then ack the marker's
		// position through CDCIterator.Ack, which ends in maybeArmDriftHalt.
		batch := <-out
		is.Equal(len(batch), 1)
		is.Equal(batch[0].Metadata[MetadataSchemaDrift], "true")

		pos, err := position.ParseSDKPosition(batch[0].Position)
		is.NoErr(err)
		lsn, err := pos.LSN()
		is.NoErr(err)
		commit, err := pos.TxCommit()
		is.NoErr(err)
		h.maybeArmDriftHalt(lsn, internal.ChangeKey{CommitLSN: commit, Seq: pos.TxSeq}, pos)
		acked = true
	}

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	cur = internal.ChangeKey{CommitLSN: 0x500, Seq: 2}
	_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)
	is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 200))

	is.True(acked) // the seam ran, so the ack really landed inside the window
	is.True(h.driftHaltArmed.Load())
	is.True(h.driftHaltError() != nil)
}

// Test_EmitDriftMarker_EarlierAckInsideQueueWindowDoesNotArm pins the other
// side of publishing first: an ack of the record delivered just before the
// marker can land while the marker's state is already published. It shares the
// marker's change key but not its position, and must not arm the halt, or the
// halt would surface before the marker's checkpoint is durable (Invariant 1).
func Test_EmitDriftMarker_EarlierAckInsideQueueWindowDoesNotArm(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	cur := internal.ChangeKey{CommitLSN: 0x500, Seq: 1}
	h.changeKey = func() internal.ChangeKey { return cur }

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	_, err := h.Handle(ctx, usersRow(2), 200) // delivered record
	is.NoErr(err)
	prevBatch := <-out
	is.Equal(len(prevBatch), 1)
	prev, err := position.ParseSDKPosition(prevBatch[0].Position)
	is.NoErr(err)

	h.afterMarkerQueued = func() {
		h.maybeArmDriftHalt(200, internal.ChangeKey{CommitLSN: 0x500, Seq: 1}, prev)
		is.True(!h.driftHaltArmed.Load())
	}
	cur = internal.ChangeKey{CommitLSN: 0x500, Seq: 2}
	_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)
	is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 200))
	is.True(!h.driftHaltArmed.Load())
}

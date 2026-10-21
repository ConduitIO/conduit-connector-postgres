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
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

const dataLossWarn = "DATA LOSS ACCEPTED"

// A slot dropped between checkSlot and the subscription is the window the
// pre-check cannot close. With a saved position and no opt-in, the open-only
// subscription must refuse it with slot_wal_gap and create nothing. This test
// fails if NewCDCIterator routes a saved position back to CreateSubscription.
func TestCDCIterator_SlotDroppedAfterCheck_RefusedAndNoSlotCreated(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, pool, table)

	// checkSlot sees the intact slot; the hook then drops it.
	afterSlotCheck = func() { slotGapDropSlot(ctx, t, pool, table) }
	t.Cleanup(func() { afterSlotCheck = nil })

	_, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	is.True(err != nil)
	is.Equal(CodeOf(err), CodeSlotWALGap)

	var n int
	is.NoErr(pool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", table).Scan(&n))
	is.Equal(n, 0) // a retry must not find a slot and see no gap
}

// With logrepl.allowWALGap the recreate is accepted, and it is warned about
// exactly once, whether the slot was already missing at the check or was
// dropped after it.
func TestCDCIterator_AllowWALGap_WarnsOnceWhenSlotRecreated(t *testing.T) {
	for name, dropAfterCheck := range map[string]bool{
		"missing at the check":    false,
		"dropped after the check": true,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := test.Context(t)
			ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
			defer cancel()
			is := is.New(t)

			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupTestTable(ctx, t, pool)
			t.Cleanup(func() {
				_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
			})

			pos := slotGapRun1(ctx, t, pool, table)
			if dropAfterCheck {
				afterSlotCheck = func() { slotGapDropSlot(ctx, t, pool, table) }
				t.Cleanup(func() { afterSlotCheck = nil })
			} else {
				slotGapDropSlot(ctx, t, pool, table)
			}

			var logs syncBuffer
			lctx := zerolog.New(&logs).WithContext(ctx)
			conf := slotGapConfig(table, pos, false)
			conf.AllowWALGap = true
			it, err := NewCombinedIterator(lctx, pool, conf)
			is.NoErr(err)
			is.NoErr(it.Teardown(ctx))

			is.Equal(strings.Count(logs.String(), dataLossWarn), 1)
			is.True(strings.Contains(logs.String(), `"level":"warn"`))
			is.True(strings.Contains(logs.String(), string(CodeSlotWALGap)))
		})
	}
}

// CREATE_REPLICATION_SLOT can succeed and the call still fail (reading the slot
// back, a timeout). The slot then exists, so the retry finds a healthy one and
// would log nothing: the gap is taken, and must be warned about, on the first
// attempt, exactly once over both attempts.
func TestCDCIterator_AllowWALGap_WarnsWhenCreateSucceedsThenFails(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, pool, table)
	slotGapDropSlot(ctx, t, pool, table)

	injected := errors.New("injected failure after CREATE_REPLICATION_SLOT")
	fail := true
	internal.AfterSlotCreateHook = func() error {
		if fail {
			fail = false // one-shot
			return injected
		}
		return nil
	}
	t.Cleanup(func() { internal.AfterSlotCreateHook = nil })

	var logs syncBuffer
	lctx := zerolog.New(&logs).WithContext(ctx)
	conf := slotGapConfig(table, pos, false)
	conf.AllowWALGap = true

	_, err := NewCombinedIterator(lctx, pool, conf)
	is.True(errors.Is(err, injected))
	is.Equal(strings.Count(logs.String(), dataLossWarn), 1) // warned on the failed attempt
	var n int
	is.NoErr(pool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", table).Scan(&n))
	is.Equal(n, 1) // the slot exists: this is why the warning could not wait

	// The retry finds a healthy slot that the failed attempt created. It is
	// not the slot of the saved position (#442: it holds no beacon of the
	// position's identity), so the opt-in accepts it again and says so again
	// (TestSlotIdentity_HalfFinishedCreate_Refused: without the opt-in it is
	// refused). Before the slot identity this retry was silent.
	it, err := NewCombinedIterator(lctx, pool, conf)
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), dataLossWarn), 2)
	is.True(strings.Contains(logs.String(), string(CodeSlotReplaced)))
}

// An intact slot with the opt-in set is not a gap: no warning.
func TestCDCIterator_AllowWALGap_IntactSlotDoesNotWarn(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})
	pos := slotGapRun1(ctx, t, pool, table)

	var logs syncBuffer
	lctx := zerolog.New(&logs).WithContext(ctx)
	conf := slotGapConfig(table, pos, false)
	conf.AllowWALGap = true
	it, err := NewCombinedIterator(lctx, pool, conf)
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.True(!strings.Contains(logs.String(), dataLossWarn))
}

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
	"errors"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func slotRows(t *testing.T, name string) int {
	t.Helper()
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Invariant 3: OpenSubscription never creates a slot. On a missing slot it
// fails with ErrMissingSlot and leaves pg_replication_slots untouched.
func TestOpenSubscription_MissingSlot(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	slot := test.RandomIdentifier(t)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	test.CreatePublication(t, pool, slot, []string{table})

	sub, err := OpenSubscription(ctx, pool, slot, slot, []string{table}, 0, nil)
	is.True(err != nil)
	is.True(errors.Is(err, ErrMissingSlot))
	is.True(sub == nil)
	is.Equal(slotRows(t, slot), 0)
}

// OpenSubscription on an existing slot reuses it: no snapshot is exported
// (nothing was created) and no second slot appears.
func TestOpenSubscription_ExistingSlot(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	slot := test.RandomIdentifier(t)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	test.CreatePublication(t, pool, slot, []string{table})
	test.CreateReplicationSlot(t, pool, slot)

	sub, err := OpenSubscription(ctx, pool, slot, slot, []string{table}, 0, nil)
	is.NoErr(err)
	defer sub.Teardown(ctx)        //nolint:errcheck // test cleanup
	is.Equal(sub.TXSnapshotID, "") // TXSnapshotID is set only when a slot was created
	is.Equal(slotRows(t, slot), 1)
}

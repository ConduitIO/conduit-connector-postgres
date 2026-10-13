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

package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func newStopTestSubscription(ctx context.Context, t *testing.T) *Subscription {
	t.Helper()
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	slot, pub := test.RandomIdentifier(t), test.RandomIdentifier(t)
	test.CreatePublication(t, pool, pub, []string{table})

	sub, err := CreateSubscription(ctx, pool, slot, pub, []string{table}, 0,
		func(context.Context, pglogrepl.Message, pglogrepl.LSN) (pglogrepl.LSN, error) { return 0, nil })
	is.NoErr(err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", slot)
	})
	return sub
}

// TestSubscription_StopBeforeReplicationStarts is the regression test for
// #346. When the context ends while the subscription is still starting
// (startReplication has released its connection and cannot acquire a new one),
// the closing sequence used the released connection and panicked with a nil
// pointer dereference. Stop and Teardown must be safe at any point after
// CreateSubscription.
func TestSubscription_StopBeforeReplicationStarts(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	sub := newStopTestSubscription(ctx, t)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- sub.Run(cancelled) }() // panicked here before the fix

	select {
	case err := <-errCh:
		is.True(err != nil) // replication never started
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()
	is.NoErr(sub.Teardown(tctx))
	select {
	case <-sub.Done():
	default:
		t.Fatal("done channel not closed")
	}
}

// TestSubscription_RunReturnsWhenContextExpires is the regression test for
// #347: a subscription whose own context hits a deadline must stop, not retry
// receiving on the expired context forever (which also made Teardown hang).
func TestSubscription_RunReturnsWhenContextExpires(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	sub := newStopTestSubscription(ctx, t)

	rctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- sub.Run(rctx) }()

	select {
	case <-sub.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("subscription did not become ready")
	}

	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Run kept running after its context expired")
	}

	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()
	is.NoErr(sub.Teardown(tctx))
}

// startBlocked runs the subscription with a hook that parks Run inside
// startReplication (old connection released, new one not yet acquired).
func startBlocked(ctx context.Context, t *testing.T) (sub *Subscription, release func(), runErr <-chan error) {
	t.Helper()
	sub = newStopTestSubscription(ctx, t)
	inStart := make(chan struct{})
	gate := make(chan struct{})
	sub.beforeAcquire = func() {
		close(inStart)
		<-gate
	}
	errCh := make(chan error, 1)
	go func() { errCh <- sub.Run(ctx) }()
	select {
	case <-inStart:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never reached startReplication")
	}
	var once sync.Once
	rel := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(rel)
	return sub, rel, errCh
}

// TestSubscription_StopBeforeRun: Stop arrives before Run has published its
// cancel function. Run must notice (the `stopped` flag) and return instead of
// replicating until the parent context ends.
func TestSubscription_StopBeforeRun(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	sub := newStopTestSubscription(ctx, t)

	sub.Stop()

	errCh := make(chan error, 1)
	go func() { errCh <- sub.Run(ctx) }()
	select {
	case err := <-errCh:
		is.True(errors.Is(err, context.Canceled))
	case <-time.After(3 * time.Second):
		t.Fatal("Run ignored a Stop that arrived before it started")
	}
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	is.NoErr(sub.Teardown(tctx))
}

// TestSubscription_StopWhileStarting: Stop is called while Run is parked inside
// startReplication. Run must abort starting and return.
func TestSubscription_StopWhileStarting(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	sub, release, runErr := startBlocked(ctx, t)

	sub.Stop()
	release()

	select {
	case err := <-runErr:
		is.True(err != nil)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Stop during start")
	}
	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("done not closed")
	}
}

// TestSubscription_TeardownWaitsForStartingRun: Teardown while Run is still
// starting (not ready) must wait for Run to finish. Returning early let the
// caller release the pool while Run still held or was about to take a
// connection (the leaked goroutine in #346).
func TestSubscription_TeardownWaitsForStartingRun(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	sub, release, _ := startBlocked(ctx, t)

	tdone := make(chan error, 1)
	go func() {
		tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		tdone <- sub.Teardown(tctx)
	}()

	select {
	case <-tdone:
		t.Fatal("Teardown returned while Run was still starting")
	case <-time.After(300 * time.Millisecond):
	}

	release()
	select {
	case err := <-tdone:
		is.NoErr(err)
	case <-time.After(15 * time.Second):
		t.Fatal("Teardown did not return after Run finished")
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("Teardown returned before done was closed")
	}
}

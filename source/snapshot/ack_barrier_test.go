// Copyright © 2024 Meroxa, Inc.
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

package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestAckBarrier_WaitCanceledLeavesNoGoroutine is the regression test for the
// goroutine leak behind suite case SRC-07: Wait canceled while records are
// unacked used to leave a goroutine parked in sync.WaitGroup.Wait forever.
func TestAckBarrier_WaitCanceledLeavesNoGoroutine(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	var b ackBarrier
	b.Add(3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	tctx, tcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer tcancel()
	if err := b.Wait(tctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want context.DeadlineExceeded", err)
	}
}

func TestAckBarrier_WaitReturnsWhenAllAcked(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	var b ackBarrier
	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("Wait on an empty barrier = %v", err)
	}

	b.Add(2)
	done := make(chan error, 1)
	go func() { done <- b.Wait(context.Background()) }()
	b.Done()
	select {
	case err := <-done:
		t.Fatalf("Wait returned %v with one record still unacked", err)
	case <-time.After(50 * time.Millisecond):
	}
	b.Done()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the last ack")
	}
}

func TestAckBarrier_DoneWithoutAddPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	var b ackBarrier
	b.Done()
}

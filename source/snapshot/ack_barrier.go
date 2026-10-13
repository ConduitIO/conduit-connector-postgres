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
	"sync"
)

// ackBarrier counts snapshot records that were handed out and not yet
// acknowledged, and lets a caller wait for the count to reach zero.
//
// It replaces csync.WaitGroup, whose Wait(ctx) runs sync.WaitGroup.Wait in a
// goroutine that nothing can interrupt: when ctx ends first, that goroutine
// stays parked for as long as any record is unacked, which after a stop is
// forever. One leaked goroutine (plus everything it references, the whole
// Iterator) per stop, accumulating across pipeline restarts in an embedded
// engine. Wait here selects on a channel and leaves nothing behind.
//
// The barrier is what keeps the snapshot-to-CDC handoff correct: the iterator
// is not reported done until every snapshot record is acked, so the first CDC
// record never overtakes an unacked snapshot position.
type ackBarrier struct {
	mu      sync.Mutex
	pending int
	// idle is closed (and reset) when pending drops to zero while a Wait is
	// parked on it. Nil when nobody is waiting.
	idle chan struct{}
}

// Add registers n records as handed out and awaiting an ack.
func (b *ackBarrier) Add(n int) {
	b.mu.Lock()
	b.pending += n
	b.mu.Unlock()
}

// Done acknowledges one record. It panics on an ack nobody is waiting for,
// like sync.WaitGroup: a negative count means the ack accounting is broken
// and continuing would let the handoff run ahead of unacked records.
func (b *ackBarrier) Done() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending <= 0 {
		panic("snapshot: ack without an outstanding record")
	}
	b.pending--
	if b.pending == 0 && b.idle != nil {
		close(b.idle)
		b.idle = nil
	}
}

// Wait blocks until no record is awaiting an ack or ctx ends, in which case it
// returns ctx.Err(). It starts no goroutine.
func (b *ackBarrier) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		if b.pending == 0 {
			b.mu.Unlock()
			return nil
		}
		if b.idle == nil {
			b.idle = make(chan struct{})
		}
		idle := b.idle
		b.mu.Unlock()

		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

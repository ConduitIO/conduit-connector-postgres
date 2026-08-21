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

// This file carries no build tag, so it runs in the default build (the one
// every release ships) and proves the no-op property directly: Reach must
// return immediately, every time, regardless of name or of PGCHAOS_PARK
// being set in the environment. There is no parking implementation to link
// against here — the assertion is that calling Reach at all is harmless.
package chaospoint

import (
	"os"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestReach_DefaultBuildIsNoOp(t *testing.T) {
	is := is.New(t)

	// Even a PGCHAOS_PARK that would target a real point under the
	// conduitchaos build must not matter here: the default build has no
	// code path that reads it.
	t.Setenv("PGCHAOS_PARK", SnapshotFetchRow+":1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			Reach(SnapshotFetchRow)
			Reach(PreStartSubscriber)
			Reach(StandbyStatusUpdate)
			Reach("")
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Reach did not return promptly in the default build — it must be a true no-op")
	}

	is.True(os.Getenv("PGCHAOS_PARK") != "") // sanity: env var really was set and ignored
}

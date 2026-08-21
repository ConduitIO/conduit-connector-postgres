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

//go:build conduitchaos

package chaos

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// chaosPrefix marks every replication slot and publication this suite ever
// creates, so reaper.go can find (and TestMain can sweep) exactly and only
// the objects this package owns, never a developer's own slots from a
// concurrently running `make test` stack — which is also why the two
// stacks use separate compose projects, ports and volumes (see
// test/docker-compose.chaos.yml).
const chaosPrefix = "pgchaos_"

// randChaosName returns a chaosPrefix-prefixed identifier built from
// crypto/rand — deliberately NOT test.RandomIdentifier, whose suffix is
// time.Now().UnixMicro()%1000: only 1000 distinct values, which nightly's
// -count=3 (nightly re-runs the whole suite three times, per the harness
// plan §7) can plausibly collide on within the same microsecond bucket
// across two overlapping test binaries (finding F-8). crypto/rand's 8 bytes
// of entropy make that collision probability negligible instead of merely
// unlikely.
//
// The result is lowercase hex only, satisfying source.Config.LogreplSlotName's
// `^[a-z0-9_]+$` validation (source/config.go) — publication names have no
// such constraint, but are built the same way for a uniform pgchaos_ prefix
// the reaper can match on.
func randChaosName() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("read crypto/rand for chaos object name: %w", err)
	}
	return chaosPrefix + hex.EncodeToString(b[:]), nil
}

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

package pgxpin

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// lockID is the advisory lock that serializes the pin tests.
const lockID = 8624001

// Lock takes a session level advisory lock for the rest of the test. The pin
// tests of different packages share the types (DROP TYPE ... CASCADE in Setup)
// and the names of the tables they create, and `go test ./...` runs packages
// in parallel against the same database.
func Lock(ctx context.Context, t *testing.T, connString string) {
	t.Helper()

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("pgxpin lock: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Close(cctx) // closing the session releases the lock
	})

	lctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if _, err := conn.Exec(lctx, "SELECT pg_advisory_lock($1)", int64(lockID)); err != nil {
		t.Fatalf("pgxpin lock: %v", err)
	}
}

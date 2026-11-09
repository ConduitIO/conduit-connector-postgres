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

package schema

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// blockingQuerier is a catalog that never answers: Query returns only when
// its context ends, with an error that, like some pgx errors, does not wrap
// the context error.
type blockingQuerier struct{}

func (blockingQuerier) Query(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
	<-ctx.Done()
	return nil, errors.New("conn closed")
}

// TestLoadNotNull_Bounded: the catalog read runs on the replication goroutine
// in CDC, so it must not hang there; an expired deadline or a cancelled
// context yields an error that says what was read and wraps the cause.
func TestLoadNotNull_Bounded(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		is := is.New(t)
		old := NotNullQueryTimeout
		NotNullQueryTimeout = 20 * time.Millisecond
		t.Cleanup(func() { NotNullQueryTimeout = old })

		start := time.Now()
		_, err := LoadNotNull(context.Background(), blockingQuerier{}, 42)
		is.True(err != nil)
		is.True(time.Since(start) < 5*time.Second) // bounded, not the caller's lifetime
		is.True(errors.Is(err, context.DeadlineExceeded))
		is.True(strings.Contains(err.Error(), "column nullability of relation 42 timed out after 20ms"))
	})
	t.Run("cancelled", func(t *testing.T) {
		is := is.New(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := LoadNotNull(ctx, blockingQuerier{}, 42)
		is.True(errors.Is(err, context.Canceled))
		is.True(strings.Contains(err.Error(), "column nullability of relation 42 was cancelled"))
	})
	t.Run("cancelled, real catalog", func(t *testing.T) {
		is := is.New(t)
		ctx := test.Context(t)
		conn := test.ConnectSimple(ctx, t, test.RegularConnString)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := LoadNotNull(cctx, conn, 1259) // pg_class
		is.True(errors.Is(err, context.Canceled))
		is.True(strings.Contains(err.Error(), "column nullability of relation 1259"))
	})
}

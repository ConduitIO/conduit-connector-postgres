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
	"time"
)

// nextNDeadline bounds a blocking NextN in a test. A NextN that never returns
// is exactly how a lost drift-halt arm presents (the pipeline stalls instead
// of halting), and without a deadline it hangs until the package timeout,
// ten minutes later. Generous enough for a loaded CI runner, short enough to
// fail fast.
const nextNDeadline = 30 * time.Second

// bounded returns ctx with a nextNDeadline deadline that is released when the
// test ends. Use it for every blocking NextN in an integration test.
func bounded(ctx context.Context, t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, nextNDeadline)
	t.Cleanup(cancel)
	return ctx
}

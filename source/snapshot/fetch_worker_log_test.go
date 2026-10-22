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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// The range bounds of a fetcher are primary-key values, which are record data.
// The start line logs hashes of them, never the values.
func TestFetchWorker_StartLogDoesNotContainKeyBounds(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	f := &FetchWorker{
		conf:        FetchConfig{Key: "id", FetchSize: 10},
		cursorName:  "cursor1",
		lastRead:    8675309,
		snapshotEnd: 9988776,
	}
	f.logStart(ctx)

	out := buf.String()
	is.True(strings.Contains(out, "starting fetcher cursor1"))
	is.True(strings.Contains(out, internal.KeyHash(map[string]any{"id": int64(8675309)})))
	is.True(!strings.Contains(out, "8675309"))
	is.True(!strings.Contains(out, "9988776"))
}

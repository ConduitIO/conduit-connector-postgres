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
	"encoding/json"
	"strings"
	"testing"
	"time"

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

// The progress line must not let a reader work out the end of the range, the
// largest key value: rows together with a percentage of the range did
// (snapshotEnd = rows * 100 / completed_perc).
func TestFetchWorker_ProgressLogDoesNotRevealSnapshotEnd(t *testing.T) {
	logLine := func(snapshotEnd int64, nfetched int) map[string]any {
		var buf bytes.Buffer
		ctx := zerolog.New(&buf).WithContext(context.Background())
		f := &FetchWorker{conf: FetchConfig{Key: "id", Table: "orders", FetchSize: 10}, snapshotEnd: snapshotEnd}
		f.logProgress(ctx, nfetched, time.Now().Add(-time.Minute))
		var out map[string]any
		if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	allowed := map[string]bool{"level": true, "rows": true, "table": true, "elapsed": true, "rate_per_min": true, "message": true}
	for _, nfetched := range []int{1, 1234, 5000000} {
		a, b := logLine(9988776, nfetched), logLine(77, nfetched)
		for k := range a {
			if !allowed[k] {
				t.Errorf("progress line has the field %q, which can depend on the end of the range", k)
			}
		}
		// the same line for two very different ends of the range
		is.New(t).Equal(len(a), len(b))
		is.New(t).Equal(a["rows"], b["rows"])
		is.New(t).Equal(a["rows"], float64(nfetched))
		for _, line := range []map[string]any{a, b} {
			enc, _ := json.Marshal(line)
			is.New(t).True(!strings.Contains(string(enc), "9988776"))
		}
	}
}

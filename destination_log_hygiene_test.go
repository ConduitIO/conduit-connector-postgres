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

package postgres

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// traceCtx returns a context whose logger writes everything, trace included,
// to the returned buffer.
func traceCtx(t *testing.T) (context.Context, *bytes.Buffer) {
	t.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	t.Cleanup(func() { zerolog.SetGlobalLevel(prev) })

	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.TraceLevel)
	return logger.WithContext(context.Background()), &buf
}

// TestDestination_TraceLogsDoNotContainRecordKeys: the trace lines for upserts
// and deletes carry the key column count and a salted key hash, never the key.
func TestDestination_TraceLogsDoNotContainRecordKeys(t *testing.T) {
	const secret = "tenant-SECRET-4711"
	key := opencdc.StructuredData{"id": int64(8675309), "tenant": secret}

	d := newSQLCacheTestDestination()
	d.getTableName = func(opencdc.Record) (string, error) { return "orders", nil }

	tests := []struct {
		name string
		run  func(context.Context, *writePlan) error
		msg  string
	}{{
		name: "upsert",
		run: func(ctx context.Context, p *writePlan) error {
			return d.upsert(ctx, 0, opencdc.Record{
				Operation: opencdc.OperationUpdate,
				Key:       key,
				Payload:   opencdc.Change{After: opencdc.StructuredData{"amount": 12}},
			}, p)
		},
		msg: "upserting record",
	}, {
		name: "delete",
		run: func(ctx context.Context, p *writePlan) error {
			return d.remove(ctx, 0, opencdc.Record{Operation: opencdc.OperationDelete, Key: key}, p)
		},
		msg: "deleting record",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx, buf := traceCtx(t)

			is.NoErr(tc.run(ctx, &writePlan{}))

			out := buf.String()
			is.True(strings.Contains(out, tc.msg))       // the line is still logged
			is.True(strings.Contains(out, `"key_hash"`)) // and can be correlated
			is.True(strings.Contains(out, `"key_columns":2`))
			is.True(!strings.Contains(out, secret)) // the key is not in it
			is.True(!strings.Contains(out, "8675309"))
		})
	}
}

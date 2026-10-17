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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func benchRel() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "bench_source_0",
	}
}

// TestBackfillFirstSeenLSN_Placeholder asserts a "0/0" placeholder is repaired
// with the real LSN, matching the pre-optimization Position.SetFirstSeenLSN
// behavior the DML handlers relied on.
func TestBackfillFirstSeenLSN_Placeholder(t *testing.T) {
	is := is.New(t)
	h := &CDCHandler{
		basePosition: position.Position{
			SchemaHistory: position.SchemaHistories{
				"public.bench_source_0": {{ColumnSetHash: "abc", FirstSeenLSN: "0/0"}},
			},
		},
	}

	h.backfillFirstSeenLSN(benchRel(), 0x16B6B38)

	got, ok := h.basePosition.LastSchemaVersion("public.bench_source_0")
	is.True(ok)
	is.Equal(got.FirstSeenLSN, "0/16B6B38")
}

// TestBackfillFirstSeenLSN_AlreadySet asserts a shape that already records a
// real first-seen LSN is left untouched: backfill is a no-op in the steady
// state, which is what lets it skip the LSN format on the hot path.
func TestBackfillFirstSeenLSN_AlreadySet(t *testing.T) {
	is := is.New(t)
	h := &CDCHandler{
		basePosition: position.Position{
			SchemaHistory: position.SchemaHistories{
				"public.bench_source_0": {{ColumnSetHash: "abc", FirstSeenLSN: "0/1000000"}},
			},
		},
	}

	h.backfillFirstSeenLSN(benchRel(), 0x16B6B38)

	got, ok := h.basePosition.LastSchemaVersion("public.bench_source_0")
	is.True(ok)
	is.Equal(got.FirstSeenLSN, "0/1000000")
}

// TestBackfillFirstSeenLSN_NoHistory asserts backfill is a no-op when the
// position records nothing for the table (and never mutates SchemaHistory).
func TestBackfillFirstSeenLSN_NoHistory(t *testing.T) {
	is := is.New(t)
	h := &CDCHandler{basePosition: position.Position{Type: position.TypeCDC}}

	h.backfillFirstSeenLSN(benchRel(), 0x16B6B38)

	_, ok := h.basePosition.LastSchemaVersion("public.bench_source_0")
	is.True(!ok)
}

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

	"github.com/jackc/pglogrepl"
	"pgregory.net/rapid"
)

// TestFormatLSN_MatchesPglogrepl asserts formatLSN is byte-for-byte identical to
// pglogrepl.LSN.String. The position bytes the connector writes embed these
// strings, so a difference here would change a serialized position (invariant 2).
func TestFormatLSN_MatchesPglogrepl(t *testing.T) {
	t.Parallel()

	// Deterministic edge cases: zero, the two halves independently, and a full
	// 64-bit value.
	edges := []pglogrepl.LSN{
		0,
		0x1,
		0xF,
		0x10,
		0xFFFFFFFF,
		0x100000000,
		0x16B6B38,
		0x4137515E8,
		0xFFFFFFFFFFFFFFFF,
	}
	for _, lsn := range edges {
		if got, want := formatLSN(lsn), lsn.String(); got != want {
			t.Errorf("formatLSN(%s) = %q, want %q", lsn, got, want)
		}
	}
}

// TestFormatLSN_MatchesPglogrepl_Property covers the whole 64-bit range with
// rapid, so no value (carry between halves, leading-zero half) escapes the
// byte-equivalence property.
func TestFormatLSN_MatchesPglogrepl_Property(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		lsn := pglogrepl.LSN(rapid.Uint64().Draw(t, "lsn"))
		if got, want := formatLSN(lsn), lsn.String(); got != want {
			t.Fatalf("formatLSN(%s) = %q, want %q", lsn, got, want)
		}
	})
}

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

package internal

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestKeyHash(t *testing.T) {
	is := is.New(t)

	a := KeyHash(map[string]any{"id": int64(42), "tenant": "acme"})
	is.Equal(len(a), 16)
	is.True(!strings.Contains(a, "acme"))

	// stable for the same key, regardless of map iteration order
	for i := 0; i < 20; i++ {
		is.Equal(a, KeyHash(map[string]any{"tenant": "acme", "id": int64(42)}))
	}
	// different keys, different hashes
	is.True(a != KeyHash(map[string]any{"id": int64(43), "tenant": "acme"}))
	// type matters: 1 and "1" are different keys
	is.True(KeyHash(map[string]any{"id": int64(1)}) != KeyHash(map[string]any{"id": "1"}))
	// an empty key does not panic
	is.Equal(len(KeyHash(nil)), 16)
}

func TestRowRef(t *testing.T) {
	is := is.New(t)

	ref := RowRef(map[string]any{"tenant": "acme-SECRET", "id": int64(8675309)})
	is.True(strings.HasPrefix(ref, "columns id,tenant, key hash "))
	is.True(!strings.Contains(ref, "SECRET"))
	is.True(!strings.Contains(ref, "8675309"))
	is.Equal(ref, RowRef(map[string]any{"id": int64(8675309), "tenant": "acme-SECRET"}))
}

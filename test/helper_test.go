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

package test

import (
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/matryer/is"
)

var unquotedIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func TestRandomIdentifier_LongNameBoundedAndUnique(t *testing.T) {
	// Subtest names are appended to the parent's, so this name is several
	// hundred bytes and contains characters that are invalid unquoted.
	t.Run(strings.Repeat("Very Long-Sub/Test Name ", 20), func(t *testing.T) {
		is := is.New(t)
		is.True(len(t.Name()) > 4*maxIdentifierLen)

		const n = 10000
		seen := make(map[string]struct{}, n)
		for range n {
			id := RandomIdentifier(t)
			is.True(len(id) <= maxIdentifierLen) // Postgres would truncate it
			is.True(unquotedIdent.MatchString(id))
			_, dup := seen[id]
			is.True(!dup) // duplicate identifier
			seen[id] = struct{}{}
		}
	})
}

func TestRandomIdentifier_ConcurrentUnique(t *testing.T) {
	is := is.New(t)
	const workers, per = 8, 1000
	var mu sync.Mutex
	seen := make(map[string]struct{}, workers*per)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]string, per)
			for i := range ids {
				ids[i] = RandomIdentifier(t)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()
	is.Equal(len(seen), workers*per)
}

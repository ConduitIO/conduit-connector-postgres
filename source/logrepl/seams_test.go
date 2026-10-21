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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestSeamsOnlyAssignedInTests: the package-level test seams are nil in
// production and are assigned only in _test.go files. Production code that
// assigned one would change slot handling for every pipeline in the process.
func TestSeamsOnlyAssignedInTests(t *testing.T) {
	assign := regexp.MustCompile(`\b(afterSlotCheck|AfterSlotCreateHook)\s*=[^=]`)
	for _, dir := range []string{".", "internal"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for i, line := range strings.Split(string(src), "\n") {
				if code, _, _ := strings.Cut(line, "//"); assign.MatchString(code) {
					t.Errorf("%s:%d assigns a test seam outside a _test.go file: %s", f, i+1, strings.TrimSpace(line))
				}
			}
		}
	}
}

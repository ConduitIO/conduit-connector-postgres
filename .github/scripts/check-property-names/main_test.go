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


package main

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestCheck runs the checker over two fixture packages. testdata/ok holds
// every shape the rule allows: an aliased import, a helper chain, MakeCheck
// in a Property test, a method whose name contains Property, recursive and
// mutually recursive helpers, and a package-level function variable.
// testdata/bad holds one function per way to break it.
func TestCheck(t *testing.T) {
	bad, err := check(filepath.Join("testdata", "ok"))
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) != 0 {
		t.Fatalf("testdata/ok reported:\n%s", strings.Join(bad, "\n"))
	}

	bad, err = check(filepath.Join("testdata", "bad"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range bad {
		// "path:line:col: name calls ..." -> name
		got = append(got, strings.Fields(strings.SplitN(line, ": ", 2)[1])[0])
	}
	slices.Sort(got)
	want := []string{"S.run", "TestMake", "TestPlain", "badVar", "cyc", "gen", "made", "shared", "tick", "unused"}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("testdata/bad reported %v, want %v", got, want)
	}
}

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

// Command check-property-names fails when a rapid property would be left out
// of the nightly long run. property-nightly.yml selects that run with
// -run Property, so every rapid.Check or rapid.MakeCheck call must be
// reached only from tests whose name contains "Property".
//
// The rule, per function in a _test.go file that calls rapid.Check or
// rapid.MakeCheck (inside the function or a closure in it):
//
//   - its name contains "Property": fine;
//   - otherwise it must be a package-level helper (not a Test, Fuzz,
//     Benchmark or Example function, not a method) that is called at least
//     once, and only from functions in the same package that pass this
//     rule themselves. Recursion is fine as long as every way into the
//     cycle comes from a Property test. A package-level var holding a
//     function literal counts as a helper.
//
// A helper called from a method, through a function value, or from another
// package cannot be followed, and is reported: name it with "Property".
//
// Run from the repository root: go run ./.github/scripts/check-property-names
// Directories starting with "." are not part of ./..., so this program is
// not built or linted with the module's packages.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const rapidPath = "pgregory.net/rapid"

type fn struct {
	name       string
	pos        token.Position
	method     bool
	callsCheck bool
	callees    map[string]bool // package-level functions it calls by name
}

type pkg struct {
	funcs map[string]*fn
}

func main() {
	bad, err := check(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(bad) > 0 {
		for _, b := range bad {
			fmt.Println(b)
		}
		fmt.Fprintln(os.Stderr, `property-nightly.yml runs -run Property, so these would never get the long run. `+
			`Put "Property" in the test's name (for example TestX_PropertyY), or call the helper only from such tests.`)
		os.Exit(1)
	}
}

// check walks the _test.go files under root, skipping directories named
// testdata or vendor or starting with "." or "_", and returns one line per
// function that breaks the rule, sorted.
func check(root string) ([]string, error) {
	fset := token.NewFileSet()
	pkgs := map[string]*pkg{} // key: directory + package name

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		key := filepath.Dir(path) + "#" + f.Name.Name
		p := pkgs[key]
		if p == nil {
			p = &pkg{funcs: map[string]*fn{}}
			pkgs[key] = p
		}
		collect(fset, f, p)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var bad []string
	for _, p := range pkgs {
		helpers := allowedHelpers(p)
		for _, f := range p.funcs {
			if f.callsCheck && !strings.Contains(f.name, "Property") && !helpers[f.name] {
				bad = append(bad, fmt.Sprintf("%s: %s calls rapid.Check but is not reached only from tests named *Property*", f.pos, f.name))
			}
		}
	}
	sort.Strings(bad)
	return bad, nil
}

// collect records every function declared in f, whether it calls
// rapid.Check or rapid.MakeCheck, and which package-level functions it calls.
func collect(fset *token.FileSet, f *ast.File, p *pkg) {
	rapidName := ""
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if path != rapidPath {
			continue
		}
		rapidName = "rapid"
		if imp.Name != nil {
			rapidName = imp.Name.Name
		}
	}
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body == nil {
				continue
			}
			name := d.Name.Name
			if d.Recv != nil {
				name = recvName(d.Recv) + "." + name
			}
			p.funcs[name] = inspect(fset, rapidName, name, d.Pos(), d.Recv != nil, d.Body)
		case *ast.GenDecl:
			// Package-level variables: var check = func(t *testing.T) { ... }
			// is called like a function, and its body counts as one.
			for _, spec := range d.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, value := range vs.Values {
					if i < len(vs.Names) && vs.Names[i].Name != "_" {
						p.funcs[vs.Names[i].Name] = inspect(fset, rapidName, vs.Names[i].Name, vs.Names[i].Pos(), false, value)
					}
				}
			}
		}
	}
}

// inspect records whether node calls rapid.Check or rapid.MakeCheck, and
// which package-level functions it calls by name.
func inspect(fset *token.FileSet, rapidName, name string, pos token.Pos, method bool, node ast.Node) *fn {
	entry := &fn{name: name, pos: fset.Position(pos), method: method, callees: map[string]bool{}}
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			if x, ok := fun.X.(*ast.Ident); ok && rapidName != "" && x.Name == rapidName &&
				(fun.Sel.Name == "Check" || fun.Sel.Name == "MakeCheck") {
				entry.callsCheck = true
			}
		case *ast.Ident:
			entry.callees[fun.Name] = true
		case *ast.IndexExpr: // a generic call, f[T](...)
			if id, ok := fun.X.(*ast.Ident); ok {
				entry.callees[id.Name] = true
			}
		}
		return true
	})
	return entry
}

func recvName(recv *ast.FieldList) string {
	if len(recv.List) == 0 {
		return ""
	}
	t := recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	switch t := t.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name
		}
	case *ast.IndexListExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name
		}
	}
	return "?"
}

// allowedHelpers returns the helpers the rule in the package doc allows, by
// a fixed point that handles recursion: start from every helper reachable
// from a Property function, then drop any helper that has a caller which is
// neither a Property function nor a helper still in the set, until nothing
// changes. A recursive or mutually recursive helper reached only from
// Property tests stays; a cycle that no Property test enters is never
// reached, and one also entered from another test is dropped.
func allowedHelpers(p *pkg) map[string]bool {
	isProperty := func(f *fn) bool { return strings.Contains(f.name, "Property") }
	isHelper := func(f *fn) bool { return !isProperty(f) && !f.method && !isTestEntry(f.name) }

	ok := map[string]bool{}
	var queue []string
	for _, f := range p.funcs {
		if isProperty(f) {
			queue = append(queue, f.name)
		}
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		for callee := range p.funcs[name].callees {
			if c, found := p.funcs[callee]; found && isHelper(c) && !ok[callee] {
				ok[callee] = true
				queue = append(queue, callee)
			}
		}
	}

	for changed := true; changed; {
		changed = false
		for name := range ok {
			for _, caller := range p.funcs {
				if caller.callees[name] && !isProperty(caller) && !ok[caller.name] {
					delete(ok, name)
					changed = true
					break
				}
			}
		}
	}
	return ok
}

func isTestEntry(name string) bool {
	for _, prefix := range []string{"Test", "Fuzz", "Benchmark", "Example"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

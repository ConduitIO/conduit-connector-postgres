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
//     rule themselves.
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
	fset := token.NewFileSet()
	pkgs := map[string]*pkg{} // key: directory + package name

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != "." && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "vendor") {
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
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var bad []string
	for _, p := range pkgs {
		for _, f := range p.funcs {
			if f.callsCheck && !allowed(p, f, map[string]bool{}) {
				bad = append(bad, fmt.Sprintf("%s: %s calls rapid.Check but is not reached only from tests named *Property*", f.pos, f.name))
			}
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		for _, b := range bad {
			fmt.Println(b)
		}
		fmt.Fprintln(os.Stderr, `property-nightly.yml runs -run Property, so these would never get the long run. `+
			`Put "Property" in the test's name (for example TestX_PropertyY), or call the helper only from such tests.`)
		os.Exit(1)
	}
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
		d, ok := decl.(*ast.FuncDecl)
		if !ok || d.Body == nil {
			continue
		}
		name := d.Name.Name
		if d.Recv != nil {
			name = recvName(d.Recv) + "." + name
		}
		entry := &fn{name: name, pos: fset.Position(d.Pos()), method: d.Recv != nil, callees: map[string]bool{}}
		ast.Inspect(d.Body, func(n ast.Node) bool {
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
		p.funcs[name] = entry
	}
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

// allowed applies the rule in the package doc. seen breaks call cycles: a
// cycle with no Property test on it is not allowed.
func allowed(p *pkg, f *fn, seen map[string]bool) bool {
	if strings.Contains(f.name, "Property") {
		return true
	}
	if f.method || isTestEntry(f.name) || seen[f.name] {
		return false
	}
	seen[f.name] = true
	defer delete(seen, f.name) // seen is the current call path, not every visit
	callers := 0
	for _, caller := range p.funcs {
		if !caller.callees[f.name] {
			continue
		}
		callers++
		if !allowed(p, caller, seen) {
			return false
		}
	}
	return callers > 0
}

func isTestEntry(name string) bool {
	for _, prefix := range []string{"Test", "Fuzz", "Benchmark", "Example"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

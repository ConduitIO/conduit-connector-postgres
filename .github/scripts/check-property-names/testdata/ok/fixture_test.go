package ok

import (
	"testing"

	r "pgregory.net/rapid"
)

func TestX_PropertyA(t *testing.T) { r.Check(t, func(*r.T) {}) }
func TestY_PropertyB(t *testing.T) { helper(t); t.Run("x", r.MakeCheck(func(*r.T) {})) }
func TestZ_PropertyC(t *testing.T) { helper(t); mid(t) }
func mid(t *testing.T)             { helper(t) }
func helper(t *testing.T)          { r.Check(t, func(*r.T) {}) }

type S struct{}

func (S) checkProperty(t *testing.T) { r.Check(t, func(*r.T) {}) }
func TestNoRapid(t *testing.T)       {}

// Recursive and mutually recursive helpers reached from a Property test.
func TestR_PropertyRecursive(t *testing.T) { rec(t, 2); ping(t, 2) }
func rec(t *testing.T, n int) {
	if n > 0 {
		rec(t, n-1)
	}
	r.Check(t, func(*r.T) {})
}
func ping(t *testing.T, n int) {
	if n > 0 {
		pong(t, n-1)
	}
	r.Check(t, func(*r.T) {})
}
func pong(t *testing.T, n int) {
	if n > 0 {
		ping(t, n-1)
	}
}

// A package-level var holding a function literal, called from a Property test.
var varHelper = func(t *testing.T) { r.Check(t, func(*r.T) {}) }

func TestV_PropertyVar(t *testing.T) { varHelper(t) }

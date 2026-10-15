package bad

import (
	"testing"

	"pgregory.net/rapid"
)

func TestPlain(t *testing.T)          { rapid.Check(t, func(*rapid.T) {}) }
func TestMake(t *testing.T)           { t.Run("x", rapid.MakeCheck(func(*rapid.T) {})) }
func TestUsesHelper(t *testing.T)     { shared(t) }
func TestA_PropertyUses(t *testing.T) { shared(t) }
func shared(t *testing.T)             { rapid.Check(t, func(*rapid.T) {}) }
func unused(t *testing.T)             { rapid.Check(t, func(*rapid.T) {}) }

type S struct{}

func (*S) run(t *testing.T)   { rapid.Check(t, func(*rapid.T) {}) }
func gen[T any](t *testing.T) { rapid.Check(t, func(*rapid.T) {}) }
func TestG(t *testing.T)      { gen[int](t) }
func cyc(t *testing.T)        { cyc(t); rapid.Check(t, func(*rapid.T) {}) }

// A mutually recursive pair that no Property test enters.
func TestEntersPair(t *testing.T) { tick(t) }
func tick(t *testing.T)           { tock(t); rapid.Check(t, func(*rapid.T) {}) }
func tock(t *testing.T)           { tick(t) }

// A package-level var holding a function literal, called from a plain test.
var badVar = func(t *testing.T) { rapid.Check(t, func(*rapid.T) {}) }

func TestUsesVar(t *testing.T) { badVar(t) }

// A package-level MakeCheck value.
var made = rapid.MakeCheck(func(*rapid.T) {})

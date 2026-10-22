# Guard markers

A guard is an `if` statement (or a call statement) that keeps an invariant from being violated: a
refusal to start, a check before an ack. A test suite that stays green when the guard is deleted
does not protect it, so guards are marked and checked mechanically.

Put `// guard: <name>` on the line directly above the statement, or trailing it. Names are lowercase
and unique. Optional tokens:

- `tests=<regexp>` limits `go test -run` for this guard.
- `pkgs=./a,./b` adds test packages (the guard's own package is always tested).
- `env=KEY=VALUE` sets an environment variable for the run (for example `CONDUIT_PG_SLOT_INVALIDATION=1`,
  which the invalidated-slot test needs, or it skips and the guard survives).

`make guardmut` runs `tools/guardmut`: for each marked guard it replaces an `if` condition with
`false && (cond)` (or does not run the call), runs the tests, and requires them to fail. Nothing is
written to disk (`go test -overlay`). A guard whose tests pass, or whose `tests=` matches no test,
survived. A survivor fails the check unless `.github/guardmut-baseline.txt` lists it with a reason, and
a listed guard that is now killed fails too, so the list only shrinks.

Mark refusals: the statement whose removal means "proceed anyway". A condition that has to hold on
the success path (`if !ok { return nil }`) is not a guard, and neither is a `switch` case.

`tools/guardmut` is a copy of the tool in `ConduitIO/conduit` (`tools/guardmut`). The two repos are
separate modules, and the tool is stdlib only, so the copy costs nothing to build; keep the two in
sync until it is published from one place.

## Why not go-mutesting, gremlins or ooze

Those tools apply every operator to every line and report a score, which on this package is hours
of Postgres-backed test runs and a number nobody acts on. The question here is narrow: does a test
notice when this specific refusal is deleted? The marker names the statement, the one mutation that
matters is deleting it, and the verdict is binary per guard, so a survivor names the test to write.
The tool is stdlib only (`go/ast`, `go test -overlay`), lives in the separate `tools/go.mod`
module and adds nothing to the connector's `go.mod`.

## Not marked yet

The guards that exist only on the open slot-identity change (#447: beacon, slot identity, WAL
history) are not marked here, because the code is not on `main`. After #447 merges, mark them in a
follow-up (`// guard:` above each refusal, `env=` where the test needs the invalidation stack), run
`make guardmut`, and baseline or fix what survives.

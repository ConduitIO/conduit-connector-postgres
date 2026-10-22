# Guard markers

A guard is an `if` statement (or a call statement) that keeps an invariant from being violated: a
refusal to start, a check before an ack. A test suite that stays green when the guard is deleted
does not protect it, so guards are marked and checked mechanically.

Put `// guard: <name> tests=<regexp>` on the line directly above the statement, or trailing it. Names are
lowercase and unique and every one is listed in `.github/guardmut-guards.txt`, so deleting a marker fails the
gate. `tests=` is required: it limits `go test -run` and names the test(s) that must fail when the guard is
removed. A kill counts only when a named test selected by `tests=` fails; a package-level failure (panic,
`TestMain`, timeout) is an error, not a kill. Optional tokens:

- `pkgs=./a,./b` adds test packages (the guard's own package is always tested).
- `env=KEY=VALUE` sets an environment variable for the run (for example `CONDUIT_PG_SLOT_INVALIDATION=1`,
  which the invalidated-slot test needs, or it skips and the guard survives).

`make guardmut` runs `tools/guardmut`: for each marked guard it replaces an `if` condition with
`false && (cond)` (or does not run the call), runs the tests, and requires them to fail. Nothing is
written to disk (`go test -overlay`). A guard whose tests pass, or whose `tests=` matches no test,
survived. A survivor fails the check. `.github/guardmut-baseline.txt` is empty and the workflow fails any PR
that adds a line to it. `make guardmut` runs in its own compose project (`pgguardmut`, ports 56170 and
56171) and runs the guards that need `CONDUIT_PG_SLOT_INVALIDATION=1` alone and last (`-with-env`),
because that mode sets `max_slot_wal_keep_size` cluster-wide.

Mark refusals: the statement whose removal means "proceed anyway". A condition that has to hold on
the success path (`if !ok { return nil }`) is not a guard, and neither is a `switch` case.

`tools/guardmut` is a copy of the tool in `ConduitIO/conduit` (`tools/guardmut`, conduit#3262). The two repos are
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

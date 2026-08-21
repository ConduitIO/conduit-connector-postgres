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

//go:build conduitchaos

package chaos

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// pollUntil blocks until cond() returns true or timeout elapses. It is the
// ONLY place in this package that sleeps on a fixed interval instead of
// waiting on a real signal (a marker line, a process exit, a query result)
// — every other wait in this package is built on top of it. That
// concentration is deliberate and enforced by CI (acceptance criterion
// B0.9: a fixed-interval sleep anywhere in this package's non-comment code
// must appear only in the poll loop a few lines below). A bare
// sleep-then-assert elsewhere would be exactly the flake class
// ConduitIO/conduit's tests/chaos package had to retrofit persistDelayMS to
// remove (harness plan §4, "observed twice in CI, never reproducible
// locally") — a fixed sleep that happens to be long enough today silently
// stops being long enough on a slower runner tomorrow, and fails for a
// reason that has nothing to do with the invariant under test. A polling
// wait with an explicit, generous deadline degrades gracefully instead: it
// only ever gets closer to its timeout, never produces a false pass.
// msg is evaluated ONLY on the timeout path, not on every poll iteration: it
// typically calls childProcess.diagnostics(), which snapshots stdout/stderr
// as of the call. Building it eagerly (as this signature used to require)
// meant every caller froze the child's diagnostics at the moment pollUntil
// was entered rather than at the moment it actually timed out - a child
// that crashed 300ms into a 60s wait would report empty/stale stderr in the
// failure message instead of its real, already-written CHILD_FATAL line.
func pollUntil(t *testing.T, timeout time.Duration, msg func() string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, msg())
		}
		// The one fixed-interval sleep in this package - see the doc
		// comment above.
		time.Sleep(10 * time.Millisecond)
	}
}

// childProcess wraps a running (or exited) chaos child and its observed
// stdout, so a test can wait for specific progress (a marker line, an
// ack count) before deciding when to act - deterministic relative to the
// child's own observed progress, never a blind wall-clock guess. Ported
// from ConduitIO/conduit's tests/chaos/harness.go, which this package's
// child-supervision model is deliberately kept close to (harness plan §2).
type childProcess struct {
	cmd *exec.Cmd

	mu      sync.Mutex
	lines   []string
	scanErr error // sc.Err() from the stdout-scanning goroutine, if it ever failed
	stderr  syncBuffer

	readerDone chan struct{}

	// reapOnce guards cmd.Wait(), which the os/exec docs require be called
	// at most once. sigkill, waitExit, and the spawnChildWithEnv-registered
	// t.Cleanup fallback can all end up trying to reap the same process
	// (e.g. a failed assertion between spawn and the test's own
	// sigkill/waitExit call would otherwise leak the process); routing all
	// of them through reap() makes that safe regardless of which one gets
	// there first.
	//
	// Every caller of reap() MUST first wait on readerDone (see
	// spawnChildWithEnv's stdout-reading goroutine). os/exec's Cmd.StdoutPipe
	// docs are explicit that "it is incorrect to call Wait before all reads
	// from the pipe have completed": Wait's internal cleanup can close the
	// pipe's read end concurrently with the scanner goroutine's in-flight
	// Read, silently truncating the last line(s) a child wrote right before
	// exiting (e.g. its final marker) from cp.lines. Waiting on readerDone
	// first is safe and never deadlocks: the child's stdout fd is closed by
	// the OS as part of process exit - including a SIGKILL exit - with zero
	// dependency on the parent having called Wait(), so readerDone always
	// closes at (or immediately after) the child's actual exit, on its own.
	reapOnce sync.Once
	waitErr  error
}

// reap calls cmd.Wait() exactly once (idempotent, see reapOnce's doc) and
// returns its result on every call.
func (c *childProcess) reap() error {
	c.reapOnce.Do(func() {
		c.waitErr = c.cmd.Wait()
	})
	return c.waitErr
}

// syncBuffer is a bytes.Buffer safe for concurrent Write (from os/exec's
// internal stderr-copying goroutine, for as long as the child is alive) and
// String (from a test goroutine building a diagnostics message while the
// child - and that copy goroutine - are still running, e.g. on a waitExit
// timeout).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// spawnChildWithEnv re-executes the current test binary (os.Args[0]) with
// the given extra environment variables appended, and is the actual
// re-exec-self-as-child-process mechanism every scenario in this package
// uses to get a real, SIGKILL-able OS process. Driving the built plugin
// binary instead was rejected (harness plan §3): it would need a
// v1-protocol gRPC client that only exists in ConduitIO/conduit, and
// importing the engine here inverts the dependency.
func spawnChildWithEnv(t *testing.T, env []string) *childProcess {
	t.Helper()

	exe := os.Args[0]
	if !filepath.IsAbs(exe) {
		resolved, err := os.Executable()
		if err != nil {
			t.Fatalf("resolve test binary path: %v", err)
		}
		exe = resolved
	}

	// exec.Command, not exec.CommandContext(context.Background(), exe): a
	// context.Background() deadline never fires, so CommandContext here
	// bought nothing but the appearance of cancellation support. This
	// package's actual "stop the child" mechanism is sigkill (below),
	// which every scenario calls explicitly.
	cmd := exec.Command(exe)
	// The child's PGCHAOS_ environment is built from scratch, not inherited.
	// Forwarding os.Environ() wholesale made the re-exec protocol additive:
	// with a stray PGCHAOS_REAL_CHILD=1 exported in the shell, an ECHO child
	// would be spawned carrying both sentinels, and main_test.go tests the
	// real-child sentinel first, so it routed into runRealChild and died on a
	// missing PGCHAOS_TOTAL. That failed red rather than green, but it blamed
	// the harness for an environment problem - the exact confusion the
	// parent-PID check was added to remove. Stripping the prefix first makes
	// the protocol hermetic: the child sees precisely the vars this call
	// passed, and nothing a developer happened to export.
	//
	// envParentPID is appended here, not by the caller: it must always be
	// THIS process's real PID, stamped at the moment of spawn - see its doc
	// comment for why isRealChildInvocation/isEchoChildInvocation key their
	// fail-closed check on it.
	cmd.Env = append(append(envWithoutChaosVars(), env...),
		envParentPID+"="+strconv.Itoa(os.Getpid()))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}

	cp := &childProcess{cmd: cmd, readerDone: make(chan struct{})}
	cmd.Stderr = &cp.stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Fallback safety net: if the test returns early (e.g. a failed
	// assertion between spawn and the test's own sigkill/waitExit call)
	// without explicitly reaping this child, don't leave a live or zombie
	// process behind. Harmless (a no-op beyond the Kill call) if the test
	// already reaped it - see reap()'s doc comment. Drains (waits for
	// readerDone) BEFORE reaping - see reap()'s doc comment on why order
	// matters here.
	t.Cleanup(func() {
		if cp.cmd.Process != nil {
			_ = cp.cmd.Process.Kill()
		}
		<-cp.readerDone
		_ = cp.reap()
	})

	go func() {
		defer close(cp.readerDone)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			cp.mu.Lock()
			cp.lines = append(cp.lines, line)
			cp.mu.Unlock()
		}
		cp.mu.Lock()
		cp.scanErr = sc.Err()
		cp.mu.Unlock()
	}()

	return cp
}

// exited reports whether the child's stdout-reading goroutine has finished
// (readerDone closed), i.e. the child process has exited - cleanly, crashed,
// or SIGKILLed - and the OS has closed its stdout fd. Non-blocking. See
// readerDone's doc comment on childProcess: it closes at/after the child's
// actual exit regardless of whether the parent has called Wait() yet, so
// this is safe to poll from waitForMarker/waitForCount without racing reap().
func (c *childProcess) exited() bool {
	select {
	case <-c.readerDone:
		return true
	default:
		return false
	}
}

func (c *childProcess) linesSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// line returns the first observed line with the given prefix, and whether
// one was found.
func (c *childProcess) line(prefix string) (string, bool) {
	for _, l := range c.linesSnapshot() {
		if strings.HasPrefix(l, prefix) {
			return l, true
		}
	}
	return "", false
}

// progressCount returns how many observed lines carry the given prefix -
// used to wait for "at least N acks/reaches observed" without caring about
// the exact values, e.g. child.go's "ACKED <n>" progress lines.
func (c *childProcess) progressCount(prefix string) int {
	n := 0
	for _, l := range c.linesSnapshot() {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func (c *childProcess) diagnostics() string {
	// stderr is its own syncBuffer (self-synchronizing, see its doc), not
	// guarded by c.mu - c.mu only ever protected lines. Reading it while the
	// child is still alive races against os/exec's internal io.Copy
	// goroutine, which keeps writing to it for as long as the child is
	// alive; syncBuffer makes that race benign.
	c.mu.Lock()
	scanErr := c.scanErr
	c.mu.Unlock()

	// scanErr is non-nil only if the stdout scanner itself failed (e.g. a
	// line exceeding its buffer, or a genuine read error) - surfaced here so
	// a timeout waiting for a marker that was actually lost to a scan
	// failure doesn't look identical to "the child never printed it".
	if scanErr != nil {
		return fmt.Sprintf("stdout lines: %v (scan error: %v)\nstderr: %s", c.linesSnapshot(), scanErr, c.stderr.String())
	}
	return fmt.Sprintf("stdout lines: %v\nstderr: %s", c.linesSnapshot(), c.stderr.String())
}

// waitForMarker blocks (via pollUntil - never a bare sleep) until a line
// with the given prefix has been observed, or fails the test after
// timeout. Used to gate an action on the child's own genuine, printed
// progress (e.g. PARKED, DONE) rather than a wall-clock guess.
func (c *childProcess) waitForMarker(t *testing.T, prefix string, timeout time.Duration) string {
	t.Helper()
	var found string
	pollUntil(t, timeout, func() string {
		return fmt.Sprintf("marker %q\n%s", prefix, c.diagnostics())
	}, func() bool {
		if l, ok := c.line(prefix); ok {
			found = l
			return true
		}
		// c.line() above and c.exited() below are two SEPARATE reads of
		// independent state (c.lines under c.mu; readerDone via a channel
		// receive) - not one atomic observation. The scanner goroutine
		// does append every line before closing readerDone (see
		// spawnChildWithEnv), but that only means the append
		// happens-before the close; it says nothing about when THIS
		// goroutine's read of c.lines happened relative to either. If the
		// scanner appends the final marker and closes readerDone in the
		// gap between the c.line() call above and this c.exited() call,
		// the snapshot c.line() already took is stale and genuinely
		// missed a marker that was, in fact, written - a spurious
		// t.Fatalf below despite the child behaving correctly. Not a data
		// race (-race is clean here; the channel op is synchronized), just
		// a stale-read race, amplified by high-volume children that print
		// their final marker and exit immediately (see this function's
		// package-level flake writeup). Re-checking c.line() AFTER
		// observing exited() fixes it: that observation is an acquire
		// against the mutex-protected appends, so this second read is
		// guaranteed to see everything the child ever wrote.
		if c.exited() {
			if l, ok := c.line(prefix); ok {
				found = l
				return true
			}
			// If we get this far the marker genuinely never arrived - a
			// dead child would otherwise burn the full timeout and blame
			// the wrong thing (it looks identical to "still running, just
			// slow"). Fresh diagnostics, not pollUntil's (possibly much
			// later) timeout message.
			t.Fatalf("child exited before marker %q\n%s", prefix, c.diagnostics())
		}
		return false
	})
	return found
}

// waitForCount blocks (via pollUntil) until at least n lines with the given
// prefix have been observed, or fails the test after timeout.
func (c *childProcess) waitForCount(t *testing.T, prefix string, n int, timeout time.Duration) {
	t.Helper()
	pollUntil(t, timeout, func() string {
		return fmt.Sprintf("%d lines with prefix %q\n%s", n, prefix, c.diagnostics())
	}, func() bool {
		if c.progressCount(prefix) >= n {
			return true
		}
		// See waitForMarker's identical shape for why this re-checks
		// AFTER observing exited(), rather than trusting the snapshot
		// already taken above: the two reads are not atomic, and a
		// child that appends its Nth line and closes readerDone in the
		// gap between them would otherwise cause a spurious t.Fatalf here.
		if c.exited() {
			if c.progressCount(prefix) >= n {
				return true
			}
			t.Fatalf("child exited before %d lines with prefix %q\n%s", n, prefix, c.diagnostics())
		}
		return false
	})
}

// sigkill sends SIGKILL (not SIGTERM, not context cancellation) and reaps
// the process - no cleanup, no graceful shutdown, no final flush. This
// package has no SIGTERM scenario (unlike ConduitIO/conduit's tests/chaos):
// DBZ-3's crash-safety question is specifically about a hard kill.
func (c *childProcess) sigkill(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL child (pid %d): %v", c.cmd.Process.Pid, err)
	}
	// Drain before reap - see reapOnce's doc comment on childProcess for why
	// the order is load-bearing, not stylistic.
	<-c.readerDone
	_ = c.reap() // a "signal: killed" wait error is expected here, not a failure
}

// waitExit blocks until the child exits on its own, failing the test if it
// doesn't within timeout or exits with a non-zero code.
func (c *childProcess) waitExit(t *testing.T, timeout time.Duration) {
	t.Helper()

	// Drain before reap - see reapOnce's doc comment on childProcess. Once
	// the child exits (cleanly or otherwise), the OS closes its stdout fd on
	// its own, so readerDone closing needs no help from - and must not be
	// raced against - this parent calling Wait() below.
	select {
	case <-c.readerDone:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for child to exit\n%s", c.diagnostics())
		return
	}

	if err := c.reap(); err != nil {
		t.Fatalf("child exited unexpectedly: %v\n%s", err, c.diagnostics())
	}
}

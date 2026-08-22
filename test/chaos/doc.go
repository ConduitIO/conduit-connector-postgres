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

// Package chaos is the DBZ-3 process-kill chaos harness for
// conduit-connector-postgres (v0.20 Workstream 7, "DBZ-3 B0 — process-kill
// test harness", plan doc: docs/design-documents/20260821-dbz3-b0-kill-harness.md
// — every "harness plan §N" citation in this package's comments refers to
// that file's numbered sections). It supervises a
// real *postgres.Source (this repo's Source, github.com/conduitio/
// conduit-connector-postgres) driven directly — no sdk.Serve, no engine, no
// gRPC — against a real, dockerized Postgres, so scenarios can assert
// exactly what a crash does and does not lose.
//
// # Layout and build tags
//
// doc.go, ledger.go and ledger_test.go carry no build tag: they compile and
// run under a plain `go test ./...`, which is why `make test` (and CI's
// default `test` job) always exercises the ledger analyzer, even though it
// never touches Postgres. A directory whose Go files are ALL excluded by a
// build tag is a build error for tools that expect every package to at
// least type-check, so this split is load-bearing, not a style choice.
//
// Everything that dials Postgres, re-execs a child process, or reads
// internal/chaospoint's counters is gated behind the conduitchaos build
// tag and only ever compiles/runs via `make test-chaos` (test/
// docker-compose.chaos.yml, port 5434) or the chaos.yml CI job. No release
// build (.goreleaser.yml, .github/workflows/publish.yml) passes -tags, so
// none of this package — nor internal/chaospoint's real parking
// implementation — ever reaches a shipped binary.
//
// # The supervision model, and what "child" means here
//
// A scenario re-execs the test binary itself (os.Args[0]) as a child OS
// process (harness.go's spawnChildWithEnv), with TestMain (main_test.go)
// intercepting on an environment-variable sentinel and routing into
// child.go's runRealChild or runEchoChild instead of running the package's
// actual Go tests.
// The child constructs a real *postgres.Source exactly the way
// source_integration_test.go's TestSource_Open/TestSource_Read already
// prove works outside sdk.Serve — sdk.Util.ParseConfig, then Open, then a
// ReadN(ctx, 1) loop — and additionally drives Ack and Teardown, which
// nothing in this repo had exercised outside a real `conduit run` before
// this package. See the B0 harness plan's finding F-1 for why that
// specific gap (Ack, the SDK batch middleware's read-ahead goroutine, and
// the schema middleware, all outside sdk.Serve, all over more than a single
// read) mattered enough to gate every kill scenario behind a smoke test
// that proves the supervision model at all: smoke_test.go's
// TestSmoke_NoKillOpenReadAckTeardown ships in this same change, before any
// SIGKILL scenario, specifically so a broken supervision model fails loud
// on day one instead of silently under a kill three files later.
//
// # The ledger, and what it is not
//
// The child's only durable downstream is ledger.go's append-only,
// fsync-before-return, CRC-checked JSON-lines Ledger. It exists to make
// "did we lose or duplicate a delivery" a property of data the test itself
// generated and can reread — not an inference from log lines or timing.
// Ledger.AppendSync's enforcement-site comment states the invariant this
// package models: append (and fsync) strictly before the corresponding
// Source.Ack call, mirroring the engine's post-045f283 persist-before-ack
// ordering (see ConduitIO/conduit's docs/design-documents/
// 20260723-source-ack-persist-ordering-fix.md and tests/chaos/sigkill_test.go
// in that repo). That is a MODEL of the engine's ordering, not the engine
// itself, and it can drift from it — the real ordering guarantee for a
// production pipeline is proven in ConduitIO/conduit, not here.
//
// # What this package does NOT prove
//
// This suite proves properties of this repo's Go source-connector code
// (source.Source, its snapshot and logrepl iterators) driven in-process by
// a re-exec'd child. It deliberately does NOT prove, and no scenario here
// should be read as evidence of:
//
//   - The gRPC plugin transport (conduit-connector-protocol) — nothing here
//     runs this connector as an out-of-process plugin over gRPC. A crash
//     mid-RPC, a partial protobuf frame, or a transport-level retry is out
//     of scope; ConduitIO/conduit-connector-protocol's own test suite is
//     the right place for that.
//   - The engine's Persister and its debounced flush-to-disk ordering
//     (pkg/connector/persister.go in ConduitIO/conduit) — this harness's
//     Ledger is a stand-in for "durable downstream," not that component.
//     ConduitIO/conduit's tests/chaos/sigkill_test.go is what actually
//     proves the engine's ack-before-persist window; see this file's
//     Ledger note above.
//   - The SDK's out-of-process serving loop (sdk.Serve, conduit-connector-sdk)
//     — every scenario here calls Source methods directly in the same
//     process that constructed them, per the supervision model above. The
//     gRPC server loop sdk.Serve normally runs is never started.
//
// # Scope of this change (B0-2)
//
// This change delivers the harness mechanism itself — spawn/kill/reap
// plumbing, the ledger, parent-side slot introspection (pgstate.go), and
// the no-kill smoke scenario — and nothing that actually kills a child.
// The AC 2 (resumable snapshot) and AC 10 (snapshot→CDC handoff) SIGKILL
// scenarios are separate, later changes (B0-3, B0-4 in the harness plan);
// this package does not yet claim either criterion is closed.
package chaos

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

// Package externalstate is the external-state chaos suite for the source.
//
// The connector's correctness depends on state it does not own: the
// replication slot, the WAL behind it, the role's privileges, the network
// between it and the server, and the identity of the database itself (a
// failover or a restore from a volume snapshot). Each of those can change
// between two runs, or during one, without the connector being told. The
// suite changes them on purpose and asserts the only two acceptable
// outcomes (no silent loss):
//
//   - every row written is delivered, or
//   - the start (or the running read) fails with a code a machine can match:
//     a logrepl.SlotError code, or a Postgres SQLSTATE.
//
// A stalled read that neither delivers nor fails is a third, unacceptable
// outcome, and so is an Open that succeeds and skips rows.
//
// test/externalstate/cases.txt lists every case and the test that proves it.
// Some cases reuse tests that already live in other packages (cited, not
// copied); run.sh runs them all, checks that every required test actually
// ran, and reports pending and known-gap cases explicitly. See `make
// test-external-state`.
//
// The tests in this package need the externalstate build tag and the stack in
// docker-compose.yml, so a plain `go test ./...` compiles only this file.
package externalstate

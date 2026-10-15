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

//go:build debezium

// Package debezium is the end-to-end test of the Debezium offset import
// (design: docs/design-documents/20261012-debezium-offset-import.md, "Tests >
// Integration harness: real Debezium"). It runs a real Debezium Postgres
// connector on Kafka Connect, performs the fresh-slot cutover of D3 against
// it, hands this repo's Postgres source the offset Debezium committed
// (logrepl.import.debeziumOffset), and diffs what Debezium put in Kafka
// against what the source delivered.
//
// # What it proves
//
// Every row the workload committed is in Debezium's topics or in Conduit's
// output (invariant 3: no loss). The rows in both are inside the duplicate
// set the plan predicts (S2), computed exactly from a probe slot that decodes
// the same WAL. Conduit's own output has no duplicates. After the acks,
// Conduit's slot has moved past Lc.
//
// # Running it
//
// Do not run it with a bare `go test`: the stack is a compose project that
// run.sh creates, with ephemeral host ports, and removes with `down -v`.
//
//	make test-debezium
//	DBZ_VERSION=2.7 make test-debezium
//	DBZ_PLUGIN=decoderbufs make test-debezium
//
// run.sh passes the addresses through DBZ_PG_ADDR, DBZ_CONNECT_URL,
// DBZ_COMPOSE_PROJECT and DBZ_COMPOSE_FILE. A missing variable is an
// "INFRA:" failure, never a skip, the same as test/chaos.
//
// # Layout
//
//   - stack_test.go: the stack handle, Kafka Connect REST and polling.
//   - workload_test.go: the ledgered workload (single-row transactions, one
//     large transaction, a COPY).
//   - observe_test.go: reading Debezium's topics, the probe slot and
//     Conduit's output.
//   - cutover_test.go: the cutover and the assertions.
package debezium

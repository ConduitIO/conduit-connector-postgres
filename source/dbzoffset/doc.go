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

// Package dbzoffset translates a Debezium Postgres connector offset into the
// point where this connector resumes, so a pipeline migrated from Debezium
// starts without a re-snapshot and without skipping a change Debezium did not
// deliver.
//
// The design is docs/design-documents/20261012-debezium-offset-import.md
// (conduit-connector-postgres#341); section names below (D4, S2, ...) refer to
// it. This package is slice 1 of that design: parsing, validation and the
// resume-point computation. It has no I/O. The checks that need Postgres (the
// D5 preflight) and the wiring into the replication stream live elsewhere.
//
// # Core types
//
//   - [Setting] is the JSON value of the connector setting
//     `logrepl.import.debeziumOffset` (D2). `conduit migrate kafka-connect`
//     writes it; [ParseSetting] reads and validates it.
//   - [Offset] is Debezium's raw offset map, parsed. [ParseOffset] runs the
//     offset-only checks of S5 (items 1 to 4) in the design's order.
//   - [Plan] is the resume point computed from an offset (D4): skip every
//     transaction whose commit LSN is below Debezium's `lsn_commit`, plus, for
//     an offset taken mid-transaction, the partial rule for that transaction.
//   - [Filter] applies a Plan to a replication stream. The connector's
//     subscription and the CLI's `--peek` dry run use the same Filter, so the
//     dry run predicts exactly what the connector skips.
//   - [Error] carries a stable code (`postgres.import.*`), the config path and
//     a suggested fix. The CLI passes the code through unchanged.
//
// # Invariants
//
// Invariant 3 (at-least-once): every change a Plan skips has a proof that
// Debezium delivered it. The proofs are the "No loss" cases of the design:
//
//   - Base rule. A transaction whose commit LSN (BeginMessage.FinalLSN, the
//     start of its commit record) is below Lc (`lsn_commit`, the END of the
//     last commit record Debezium processed) committed at or before that
//     transaction, because commit records do not overlap. Debezium emitted
//     and Kafka Connect acknowledged every record of it before the offset.
//     The two LSN domains differ (start vs end of a commit record), so the
//     comparison is strictly below, never at or below (debezium/dbz#76).
//   - Partial rule. Inside the one transaction the offset was taken in (xid
//     X, and commit time ts_usec when present), pgoutput and decoderbufs send
//     changes in WAL order, so a change whose LSN is below the offset's `lsn`
//     was emitted before the offset's record. Changes AT that LSN are never
//     skipped: rows of one COPY share an LSN, and a multi-table TRUNCATE is
//     one message here but one event per relation in Debezium (B2).
//
// Anything a Plan cannot prove is delivered again: an unknown message type,
// a transactional logical message, a missing xid, a commit-time mismatch, a
// restart after the first ack. Those are duplicates, never loss.
//
// Invariant 2: the import writes no position of its own. A Plan lives only
// in memory. It is recomputed from the setting at every start that has an
// empty position, so a crash before the first ack recomputes the same Plan
// (the import is idempotent), and the first ack writes an ordinary version 2
// position, after which the setting is ignored.
//
// Refuse rather than guess: every input this package cannot prove safe fails
// with a coded Error. It never falls back to an approximate rule.
package dbzoffset

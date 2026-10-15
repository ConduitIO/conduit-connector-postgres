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

// Package debezium builds the Postgres source's records in the Debezium
// mode (debezium.enabled): one record per WAL change, with the values in
// Debezium's types and the debezium.* metadata the debezium.encode processor
// (ConduitIO/conduit, pkg/plugin/processor/builtin/impl/debezium) turns into
// the bytes a Debezium Postgres connector writes. The design is ConduitIO/conduit
// docs/design-documents/20261012-debezium-compatible-output.md.
//
// Core types:
//
//   - Settings: the debezium.* settings and their validation, including the
//     standalone-build refusal (decision 18, MarkStandalone).
//   - Builder: Relation, Begin, Commit, Insert, Update, Delete and Skipped,
//     called by the logrepl handler on the subscription goroutine.
//   - table and column: one relation shape, built from its Relation message and
//     a catalog read (nullability, defaults, primary key, enum members, which
//     pgoutput does not send), with the Connect schema of each column and the
//     descriptor text.
//
// Invariants:
//
//   - The package does not touch positions. The handler stamps every record
//     with the position the default mode gives the same change, so ack
//     propagation, flush and resume are unchanged (invariants 1 and 2).
//   - Every column is in every row; NULL is nil, never an absent key. An
//     unchanged TOAST value is the placeholder, never NULL (invariant 6).
//   - A value the Debezium type cannot hold, a type the mode does not map, or a
//     delete whose old row lacks the key fails with a coded *Error; nothing is
//     rounded, replaced or dropped (invariants 3 and 6).
//   - Decoding reads pgoutput's text form under pinned session settings
//     (ReplicationSessionParams), so server, database and role settings such as
//     TimeZone and DateStyle do not change the values.
//   - Transaction orders count every event Debezium emits for the transaction,
//     including the changes a resume skips (Skipped), and a primary-key change
//     counts two.
package debezium

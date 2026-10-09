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

package logrepl

import (
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
)

// SchemaDriftPolicy is the `logrepl.schemaDrift.policy` config value: what the
// connector does when CDC detects that a table's schema changed (DBZ-3 B1,
// docs/design-documents/20260829-dbz3-b1-schema-drift-escape-hatch.md, Area 2
// of the postgres CDC parity doc).
type SchemaDriftPolicy string

const (
	// SchemaDriftPolicyHalt stops the pipeline: the connector emits a drift
	// marker record, checkpoints before it (the marker's position carries the
	// new schema shape), and then returns a terminal coded error once the
	// marker is acked. A restart is the operator's approval: it resumes from
	// the marker, and the new shape is what the history already records.
	SchemaDriftPolicyHalt SchemaDriftPolicy = "halt"

	// SchemaDriftPolicyEvolve accepts additive schema changes silently.
	// Incompatible changes (drops, retypes that alter the Avro shape, per
	// SchemaDiff.IsIncompatible) still halt: admitting a narrowing change
	// without review would silently mangle data downstream (invariant 6).
	SchemaDriftPolicyEvolve SchemaDriftPolicy = "evolve"

	// SchemaDriftPolicyDLQ is reserved for a future version (route drifted
	// records to a DLQ instead of halting). Not supported in this version;
	// rejected at config validation with ErrorCodeSchemaDriftPolicyUnsupported.
	SchemaDriftPolicyDLQ SchemaDriftPolicy = "dlq"
)

// ParseSchemaDriftPolicy validates and normalizes the configured policy.
//
// The empty string is accepted as halt so that a handler constructed without
// an explicit policy fails closed (halts) rather than silently accepting
// drift. "dlq" and unknown values are rejected; both get the same stable code
// (the caller can distinguish them by message).
func ParseSchemaDriftPolicy(s string) (SchemaDriftPolicy, error) {
	switch SchemaDriftPolicy(s) {
	case "", SchemaDriftPolicyHalt:
		return SchemaDriftPolicyHalt, nil
	case SchemaDriftPolicyEvolve:
		return SchemaDriftPolicyEvolve, nil
	case SchemaDriftPolicyDLQ:
		return "", fmt.Errorf(
			"%s: schema drift policy %q is not supported in this version (supported: %s, %s)",
			ErrorCodeSchemaDriftPolicyUnsupported, s, SchemaDriftPolicyHalt, SchemaDriftPolicyEvolve)
	default:
		return "", fmt.Errorf(
			"%s: unknown schema drift policy %q (supported: %s, %s)",
			ErrorCodeSchemaDriftPolicyUnsupported, s, SchemaDriftPolicyHalt, SchemaDriftPolicyEvolve)
	}
}

// Stable, machine-actionable error codes. The connector's errors carry no
// structured code field today (see Q1 in the B1 design doc), so the token is
// embedded at the start of the message string, engine-style
// (`postgres.schema_drift.halt: ...`).
const (
	// ErrorCodeSchemaDriftHalt prefixes the terminal error returned by NextN
	// once a drift marker has been acked (D5). It is never ErrBackoffRetry:
	// retrying without an explicit operator decision cannot resolve drift, and
	// the acked marker is the durable approval checkpoint.
	ErrorCodeSchemaDriftHalt = "postgres.schema_drift.halt"

	// ErrorCodeSchemaDriftPolicyUnsupported prefixes config-validation errors
	// for an unknown, empty (when the SDK default does not apply), or
	// not-yet-supported schemaDrift.policy value.
	ErrorCodeSchemaDriftPolicyUnsupported = "postgres.schema_drift.policy.unsupported"
)

// Drift-marker metadata keys (D1). Keys follow the `postgres.snapshot.resumed`
// precedent. The marker record itself is OperationCreate with nil key and nil
// payload; all evidence lives in this metadata.
const (
	// MetadataSchemaDrift marks the record as a drift marker ("true").
	MetadataSchemaDrift = "postgres.schema.drift"
	// MetadataSchemaDriftTable is the affected table as `namespace.table`.
	MetadataSchemaDriftTable = "postgres.schema.drift.table"
	// MetadataSchemaDriftLSN is the LSN of the first DML that used the new
	// shape — the marker's position LastLSN. pgoutput delivers the
	// RelationMessage with WALStart 0, so the relation message itself carries
	// no usable LSN (see emitDriftMarker).
	MetadataSchemaDriftLSN = "postgres.schema.drift.lsn"
	// MetadataSchemaDriftPolicy is the policy in effect when the marker was
	// emitted (halt, dlq, or evolve), so a later version's DLQ approval logic
	// can recognize markers from this version (D6: marker-shape
	// forward-compatibility only).
	MetadataSchemaDriftPolicy = "postgres.schema.drift.policy"
	// MetadataSchemaDriftNarrowing is "true" or "false" for driftInProcess
	// markers: whether the change is incompatible under the evolve rules.
	MetadataSchemaDriftNarrowing = "postgres.schema.drift.narrowing"
	// MetadataSchemaDriftDiff is the operator-facing column diff
	// (SchemaDiff.String()), present only for driftInProcess markers.
	MetadataSchemaDriftDiff = "postgres.schema.drift.diff"
)

// haltRevertTrap is the D5 sentence that makes the escape hatch
// self-explanatory. It is load-bearing, word for word: an operator who
// approved by restarting and then reverted the DDL must learn from the second
// halt message that the revert itself halts once more — the approved shape is
// still the last checkpointed one. AC5 asserts both halt messages contain it.
const haltRevertTrap = "Restart this pipeline to approve the change, or revert the DDL" +
	" — a restart after reverting halts once more before resuming (the approved" +
	" shape is still the last checkpointed one)."

// newDriftHaltError builds the D5 terminal error for a halting drift kind.
//
// driftInProcess renders the full column diff (the diff exists — this process
// saw the previous shape). driftAcrossRestart renders only what survived: the
// durable hash transition and the previous shape's FirstSeenLSN. It never
// fabricates a column diff — the columns are not recoverable, and inventing
// them would send an operator chasing a diff that never happened (FM7/AC7).
func newDriftHaltError(
	kind driftKind,
	key string,
	diff internal.SchemaDiff,
	prev position.SchemaVersion,
	hash string,
	lsn pglogrepl.LSN,
) error {
	switch kind {
	case driftInProcess:
		return fmt.Errorf("%s: %s; observed at LSN %s. %s",
			ErrorCodeSchemaDriftHalt, diff.String(), lsn.String(), haltRevertTrap)
	case driftAcrossRestart:
		return fmt.Errorf(
			"%s: table %s changed while the connector was not running"+
				" (schema hash %s -> %s; last durable shape first seen at LSN %s);"+
				" observed at LSN %s. %s Compare against your DDL history for the"+
				" exact columns",
			ErrorCodeSchemaDriftHalt, key,
			prev.ColumnSetHash, hash, prev.FirstSeenLSN,
			lsn.String(), haltRevertTrap)
	default:
		// driftNone/driftInitial never halt; this is defensive and unreachable.
		return fmt.Errorf("%s: unexpected drift kind %d (this smells like a bug)",
			ErrorCodeSchemaDriftHalt, kind)
	}
}

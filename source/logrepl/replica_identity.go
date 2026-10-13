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
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/jackc/pgx/v5"
)

// ErrorCodeReplicaIdentityExcludesKey prefixes the error returned when a
// table's REPLICA IDENTITY does not include the column the connector uses as
// the record key. Under that identity Postgres sends deletes without the key
// value, so the connector would emit a delete with a null key, which a
// destination cannot apply (conduit-connector-postgres#380). It is returned by
// CheckReplicaIdentity at Open, and by the CDC handler if such a delete
// arrives anyway (the identity was changed while the pipeline ran).
const ErrorCodeReplicaIdentityExcludesKey = "postgres.source.replica_identity_excludes_key"

// RowQuerier is the subset of pgx used by CheckReplicaIdentity. *pgxpool.Pool
// and *pgx.Conn satisfy it.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CheckReplicaIdentity returns an error coded
// ErrorCodeReplicaIdentityExcludesKey if the replica identity of table does
// not carry keyColumn in the old tuple of a delete.
//
// DEFAULT (the primary key) and FULL always carry it; the connector keys on
// the primary key, so DEFAULT covers it by construction. USING INDEX carries
// only the index's columns, so the index must contain keyColumn. If the
// identity index has been dropped, Postgres behaves as NOTHING and rejects
// deletes on the published table itself; that is refused too, because the
// identity does not carry the key either. NOTHING is left to Postgres, which
// rejects UPDATE and DELETE on a table that publishes them.
func CheckReplicaIdentity(ctx context.Context, q RowQuerier, table, keyColumn string) error {
	const query = `SELECT c.relreplident::text,
		COALESCE(i.indexrelid::regclass::text, ''),
		COALESCE((SELECT bool_or(a.attname = $2)
			FROM pg_attribute a
			WHERE a.attrelid = c.oid AND a.attnum = ANY(i.indkey)), false)
	FROM pg_class c
	LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisreplident
	WHERE c.oid = $1::regclass`

	var identity, index string
	var covered bool
	if err := q.QueryRow(ctx, query, internal.WrapSQLIdent(table), keyColumn).Scan(&identity, &index, &covered); err != nil {
		return fmt.Errorf("failed to read replica identity of table %q: %w", table, err)
	}
	if identity != "i" || covered {
		return nil
	}
	if index == "" {
		index = "(dropped)"
	}
	return fmt.Errorf(
		"%s: table %q has REPLICA IDENTITY USING INDEX %s, which does not include the key column %q; "+
			"Postgres would send deletes without the key, and they could not be applied downstream. "+
			"Fix: ALTER TABLE %s REPLICA IDENTITY FULL, or use a unique index that includes %q "+
			"(ALTER TABLE %s REPLICA IDENTITY USING INDEX <index>), or REPLICA IDENTITY DEFAULT",
		ErrorCodeReplicaIdentityExcludesKey, table, index, keyColumn,
		internal.WrapSQLIdent(table), keyColumn, internal.WrapSQLIdent(table))
}

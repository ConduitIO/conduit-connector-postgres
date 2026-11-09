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
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

// ErrorCodeReplicaIdentityExcludesKey prefixes the error returned when a
// table's REPLICA IDENTITY does not include the column the connector uses as
// the record key. Under that identity Postgres sends deletes without the key
// value, so the connector would emit a delete with a null key, which a
// destination cannot apply (conduit-connector-postgres#380). It is returned by
// CheckReplicaIdentity at Open, and by the CDC handler if such a delete
// arrives anyway (the identity was changed while the pipeline ran).
const ErrorCodeReplicaIdentityExcludesKey = "postgres.replica_identity.excludes_key"

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
// only the index's key columns (indnkeyatts), so the index must contain
// keyColumn among them: an INCLUDE column is listed in indkey but is not sent
// in the old tuple. pg_index.indnkeyatts exists from Postgres 11; reading it
// through to_jsonb falls back to indnatts (all columns are key columns) on
// Postgres 10, where a direct reference would fail Open for every table. If the identity index has been dropped, Postgres behaves as NOTHING and rejects
// deletes on the published table itself; that is refused too, because the
// identity does not carry the key either. NOTHING is left to Postgres, which
// rejects UPDATE and DELETE on a table that publishes them.
func CheckReplicaIdentity(ctx context.Context, q RowQuerier, table, keyColumn string) error {
	const query = `SELECT c.relreplident::text,
		COALESCE(i.indexrelid::regclass::text, ''),
		COALESCE((SELECT bool_or(a.attname = $2)
			FROM pg_attribute a
			WHERE a.attrelid = c.oid
			AND a.attnum = ANY((i.indkey::int2[])[0:COALESCE((to_jsonb(i)->>'indnkeyatts')::int, i.indnatts)-1])), false)
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

// WarnCodeAvroToastIdentity is the code of the warning WarnToastIdentity logs
// at Open.
const WarnCodeAvroToastIdentity = "postgres.avro.toast_identity"

// WarnToastIdentity logs a warning coded WarnCodeAvroToastIdentity when table
// has columns that can be TOASTed (pg_attribute.attstorage other than
// 'p'lain) and a REPLICA IDENTITY other than FULL. Open calls it once per
// captured table. With Avro schemas on, an UPDATE that leaves such a
// column's value unchanged while the value is stored out of line (Postgres
// does so only for rows above about 2 kB) arrives without the value and
// stops the source with ErrorCodeAvroUnchangedToast (see requireAllColumns);
// a column that can be TOASTed but only holds short values never does, and
// the warning says so. It lets the operator set REPLICA IDENTITY FULL before
// the first such update instead of after a stopped pipeline. It is a warning only: it
// never refuses Open, and a failed catalog read is logged, not returned. The
// read is bounded by schema.NotNullQueryTimeout, like the nullability read,
// so a starved pool cannot hold Open for the caller's whole deadline. It
// names the table and columns, never values.
func WarnToastIdentity(ctx context.Context, q RowQuerier, table string) {
	const query = `SELECT c.relreplident::text,
		COALESCE(array_agg(a.attname::text ORDER BY a.attnum) FILTER (WHERE a.attstorage <> 'p'), '{}')
	FROM pg_class c
	LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
	WHERE c.oid = $1::regclass
	GROUP BY c.relreplident`

	var identity string
	var toastable []string
	qctx, cancel := context.WithTimeout(ctx, schema.NotNullQueryTimeout)
	defer cancel()
	if err := q.QueryRow(qctx, query, internal.WrapSQLIdent(table)).Scan(&identity, &toastable); err != nil {
		sdk.Logger(ctx).Warn().
			Str("table", table).
			Str("error_type", fmt.Sprintf("%T", err)).
			Bool("timed_out", qctx.Err() != nil && ctx.Err() == nil).
			Msg("could not check whether the table's replica identity covers its TOASTable columns")
		return
	}
	if identity == "f" || len(toastable) == 0 {
		return
	}
	sdk.Logger(ctx).Warn().
		Str("code", WarnCodeAvroToastIdentity).
		Str("table", table).
		Str("replica_identity", replicaIdentityName(identity)).
		Strs("toastable_columns", toastable).
		Msgf("with logrepl.withAvroSchema, an UPDATE that leaves a value of these columns unchanged stops the pipeline with %s "+
			"if that value is stored out of line (TOASTed), because Postgres does not send it. "+
			"Postgres stores a value out of line only when its row exceeds about 2 kB, so short values are not affected. "+
			"To rule it out: ALTER TABLE %s REPLICA IDENTITY FULL",
			ErrorCodeAvroUnchangedToast, internal.WrapSQLIdent(table))
}

// replicaIdentityName spells pg_class.relreplident the way ALTER TABLE does.
func replicaIdentityName(code string) string {
	switch code {
	case "d":
		return "DEFAULT"
	case "n":
		return "NOTHING"
	case "i":
		return "USING INDEX"
	case "f":
		return "FULL"
	}
	return code
}

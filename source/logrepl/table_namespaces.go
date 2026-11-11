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
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/jackc/pgx/v5"
)

// ResolveTableNamespaces returns the schema (namespace) each table in tables
// resolves to, keyed by the table name as configured.
//
// It resolves a name the way the rest of the connector does (the snapshot,
// the primary key lookup, the publication it creates): as one quoted
// identifier (internal.WrapSQLIdent) through the session's search_path, so
// the schema is the one whose table the snapshot reads and the publication
// streams. CDCHandler.tableName compares it with a Relation message's
// namespace.
func ResolveTableNamespaces(ctx context.Context, q RowQuerier, tables []string) (map[string]string, error) {
	out := make(map[string]string, len(tables))
	for _, tbl := range tables {
		var ns *string
		err := q.QueryRow(ctx, `SELECT n.nspname FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass($1)`,
			internal.WrapSQLIdent(tbl)).Scan(&ns)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to resolve the schema of table %q: %w", tbl, err)
		}
		if ns == nil {
			return nil, fmt.Errorf("failed to resolve the schema of table %q: the table does not exist "+
				"(or is not visible through the connection's search_path)", tbl)
		}
		out[tbl] = *ns
	}
	return out, nil
}

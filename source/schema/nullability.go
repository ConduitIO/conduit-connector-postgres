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

package schema

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iskorotkov/avro/v2"
	"github.com/jackc/pgx/v5"
)

// NotNull maps a column name to whether the catalog guarantees that the column
// never holds NULL. Extract and ExtractLogrepl need an entry for every column
// they put in a schema: a missing entry is an error, never a default, so a
// caller that forgot to look nullability up cannot make every field optional
// by accident (#326).
type NotNull map[string]bool

// Querier is the subset of pgx LoadNotNull uses. pgx.Tx, *pgx.Conn and
// *pgxpool.Pool satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// notNullQuery reads, for every live column of a relation, whether every row
// is guaranteed to be non-NULL. pg_attribute.attnotnull alone is not that
// guarantee from Postgres 18 on, where it is also set by a NOT NULL constraint
// added NOT VALID: existing rows may still hold NULL until it is validated. The
// NOT EXISTS clause excludes those. Before 18, NOT NULL is not a pg_constraint
// row (contype 'n' does not occur for table columns), so the clause is always
// true and the result is attnotnull.
const notNullQuery = `SELECT a.attname,
	a.attnotnull AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_constraint c
		WHERE c.conrelid = a.attrelid
		AND c.contype = 'n'
		AND c.conkey = ARRAY[a.attnum]
		AND NOT c.convalidated)
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped`

// LoadNotNull reads the nullability of every live column of the relation
// with the given OID from the catalog. The result is empty, not an error, when
// the relation does not exist (anymore); callers decide what a missing column
// means.
func LoadNotNull(ctx context.Context, q Querier, relOID uint32) (NotNull, error) {
	rows, err := q.Query(ctx, notNullQuery, relOID)
	if err != nil {
		return nil, fmt.Errorf("failed to read column nullability of relation %d: %w", relOID, err)
	}
	defer rows.Close()

	nn := make(NotNull)
	for rows.Next() {
		var name string
		var notNull bool
		if err := rows.Scan(&name, &notNull); err != nil {
			return nil, fmt.Errorf("failed to read column nullability of relation %d: %w", relOID, err)
		}
		nn[name] = notNull
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read column nullability of relation %d: %w", relOID, err)
	}
	return nn, nil
}

// field returns the Avro field for a column of type s. A column the catalog
// does not declare NOT NULL is ["null", s] with a default of null, the shape
// Debezium's Avro converter gives an optional column, so a NULL encodes and
// decodes as null for every type, logical types included. A NOT NULL column
// keeps the plain type: it stays a required field, so a record that lacks it
// fails to encode instead of decoding as NULL (invariant 6; see #317 and
// #388 for why absence must stay distinguishable from NULL).
func (nn NotNull) field(name string, s avro.Schema) (*avro.Field, error) {
	notNull, ok := nn[name]
	if !ok {
		return nil, fmt.Errorf("field %q: nullability unknown, the column is not in the catalog", name)
	}
	if notNull {
		return avro.NewField(name, s)
	}
	u, err := avro.NewUnionSchema([]avro.Schema{avro.NewNullSchema(), s})
	if err != nil {
		return nil, fmt.Errorf("field %q: %w", name, err)
	}
	return avro.NewField(name, u, avro.WithDefault(nil))
}

// Text returns the schema text to register for sch. It is the full JSON form,
// not sch.String(): String is the Parsing Canonical Form, which drops field
// defaults, so a nullable field would be registered without its default of
// null and a consumer's registry would see a field it cannot default when
// a column is added. For a schema without defaults (every column NOT NULL)
// the two are the same text, so such a table registers what it did before
// #326 (pinned by the hamba fixtures).
func Text(sch avro.Schema) ([]byte, error) {
	b, err := json.Marshal(sch)
	if err != nil {
		return nil, fmt.Errorf("failed to render avro schema: %w", err)
	}
	return b, nil
}

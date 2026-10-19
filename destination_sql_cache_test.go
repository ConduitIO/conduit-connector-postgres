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

package postgres

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func newSQLCacheTestDestination() *Destination {
	return &Destination{stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
}

// TestFormatUpsertQuery pins the exact SQL text the destination emits for each
// record shape. The SQL must stay byte-identical when the generation is
// memoized: the shape (table + sorted key/non-key column names) fully determines
// the statement, and every value is one placeholder.
func TestFormatUpsertQuery(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		key     opencdc.StructuredData
		payload opencdc.StructuredData
		wantSQL string
		wantArg any
	}{
		{
			name:    "key plus payload",
			table:   "bench_dest",
			key:     opencdc.StructuredData{"id": int64(1)},
			payload: opencdc.StructuredData{"payload": "x"},
			wantSQL: `INSERT INTO "bench_dest" ("id","payload") VALUES ($1,$2) ON CONFLICT ("id") DO UPDATE SET "payload"=EXCLUDED."payload"`,
			wantArg: []any{int64(1), "x"},
		},
		{
			name:    "multiple payload columns are sorted",
			table:   "MyTable",
			key:     opencdc.StructuredData{"id": int64(1)},
			payload: opencdc.StructuredData{"payload": "x", "amt": 1.5},
			wantSQL: `INSERT INTO "MyTable" ("id","amt","payload") VALUES ($1,$2,$3) ON CONFLICT ("id") DO UPDATE SET "amt"=EXCLUDED."amt","payload"=EXCLUDED."payload"`,
			wantArg: []any{int64(1), 1.5, "x"},
		},
		{
			name:    "all columns are key columns becomes do nothing",
			table:   "k",
			key:     opencdc.StructuredData{"id": int64(1), "k2": "a"},
			payload: opencdc.StructuredData{},
			wantSQL: `INSERT INTO "k" ("id","k2") VALUES ($1,$2) ON CONFLICT ("id","k2") DO NOTHING`,
			wantArg: []any{int64(1), "a"},
		},
		{
			name:    "identifiers are quoted",
			table:   "T",
			key:     opencdc.StructuredData{"UppercaseKey": int64(1)},
			payload: opencdc.StructuredData{"UppercaseCol": "x"},
			wantSQL: `INSERT INTO "T" ("UppercaseKey","UppercaseCol") VALUES ($1,$2) ON CONFLICT ("UppercaseKey") DO UPDATE SET "UppercaseCol"=EXCLUDED."UppercaseCol"`,
			wantArg: []any{int64(1), "x"},
		},
		{
			name:    "composite key",
			table:   "t",
			key:     opencdc.StructuredData{"a": int64(1), "b": int64(2)},
			payload: opencdc.StructuredData{"c": "x"},
			wantSQL: `INSERT INTO "t" ("a","b","c") VALUES ($1,$2,$3) ON CONFLICT ("a","b") DO UPDATE SET "c"=EXCLUDED."c"`,
			wantArg: []any{int64(1), int64(2), "x"},
		},
	}

	d := newSQLCacheTestDestination()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			query, args, err := d.formatUpsertQuery(tt.key, tt.payload, tt.table)
			is.NoErr(err)
			is.Equal(tt.wantSQL, query)
			is.Equal(tt.wantArg, args)
		})
	}
}

// TestFormatUpsertQueryIsMemoized proves the cache is used: two records of the
// same shape produce the identical SQL string (and so one build), and the
// cache is keyed on the shape, not the values.
func TestFormatUpsertQueryIsMemoized(t *testing.T) {
	is := is.New(t)
	d := newSQLCacheTestDestination()
	key := opencdc.StructuredData{"id": int64(1)}
	payload := opencdc.StructuredData{"payload": "x"}

	q1, _, err := d.formatUpsertQuery(key, payload, "t")
	is.NoErr(err)
	q2, _, err := d.formatUpsertQuery(opencdc.StructuredData{"id": int64(2)}, opencdc.StructuredData{"payload": "y"}, "t")
	is.NoErr(err)
	is.Equal(q1, q2)
	is.Equal(1, len(d.upsertSQLCache))

	// A different column set is a different shape and a different statement.
	q3, _, err := d.formatUpsertQuery(key, opencdc.StructuredData{"payload": "x", "extra": 1}, "t")
	is.NoErr(err)
	is.True(q3 != q1)
	is.Equal(2, len(d.upsertSQLCache))
}

// TestFormatUpsertQuerySQLizerBypassesCache pins the one case where the SQL is
// not a pure function of the column names: a value that implements
// squirrel.Sqlizer inlines SQL text instead of a placeholder. Such a record must
// not be memoized, and must not poison the cache for the ordinary records that
// share its column names.
func TestFormatUpsertQuerySQLizerBypassesCache(t *testing.T) {
	is := is.New(t)
	d := newSQLCacheTestDestination()
	key := opencdc.StructuredData{"id": int64(1)}

	// sq.Expr is a Sqlizer: it inlines its SQL instead of taking a placeholder.
	expr := sq.Expr("gen_random_uuid()")
	qSQLizer, args, err := d.formatUpsertQuery(key, opencdc.StructuredData{"payload": expr}, "t")
	is.NoErr(err)
	is.Equal(`INSERT INTO "t" ("id","payload") VALUES ($1,gen_random_uuid()) ON CONFLICT ("id") DO UPDATE SET "payload"=EXCLUDED."payload"`, qSQLizer)
	// The Sqlizer contributes no argument of its own; only the key value remains.
	is.Equal([]any{int64(1)}, args)
	is.Equal(0, len(d.upsertSQLCache)) // must not have been cached

	// The same column names with an ordinary value still produce the standard
	// statement: the cache was not poisoned.
	qPlain, _, err := d.formatUpsertQuery(key, opencdc.StructuredData{"payload": "x"}, "t")
	is.NoErr(err)
	is.Equal(`INSERT INTO "t" ("id","payload") VALUES ($1,$2) ON CONFLICT ("id") DO UPDATE SET "payload"=EXCLUDED."payload"`, qPlain)
	is.Equal(1, len(d.upsertSQLCache))
}

// TestDeleteSQL pins the exact DELETE text and that it is memoized.
func TestDeleteSQL(t *testing.T) {
	is := is.New(t)
	d := newSQLCacheTestDestination()

	q1, err := d.deleteSQL("MyTable", []string{`"a"`, `"b"`})
	is.NoErr(err)
	is.Equal(`DELETE FROM "MyTable" WHERE "a" = $1 AND "b" = $2`, q1)

	q2, err := d.deleteSQL("MyTable", []string{`"a"`, `"b"`})
	is.NoErr(err)
	is.Equal(q1, q2)
	is.Equal(1, len(d.deleteSQLCache))
}

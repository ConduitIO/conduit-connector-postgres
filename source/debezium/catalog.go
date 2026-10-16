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

package debezium

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/jackc/pgx/v5"
)

// Querier is the part of pgx the catalog reader uses; *pgxpool.Pool
// satisfies it. It must be a regular (not a replication) connection.
type Querier interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// catalogColumn is what the catalog says about one column. The Relation
// message carries neither nullability nor defaults, so they come from here.
type catalogColumn struct {
	name     string
	typeOID  uint32
	notNull  bool
	defaultX string // pg_get_expr of the default, "" when none
}

// catalogTable is the catalog's view of a table.
type catalogTable struct {
	columns []catalogColumn
	// keyColumns are the primary key's columns in key order (pg_index.indkey
	// order), empty for a table without a primary key.
	keyColumns []string
}

// sessionSettings pins the settings that change the text of values, here
// for the catalog read (defaults are rendered by pg_get_expr) as on the
// replication connection (design: Type mapping).
var sessionSettings = []string{
	"SET LOCAL DateStyle = 'ISO, MDY'",
	"SET LOCAL IntervalStyle = 'postgres'",
	"SET LOCAL TimeZone = 'UTC'",
	"SET LOCAL extra_float_digits = 3",
	"SET LOCAL bytea_output = 'hex'",
	"SET LOCAL lc_monetary = 'C'",
}

// ReplicationSessionParams are the same settings as run-time parameters of
// the replication connection, so pgoutput writes values in the text form
// the decoder parses whatever the server's, database's or role's settings
// are (design: Failure modes, timezone handling).
func ReplicationSessionParams() map[string]string {
	return map[string]string{
		"DateStyle":          "ISO, MDY",
		"IntervalStyle":      "postgres",
		"TimeZone":           "UTC",
		"extra_float_digits": "3",
		"bytea_output":       "hex",
		"lc_monetary":        "C",
	}
}

// readTable reads the catalog for one relation and the types its columns
// use, in one read-only transaction with the pinned session settings.
func readTable(ctx context.Context, q Querier, relID uint32, types map[uint32]*PGType) (catalogTable, string, error) {
	var out catalogTable
	tx, err := q.Begin(ctx)
	if err != nil {
		return out, "", fmt.Errorf("debezium mode: catalog read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, s := range sessionSettings {
		if _, err := tx.Exec(ctx, s); err != nil {
			return out, "", fmt.Errorf("debezium mode: catalog read: %s: %w", s, err)
		}
	}
	var db string
	if err := tx.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
		return out, "", fmt.Errorf("debezium mode: catalog read: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT a.attname, a.atttypid, a.attnotnull, COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, relID)
	if err != nil {
		return out, "", fmt.Errorf("debezium mode: catalog read of relation %d: %w", relID, err)
	}
	for rows.Next() {
		var c catalogColumn
		if err := rows.Scan(&c.name, &c.typeOID, &c.notNull, &c.defaultX); err != nil {
			rows.Close()
			return out, "", fmt.Errorf("debezium mode: catalog read of relation %d: %w", relID, err)
		}
		out.columns = append(out.columns, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, "", fmt.Errorf("debezium mode: catalog read of relation %d: %w", relID, err)
	}

	krows, err := tx.Query(ctx, `SELECT a.attname
		FROM pg_index i
		CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
		WHERE i.indrelid = $1 AND i.indisprimary
		ORDER BY k.ord`, relID)
	if err != nil {
		return out, "", fmt.Errorf("debezium mode: primary key of relation %d: %w", relID, err)
	}
	for krows.Next() {
		var name string
		if err := krows.Scan(&name); err != nil {
			krows.Close()
			return out, "", err
		}
		out.keyColumns = append(out.keyColumns, name)
	}
	krows.Close()
	if err := krows.Err(); err != nil {
		return out, "", err
	}

	for _, c := range out.columns {
		if _, err := readType(ctx, tx, c.typeOID, types); err != nil {
			return out, "", err
		}
	}
	return out, db, nil
}

// readType fills types with the type and, for arrays, its element type.
func readType(ctx context.Context, tx pgx.Tx, oid uint32, types map[uint32]*PGType) (*PGType, error) {
	if t, ok := types[oid]; ok {
		return t, nil
	}
	t := &PGType{OID: oid}
	var elem uint32
	var kind string
	if err := tx.QueryRow(ctx, `SELECT typname, typtype::text, typelem FROM pg_type WHERE oid = $1`, oid).
		Scan(&t.Name, &kind, &elem); err != nil {
		return nil, fmt.Errorf("debezium mode: type %d: %w", oid, err)
	}
	if kind != "" {
		t.Kind = kind[0]
	}
	if t.Kind == 'e' {
		rows, err := tx.Query(ctx, `SELECT enumlabel FROM pg_enum WHERE enumtypid = $1 ORDER BY enumsortorder`, oid)
		if err != nil {
			return nil, fmt.Errorf("debezium mode: enum %s: %w", t.Name, err)
		}
		for rows.Next() {
			var l string
			if err := rows.Scan(&l); err != nil {
				rows.Close()
				return nil, err
			}
			t.EnumLabels = append(t.EnumLabels, l)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	types[oid] = t
	if elem != 0 && len(t.Name) > 0 && t.Name[0] == '_' {
		e, err := readType(ctx, tx, elem, types)
		if err != nil {
			return nil, err
		}
		t.Elem = e
	}
	return t, nil
}

// RowQuerier is the part of pgx CheckTable uses; *pgxpool.Pool satisfies it.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CheckTable refuses, before replication starts, a table whose replica
// identity cannot carry the record key (D19, check e): REPLICA IDENTITY
// USING INDEX on an index whose key columns (the first indnkeyatts of
// indkey; INCLUDE columns are not sent) lack a primary key column, or whose
// index was dropped. indnkeyatts is read through to_jsonb because Postgres
// 10 lacks it (every column is a key column there). The same condition is checked again per relation shape
// while streaming, for DDL after the start. table is the name as configured.
func CheckTable(ctx context.Context, q RowQuerier, table string) error {
	const query = `SELECT c.relreplident::text, n.nspname, c.relname,
		COALESCE((SELECT array_agg(a.attname ORDER BY a.attname)
			FROM pg_index p JOIN pg_attribute a ON a.attrelid = p.indrelid AND a.attnum = ANY(p.indkey)
			WHERE p.indrelid = c.oid AND p.indisprimary), '{}'),
		COALESCE((SELECT array_agg(a.attname ORDER BY a.attname)
			FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY((i.indkey::int2[])[0:COALESCE((to_jsonb(i)->>'indnkeyatts')::int, i.indnatts)-1])
			WHERE i.indrelid = c.oid AND i.indisreplident), '{}')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = $1::regclass`
	var identity, schemaName, relName string
	var pk, ident []string
	if err := q.QueryRow(ctx, query, internal.WrapSQLIdent(table)).Scan(&identity, &schemaName, &relName, &pk, &ident); err != nil {
		return fmt.Errorf("debezium mode: replica identity of table %q: %w", table, err)
	}
	if identity != "i" || len(pk) == 0 {
		return nil
	}
	have := map[string]bool{}
	for _, c := range ident {
		have[c] = true
	}
	for _, k := range pk {
		if !have[k] {
			return &Error{Code: CodeReplicaIdentityIndexNotKey,
				Message: fmt.Sprintf("table %s.%s: REPLICA IDENTITY USING INDEX does not contain primary key column %q (or the index was dropped), so its old rows cannot carry the record key",
					schemaName, relName, k),
				Fix: "Set REPLICA IDENTITY DEFAULT or FULL on the table, or use an identity index that contains the primary key."}
		}
	}
	return nil
}

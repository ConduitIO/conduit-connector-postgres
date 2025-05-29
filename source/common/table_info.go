// Copyright © 2024 Meroxa, Inc.
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

package common

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TableInfo struct {
	Name    string
	Columns map[string]*ColumnInfo
}

func NewTableInfo(tableName string) *TableInfo {
	return &TableInfo{
		Name:    tableName,
		Columns: make(map[string]*ColumnInfo),
	}
}

type ColumnInfo struct {
	IsNotNull bool
}

type TableInfoFetcher struct {
	connPool  *pgxpool.Pool
	tableInfo map[string]*TableInfo
}

func NewTableInfoFetcher(connPool *pgxpool.Pool) *TableInfoFetcher {
	return &TableInfoFetcher{
		connPool:  connPool,
		tableInfo: make(map[string]*TableInfo),
	}
}

func (i TableInfoFetcher) Refresh(ctx context.Context, tableName string) error {
	tx, err := i.connPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start tx for getting table info: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			sdk.Logger(ctx).Warn().
				Err(err).
				Msgf("error on tx rollback for getting table info")
		}
	}()

	query := `
		SELECT a.attname as column_name, a.attnotnull as is_not_null
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1::regclass
			AND a.attnum > 0 
			AND NOT a.attisdropped
		ORDER BY a.attnum;
	`

	rows, err := tx.Query(context.Background(), query, internal.WrapSQLIdent(tableName))
	if err != nil {
		sdk.Logger(ctx).
			Err(err).
			Str("query", query).
			Msgf("failed to execute table info query")

		return fmt.Errorf("failed to get table info: %w", err)
	}
	defer rows.Close()

	ti := NewTableInfo(tableName)
	for rows.Next() {
		var columnName string
		var isNotNull bool

		err := rows.Scan(&columnName, &isNotNull)
		if err != nil {
			return fmt.Errorf("failed to scan table info row: %w", err)
		}

		ci := ti.Columns[columnName]
		if ci == nil {
			ci = &ColumnInfo{}
			ti.Columns[columnName] = ci
		}
		ci.IsNotNull = isNotNull
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get table info rows: %w", err)
	}

	i.tableInfo[tableName] = ti
	return nil
}

func (i TableInfoFetcher) GetTable(name string) *TableInfo {
	return i.tableInfo[name]
}

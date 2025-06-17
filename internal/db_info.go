// Copyright © 2025 Meroxa, Inc.
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

package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// DbInfo provides information about tables in a database.
type DbInfo struct {
	conn  *pgx.Conn
	cache map[string]*tableCache
}

// tableCache stores information about a table.
// The information is cached and refreshed every 'cacheExpiration'.
type tableCache struct {
	columns map[string]int
}

func NewDbInfo(conn *pgx.Conn) *DbInfo {
	return &DbInfo{
		conn:  conn,
		cache: map[string]*tableCache{},
	}
}

func (d *DbInfo) GetNumericColumnScale(ctx context.Context, table string, column string) (int, error) {
	// Check if table exists in cache and is not expired
	tableInfo, ok := d.cache[table]
	if ok {
		scale, ok := tableInfo.columns[column]
		if ok {
			return scale, nil
		}
	} else {
		// Table info has expired, refresh the cache
		d.cache[table] = &tableCache{
			columns: map[string]int{},
		}
	}

	// Fetch scale from database
	scale, err := d.numericScaleFromDb(ctx, table, column)
	if err != nil {
		return 0, err
	}

	d.cache[table].columns[column] = scale

	return scale, nil
}

func (d *DbInfo) numericScaleFromDb(ctx context.Context, table string, column string) (int, error) {
	// Query to get the column type and numeric scale
	query := `
		SELECT 
			data_type,
			numeric_scale
		FROM 
			information_schema.columns
		WHERE 
			table_name = $1
			AND column_name = $2
	`

	var dataType string
	var numericScale *int

	err := d.conn.QueryRow(ctx, query, table, column).Scan(&dataType, &numericScale)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("column %s not found in table %s", column, table)
		}
		return 0, fmt.Errorf("error querying column info: %w", err)
	}

	// Check if the column is of the numeric/decimal type
	if dataType != "numeric" && dataType != "decimal" {
		return 0, fmt.Errorf("column %s in table %s is not a numeric type (actual type: %s)", column, table, dataType)
	}

	// Handle case where numeric_scale is NULL
	if numericScale == nil {
		return 0, nil // The default scale is 0 when not specified
	}

	return *numericScale, nil
}

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
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

// WarnCodeJSONServerEncoding is the "code" field of the warning Open logs
// when json text in the captured tables may not be valid UTF-8, and so may be
// emitted as base64 (json.invalidUTF8).
const WarnCodeJSONServerEncoding = "postgres.source.json_server_encoding"

// jsonColumnsQuery lists the json, jsonb, json[] and jsonb[] columns of the
// given tables, as "table" and "column (type)".
const jsonColumnsQuery = `SELECT a.attrelid::regclass::text, a.attname, format_type(a.atttypid, a.atttypmod)
	FROM pg_attribute a
	WHERE a.attrelid = ANY($1::text[]::regclass[]) AND a.attnum > 0 AND NOT a.attisdropped
	AND a.atttypid IN ('json'::regtype, 'jsonb'::regtype, 'json[]'::regtype, 'jsonb[]'::regtype)
	ORDER BY 1, a.attnum`

// querier is the part of *pgxpool.Pool that warnJSONServerEncoding uses.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// warnJSONServerEncoding logs a warning when the database's server_encoding
// is not UTF8 and a captured table has a json or jsonb column (or an array of
// them). Only a server that does not validate json text as UTF-8 on input
// (SQL_ASCII stores the bytes unchecked) can hold json text that is not valid
// UTF-8. The source never replaces such bytes (invariant 6): with
// json.invalidUTF8 base64, the default, it emits them as base64 and names the
// column in the record metadata types.MetadataJSONRawBytes, so consumers must
// be ready to decode it; with halt such a row stops the source, and a restart
// stops on the same change again. The operator is told at Open, before the
// first such row, which of the two applies.
//
// It only warns. A failure to run the check is logged and Open goes on: the
// check is advice, and the handling of such values does not depend on it.
func (s *Source) warnJSONServerEncoding(ctx context.Context, q querier, tables []string) {
	logger := sdk.Logger(ctx)
	policy := s.jsonPolicy()
	var encoding string
	if err := q.QueryRow(ctx, "SELECT current_setting('server_encoding')").Scan(&encoding); err != nil {
		logger.Warn().Err(err).Str("code", WarnCodeJSONServerEncoding).
			Msg("could not read server_encoding to check json and jsonb columns for text that is not valid UTF-8")
		return
	}
	if encoding == "UTF8" {
		return
	}

	idents := make([]string, len(tables))
	for i, t := range tables {
		idents[i] = internal.WrapSQLIdent(t)
	}
	columns, err := jsonColumns(ctx, q, idents)
	if err != nil {
		logger.Warn().Err(err).Str("code", WarnCodeJSONServerEncoding).Str("server_encoding", encoding).
			Msg("could not list json and jsonb columns to check them for text that is not valid UTF-8")
		return
	}
	if len(columns) == 0 {
		return
	}
	effect := fmt.Sprintf("With json.invalidUTF8 base64 (the setting in use) such a value is emitted as the base64 of its bytes "+
		"and the record's metadata %s lists the column: a consumer that does not check %s reads that base64 text as if it were the JSON value. "+
		"Set json.invalidUTF8 to halt to stop the source on such a row instead (fail closed) if consumers cannot be updated",
		types.MetadataJSONRawBytes, types.MetadataJSONRawBytes)
	setting := types.JSONInvalidUTF8Base64
	if policy.Halts() {
		setting = types.JSONInvalidUTF8Halt
		effect = fmt.Sprintf("With json.invalidUTF8 halt (the setting in use) such a row stops the source with %s, "+
			"and a restart resumes at the same position and stops on the same change again; "+
			"the default, base64, delivers it as the base64 of its bytes and names the column in the record's metadata %s instead",
			types.ErrorCodeJSONNotUTF8, types.MetadataJSONRawBytes)
	}
	logger.Warn().
		Str("code", WarnCodeJSONServerEncoding).
		Str("server_encoding", encoding).
		Strs("columns", columns).
		Str("json_invalid_utf8", string(setting)).
		Str("runbook", types.JSONNotUTF8Runbook).
		Msgf("server_encoding is %s, not UTF8, and the captured tables have json/jsonb columns %v. "+
			"In a SQL_ASCII database Postgres stores json text unchecked, so a writer can store text that is not valid UTF-8. "+
			"The source never replaces characters. %s. "+
			"Remediation: move the database to UTF8, or make sure every writer stores valid UTF-8 in these columns; "+
			"see the runbook",
			encoding, columns, effect)
}

func jsonColumns(ctx context.Context, q querier, idents []string) ([]string, error) {
	rows, err := q.Query(ctx, jsonColumnsQuery, idents)
	if err != nil {
		return nil, fmt.Errorf("failed to query json columns: %w", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var table, column, typ string
		if err := rows.Scan(&table, &column, &typ); err != nil {
			return nil, fmt.Errorf("failed to scan json column: %w", err)
		}
		columns = append(columns, fmt.Sprintf("%s.%s (%s)", table, column, typ))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read json columns: %w", err)
	}
	return columns, nil
}

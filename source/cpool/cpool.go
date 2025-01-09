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

package cpool

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type replicationCtxKey struct{}

func WithReplication(ctx context.Context) context.Context {
	return context.WithValue(ctx, replicationCtxKey{}, true)
}

// New returns new pgxpool.Pool with added hooks.
func New(ctx context.Context, conninfo string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(conninfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	config.BeforeAcquire = beforeAcquireHook
	config.BeforeConnect = beforeConnectHook
	config.AfterConnect = afterConnectHook
	config.AfterRelease = afterReleaseHook

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func afterConnectHook(_ context.Context, conn *pgx.Conn) error {
	// Override the JSON and JSONB codec to return bytes rather than the
	// unmarshalled representation of map.
	conn.TypeMap().RegisterType(&pgtype.Type{
		Name:  "json",
		OID:   pgtype.JSONOID,
		Codec: &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: jsonNoopUnmarshal},
	})
	conn.TypeMap().RegisterType(&pgtype.Type{
		Name:  "jsonb",
		OID:   pgtype.JSONBOID,
		Codec: &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: jsonNoopUnmarshal},
	})

	return nil
}

// beforeAcquireHook ensures purpose specific connections are returned:
// * If a replication connection is requested, ensure the connection has replication enabled.
// * If a regular connection is requested, return non-replication connections.
func beforeAcquireHook(ctx context.Context, conn *pgx.Conn) bool {
	replReq := ctx.Value(replicationCtxKey{}) != nil
	replOn := conn.Config().RuntimeParams["replication"] != ""

	return replReq == replOn
}

// beforeConnectHook customizes the configuration of the new connection.
func beforeConnectHook(ctx context.Context, config *pgx.ConnConfig) error {
	if config.RuntimeParams["application_name"] == "" {
		config.RuntimeParams["application_name"] = "conduit-connector-postgres"
	}

	if v := ctx.Value(replicationCtxKey{}); v != nil {
		config.RuntimeParams["replication"] = "database"
	}

	return nil
}

// afterReleaseHook marks all replication connections for disposal.
func afterReleaseHook(conn *pgx.Conn) bool {
	return conn.Config().RuntimeParams["replication"] == ""
}

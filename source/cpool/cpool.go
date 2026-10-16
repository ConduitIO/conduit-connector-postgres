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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type replicationCtxKey struct{}

func WithReplication(ctx context.Context) context.Context {
	return context.WithValue(ctx, replicationCtxKey{}, true)
}

type sessionParamsCtxKey struct{}

// WithSessionParams asks for a replication connection started with the
// given run-time parameters (startup-packet GUCs). Only replication
// connections take them; it is used by the Debezium mode to pin the
// settings that change pgoutput's text form of values (DateStyle, TimeZone
// and others). A connection that lacks one of them is never handed out for
// such a request.
func WithSessionParams(ctx context.Context, params map[string]string) context.Context {
	if len(params) == 0 {
		return ctx
	}
	return context.WithValue(ctx, sessionParamsCtxKey{}, params)
}

func sessionParams(ctx context.Context) map[string]string {
	p, _ := ctx.Value(sessionParamsCtxKey{}).(map[string]string)
	return p
}

// New returns new pgxpool.Pool with added hooks.
func New(ctx context.Context, conninfo string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(conninfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	DefaultSCRAMCompat(&config.ConnConfig.Config)

	config.BeforeAcquire = beforeAcquireHook //nolint:staticcheck // pgx deprecated this in favor of PrepareConn, which takes precedence and has different semantics; migrating is a data-path change outside this toolchain PR
	config.BeforeConnect = beforeConnectHook
	config.AfterConnect = afterConnectHook
	config.AfterRelease = afterReleaseHook

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

// DefaultSCRAMCompat keeps the SCRAM behavior of pgx 5.7 for a connection
// configuration parsed by pgx 5.9 or later: main keeps pgx 5.7's SCRAM
// behaviour by default; channel_binding=require is opt-in.
//
// pgx 5.9 defaults channel_binding to "prefer", which makes it use
// SCRAM-SHA-256-PLUS whenever the connection is TLS and the server offers it.
// That breaks authentication that worked on pgx 5.7: a server with an Ed25519
// certificate fails with "SCRAM channel binding negotiation error", and a TLS
// re-terminating proxy that passes SASL through fails with "channel binding
// check failed". Mapping the default to "disable" restores what 5.7 did.
//
// "prefer" is mapped whether it is the default, was written in the URL or came
// from the PGCHANNELBINDING environment variable, which pgx 5.9 and later read
// when the URL has no channel_binding; the sources cannot be told apart after
// parsing, and the explicit forms were never usable before (pgx 5.7 sent
// channel_binding to the server as a startup parameter, which the server
// rejects). "require" and "disable" were chosen by the user, in the URL or in
// PGCHANNELBINDING, and are kept. Apply it to every configuration this connector parses
// (cpool.New, Destination.Open).
func DefaultSCRAMCompat(cfg *pgconn.Config) {
	if cfg.ChannelBinding == "prefer" {
		cfg.ChannelBinding = "disable"
	}
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
	if replReq != replOn {
		return false
	}
	if replReq {
		for k, v := range sessionParams(ctx) {
			if conn.Config().RuntimeParams[k] != v {
				return false
			}
		}
	}
	return true
}

// beforeConnectHook customizes the configuration of the new connection.
func beforeConnectHook(ctx context.Context, config *pgx.ConnConfig) error {
	if config.RuntimeParams["application_name"] == "" {
		config.RuntimeParams["application_name"] = "conduit-connector-postgres"
	}

	if v := ctx.Value(replicationCtxKey{}); v != nil {
		config.RuntimeParams["replication"] = "database"
		for k, v := range sessionParams(ctx) {
			config.RuntimeParams[k] = v
		}
	}

	return nil
}

// afterReleaseHook marks all replication connections for disposal.
func afterReleaseHook(conn *pgx.Conn) bool {
	return conn.Config().RuntimeParams["replication"] == ""
}

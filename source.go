// Copyright © 2022 Meroxa, Inc.
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
	"strings"

	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/longpoll"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

// Source is a Postgres source plugin.
type Source struct {
	sdk.UnimplementedSource

	iterator  source.Iterator
	config    source.Config
	conn      *pgx.Conn
	tableKeys map[string]string
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

func (s *Source) Configure(_ context.Context, cfg map[string]string) error {
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return err
	}
	// try parsing the url
	_, err = pgx.ParseConfig(s.config.URL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	// todo: when cdcMode "auto" is implemented, change this check
	if len(s.config.Table) != 1 && s.config.CDCMode == source.CDCModeLongPolling {
		return fmt.Errorf("multi tables are only supported for logrepl CDCMode, please provide only one table")
	}
	s.tableKeys = make(map[string]string, len(s.config.Table))
	for _, pair := range s.config.Key {
		// Split each pair into key and value
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			return fmt.Errorf("wrong format for the configuration %q, use comma separated pairs of tables and keys, example: table1:key1,table2:key2", "key")
		}
		s.tableKeys[parts[0]] = parts[1]
	}
	return nil
}
func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	conn, err := pgx.Connect(ctx, s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	columns, err := s.getTableColumns(conn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	s.conn = conn

	switch s.config.CDCMode {
	case source.CDCModeAuto:
		// TODO add logic that checks if the DB supports logical replication and
		//  switches to long polling if it's not. For now use logical replication
		fallthrough
	case source.CDCModeLogrepl:
		if s.config.SnapshotMode == source.SnapshotModeInitial {
			// TODO create snapshot iterator for logical replication and pass
			//  the snapshot mode in the config
			sdk.Logger(ctx).Warn().Msg("snapshot not supported in logical replication mode")
		}

		i, err := logrepl.NewCDCIterator(ctx, s.conn, logrepl.Config{
			Position:        pos,
			SlotName:        s.config.LogreplSlotName,
			PublicationName: s.config.LogreplPublicationName,
			Tables:          s.config.Table,
			TableKeys:       s.tableKeys,
		})
		if err != nil {
			return fmt.Errorf("failed to create logical replication iterator: %w", err)
		}
		s.iterator = i
	case source.CDCModeLongPolling:
		sdk.Logger(ctx).Warn().Msg("long polling not supported yet, only snapshot is supported")
		if s.config.SnapshotMode != source.SnapshotModeInitial {
			// TODO create long polling iterator and pass snapshot mode in the config
			sdk.Logger(ctx).Warn().Msg("snapshot disabled, can't do anything right now")
			return sdk.ErrUnimplemented
		}

		snap, err := longpoll.NewSnapshotIterator(
			ctx,
			s.conn,
			s.config.Table[0], //todo: only the first table for now
			columns,
			s.tableKeys[s.config.Table[0]])
		if err != nil {
			return fmt.Errorf("failed to create long polling iterator: %w", err)
		}
		s.iterator = snap
	default:
		// shouldn't happen, config was validated
		return fmt.Errorf("unsupported CDC mode %q", s.config.CDCMode)
	}
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	return s.iterator.Next(ctx)
}

func (s *Source) Ack(ctx context.Context, pos sdk.Position) error {
	return s.iterator.Ack(ctx, pos)
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		if err := s.iterator.Teardown(ctx); err != nil {
			return fmt.Errorf("failed to tear down iterator: %w", err)
		}
	}
	if s.conn != nil {
		if err := s.conn.Close(ctx); err != nil {
			return fmt.Errorf("failed to close DB connection: %w", err)
		}
	}
	return nil
}

func (s *Source) getTableColumns(conn *pgx.Conn) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", s.config.Table[0])

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
	}

	return columns, nil
}

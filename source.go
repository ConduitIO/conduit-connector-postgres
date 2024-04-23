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
	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Source is a Postgres source plugin.
type Source struct {
	sdk.UnimplementedSource

	iterator  source.Iterator
	config    source.Config
	conn      *pgx.Conn // TODO: Migrate to pgxpool once all iterators are replaced by the new one
	connPool  *pgxpool.Pool
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
	s.tableKeys, err = s.config.Validate()
	if err != nil {
		return err
	}
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	conn, err := pgx.Connect(ctx, s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	s.conn = conn

	connPool, err := pgxpool.New(ctx, s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to create a connection pool to database: %w", err)
	}
	s.connPool = connPool

	if s.readingAllTables() {
		s.config.Table, err = s.getAllTables(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
	}

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

		i, err := logrepl.NewCDCIterator(ctx, conn, logrepl.Config{
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

		snap, err := snapshot.NewIterator(ctx, connPool, snapshot.Config{
			Tables:     s.config.Table,
			TablesKeys: nil,
		})
		if err != nil {
			return fmt.Errorf("failed to create long polling iterator: %w", err)
		}

		// Cannot use 'snap' (type *Iterator) as the type source.Iterator Type does not implement 'source.Iterator'
		// need the method: Ack(context.Context, sdk.Position) error have the method: Ack(_ context.Context) error
		// slack message https://meroxa.slack.com/archives/C01ERTL0MJR/p1713892954250549
		s.iterator = snap

		//	columns, err := s.getTableColumns(ctx, conn, table)
		//	if err != nil {
		//		return fmt.Errorf("failed to connect to database: %w", err)
		//	}
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
	if s.connPool != nil {
		s.connPool.Close()
	}
	return nil
}

func (s *Source) getTableColumns(ctx context.Context, conn *pgx.Conn, table string) ([]string, error) {
	query := "SELECT column_name FROM information_schema.columns WHERE table_name = $1"

	rows, err := conn.Query(ctx, query, table)
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
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return columns, nil
}

func (s *Source) readingAllTables() bool {
	return len(s.config.Table) == 1 && s.config.Table[0] == source.AllTablesWildcard
}

func (s *Source) getAllTables(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return tables, nil
}

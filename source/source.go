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

package source

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/trigger"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Source is a Postgres source plugin.
type Source struct {
	sdk.UnimplementedSource

	iterator Iterator
	config   Config
	conn     *pgxpool.Pool
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		ConfigKeyURL: {
			Default:     "",
			Required:    true,
			Description: "Connection string for the Postgres database.",
		},
		ConfigKeyTable: {
			Default:     "",
			Required:    true,
			Description: "The name of the table in Postgres that the connector should read.",
		},
		ConfigKeyOrderingColumn: {
			Default:  "",
			Required: true,
			Description: "Column name that the connector will use for ordering rows. Column must contain unique " +
				"values and suitable for sorting, otherwise the snapshot won't work correctly.",
		},
		ConfigKeyColumns: {
			Default:     "",
			Required:    false,
			Description: "Comma separated list of column names that should be included in each Record's payload.",
		},
		ConfigKeyKey: {
			Default:     "",
			Required:    false,
			Description: "Column name that records should use for their `Key` fields.",
		},
		ConfigKeySnapshotMode: {
			Default:     "initial",
			Required:    false,
			Description: "Whether or not the plugin will take a snapshot of the entire table before starting cdc mode (allowed values: `initial` or `never`).",
		},
		ConfigKeyCDCMode: {
			Default:     "auto",
			Required:    false,
			Description: "Determines the CDC mode (allowed values: `auto`, `logrepl` or `trigger`).",
		},
		ConfigKeyBatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of rows batch. Min is 1 and max is 100000.",
		},
		ConfigKeyLogreplPublicationName: {
			Default:     "conduitpub",
			Required:    false,
			Description: "Name of the publication to listen for WAL events.",
		},
		ConfigKeyLogreplSlotName: {
			Default:     "conduitslot",
			Required:    false,
			Description: "Name of the slot opened for replication events.",
		},
	}
}

func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := ParseConfig(cfgRaw)
	if err != nil {
		return err
	}
	s.config = cfg
	return nil
}
func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	var err error

	s.conn, err = pgxpool.Connect(ctx, s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	switch s.config.CDCMode {
	case CDCModeAuto:
		// TODO add logic that checks if the DB supports logical replication and
		//  switches to trigger if it's not. For now use logical replication
		fallthrough
	case CDCModeLogrepl:
		if s.config.SnapshotMode == SnapshotModeInitial {
			// TODO create snapshot iterator for logical replication and pass
			//  the snapshot mode in the config
			sdk.Logger(ctx).Warn().Msg("snapshot not supported in logical replication mode")
		}

		conn, err := s.conn.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("return a connection from the pool: %w", err)
		}

		s.iterator, err = logrepl.NewCDCIterator(ctx, conn.Conn(), logrepl.Config{
			Position:        pos,
			SlotName:        s.config.LogreplSlotName,
			PublicationName: s.config.LogreplPublicationName,
			TableName:       s.config.Table,
			KeyColumnName:   s.config.Key,
			Columns:         s.config.Columns,
		})
		if err != nil {
			err = fmt.Errorf("failed to create logical replication iterator: %w", err)

			if s.config.CDCMode != CDCModeAuto {
				return err
			}

			sdk.Logger(ctx).Err(err)
		}

		fallthrough
	case CDCModeTrigger:
		s.iterator, err = trigger.New(ctx, trigger.Params{
			Pos:            pos,
			Conn:           s.conn,
			Table:          s.config.Table,
			OrderingColumn: s.config.OrderingColumn,
			Key:            s.config.Key,
			Snapshot:       s.config.SnapshotMode == SnapshotModeInitial,
			Columns:        s.config.Columns,
			BatchSize:      s.config.BatchSize,
		})
		if err != nil {
			return fmt.Errorf("create trigger iterator: %w", err)
		}
	default:
		// shouldn't happen, config was validated
		return fmt.Errorf("%q contains unsupported value %q, expected one of %v", ConfigKeyCDCMode, s.config.CDCMode, cdcModeAll)
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
		s.conn.Close()
	}
	return nil
}

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
	"database/sql"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/cdc"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var _ Strategy = (*cdc.Iterator)(nil)
var _ Strategy = (*snapshot.Snapshotter)(nil)

// Source implements the new transition to the new plugin SDK for Postgres.
type Source struct {
	sdk.UnimplementedSource

	Iterator Strategy

	config Config
}

func NewSource() sdk.Source {
	return &Source{}
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
	switch s.config.Mode {
	case ModeSnapshot:
		db, err := sql.Open("postgres", s.config.URL)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		snap, err := snapshot.NewSnapshotter(
			db,
			s.config.Table,
			s.config.Columns,
			s.config.Key)
		if err != nil {
			return fmt.Errorf("failed to open snapshotter: %w", err)
		}
		s.Iterator = snap
	default:
		i, err := cdc.NewCDCIterator(ctx, cdc.Config{
			Position:        pos,
			URL:             s.config.URL,
			SlotName:        s.config.SlotName,
			PublicationName: s.config.PublicationName,
			TableName:       s.config.Table,
			KeyColumnName:   s.config.Key,
			Columns:         s.config.Columns,
		})
		if err != nil {
			return fmt.Errorf("failed to open cdc connection: %w", err)
		}
		s.Iterator = i
	}
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	return s.Iterator.Next(ctx)
}

func (s *Source) Ack(context.Context, sdk.Position) error {
	return nil
}

func (s *Source) Teardown(context.Context) error {
	if s.Iterator == nil {
		return nil
	}
	return s.Iterator.Teardown()
}

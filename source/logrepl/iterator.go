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

package logrepl

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

const (
	TypeSnapshot int = iota
	TypeCDC
)

// Iterator binds a CDCIterator and a SnapshotIterator together for
// coordinating the hand-off from snapshot to CDC operation modes.
type Iterator struct {
	cdc  *CDCIterator
	snap *SnapshotIterator
}

func NewIterator(ctx context.Context, conn *pgx.Conn, cfg Config) (*Iterator, error) {
	// TODO: Need to figure out exactly when the subscription is committed / closes.
	// Otherwise our snapshot will not be available.
	cdc, err := NewCDCIterator(ctx, conn, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc iterator: %w", err)
	}

	snap, err := NewSnapshotIterator(ctx, conn, SnapshotConfig{
		KeyColumn:    cfg.KeyColumnName,
		Table:        cfg.TableName,
		Columns:      cfg.Columns,
		SnapshotName: cdc.sub.SnapshotName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	ci := &Iterator{
		cdc:  cdc,
		snap: snap,
	}

	return ci, nil
}

func (*Iterator) Next(ctx context.Context) (sdk.Record, error) {
	return sdk.Record{}, sdk.ErrUnimplemented
}

func (*Iterator) Ack(context.Context, sdk.Position) error {
	return sdk.ErrUnimplemented
}

func (*Iterator) Teardown(context.Context) error {
	return sdk.ErrUnimplemented
}

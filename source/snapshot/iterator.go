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

package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/tomb.v2"
)

var ErrIteratorDone = errors.New("snapshot complete")

type Config struct {
	Position     sdk.Position
	Tables       []string
	TablesKeys   map[string]string
	TXSnapshotID string
}

type Iterator struct {
	db      *pgxpool.Pool
	t       *tomb.Tomb
	workers []*FetchWorker

	conf Config

	lastPosition position.Position

	data chan FetchData
}

func NewIterator(ctx context.Context, db *pgxpool.Pool, c Config) (*Iterator, error) {
	p, err := position.ParseSDKPosition(c.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to parse position: %w", err)
	}

	if p.Snapshot == nil {
		p.Snapshot = make(position.SnapshotPositions)
	}

	t, _ := tomb.WithContext(ctx)
	i := &Iterator{
		db:           db,
		t:            t,
		conf:         c,
		data:         make(chan FetchData),
		lastPosition: p,
	}

	if err := i.initFetchers(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize table fetchers: %w", err)
	}

	i.startWorkers()

	return i, nil
}

func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("iterator stopped: %w", ctx.Err())
	case d, ok := <-i.data:
		if !ok { // closed
			if err := i.t.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("fetchers exited unexpectedly: %w", err)
			}
			return sdk.Record{}, ErrIteratorDone
		}

		return i.buildRecord(d), nil
	}
}

func (i *Iterator) Ack(_ context.Context) error {
	return nil
}

func (i *Iterator) Teardown(_ context.Context) error {
	if i.t != nil {
		i.t.Kill(errors.New("tearing down snapshot iterator"))
	}

	return nil
}

func (i *Iterator) buildRecord(d FetchData) sdk.Record {
	// merge this position with latest position
	i.lastPosition.Type = position.TypeSnapshot
	i.lastPosition.Snapshot[d.Table] = d.Position

	pos := i.lastPosition.ToSDKPosition()
	metadata := make(sdk.Metadata)
	metadata["postgres.table"] = d.Table

	return sdk.Util.Source.NewRecordCreate(pos, metadata, d.Key, d.Payload)
}

func (i *Iterator) initFetchers(ctx context.Context) error {
	var errs []error

	i.workers = make([]*FetchWorker, len(i.conf.Tables))

	for j, t := range i.conf.Tables {
		w := NewFetchWorker(i.db, i.data, FetchConfig{
			Table:        t,
			Key:          i.conf.TablesKeys[t],
			TXSnapshotID: i.conf.TXSnapshotID,
			Position:     i.lastPosition,
		})

		if err := w.Validate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to validate table fetcher %q config: %w", t, err))
		}

		i.workers[j] = w
	}

	return errors.Join(errs...)
}

func (i *Iterator) startWorkers() {
	for j := range i.workers {
		f := i.workers[j]
		i.t.Go(func() error {
			ctx := i.t.Context(nil) //nolint:staticcheck // This is the correct usage of tomb.Context
			if err := f.Run(ctx); err != nil {
				return fmt.Errorf("fetcher for table %q exited: %w", f.conf.Table, err)
			}
			return nil
		})
	}
	go func() {
		<-i.t.Dead()
		close(i.data)
	}()
}

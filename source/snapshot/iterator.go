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
	"sync"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/tomb.v2"
)

var ErrIteratorDone = errors.New("snapshot complete")

type Config struct {
	Position   sdk.Position
	Tables     []string
	TablesKeys map[string]string
	Snapshot   string
}

type Iterator struct {
	db      *pgxpool.Pool
	t       *tomb.Tomb
	workers []*FetchWorker

	conf Config

	lastPosition position.Position

	records chan sdk.Record
}

func NewIterator(ctx context.Context, db *pgxpool.Pool, c Config) (*Iterator, error) {
	p, err := position.ParseSDKPosition(c.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to parse position: %w", err)
	}

	if p.Snapshot == nil {
		p.Snapshot = make(position.SnapshotPositions)
	}

	i := &Iterator{
		db:           db,
		t:            &tomb.Tomb{},
		conf:         c,
		records:      make(chan sdk.Record),
		lastPosition: p,
	}

	if err := i.initFetchers(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize table fetchers: %w", err)
	}

	go i.start(ctx)

	return i, nil
}

func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("iterator stopped: %w", ctx.Err())
	case r, ok := <-i.records:
		if !ok { // closed
			if err := i.t.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("fetchers exited unexpectedly: %w", err)
			}
			return sdk.Record{}, ErrIteratorDone
		}

		if err := i.updateLastPosition(&r); err != nil {
			return sdk.Record{}, fmt.Errorf("failed to update last fetched position: %w", err)
		}

		return r, nil
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

func (i *Iterator) updateLastPosition(r *sdk.Record) error {
	pos, err := position.ParseSDKPosition(r.Position)
	if err != nil {
		return fmt.Errorf("failed to parse position: %w", err)
	}

	// merge this position with latest position
	i.lastPosition.Type = pos.Type
	for k, v := range pos.Snapshot {
		i.lastPosition.Snapshot[k] = v
	}

	r.Position = i.lastPosition.ToSDKPosition()

	return nil
}

func (i *Iterator) initFetchers(ctx context.Context) error {
	var errs []error

	i.workers = make([]*FetchWorker, len(i.conf.Tables))

	for j, t := range i.conf.Tables {
		w := NewFetchWorker(i.db, i.records, FetchConfig{
			Table:    t,
			Key:      i.conf.TablesKeys[t],
			Snapshot: i.conf.Snapshot,
			Position: i.lastPosition,
		})

		if err := w.Validate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to validate table fetcher %q config: %w", t, err))
		}

		i.workers[j] = w
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (i *Iterator) start(ctx context.Context) {
	var wg sync.WaitGroup
	for j := range i.workers {
		f := i.workers[j]

		wg.Add(1)
		i.t.Go(func() error {
			ctx := i.t.Context(ctx)
			defer wg.Done()

			if err := f.Run(ctx); err != nil {
				return fmt.Errorf("fetcher exited: %w", err)
			}

			return nil
		})
	}
	defer close(i.records)

	wg.Wait()
}

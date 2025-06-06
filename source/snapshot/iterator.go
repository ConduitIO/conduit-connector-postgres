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

	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/tomb.v2"
)

var ErrIteratorDone = errors.New("snapshot complete")

type Config struct {
	Position       opencdc.Position
	Tables         []string
	TableKeys      map[string]string
	TXSnapshotID   string
	FetchSize      int
	WithAvroSchema bool
}

type Iterator struct {
	db *pgxpool.Pool

	workersTomb *tomb.Tomb
	workers     []*FetchWorker
	acks        csync.WaitGroup

	conf Config

	lastPosition position.Position

	data chan []FetchData
}

func NewIterator(ctx context.Context, db *pgxpool.Pool, c Config) (*Iterator, error) {
	p, err := position.ParseSDKPosition(c.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to parse position: %w", err)
	}

	if p.Snapshots == nil {
		p.Snapshots = make(position.SnapshotPositions)
	}

	t, _ := tomb.WithContext(ctx)
	i := &Iterator{
		db:           db,
		workersTomb:  t,
		conf:         c,
		data:         make(chan []FetchData),
		lastPosition: p,
	}

	if err := i.initFetchers(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize table fetchers: %w", err)
	}

	i.startWorkers()

	return i, nil
}

// NextN takes and returns up to n records from the queue. NextN is allowed to
// block until either at least one record is available or the context gets canceled.
func (i *Iterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	var records []opencdc.Record

	// Get first record (blocking)
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("iterator stopped: %w", ctx.Err())
	case batch, ok := <-i.data:
		if !ok { // closed
			if err := i.workersTomb.Err(); err != nil {
				return nil, fmt.Errorf("fetchers exited unexpectedly: %w", err)
			}
			if err := i.acks.Wait(ctx); err != nil {
				return nil, fmt.Errorf("failed to wait for acks: %w", err)
			}
			return nil, ErrIteratorDone
		}

		for _, d := range batch {
			i.acks.Add(1)
			records = append(records, i.buildRecord(d))
		}
	}

	// Try to get remaining records non-blocking
	for len(records) < n {
		select {
		case <-ctx.Done():
			return records, ctx.Err()
		case batch, ok := <-i.data:
			if !ok { // closed
				return records, nil
			}
			for _, d := range batch {
				i.acks.Add(1)
				records = append(records, i.buildRecord(d))
			}
		default:
			// No more records currently available
			return records, nil
		}
	}

	return records, nil
}

func (i *Iterator) Ack(_ context.Context, _ opencdc.Position) error {
	i.acks.Done()
	return nil
}

func (i *Iterator) Teardown(_ context.Context) error {
	if i.workersTomb != nil {
		i.workersTomb.Kill(errors.New("tearing down snapshot iterator"))
	}

	return nil
}

func (i *Iterator) buildRecord(d FetchData) opencdc.Record {
	// merge this position with latest position
	i.lastPosition.Type = position.TypeSnapshot
	i.lastPosition.Snapshots[d.Table] = d.Position

	pos := i.lastPosition.ToSDKPosition()
	metadata := make(opencdc.Metadata)
	metadata[opencdc.MetadataCollection] = d.Table

	rec := sdk.Util.Source.NewRecordSnapshot(pos, metadata, d.Key, d.Payload)
	if i.conf.WithAvroSchema {
		cschema.AttachKeySchemaToRecord(rec, d.KeySchema)
		cschema.AttachPayloadSchemaToRecord(rec, d.PayloadSchema)
	}

	return rec
}

func (i *Iterator) initFetchers(ctx context.Context) error {
	var errs []error

	i.workers = make([]*FetchWorker, len(i.conf.Tables))

	for j, t := range i.conf.Tables {
		w := NewFetchWorker(i.db, i.data, FetchConfig{
			Table:          t,
			Key:            i.conf.TableKeys[t],
			TXSnapshotID:   i.conf.TXSnapshotID,
			Position:       i.lastPosition,
			FetchSize:      i.conf.FetchSize,
			WithAvroSchema: i.conf.WithAvroSchema,
		})

		if err := w.Validate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to validate table fetcher %q config: %w", t, err))
		}

		i.workers[j] = w
	}

	return errors.Join(errs...)
}

func (i *Iterator) startWorkers() {
	for _, worker := range i.workers {
		i.workersTomb.Go(func() error {
			ctx := i.workersTomb.Context(nil) //nolint:staticcheck // This is the correct usage of tomb.Context
			if err := worker.Run(ctx); err != nil {
				return fmt.Errorf("fetcher for table %q exited: %w", worker.conf.Table, err)
			}
			return nil
		})
	}
	go func() {
		<-i.workersTomb.Dead()
		close(i.data)
	}()
}

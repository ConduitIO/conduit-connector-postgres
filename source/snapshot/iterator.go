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
	workers []*FetcherWorker

	conf Config

	lastPosition position.Position

	records chan sdk.Record
}

func NewIterator(ctx context.Context, conninfo string, c Config) (*Iterator, error) {
	p, err := position.ParseSDKPosition(c.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to parse position: %w", err)
	}

	if p.Snapshot == nil {
		p.Snapshot = make(position.SnapshotPositions)
	}

	db, err := pgxpool.New(ctx, conninfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
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
		return sdk.Record{}, ctx.Err()
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

func (i *Iterator) Teardown(ctx context.Context) error {
	if i.t != nil {
		i.t.Killf("tearing down snapshot iterator")
	}

	if i.db != nil {
		i.db.Close()
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

	i.workers = make([]*FetcherWorker, len(i.conf.Tables))

	for j, t := range i.conf.Tables {
		w := NewFetcherWorker(i.db, i.records, FetcherConfig{
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

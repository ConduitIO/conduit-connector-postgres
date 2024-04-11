package snapshot

import (
	"context"
	//"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/tomb.v2"
)

type Config struct {
	Position   sdk.Position
	Tables     []string
	TablesKeys map[string]string
	Snapshot   string
}

type Snapshotter struct {
	db   *pgxpool.Pool
	t    *tomb.Tomb
	conf Config

	lastPosition Position

	fetcherChan chan sdk.Record
}

func New(db *pgxpool.Pool, dbconf *pgx.ConnConfig, c Config) (*Snapshotter, error) {
	p, err := ParseSDKPosition(c.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to parse position: %w", err)
	}

	return &Snapshotter{
		db:           db,
		t:            &tomb.Tomb{},
		conf:         c,
		fetcherChan:  make(chan sdk.Record),
		lastPosition: p,
	}, nil
}

func (s *Snapshotter) Start(ctx context.Context) error {
	/*
		for _, t := range s.Tables {
			var pos SnapshotPosition
			if s.lastPosition.Snapshot != nil {
				if sp, ok := s.lastPosition.Snapshot[t]; ok {
					pos := sp
				}
			}

			f, err := NewFetcher(s.db, s.fetcherChan, FetcherConfig{
				Table:    t,
				Key:      s.conf.TableKeys[t],
				Limit:    pos.Limit,
				LastRead: pos.LastRead,
				Snapshot: s.conf.Snapshot,
			})

			fetchers = append(fetchers, f)
		}

		// run all fetchers
	*/
	return nil
}

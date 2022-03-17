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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-connector-postgres/pgutil"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

const (
	pgDuplicateObjectErrorCode = "42710"
	pgOutputPlugin             = "pgoutput"
)

type Subscription struct {
	SlotName      string
	Publication   string
	WaitTimeout   time.Duration
	StatusTimeout time.Duration

	connConfig pgconn.Config
	conn       *pgconn.PgConn

	maxWal     uint64
	walRetain  uint64
	walFlushed uint64

	failOnHandler bool

	// TODO make this nicer
	stop    chan struct{}
	stopped chan struct{}

	// Mutex is used to prevent reading and writing to a connection at the same time
	sync.Mutex
}

type Handler func(pglogrepl.Message, pglogrepl.LSN) error

func NewSubscription(config pgconn.Config, slotName, publication string, walRetain uint64, failOnHandler bool) *Subscription {
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}
	config.RuntimeParams["replication"] = "database"

	return &Subscription{
		SlotName:      slotName,
		Publication:   publication,
		WaitTimeout:   2 * time.Second,
		StatusTimeout: 10 * time.Second,

		connConfig:    config,
		walRetain:     walRetain,
		failOnHandler: failOnHandler,
		stop:          make(chan struct{}),
		stopped:       make(chan struct{}),
	}
}

func (s *Subscription) pluginArgs(version, publication string) []string {
	return []string{
		fmt.Sprintf(`"proto_version" '%s'`, version),
		fmt.Sprintf(`"publication_names" '%s'`, publication),
	}
}

func (s *Subscription) sendStatus(ctx context.Context, walWrite, walFlush uint64) error {
	if walFlush > walWrite {
		return fmt.Errorf("walWrite should be >= walFlush")
	}

	s.Lock()
	defer s.Unlock()
	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(walWrite),
		WALFlushPosition: pglogrepl.LSN(walFlush),
		WALApplyPosition: pglogrepl.LSN(walFlush),
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	return nil
}

func (s *Subscription) sendDone(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	_, err := pglogrepl.SendStandbyCopyDone(ctx, s.conn)
	if err != nil {
		return fmt.Errorf("failed to send standby copy done: %w", err)
	}

	return nil
}

// Flush sends the status message to server indicating that we've fully applied
// all events until maxWal. This allows PostgreSQL to purge its WAL logs.
func (s *Subscription) Flush(ctx context.Context) error {
	wp := atomic.LoadUint64(&s.maxWal)
	err := s.sendStatus(ctx, wp, wp)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&s.walFlushed, wp)
	return nil
}

func (s *Subscription) AdvanceLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	atomic.StoreUint64(&s.maxWal, uint64(lsn))
	return s.Flush(ctx)
}

// Start replication and block until error or ctx is canceled.
func (s *Subscription) Start(ctx context.Context, startLSN pglogrepl.LSN, h Handler) error {
	defer close(s.stopped)

	var err error
	s.conn, err = pgconn.ConnectConfig(ctx, &s.connConfig)
	if err != nil {
		return err
	}
	defer s.conn.Close(context.Background())

	if _, err := pglogrepl.CreateReplicationSlot(
		ctx,
		s.conn,
		s.SlotName,
		pgOutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true,
			SnapshotAction: "NOEXPORT_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		},
	); err != nil {
		// If creating the replication slot fails with code 42710, this means
		// the replication slot already exists.
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != pgDuplicateObjectErrorCode {
			return err
		}
	} else {
		// replication slot was created, make sure it's cleaned up at the end
		defer func() {
			err := pglogrepl.DropReplicationSlot(
				context.Background(),
				s.conn,
				s.SlotName,
				pglogrepl.DropReplicationSlotOptions{Wait: true},
			)
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("failed to drop replication slot")
			}
		}()
	}

	if err := pglogrepl.StartReplication(
		ctx,
		s.conn,
		s.SlotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			Timeline:   0,
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: s.pluginArgs("1", s.Publication),
		},
	); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	s.maxWal = uint64(startLSN)

	sendStatus := func(ctx context.Context) error {
		walWrite := atomic.LoadUint64(&s.maxWal)
		walLastFlushed := atomic.LoadUint64(&s.walFlushed)

		// Confirm only walRetain bytes in past
		// If walRetain is zero - will confirm current walPos as flushed
		walFlush := walWrite - s.walRetain

		if walLastFlushed > walFlush {
			// If there was a manual flush - report its position until we're past it
			walFlush = walLastFlushed
		} else if walFlush < 0 {
			// If we have less than walRetain bytes - just report zero
			walFlush = 0
		}

		return s.sendStatus(ctx, walWrite, walFlush)
	}

	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err = sendStatus(ctx); err != nil {
					// TODO don't swallow error
					return
				}
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Send final status and exit
			if err = s.sendDone(context.Background()); err != nil {
				return fmt.Errorf("unable to send final status: %w", err)
			}
			return nil

		case <-s.stop:
			// Send final status and exit
			if err = s.sendDone(context.Background()); err != nil {
				return fmt.Errorf("unable to send final status: %w", err)
			}
			return nil

		default:
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			s.Lock()
			msg, err := s.conn.ReceiveMessage(wctx)
			s.Unlock()
			cancel()

			if errors.Is(err, context.DeadlineExceeded) {
				continue
			} else if errors.Is(err, context.Canceled) {
				return err
			} else if err != nil {
				return fmt.Errorf("replication failed: %w", err)
			}

			if msg == nil {
				return fmt.Errorf("replication failed: nil message received, should not happen")
			}

			copyDataMsg, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("unexpected message type %T", msg)
			}

			switch copyDataMsg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyDataMsg.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %w", err)
				}
				if pkm.ReplyRequested {
					if err = sendStatus(ctx); err != nil {
						return fmt.Errorf("failed to send status: %w", err)
					}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(copyDataMsg.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse xlog data: %w", err)
				}

				if xld.WALStart > 0 && xld.WALStart <= startLSN {
					// skip stuff that's in the past
					continue
				}

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("invalid message: %w", err)
				}

				if err = h(logicalMsg, xld.WALStart); err != nil && s.failOnHandler {
					return fmt.Errorf("handler error: %w", err)
				}
			}
		}
	}
}

func (s *Subscription) Stop() {
	select {
	case <-s.stop:
		return // already received stop signal
	default:
		close(s.stop)
	}
}

func (s *Subscription) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopped:
		return nil
	}
}

// RelationSet can be used to build a cache of relations returned by logical
// replication.
type RelationSet struct {
	relations map[pgtype.OID]*pglogrepl.RelationMessage
	connInfo  *pgtype.ConnInfo
}

// NewRelationSet creates a new relation set.
func NewRelationSet(ci *pgtype.ConnInfo) *RelationSet {
	return &RelationSet{
		relations: map[pgtype.OID]*pglogrepl.RelationMessage{},
		connInfo:  ci,
	}
}

func (rs *RelationSet) Add(r *pglogrepl.RelationMessage) {
	rs.relations[pgtype.OID(r.RelationID)] = r
}

func (rs *RelationSet) Values(id pgtype.OID, row *pglogrepl.TupleData) (map[string]pgtype.Value, error) {
	values := map[string]pgtype.Value{}
	rel, ok := rs.relations[id]
	if !ok {
		return values, fmt.Errorf("no relation for %d", id)
	}

	// assert same number of row and columns
	for i, tuple := range row.Columns {
		col := rel.Columns[i]
		decoder := rs.oidToDecoderValue(pgtype.OID(col.DataType))

		if err := decoder.DecodeText(rs.connInfo, tuple.Data); err != nil {
			return nil, fmt.Errorf("failed to decode tuple %d: %w", i, err)
		}

		values[col.Name] = decoder
	}

	return values, nil
}

type decoderValue interface {
	pgtype.Value
	pgtype.TextDecoder
}

func (rs *RelationSet) oidToDecoderValue(id pgtype.OID) decoderValue {
	t, ok := pgutil.OIDToPgType(id).(decoderValue)
	if !ok {
		// not all pg types implement pgtype.Value and pgtype.TextDecoder
		return &pgtype.Unknown{}
	}
	return t
}

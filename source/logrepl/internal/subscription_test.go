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

package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/matryer/is"
)

func TestSubscription_Create(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	conn := test.ConnectReplication(ctx, t, test.RepmgrConnString)
	conn.Close(ctx)

	_, err := CreateSubscription(ctx, conn, "slotname", "pubname", nil, 0, nil)
	is.Equal(err.Error(), "conn closed")
}

func TestSubscription_WithRepmgr(t *testing.T) {
	ctx := context.Background()

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	replConn := test.ConnectReplication(ctx, t, test.RepmgrConnString)

	table1 := test.SetupTestTable(ctx, t, conn)
	table2 := test.SetupTestTable(ctx, t, conn)

	_, messages := setupSubscription(ctx, t, replConn, table1, table2)

	fetchAndAssertMessageTypes := func(is *is.I, m chan pglogrepl.Message, msgTypes ...pglogrepl.MessageType) []pglogrepl.Message {
		out := make([]pglogrepl.Message, len(msgTypes))
		for i, msgType := range msgTypes {
			select {
			case msg := <-m:
				is.Equal(msg.Type(), msgType)
				out[i] = msg
			case <-time.After(time.Second):
				is.Fail() // timeout while waiting to receive message
			}
		}
		return out
	}

	t.Run("first insert table1", func(t *testing.T) {
		is := is.New(t)
		query := `INSERT INTO %s (id, column1, column2, column3)
		VALUES (6, 'bizz', 456, false)`
		_, err := conn.Exec(ctx, fmt.Sprintf(query, table1))
		is.NoErr(err)

		_ = fetchAndAssertMessageTypes(
			is,
			messages,
			// first insert should contain the relation as well
			pglogrepl.MessageTypeBegin,
			pglogrepl.MessageTypeRelation,
			pglogrepl.MessageTypeInsert,
			pglogrepl.MessageTypeCommit,
		)
	})

	t.Run("second insert table1", func(t *testing.T) {
		is := is.New(t)
		query := `INSERT INTO %s (id, column1, column2, column3)
		VALUES (7, 'bizz', 456, false)`
		_, err := conn.Exec(ctx, fmt.Sprintf(query, table1))
		is.NoErr(err)

		_ = fetchAndAssertMessageTypes(
			is,
			messages,
			// second insert does not ship the relation
			pglogrepl.MessageTypeBegin,
			pglogrepl.MessageTypeInsert,
			pglogrepl.MessageTypeCommit,
		)
	})

	t.Run("first update table2", func(t *testing.T) {
		is := is.New(t)
		query := `UPDATE %s SET column1 = 'foo' WHERE id = 1`
		_, err := conn.Exec(ctx, fmt.Sprintf(query, table2))
		is.NoErr(err)

		_ = fetchAndAssertMessageTypes(
			is,
			messages,
			// first insert should contain the relation as well
			pglogrepl.MessageTypeBegin,
			pglogrepl.MessageTypeRelation,
			pglogrepl.MessageTypeUpdate,
			pglogrepl.MessageTypeCommit,
		)
	})

	t.Run("update all table 2", func(t *testing.T) {
		is := is.New(t)
		query := `UPDATE %s SET column1 = 'bar'` // update all rows
		_, err := conn.Exec(ctx, fmt.Sprintf(query, table2))
		is.NoErr(err)

		_ = fetchAndAssertMessageTypes(
			is,
			messages,
			// we already got the relation so second update is without relation
			pglogrepl.MessageTypeBegin,
			pglogrepl.MessageTypeUpdate,
			pglogrepl.MessageTypeUpdate,
			pglogrepl.MessageTypeUpdate,
			pglogrepl.MessageTypeUpdate,
			pglogrepl.MessageTypeCommit,
		)
	})

	t.Run("no more messages", func(t *testing.T) {
		isNoMoreMessages(t, messages, time.Millisecond*500)
	})
}

func TestSubscription_ClosedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	replConn := test.ConnectReplication(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, conn)

	sub, messages := setupSubscription(ctx, t, replConn, table)

	// insert to get new messages into publication
	query := `INSERT INTO %s (id, column1, column2, column3)
		VALUES (6, 'bizz', 456, false)`
	_, err := conn.Exec(ctx, fmt.Sprintf(query, table))
	is.NoErr(err)

	cancel()
	// do not fetch messages, just close context instead
	select {
	case <-time.After(time.Second):
		is.Fail() // timed out while waiting for subscription to close
	case <-sub.Done():
		// all good
	}

	is.True(errors.Is(sub.Err(), context.Canceled))
	isNoMoreMessages(t, messages, time.Millisecond*500)
}

func TestSubscription_Ack(t *testing.T) {
	is := is.New(t)

	s := &Subscription{}
	s.Ack(12345)

	is.Equal(s.walFlushed, pglogrepl.LSN(12345))
}

func TestSubscription_Stop(t *testing.T) {
	t.Run("with stop function", func(t *testing.T) {
		is := is.New(t)

		var stopped bool

		s := &Subscription{
			stop: func() {
				stopped = true
			},
		}

		s.Stop()
		is.True(stopped)
	})

	t.Run("with missing stop function", func(*testing.T) {
		s := &Subscription{}
		s.Stop()
	})
}

func setupSubscription(
	ctx context.Context,
	t *testing.T,
	replConn *pgconn.PgConn,
	tables ...string,
) (*Subscription, chan pglogrepl.Message) {
	is := is.New(t)

	slotName := test.RandomIdentifier(t)
	publication := test.RandomIdentifier(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	test.CreatePublication(t, conn, publication, tables)

	messages := make(chan pglogrepl.Message)
	sub, err := CreateSubscription(
		ctx,
		replConn,
		slotName,
		publication,
		tables,
		0,
		func(ctx context.Context, msg pglogrepl.Message, _ pglogrepl.LSN) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case messages <- msg:
				return nil
			}
		},
	)
	is.NoErr(err)

	go func() {
		err := sub.Run(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Logf("unexpected error: %+v", err)
			is.Fail()
		}
	}()

	// wait for subscription to be ready
	select {
	case <-sub.Ready():
		// all good
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out while waiting for subscription to be ready")
	}

	t.Cleanup(func() {
		// stop subscription
		sub.Stop()
		cctx, cancel := context.WithTimeout(context.Background(), time.Second)
		is.NoErr(sub.Wait(cctx, 0))
		cancel()

		_, err := conn.Exec(
			context.Background(),
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",
			slotName,
		)
		is.NoErr(err)
	})

	return sub, messages
}

// isNoMoreMessages waits for the duration of the timeout and logs any new
// messages if they are received. If a message is received that is not a "begin"
// or "commit" message, the test is marked as failed.
func isNoMoreMessages(t *testing.T, messages <-chan pglogrepl.Message, timeout time.Duration) {
	is := is.New(t)

	// there should be no more messages, wait shortly to make sure and log any
	// messages that we receive in the meantime
	var messagesReceived bool
	timeoutChan := time.After(timeout)
	for {
		select {
		case msg := <-messages:
			// empty begin/commit blocks are expected, work is being done to
			// reduce them (https://commitfest.postgresql.org/33/3093/)
			if msg.Type() == pglogrepl.MessageTypeBegin ||
				msg.Type() == pglogrepl.MessageTypeCommit {
				t.Logf("got message of type %s: %+v", msg.Type(), msg)
			} else {
				t.Logf("unexpected message of type %s: %+v", msg.Type(), msg)
				messagesReceived = true
			}
		case <-timeoutChan:
			if messagesReceived {
				is.Fail() // expected no more messages
			}
			return
		}
	}
}

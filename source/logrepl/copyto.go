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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/conduitio/conduit-connector-postgres/pgutil"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type CopyDataWriter struct {
	config            Config
	fieldDescriptions []pgproto3.FieldDescription
	decoders          []decoderValue
	connInfo          *pgtype.ConnInfo
	messages          chan sdk.Record
	done              chan struct{}
}

// NewCopyDataWriter builds a new CopyDataWriter from a postgres connection.
func NewCopyDataWriter(ctx context.Context, conn *pgx.Conn, config Config) (*CopyDataWriter, error) {
	fds, err := getFieldDescriptions(ctx, conn.PgConn(), []string{config.TableName})
	if err != nil {
		return nil, err
	}

	decoders := make([]decoderValue, len(fds))
	for i, fd := range fds {
		decoders[i] = oidToDecoderValue(pgtype.OID(fd.DataTypeOID))
	}

	c := &CopyDataWriter{
		config:            config,
		fieldDescriptions: fds,
		decoders:          decoders,
		connInfo:          conn.ConnInfo(),
		messages:          make(chan sdk.Record, 1),
		done:              make(chan struct{}, 1),
	}

	return c, nil
}

// Copy is meant to be called concurrently after a writer is created.
func (c *CopyDataWriter) Copy(ctx context.Context, conn *pgx.Conn) {
	copyquery := fmt.Sprintf("COPY %s TO STDOUT", c.config.TableName)
	_, err := conn.PgConn().CopyTo(ctx, c, copyquery)
	if err != nil {
		fmt.Printf("failed to copy data from table: %v", err)
	}
	c.done <- struct{}{}
}

func (c *CopyDataWriter) Write(line []byte) (n int, err error) {
	tokens := bytes.Split(line, []byte{'\t'})
	rec := sdk.Record{}
	payload := sdk.StructuredData{}

	for i, decoder := range c.decoders {
		token := tokens[i]
		if bytes.Equal(token, []byte(`\N`)) {
			token = nil
		}

		err = decoder.DecodeText(c.connInfo, token)
		if err != nil {
			return 0, fmt.Errorf("failed to decode text: %v", err)
		}

		key := c.fieldDescriptions[i].Name
		payload[string(key)] = decoder.Get()
	}

	rec.CreatedAt = time.Now()
	rec.Payload = payload
	rec.Key = sdk.StructuredData{}         // TODO
	rec.Metadata = make(map[string]string) // TODO

	c.messages <- rec

	return len(line), nil
}

// Next returns the next message in the queue. It's a blocking operation to
// match the Iterator pattern we've previously established.
func (c *CopyDataWriter) Next(ctx context.Context) (sdk.Record, error) {
	for {
		select {
		case msg := <-c.messages:
			return msg, nil
		case <-ctx.Done():
			return sdk.Record{}, ctx.Err()
		}
	}
}

// Teardown closes the messages channel and returns.
func (c *CopyDataWriter) Teardown(ctx context.Context) error {
	close(c.messages)
	return nil
}

func (c *CopyDataWriter) Ack(ctx context.Context, pos sdk.Position) error {
	return nil // noop in copy writer terms
}

func (c *CopyDataWriter) Done() <-chan struct{} {
	return c.done
}

func getFieldDescriptions(ctx context.Context, conn *pgconn.PgConn, table pgx.Identifier) ([]pgproto3.FieldDescription, error) {
	mrr := conn.Exec(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", table.Sanitize()))
	hasNext := mrr.NextResult()
	if !hasNext {
		err := mrr.Close()
		if err == nil {
			// shouldn't happen, just to be safe
			err = errors.New("MultiResultReader did not contain any results")
		}
		return nil, err
	}

	rr := mrr.ResultReader()
	fds := rr.FieldDescriptions()

	// NB: copy slice since it is reused for other queries after rr is closed
	fdsCopy := make([]pgproto3.FieldDescription, len(fds))
	copy(fdsCopy, fds)

	_, err := rr.Close()
	if err != nil {
		_ = mrr.Close()
		return nil, err
	}

	err = mrr.Close()
	if err != nil {
		return nil, err
	}

	return fdsCopy, nil
}

type decoderValue interface {
	pgtype.Value
	pgtype.TextDecoder
}

func oidToDecoderValue(id pgtype.OID) decoderValue {
	t, ok := pgutil.OIDToPgType(id).(decoderValue)
	if !ok {
		// not all pg types implement pgtype.Value and pgtype.TextDecoder
		return &pgtype.Unknown{}
	}
	return t
}

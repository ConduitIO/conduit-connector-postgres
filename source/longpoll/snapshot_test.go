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

package longpoll

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSnapshotIterator_Next(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, conn)

	s, err := NewSnapshotIterator(ctx, conn, table,
		[]string{"id", "column1", "key"}, "key")
	is.NoErr(err)
	i := 0
	for {
		_, err := s.Next(ctx)
		if err == ErrNoRows {
			break
		}
		is.NoErr(err)
		i++
	}
	is.Equal(4, i)
	is.NoErr(s.Teardown(ctx))
	is.True(s.snapshotComplete == true) // failed to mark snapshot complete
}

func TestSnapshotIterator_Teardown(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, conn)

	s, err := NewSnapshotIterator(ctx, conn, table,
		[]string{"id", "column1", "key"}, "key")
	is.NoErr(err)
	_, err = s.Next(ctx)
	is.NoErr(err)
	is.True(!s.snapshotComplete) // snapshot prematurely marked complete
	got := s.Teardown(ctx)
	is.True(errors.Is(got, ErrSnapshotInterrupt)) // failed to get snapshot interrupt
}

func TestPrematureDBClose(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, conn)

	s, err := NewSnapshotIterator(ctx, conn, table,
		[]string{"id", "column1", "key"}, "key")
	is.NoErr(err)
	teardownErr := s.Teardown(ctx)
	is.True(errors.Is(teardownErr, ErrSnapshotInterrupt)) // failed to get snapshot interrupt error

	// now has next should return false because rows were closed prematurely
	rec, err := s.Next(ctx)
	is.Equal(rec, sdk.Record{})
	is.True(errors.Is(err, ErrNoRows)) // failed to get snapshot incomplete
}

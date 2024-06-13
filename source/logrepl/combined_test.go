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
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestConfig_Validate(t *testing.T) {
	is := is.New(t)

	errs := Config{
		Tables: []string{
			"t1", "t2", "t3", "t4",
		},
		TableKeys: map[string]string{
			"t1": "k1", "t4": "k4",
		},
	}.Validate()

	is.Equal(errs, errors.Join(
		errors.New(`missing key for table "t2"`),
		errors.New(`missing key for table "t3"`),
	))
}

func TestCombinedIterator_New(t *testing.T) {
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	t.Run("fails to parse initial position", func(t *testing.T) {
		is := is.New(t)

		_, err := NewCombinedIterator(ctx, nil, Config{
			Position: sdk.Position(`{`),
		})
		is.Equal(err.Error(), "failed to create logrepl iterator: invalid position: unexpected end of JSON input")
	})

	t.Run("snapshot and cdc", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        sdk.Position{},
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    true,
		})
		is.NoErr(err)

		is.True(i.snapshotIterator != nil)
		is.True(i.cdcIterator != nil)
		is.Equal(i.activeIterator, i.snapshotIterator)

		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(context.Background(), CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})

	t.Run("initial cdc only", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        sdk.Position{},
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    false,
		})
		is.NoErr(err)

		is.True(i.cdcIterator != nil)
		is.Equal(i.activeIterator, i.cdcIterator)
		is.Equal(i.snapshotIterator, nil)

		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(context.Background(), CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})

	t.Run("position cdc only", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        sdk.Position(`{"type":2, "last_lsn":"0/0"}`),
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    true,
		})
		is.NoErr(err)

		is.True(i.cdcIterator != nil)
		is.Equal(i.activeIterator, i.cdcIterator)
		is.Equal(i.snapshotIterator, nil)

		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(context.Background(), CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})
}

func TestCombinedIterator_Next(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	i, err := NewCombinedIterator(ctx, pool, Config{
		Position:        sdk.Position{},
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    true,
	})
	is.NoErr(err)

	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, column1, column2, column3, column4, column5)
			VALUES (6, 'bizz', 1010, false, 872.2, 101)`,
		table,
	))
	is.NoErr(err)

	var lastPos sdk.Position

	expectedRecords := testRecords()

	// compare snapshot
	for id := 1; id < 5; id++ {
		t.Run(fmt.Sprint("next_snapshot", id), func(t *testing.T) {
			is := is.New(t)
			r, err := i.Next(ctx)
			is.NoErr(err)

			jsonPos := fmt.Sprintf(`{"type":1,"snapshots":{"%s":{"last_read":%d,"snapshot_end":4}}}`, table, id)
			is.Equal(string(r.Position), jsonPos)

			is.Equal("", cmp.Diff(
				expectedRecords[id],
				r.Payload.After.(sdk.StructuredData),
			))

			is.NoErr(i.Ack(ctx, r.Position))
		})
	}

	t.Run("next_cdc_5", func(t *testing.T) {
		is := is.New(t)

		r, err := i.Next(ctx)
		is.NoErr(err)

		pos, err := position.ParseSDKPosition(r.Position)
		is.NoErr(err)
		is.Equal(pos.Type, position.TypeCDC)

		lsn, err := pos.LSN()
		is.NoErr(err)
		is.True(lsn != 0)

		is.Equal("", cmp.Diff(
			expectedRecords[5],
			r.Payload.After.(sdk.StructuredData),
		))

		is.NoErr(i.Ack(ctx, r.Position))
		lastPos = r.Position
	})

	is.NoErr(i.Teardown(ctx))

	t.Run("next_connector_resume_cdc_6", func(t *testing.T) {
		is := is.New(t)
		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        lastPos,
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    true,
		})
		is.NoErr(err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, column1, column2, column3, column4, column5)
				VALUES (7, 'buzz', 10101, true, 121.9, 51)`,
			table,
		))
		is.NoErr(err)

		r, err := i.Next(ctx)
		is.NoErr(err)

		pos, err := position.ParseSDKPosition(r.Position)
		is.NoErr(err)
		is.Equal(pos.Type, position.TypeCDC)

		lsn, err := pos.LSN()
		is.NoErr(err)
		is.True(lsn != 0)

		is.Equal("", cmp.Diff(
			expectedRecords[6],
			r.Payload.After.(sdk.StructuredData),
		))

		is.NoErr(i.Ack(ctx, r.Position))
		is.NoErr(i.Teardown(ctx))
	})

	is.NoErr(Cleanup(context.Background(), CleanupConfig{
		URL:             pool.Config().ConnString(),
		SlotName:        table,
		PublicationName: table,
	}))
}

func testRecords() []sdk.StructuredData {
	return []sdk.StructuredData{
		{},
		{
			"id":      int64(1),
			"key":     []uint8("1"),
			"column1": "foo",
			"column2": int32(123),
			"column3": false,
			"column4": 12.2,
			"column5": int64(4),
		},
		{
			"id":      int64(2),
			"key":     []uint8("2"),
			"column1": "bar",
			"column2": int32(456),
			"column3": true,
			"column4": 13.42,
			"column5": int64(8),
		},
		{
			"id":      int64(3),
			"key":     []uint8("3"),
			"column1": "baz",
			"column2": int32(789),
			"column3": false,
			"column4": nil,
			"column5": int64(9),
		},
		{
			"id":      int64(4),
			"key":     []uint8("4"),
			"column1": nil,
			"column2": nil,
			"column3": nil,
			"column4": 91.1,
			"column5": nil,
		},
		{
			"id":      int64(6),
			"key":     nil,
			"column1": "bizz",
			"column2": int32(1010),
			"column3": false,
			"column4": 872.2,
			"column5": int64(101),
		},
		{
			"id":      int64(7),
			"key":     nil,
			"column1": "buzz",
			"column2": int32(10101),
			"column3": true,
			"column4": 121.9,
			"column5": int64(51),
		},
	}
}

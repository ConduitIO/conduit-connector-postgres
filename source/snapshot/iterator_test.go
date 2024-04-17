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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func Test_Iterator_Next(t *testing.T) {
	var (
		ctx   = context.Background()
		pool  = test.ConnectPool(ctx, t, test.RegularConnString)
		table = test.SetupTestTable(ctx, t, pool)
	)

	t.Run("success", func(t *testing.T) {
		is := is.New(t)

		i, err := NewIterator(ctx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TablesKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		for j := 1; j <= 4; j++ {
			_, err := i.Next(ctx)
			is.NoErr(err)
		}

		_, err = i.Next(ctx)
		is.Equal(err, ErrIteratorDone)
	})

	t.Run("context cancelled", func(t *testing.T) {
		is := is.New(t)

		i, err := NewIterator(ctx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TablesKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err = i.Next(cancelCtx)
		is.Equal(err.Error(), "iterator stopped: context canceled")
	})

	t.Run("tomb exited", func(t *testing.T) {
		is := is.New(t)
		cancelCtx, cancel := context.WithCancel(ctx)

		i, err := NewIterator(cancelCtx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TablesKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		cancel()

		_, err = i.Next(ctx)
		is.True(err != nil)
		is.Equal(err.Error(), "fetchers exited unexpectedly: fetcher exited: context canceled")
	})
}

func Test_Iterator_updateLastPosition(t *testing.T) {
	is := is.New(t)
	i := Iterator{lastPosition: position.Position{}}

	err := i.updateLastPosition(&sdk.Record{Position: []byte(`{`)})
	is.True(err != nil)
	is.Equal(err.Error(), "failed to parse position: invalid position: unexpected end of JSON input")
}

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

package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func Test_ReadReplicationSlot(t *testing.T) {
	var (
		ctx      = context.Background()
		pool     = test.ConnectPool(ctx, t, test.RepmgrConnString)
		slotName = test.RandomIdentifier(t)
	)

	t.Run("read replication slot", func(t *testing.T) {
		is := is.New(t)

		test.CreateReplicationSlot(t, pool, slotName)
		res, err := ReadReplicationSlot(ctx, pool, slotName)
		is.NoErr(err)
		is.Equal(res.Name, slotName)
		is.True(res.ConfirmedFlushLSN > 0)
		is.True(res.RestartLSN > 0)
	})

	t.Run("fails when slot is missing", func(t *testing.T) {
		is := is.New(t)

		_, err := ReadReplicationSlot(ctx, pool, slotName)
		is.True(err != nil)
		is.True(errors.Is(err, ErrMissingSlot))
	})

	t.Run("fails when conn errors", func(t *testing.T) {
		is := is.New(t)
		pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
		pool.Close()

		_, err := ReadReplicationSlot(ctx, pool, slotName)
		is.True(err != nil)
		is.Equal(err.Error(), fmt.Sprintf("failed to read replication slot %q: closed pool", slotName))
	})
}

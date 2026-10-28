// Copyright © 2026 Meroxa, Inc.
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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/matryer/is"
)

// TestCombinedIterator_SnapshotConfig_FetchSize is a regression test: the
// snapshot FETCH size came from sdk.batch.size, so any non-zero SDK batch
// size (100 by default in SDK v0.18.0 for connectors that do not pin it)
// turned the 50000-row snapshot pages into 100-row pages, and the documented
// snapshot.fetchSize parameter had no effect.
func TestCombinedIterator_SnapshotConfig_FetchSize(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		fetchSize int
		want      int
	}{
		{name: "snapshot.fetchSize wins over sdk.batch.size", batchSize: 100, fetchSize: 777, want: 777},
		{name: "unset fetch size is left to the snapshot default", batchSize: 100, fetchSize: 0, want: 0},
		{name: "sdk.batch.size 0 does not matter", batchSize: 0, fetchSize: 50000, want: 50000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			c := &CombinedIterator{conf: Config{BatchSize: tc.batchSize, SnapshotFetchSize: tc.fetchSize}}
			conf := c.snapshotConfig(position.Position{}, "tx-1")
			is.Equal(conf.FetchSize, tc.want)
			is.Equal(conf.TXSnapshotID, "tx-1")
		})
	}
}

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

package postgres

import (
	"testing"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/matryer/is"
)

// TestSource_LogreplConfig_SnapshotFetchSize is a regression test: the
// documented snapshot.fetchSize parameter was parsed and never read, so the
// snapshot used sdk.batch.size as its FETCH size instead.
func TestSource_LogreplConfig_SnapshotFetchSize(t *testing.T) {
	is := is.New(t)

	s := &Source{config: source.Config{
		SnapshotFetchSize: 777,
	}}
	s.config.BatchSize = lang.Ptr(5)

	conf := s.logreplConfig(nil, nil)
	is.Equal(conf.SnapshotFetchSize, 777) // snapshot.fetchSize must reach the logrepl config
	is.Equal(conf.BatchSize, 5)           // sdk.batch.size stays the CDC batch size
}

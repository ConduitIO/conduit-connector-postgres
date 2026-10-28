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
	"context"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestSource_SnapshotFetchSize_Validated is a regression test for a data-loss
// path (invariant 3). snapshot.fetchSize is the snapshot FETCH size. Postgres
// answers FETCH with a negative count with zero rows and no error, the fetch
// worker reads zero rows as the end of the cursor, and the table would be
// marked snapshotted with no records emitted before the source moves to CDC.
// The parameter must therefore be rejected at configuration time unless it is
// positive.
func TestSource_SnapshotFetchSize_Validated(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{value: "-1", wantErr: true},
		{value: "0", wantErr: true},
		{value: "1", wantErr: false},
		{value: "50000", wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			is := is.New(t)
			src := NewSource()
			err := sdk.Util.ParseConfig(context.Background(), map[string]string{
				"url":                "postgres://localhost:5432/db",
				"tables":             "t",
				"snapshot.fetchSize": tc.value,
			}, src.Config(), Connector.NewSpecification().SourceParams)
			if tc.wantErr {
				is.True(err != nil)
				return
			}
			is.NoErr(err)
		})
	}
}

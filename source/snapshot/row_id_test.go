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

package snapshot

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/matryer/is"
)

// rowID names a row in errors that reach the DLQ, the API and CLI output: the
// key column and a hash of the value, never the value.
func TestFetchWorker_RowIDDoesNotContainTheKeyValue(t *testing.T) {
	is := is.New(t)
	f := &FetchWorker{conf: FetchConfig{Key: "id"}}

	got := f.rowID([]pgconn.FieldDescription{{Name: "note"}, {Name: "id"}}, []any{"note-SECRET", "customer-SECRET-4711"})
	is.True(strings.HasPrefix(got, "columns id, key hash "))
	is.True(!strings.Contains(got, "SECRET"))
	is.Equal(f.rowID([]pgconn.FieldDescription{{Name: "note"}}, []any{"x"}), "") // no key column
}

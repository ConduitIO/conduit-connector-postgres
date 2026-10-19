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
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/matryer/is"
)

// TestIsSchemaChanged pins the detection of the guard's division-by-zero
// (SQLSTATE 22012), which is the only error the destination's own statements
// never produce. A different code, a plain error, nil and a wrapped 22012 all
// behave as expected.
func TestIsSchemaChanged(t *testing.T) {
	is := is.New(t)

	is.True(!isSchemaChanged(nil))
	is.True(!isSchemaChanged(errors.New("boom")))
	is.True(!isSchemaChanged(&pgconn.PgError{Code: "22001"})) // value too long, not the guard
	is.True(isSchemaChanged(&pgconn.PgError{Code: "22012"}))
	is.True(isSchemaChanged(fmt.Errorf("rolled back: %w", &pgconn.PgError{Code: "22012"})))
}

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

package types

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func Test_Format(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name        string
		input       []any
		expect      []any
		withBuiltin bool
	}{
		{
			name: "int float string bool",
			input: []any{
				1021, 199.2, "foo", true,
			},
			expect: []any{
				1021, 199.2, "foo", true,
			},
		},
		{
			name: "pgtype.Numeric",
			input: []any{
				pgxNumeric(t, "12.2121"), pgxNumeric(t, "101"), &pgtype.Numeric{}, nil,
			},
			expect: []any{
				float64(12.2121), int64(101), nil, nil,
			},
		},
		{
			name: "time.Time",
			input: []any{
				func() time.Time {
					is := is.New(t)
					is.Helper()
					t, err := time.Parse(time.DateTime, "2009-11-10 23:00:00")
					is.NoErr(err)
					return t
				}(),
				nil,
			},
			expect: []any{
				"2009-11-10 23:00:00 +0000 UTC", nil,
			},
		},
		{
			name: "builtin time.Time",
			input: []any{
				now,
			},
			expect: []any{
				now,
			},
			withBuiltin: true,
		},
	}
	_ = time.Now()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			prevWithBuiltinPlugin := WithBuiltinPlugin
			WithBuiltinPlugin = tc.withBuiltin

			t.Cleanup(func() {
				WithBuiltinPlugin = prevWithBuiltinPlugin
			})

			for i, in := range tc.input {
				v, err := Format(in)
				is.NoErr(err)
				is.Equal(v, tc.expect[i])
			}
		})
	}
}

// as per https://github.com/jackc/pgx/blob/master/pgtype/numeric_test.go#L66
func pgxNumeric(t *testing.T, num string) pgtype.Numeric {
	is := is.New(t)
	is.Helper()

	var n pgtype.Numeric
	plan := pgtype.NumericCodec{}.PlanScan(nil, pgtype.NumericOID, pgtype.TextFormatCode, &n)
	is.True(plan != nil)
	is.NoErr(plan.Scan([]byte(num), &n))

	return n
}

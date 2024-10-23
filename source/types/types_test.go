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
		inputOID    []uint32
		expect      []any
		withBuiltin bool
	}{
		{
			name: "int float string bool",
			input: []any{
				1021, 199.2, "foo", true,
			},
			inputOID: []uint32{
				0, 0, 0, 0,
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
			inputOID: []uint32{
				0, 0, 0, 0,
			},
			expect: []any{
				float64(12.2121), int64(101), nil, nil,
			},
		},
		{
			name: "builtin time.Time",
			input: []any{
				now,
			},
			inputOID: []uint32{
				0,
			},
			expect: []any{
				now,
			},
			withBuiltin: true,
		},
		{
			name: "uuid",
			input: []any{
				[16]uint8{0xbd, 0x94, 0xee, 0x0b, 0x56, 0x4f, 0x40, 0x88, 0xbf, 0x4e, 0x8d, 0x5e, 0x62, 0x6c, 0xaf, 0x66}, nil,
			},
			inputOID: []uint32{
				pgtype.UUIDOID, pgtype.UUIDOID,
			},
			expect: []any{
				"bd94ee0b-564f-4088-bf4e-8d5e626caf66", "",
			},
		},
	}
	_ = time.Now()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			for i, in := range tc.input {
				v, err := Format(tc.inputOID[i], in)
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

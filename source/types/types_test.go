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
	"math/big"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func Test_Format(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name           string
		input          []any
		inputOID       []uint32
		expect         []any
		expectNullable []any
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
			expectNullable: []any{
				lang.Ptr(1021), lang.Ptr(199.2), lang.Ptr("foo"), lang.Ptr(true),
			},
		},
		{
			name: "pgtype.Numeric",
			input: []any{
				pgxNumeric(t, "12.2121"), pgxNumeric(t, "101"), pgxNumeric(t, "0"), &pgtype.Numeric{}, nil,
			},
			inputOID: []uint32{
				0, 0, 0, 0, 0,
			},
			expect: []any{
				big.NewRat(122121, 10000), big.NewRat(101, 1), big.NewRat(0, 1), nil, nil,
			},
			expectNullable: []any{
				big.NewRat(122121, 10000), big.NewRat(101, 1), big.NewRat(0, 1), nil, nil,
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
			expectNullable: []any{
				lang.Ptr(now),
			},
		},
		{
			name: "uuid",
			input: []any{
				[16]uint8{0xbd, 0x94, 0xee, 0x0b, 0x56, 0x4f, 0x40, 0x88, 0xbf, 0x4e, 0x8d, 0x5e, 0x62, 0x6c, 0xaf, 0x66},
				nil,
			},
			inputOID: []uint32{
				pgtype.UUIDOID, pgtype.UUIDOID,
			},
			expect: []any{
				"bd94ee0b-564f-4088-bf4e-8d5e626caf66", nil,
			},
			expectNullable: []any{
				lang.Ptr("bd94ee0b-564f-4088-bf4e-8d5e626caf66"), nil,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			for i, in := range tc.input {
				v, err := Format(tc.inputOID[i], in, true)
				is.NoErr(err)
				is.Equal(v, tc.expect[i])

				vNullable, err := Format(tc.inputOID[i], in, false)
				is.NoErr(err)
				is.Equal(vNullable, tc.expectNullable[i])
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

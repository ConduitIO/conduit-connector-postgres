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
	"github.com/jackc/pgx/v5/pgtype"
)

type NumericFormatter struct{}

// Format coerces `pgtype.Numeric` to int or double depending on the exponent.
// Returns error when value is invalid.
func (NumericFormatter) Format(num pgtype.Numeric) (any, error) {
	// N.B. The numeric type in pgx is represented by two ints.
	//      When the type in Postgres is defined as `NUMERIC(10)' the scale is assumed to be 0.
	//      However, pgx may represent the number as two ints e.g. 1200 -> (int=12,exp=2) = 12*10^2. as well
	//      as a type with zero exponent, e.g. 121 -> (int=121,exp=0).
	//      Thus, a Numeric type with positive or zero exponent is assumed to be an integer.
	if num.Exp >= 0 {
		i8v, err := num.Int64Value()
		if err != nil {
			return nil, err
		}

		v, err := i8v.Value()
		if err != nil {
			return nil, err
		}

		return v, nil
	}

	f8v, err := num.Float64Value()
	if err != nil {
		return nil, err
	}

	v, err := f8v.Value()
	if err != nil {
		return nil, err
	}

	return v, nil
}

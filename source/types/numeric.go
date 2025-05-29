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

	"github.com/jackc/pgx/v5/pgtype"
)

type NumericFormatter struct{}

// Format converts a pgtype.Numeric to a big.Rat.
func (NumericFormatter) Format(num pgtype.Numeric) (*big.Rat, error) {
	if num.Int == nil {
		return nil, nil
	}
	v := new(big.Rat)
	driverVal, err := num.Value()
	if err != nil {
		return nil, err
	}
	v.SetString(driverVal.(string))
	return v, nil
}

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
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	Numeric = NumericFormatter{}
	Time    = TimeFormatter{}
)

func Format(v any) (any, error) {
	switch t := v.(type) {
	case pgtype.Numeric:
		return Numeric.Format(t)
	case *pgtype.Numeric:
		return Numeric.Format(*t)
	case time.Time:
		return Time.Format(t)
	case *time.Time:
		return Time.Format(*t)
	default: // supported type
		return t, nil
	}
}

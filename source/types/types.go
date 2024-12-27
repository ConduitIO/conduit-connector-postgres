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
	"reflect"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	Numeric = NumericFormatter{}

	UUID = UUIDFormatter{}
)

func Format(oid uint32, v any, isNotNull bool) (any, error) {
	val, err := format(oid, v)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}
	
	if reflect.TypeOf(val).Kind() != reflect.Ptr && !isNotNull {
		return &val, nil
	}

	return val, nil
}

func isPrimitive(v any) bool {
	switch v.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return true
	default:
		return false
	}
}

func format(oid uint32, v any) (any, error) {
	if oid == pgtype.UUIDOID {
		return UUID.Format(v)
	}

	switch t := v.(type) {
	case pgtype.Numeric:
		return Numeric.Format(t)
	case *pgtype.Numeric:
		return Numeric.Format(*t)
	case []uint8:
		if oid == pgtype.XMLOID {
			return string(t), nil
		}
		return t, nil
	default:
		return t, nil
	}
}

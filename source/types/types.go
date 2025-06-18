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

// Format formats the input value v with the corresponding Postgres OID
// into an appropriate Go value (that can later be serialized with Avro).
// If the input value is nullable (i.e. isNotNull is false), then the method
// returns a pointer.
//
// The following types are currently not supported:
// bit, varbit, box, char(n), cidr, circle, inet, interval, line, lseg,
// macaddr, macaddr8, money, path, pg_lsn, pg_snapshot, point, polygon,
// time, timetz, tsquery, tsvector, xml
func Format(oid uint32, v any, isNotNull bool) (any, error) {
	val, err := format(oid, v)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	if reflect.TypeOf(val).Kind() != reflect.Ptr && !isNotNull {
		return GetPointer(val), nil
	}

	return val, nil
}

func format(oid uint32, v any) (any, error) {
	if oid == pgtype.UUIDOID {
		return UUID.Format(v)
	}

	switch t := v.(type) {
	case pgtype.Numeric:
		return Numeric.BigRatFromNumeric(t)
	case *pgtype.Numeric:
		return Numeric.BigRatFromNumeric(*t)
	case []uint8:
		if oid == pgtype.XMLOID {
			return string(t), nil
		}
		return t, nil
	default:
		return t, nil
	}
}

func GetPointer(v any) any {
	rv := reflect.ValueOf(v)

	// If the value is nil or invalid, return nil
	if !rv.IsValid() {
		return nil
	}

	// If it's already a pointer, return it as-is
	if rv.Kind() == reflect.Ptr {
		return rv.Interface()
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Map || rv.Kind() == reflect.Array {
		return rv.Interface()
	}

	// For non-pointer values, we need to get the address
	// If the value is addressable, return its address
	if rv.CanAddr() {
		return rv.Addr().Interface()
	}

	// If we can't get the address directly, create an addressable copy
	// This happens when the interface{} contains a literal value
	ptr := reflect.New(rv.Type())
	ptr.Elem().Set(rv)
	return ptr.Interface()
}

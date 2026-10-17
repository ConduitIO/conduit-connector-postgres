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

package types

import (
	"math/big"
	"reflect"
	"time"
)

// CloneValue returns a deep copy of a decoded column value, so that no
// slice, map or pointer in the copy is shared with v. It copies byte
// slices, slices, arrays and maps (typed ones too, such as an int4[]
// decoded as []int32), structs field by field (pgtype.Bits.Bytes,
// pgtype.Path.P, the bounds of pgtype.Range), pointers through their
// target, and the math/big numbers (*big.Int inside pgtype.Numeric, the
// *big.Rat of numeric), whose methods mutate the receiver.
//
// Invariant 6: a record's Key, Before and After own their values. A value
// placed in two of them (the unchanged-TOAST backfill, the key column)
// would otherwise be one Go value, and a processor that edits it in After
// would rewrite Before or Key with it (conduit-connector-postgres#425).
//
// Unexported struct fields are copied by value, not deeply. The decoded
// types that have such fields hold only immutable references: the
// *time.Location of a time.Time, the interned zone of a netip.Addr, and the
// digits inside math/big numbers, which are copied with their own Set.
// TestCloneValue_EveryDecodedType decodes every type pgx registers and
// fails if any clone still shares memory with its original, so a decoder
// that starts returning such a type is caught.
func CloneValue(v any) any {
	if v == nil {
		return nil
	}
	// Fast path: immutable value types are their own deep copy. They carry no
	// slice, map or pointer that the copy could share, so returning the value
	// itself is exactly what cloneValue does, without the reflection pass.
	// time.Time is included: its methods return new values (never mutate the
	// receiver), and the *time.Location it holds is immutable, so a shared
	// time.Time is indistinguishable from a copied one.
	switch v.(type) {
	case bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		string,
		time.Time:
		return v
	}
	return cloneValue(reflect.ValueOf(v)).Interface()
}

var timeType = reflect.TypeOf(time.Time{})

//nolint:exhaustive,gocyclo // one case per reflect kind; the kinds not listed hold no reference
func cloneValue(v reflect.Value) reflect.Value {
	switch x := valueInterface(v).(type) {
	case *big.Int:
		if x != nil {
			return reflect.ValueOf(new(big.Int).Set(x)).Convert(v.Type())
		}
		return v
	case *big.Rat:
		if x != nil {
			return reflect.ValueOf(new(big.Rat).Set(x)).Convert(v.Type())
		}
		return v
	case *big.Float:
		if x != nil {
			return reflect.ValueOf(new(big.Float).Copy(x)).Convert(v.Type())
		}
		return v
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(cloneValue(v.Elem()))
		return out
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(cloneValue(v.Elem()))
		return out
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		if v.Type().Elem().Kind() == reflect.Uint8 {
			reflect.Copy(out, v)
			return out
		}
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(cloneValue(v.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(cloneValue(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), cloneValue(iter.Value()))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v) // unexported fields by value
		if v.Type() == timeType {
			return out
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				out.Field(i).Set(cloneValue(v.Field(i)))
			}
		}
		return out
	}
	return v
}

// valueInterface is v.Interface() where that is allowed (it is not for a
// value read through an unexported field, which cloneValue never visits).
func valueInterface(v reflect.Value) any {
	if !v.IsValid() || !v.CanInterface() {
		return nil
	}
	return v.Interface()
}

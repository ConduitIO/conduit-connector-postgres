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
	"bytes"
	"math/big"
	"reflect"
)

// CloneValue returns a deep copy of a decoded column value: byte slices,
// slices and maps (including typed ones, such as an int4[] decoded as
// []int32 or a jsonb object) are copied all the way down, and everything
// else is returned as is.
//
// Invariant 6: a record's Key, Before and After own their values. A value
// placed in two of them (the unchanged-TOAST backfill, the key column)
// would otherwise be one Go value, and a processor that edits it in After
// would rewrite Before or Key with it (conduit-connector-postgres#425).
// *big.Rat (numeric, one of the encodings) is copied too, since its methods
// mutate the receiver; no other pointer type is a column value here, and
// other pointers are returned as is.
func CloneValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case *big.Rat:
		if x == nil {
			return x
		}
		return new(big.Rat).Set(x)
	case []byte:
		if x == nil {
			return x
		}
		return bytes.Clone(x)
	case []any:
		if x == nil {
			return x
		}
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = CloneValue(e)
		}
		return out
	case map[string]any:
		if x == nil {
			return x
		}
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = CloneValue(e)
		}
		return out
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only containers need a copy
	case reflect.Slice:
		if rv.IsNil() {
			return v
		}
		out := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out.Index(i).Set(cloneInto(rv.Index(i)))
		}
		return out.Interface()
	case reflect.Map:
		if rv.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), cloneInto(iter.Value()))
		}
		return out.Interface()
	}
	return v
}

// cloneInto deep-copies one element of a typed container, keeping its type.
func cloneInto(e reflect.Value) reflect.Value {
	if e.Kind() == reflect.Interface && e.IsNil() {
		return e
	}
	c := CloneValue(e.Interface())
	if c == nil {
		return reflect.Zero(e.Type())
	}
	return reflect.ValueOf(c).Convert(e.Type())
}

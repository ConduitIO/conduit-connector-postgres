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

package cpool

import (
	"encoding/json"
	"reflect"
)

// noopUnmarshal will copy source into dst.
// this is to be used with the pgtype JSON codec
func jsonNoopUnmarshal(src []byte, dst any) error {
	dstptr, ok := (dst.(*any))
	if dst == nil || !ok {
		return &json.InvalidUnmarshalError{Type: reflect.TypeOf(dst)}
	}

	v := make([]byte, len(src))
	copy(v, src)

	// set the slice to the value of the ptr.
	*dstptr = v

	return nil
}

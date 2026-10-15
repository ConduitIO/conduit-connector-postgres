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
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestCloneValue(t *testing.T) {
	is := is.New(t)
	now := time.Now()
	for _, v := range []any{
		nil, int32(1), "s", now,
		[]byte{1, 2}, []int32{1, 2}, []string{"a"}, []any{[]byte{3}, map[string]any{"k": []any{1}}},
		map[string]any{"a": map[string]any{"b": []byte{4}}}, map[string]int{"x": 1},
		[]byte(nil), []any(nil), map[string]any(nil),
	} {
		c := CloneValue(v)
		is.True(reflect.DeepEqual(c, v)) // same value, same type
	}

	// Containers are copied all the way down.
	orig := []any{[]byte{3}, map[string]any{"k": []any{int32(1)}}, []int32{5}}
	c := CloneValue(orig).([]any)
	c[0].([]byte)[0] = 9
	c[1].(map[string]any)["k"].([]any)[0] = int32(9)
	c[2].([]int32)[0] = 9
	is.Equal(orig, []any{[]byte{3}, map[string]any{"k": []any{int32(1)}}, []int32{5}})

	// numeric as *big.Rat: its methods mutate the receiver.
	r := big.NewRat(3, 2)
	cr := CloneValue(r).(*big.Rat)
	cr.SetInt64(9)
	is.Equal(r.RatString(), "3/2")
}

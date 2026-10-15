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

	"github.com/jackc/pgx/v5/pgtype"
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

// The pgx struct types the review of #427 found still shared: each mutable
// part is independent after the clone.
func TestCloneValue_PgxStructs(t *testing.T) {
	is := is.New(t)

	bits := pgtype.Bits{Bytes: []byte{0xA0}, Len: 3, Valid: true}
	cb := CloneValue(bits).(pgtype.Bits)
	cb.Bytes[0] = 0
	is.Equal(bits.Bytes, []byte{0xA0}) // bit, varbit

	path := pgtype.Path{P: []pgtype.Vec2{{X: 1, Y: 2}}, Valid: true}
	cp := CloneValue(path).(pgtype.Path)
	cp.P[0].X = 9
	is.Equal(path.P[0].X, 1.0) // path

	poly := pgtype.Polygon{P: []pgtype.Vec2{{X: 1, Y: 2}}, Valid: true}
	cpo := CloneValue(poly).(pgtype.Polygon)
	cpo.P[0].X = 9
	is.Equal(poly.P[0].X, 1.0) // polygon

	num := pgtype.Numeric{Int: big.NewInt(15), Exp: -1, Valid: true}
	cn := CloneValue(num).(pgtype.Numeric)
	cn.Int.SetInt64(0)
	is.Equal(num.Int.Int64(), int64(15)) // numeric[] elements

	rng := pgtype.Range[any]{Lower: num, Upper: num, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}
	cr := CloneValue(rng).(pgtype.Range[any])
	cr.Lower.(pgtype.Numeric).Int.SetInt64(0)
	cr.Upper.(pgtype.Numeric).Int.SetInt64(0)
	is.Equal(num.Int.Int64(), int64(15)) // numrange bounds

	multi := pgtype.Multirange[pgtype.Range[any]]{rng}
	cm := CloneValue(multi).(pgtype.Multirange[pgtype.Range[any]])
	cm[0].Lower.(pgtype.Numeric).Int.SetInt64(0)
	is.Equal(num.Int.Int64(), int64(15)) // nummultirange

	arr := []any{bits, path, num, rng}
	is.True(reflect.DeepEqual(CloneValue(arr), arr)) // bit[], path[], numeric[], numrange[]
}

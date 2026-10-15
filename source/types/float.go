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
	"math"
)

// formatFloat maps a float4 or float8 to its record value for EncodingNative.
//
// Invariant 6 / Invariant 3: NaN, Infinity and -Infinity are emitted as their
// canonical Postgres text, the same strings a non-finite numeric gets (#368).
// A Go NaN or infinity in the payload cannot be marshaled to JSON, and
// opencdc.Record.Bytes panics on it, so the pipeline stopped on the row and
// replayed it after every restart (#376). Finite values and anything that is not
// a float are returned as they are, with their Go type (float32 for float4).
//
// Under EncodingAvro the value is not touched: Avro float and double hold NaN
// and the infinities.
func formatFloat(v any) any {
	var f float64
	switch t := v.(type) {
	case float32:
		f = float64(t)
	case float64:
		f = t
	default:
		return v
	}
	switch {
	case math.IsNaN(f):
		return numericNaN
	case math.IsInf(f, 1):
		return numericInfinity
	case math.IsInf(f, -1):
		return numericNegInfinity
	}
	return v
}

// formatFloatArray applies formatFloat to the elements of a float4[] or
// float8[]. Finite and NULL elements are returned exactly as pgx decoded them.
func formatFloatArray(v any) (any, error) {
	if _, ok := v.([]any); !ok {
		return v, nil // nil or a shape pgx does not produce for float arrays: leave it alone
	}
	return mapArray(v, "float array", func(e any) (any, error) {
		return formatFloat(e), nil
	})
}

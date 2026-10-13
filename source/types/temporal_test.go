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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func TestFormatFor_TemporalInfinity(t *testing.T) {
	oids := map[string]uint32{"date": pgtype.DateOID, "timestamp": pgtype.TimestampOID, "timestamptz": pgtype.TimestamptzOID}
	mods := map[string]pgtype.InfinityModifier{"infinity": pgtype.Infinity, "-infinity": pgtype.NegativeInfinity}

	for typ, oid := range oids {
		for text, m := range mods {
			t.Run(typ+"/"+text, func(t *testing.T) {
				is := is.New(t)

				got, err := FormatFor(EncodingNative, oid, m)
				is.NoErr(err)
				is.Equal(got, text)

				got, err = FormatFor(EncodingAvro, oid, m)
				is.True(got == nil)
				var tre *TemporalNotRepresentableError
				is.True(errors.As(err, &tre))
				is.Equal(tre.Value, text)
				is.Equal(tre.Type, typ)

				located, ok := Locate(err, "public.events", "day")
				is.True(ok)
				msg := located.Error()
				is.True(strings.HasPrefix(msg, ErrorCodeTemporalNotRepresentable+": "))
				is.True(strings.Contains(msg, `"public.events"`))
				is.True(strings.Contains(msg, `"day"`))
				is.True(strings.Contains(msg, text))
				is.True(strings.Contains(msg, "logrepl.withAvroSchema"))
			})
		}
	}
	is.New(t).Equal(ErrorCodeTemporalNotRepresentable, "postgres.source.temporal_not_representable")
}

// No other type's output changes: finite values pass through as decoded, and
// an InfinityModifier under any other OID is not touched.
func TestFormatFor_TemporalUnchanged(t *testing.T) {
	is := is.New(t)
	now := time.Now()
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for _, oid := range []uint32{pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.TextOID} {
			got, err := FormatFor(enc, oid, now)
			is.NoErr(err)
			is.Equal(got, now)
		}
		got, err := FormatFor(enc, pgtype.Int2OID, pgtype.Infinity)
		is.NoErr(err)
		is.Equal(got, pgtype.Infinity)
	}
}

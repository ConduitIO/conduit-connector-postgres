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
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// formatTSVector returns the canonical Postgres text form of a tsvector
// ('a':1 'cat':3A,4), as printed by tsvectorout. Since pgx 5.9 a tsvector is
// decoded as pgtype.TSVector, whose JSON form is a struct; before that it was
// the text form. The text form is what the connector has always emitted for
// built-in pipelines on pgx < 5.9 and is what a destination can write back, so
// it is the one we pin, independent of what pgx decodes (or how a later pgx
// chooses to print it).
func formatTSVector(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return t, nil // pgx < 5.9, already the text form
	case pgtype.TSVector:
		if !t.Valid {
			return nil, nil
		}
		return tsvectorText(t), nil
	case *pgtype.TSVector:
		if t == nil || !t.Valid {
			return nil, nil
		}
		return tsvectorText(*t), nil
	default:
		return nil, unexpectedType("tsvector", v)
	}
}

func formatTSVectorArray(v any) (any, error) {
	return mapArray(v, "tsvector[]", formatTSVector)
}

func tsvectorText(t pgtype.TSVector) string {
	var b strings.Builder
	for i, l := range t.Lexemes {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteByte('\'')
		for _, r := range l.Word {
			// tsvectorout doubles quotes and backslashes inside a lexeme.
			if r == '\'' || r == '\\' {
				b.WriteRune(r)
			}
			b.WriteRune(r)
		}
		b.WriteByte('\'')
		for j, p := range l.Positions {
			if j == 0 {
				b.WriteByte(':')
			} else {
				b.WriteByte(',')
			}
			b.WriteString(strconv.FormatUint(uint64(p.Position), 10))
			switch p.Weight {
			case pgtype.TSVectorWeightA, pgtype.TSVectorWeightB, pgtype.TSVectorWeightC:
				b.WriteByte(byte(p.Weight))
			}
		}
	}
	return b.String()
}

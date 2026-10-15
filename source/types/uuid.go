// Copyright © 2022 Meroxa, Inc.
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
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

type UUIDFormatter struct{}

// Format takes a slice of bytes and returns a UUID in string format
// Returns error when byte array cannot be parsed.
//
// A NULL (nil) has no string form and is an error: callers must handle NULL
// before calling Format, see FormatFor. Returning "" for it made a NULL
// indistinguishable from an empty value, and "" is not a valid uuid (#392).
func (UUIDFormatter) Format(v any) (string, error) {
	if v == nil {
		return "", errors.New("uuid is NULL, it has no string form")
	}

	b, ok := v.([16]byte)
	if !ok {
		return "", fmt.Errorf("failed to parse uuid byte array %v", v)
	}

	uuid := pgtype.UUID{Bytes: b, Valid: true}

	uv, err := uuid.Value()
	if err != nil {
		return "", fmt.Errorf("failed to format uuid: %w", err)
	}

	return uv.(string), nil
}

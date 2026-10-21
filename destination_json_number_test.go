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

package postgres

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

// A JSON number comes from the record. Errors reach the DLQ, the API and CLI
// output, so they report its size, never the number.
func TestJSONNumber_ErrorsDoNotContainTheNumber(t *testing.T) {
	huge := jsonNumber("123456789.SECRET0987654321e99999")
	for name, call := range map[string]func() error{
		"exponent":  func() error { _, err := jsonNumber("1234567e99999").rat(); return err },
		"invalid":   func() error { _, err := huge.rat(); return err },
		"numeric":   func() error { _, err := huge.NumericValue(); return err },
		"int64":     func() error { _, err := jsonNumber("12345678901234567890123.5").Int64Value(); return err },
		"float":     func() error { _, err := jsonNumber("1234567e99999").Float64Value(); return err },
		"not whole": func() error { _, err := jsonNumber("98765.4321").Int64Value(); return err },
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			err := call()
			is.True(err != nil)
			for _, digits := range []string{"1234567", "98765", "SECRET", "12345678901234567890123"} {
				is.True(!strings.Contains(err.Error(), digits))
			}
		})
	}
}

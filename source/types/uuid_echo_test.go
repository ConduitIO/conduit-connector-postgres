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
	"strings"
	"testing"

	"github.com/matryer/is"
)

// The value that is not a uuid is record data: the error names its Go type.
func TestUUIDFormatter_ErrorDoesNotContainTheValue(t *testing.T) {
	is := is.New(t)
	_, err := UUIDFormatter{}.Format([]byte("customer-SECRET-4711"))
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "[]uint8"))
	is.True(!strings.Contains(err.Error(), "SECRET"))
	is.True(!strings.Contains(err.Error(), "99 117")) // the bytes printed with %v
}

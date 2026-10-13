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

package logrepl

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestNextLSN(t *testing.T) {
	tests := []struct {
		in   pglogrepl.LSN
		want string
	}{
		{0x40C5318, "0/40C5319"},
		{0x0, "0/1"},
		{0xFFFFFFFE, "0/FFFFFFFF"},
		{0xFFFFFFFF, "1/0"}, // the low part carries into the high part
		{0x1FFFFFFFF, "2/0"},
		{0xA00000000, "A/1"},
	}
	for _, tc := range tests {
		t.Run(tc.in.String(), func(t *testing.T) {
			is.New(t).Equal(nextLSN(tc.in), tc.want)
		})
	}
}

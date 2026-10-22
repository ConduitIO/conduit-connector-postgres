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

package internal

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// TestLogUnknownCopyData_DoesNotLogPayload: an unknown copy data message is
// logged by size and type only.
func TestLogUnknownCopyData_DoesNotLogPayload(t *testing.T) {
	is := is.New(t)
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	t.Cleanup(func() { zerolog.SetGlobalLevel(prev) })

	var buf bytes.Buffer
	ctx := zerolog.New(&buf).Level(zerolog.TraceLevel).WithContext(context.Background())

	const secret = "row-SECRET-4711"
	logUnknownCopyData(ctx, []byte("Z"+secret))

	out := buf.String()
	is.True(strings.Contains(out, "ignoring unknown copy data message"))
	is.True(strings.Contains(out, `"length":16`))
	is.True(!strings.Contains(out, secret))
}

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
	"time"
)

type TimeFormatter struct{}

// Format returns:
// * string format of Time when connectorn is not builtin
// * time type in UTC when connector is builtin
func (n TimeFormatter) Format(t time.Time) (any, error) {
	if WithBuiltinPlugin {
		return t.UTC(), nil
	}
	return t.UTC().String(), nil
}

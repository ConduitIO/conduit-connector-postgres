// Copyright © 2024 Meroxa, Inc.
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

package cpool

import (
	"testing"

	"github.com/matryer/is"
)

func Test_jsonNoopUnmarshal(t *testing.T) {
	is := is.New(t)

	var dst any
	data := []byte(`{"foo":"bar"}`)

	is.NoErr(jsonNoopUnmarshal(data, &dst))
	is.Equal(data, dst.([]byte))

	var err error

	err = jsonNoopUnmarshal(data, dst)
	is.True(err != nil)
	if err != nil {
		is.Equal(err.Error(), "json: Unmarshal(non-pointer []uint8)")
	}

	err = jsonNoopUnmarshal(data, nil)
	is.True(err != nil)
	if err != nil {
		is.Equal(err.Error(), "json: Unmarshal(nil)")
	}
}

// Copyright © 2023 Meroxa, Inc.
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

package source

import (
	"testing"

	"github.com/matryer/is"
)

func TestConfig_Validate(t *testing.T) {
	testCases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				URL:     "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
				Table:   []string{"table1", "table2"},
				Key:     []string{"table1:key1"},
				CDCMode: CDCModeLogrepl,
			},
			wantErr: false,
		}, {
			name: "invalid postgres url",
			cfg: Config{
				URL:     "postgresql",
				Table:   []string{"table1", "table2"},
				Key:     []string{"table1:key1"},
				CDCMode: CDCModeLogrepl,
			},
			wantErr: true,
		}, {
			name: "invalid multiple tables for long polling",
			cfg: Config{
				URL:     "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
				Table:   []string{"table1", "table2"},
				Key:     []string{"table1:key1"},
				CDCMode: CDCModeLongPolling,
			},
			wantErr: true,
		}, {
			name: "invalid key list format",
			cfg: Config{
				URL:     "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
				Table:   []string{"table1", "table2"},
				Key:     []string{"key1,key2"},
				CDCMode: CDCModeLogrepl,
			},
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			_, err := tc.cfg.Validate()
			if tc.wantErr {
				is.True(err != nil)
				return
			}
			is.True(err == nil)
		})
	}
}

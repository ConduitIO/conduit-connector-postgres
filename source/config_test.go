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

package source

import (
	"errors"
	"testing"

	"github.com/matryer/is"
)

func TestParseConfig(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name       string
		setupGiven func(map[string]string)
		setupWant  func(*Config)
		wantErr    error
	}{{
		name:       "valid",
		setupGiven: func(map[string]string) { /* cfg is already valid */ },
		setupWant:  func(cfg *Config) { /* expectation is already set */ },
	}, {
		name: "parse columns",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyColumns] = "col1,col2,col3"
		},
		setupWant: func(cfg *Config) {
			cfg.Columns = []string{"col1", "col2", "col3"}
		},
	}, {
		name: "snapshot mode",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyMode] = "snapshot"
		},
		setupWant: func(cfg *Config) {
			cfg.Mode = ModeSnapshot
		},
	}, {
		name: "cdc mode",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyMode] = "cdc"
		},
		setupWant: func(cfg *Config) {
			cfg.Mode = ModeCDC
		},
	}, {
		name: "publication name",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyPublicationName] = "mypublicationname"
		},
		setupWant: func(cfg *Config) {
			cfg.PublicationName = "mypublicationname"
		},
	}, {
		name: "slot name",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeySlotName] = "myslotname"
		},
		setupWant: func(cfg *Config) {
			cfg.SlotName = "myslotname"
		},
	}, {
		name: "empty url",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyURL] = ""
		},
		wantErr: errors.New(`"url" config value must be set`),
	}, {
		name: "empty table",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyTable] = ""
		},
		wantErr: errors.New(`"table" config value must be set`),
	}, {
		name: "invalid mode",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyMode] = "invalid"
		},
		wantErr: errors.New(`"mode" contains unsupported value "invalid", expected one of [full snapshot cdc]`),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given is a basic valid config
			given := map[string]string{
				ConfigKeyURL:   "postgres://user:pass@localhost:5432/testdb?sslmode=disable",
				ConfigKeyTable: "my_table",
			}
			tc.setupGiven(given)

			got, err := ParseConfig(given)
			if tc.wantErr == nil {
				is.NoErr(err)

				// want is parsed corresponding to the basic valid config
				want := Config{
					URL:             "postgres://user:pass@localhost:5432/testdb?sslmode=disable",
					Table:           "my_table",
					Mode:            ModeFull,
					PublicationName: DefaultPublicationName,
					SlotName:        DefaultSlotName,
				}
				tc.setupWant(&want)
				is.Equal(got, want)
			} else {
				is.Equal(err, tc.wantErr)
				is.Equal(got, Config{})
			}
		})
	}
}

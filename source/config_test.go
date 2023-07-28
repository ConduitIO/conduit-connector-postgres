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
		name: "snapshot mode = initial",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeySnapshotMode] = "initial"
		},
		setupWant: func(cfg *Config) {
			cfg.SnapshotMode = SnapshotModeInitial
		},
	}, {
		name: "snapshot mode = never",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeySnapshotMode] = "never"
		},
		setupWant: func(cfg *Config) {
			cfg.SnapshotMode = SnapshotModeNever
		},
	}, {
		name: "cdc mode = auto",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyCDCMode] = "auto"
		},
		setupWant: func(cfg *Config) {
			cfg.CDCMode = CDCModeAuto
		},
	}, {
		name: "cdc mode = logrepl",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyCDCMode] = "logrepl"
		},
		setupWant: func(cfg *Config) {
			cfg.CDCMode = CDCModeLogrepl
		},
	}, {
		name: "cdc mode = trigger",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyCDCMode] = "trigger"
		},
		setupWant: func(cfg *Config) {
			cfg.CDCMode = CDCModeTrigger
		},
	}, {
		name: "publication name",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyLogreplPublicationName] = "mypublicationname"
		},
		setupWant: func(cfg *Config) {
			cfg.LogreplPublicationName = "mypublicationname"
		},
	}, {
		name: "slot name",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyLogreplSlotName] = "myslotname"
		},
		setupWant: func(cfg *Config) {
			cfg.LogreplSlotName = "myslotname"
		},
	}, {
		name: "batch size",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyBatchSize] = "10000"
		},
		setupWant: func(cfg *Config) {
			cfg.BatchSize = 10000
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
		name: "snapshot mode = invalid",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeySnapshotMode] = "invalid"
		},
		wantErr: errors.New(`"snapshotMode" contains unsupported value "invalid", expected one of [initial never]`),
	}, {
		name: "cdc mode = invalid",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyCDCMode] = "invalid"
		},
		wantErr: errors.New(`"cdcMode" contains unsupported value "invalid", expected one of [auto logrepl trigger]`),
	}, {
		name: "batch size = invalid",
		setupGiven: func(cfg map[string]string) {
			cfg[ConfigKeyBatchSize] = "invalid"
		},
		wantErr: errors.New(`parse "batchSize": strconv.ParseUint: parsing "invalid": invalid syntax`),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given is a basic valid config
			given := map[string]string{
				ConfigKeyURL:            "postgres://user:pass@localhost:5432/testdb?sslmode=disable",
				ConfigKeyTable:          "my_table",
				ConfigKeyOrderingColumn: "col1",
			}
			tc.setupGiven(given)

			got, err := ParseConfig(given)
			if tc.wantErr == nil {
				is.NoErr(err)

				// want is parsed corresponding to the basic valid config
				want := Config{
					URL:                    "postgres://user:pass@localhost:5432/testdb?sslmode=disable",
					Table:                  "my_table",
					OrderingColumn:         "col1",
					SnapshotMode:           SnapshotModeInitial,
					CDCMode:                CDCModeAuto,
					BatchSize:              DefaultBatchSize,
					LogreplPublicationName: DefaultPublicationName,
					LogreplSlotName:        DefaultSlotName,
				}
				tc.setupWant(&want)
				is.Equal(got, want)
			} else {
				errors.Is(err, tc.wantErr)
				is.Equal(got, Config{})
			}
		})
	}
}

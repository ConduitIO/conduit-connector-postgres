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

package source

import (
	"context"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/debezium"
	"github.com/matryer/is"
)

func TestConfig_ValidateDebezium(t *testing.T) {
	off := false
	on := true
	enabled := func() Config {
		return Config{ //nolint:gosec // credentials of the docker-compose test database
			URL:          "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
			Tables:       []string{"orders"},
			CDCMode:      CDCModeLogrepl,
			SnapshotMode: SnapshotModeNever,
			Debezium: DebeziumConfig{
				Enabled: true, Compat: "3.5", FormatVersion: 1, TopicPrefix: "dbz", TopicDelimiter: ".",
				SkippedOperations: "t", UnavailableValuePlaceholder: "__debezium_unavailable_value", MoneyFractionDigits: 2,
				DecimalHandlingMode: "precise", TimePrecisionMode: "adaptive", BinaryHandlingMode: "bytes",
				IntervalHandlingMode: "numeric", HstoreHandlingMode: "json", SchemaDrift: "evolve",
			},
		}
	}
	cases := []struct {
		name   string
		modify func(*Config)
		want   debezium.Code
	}{
		{"valid", func(*Config) {}, ""},
		{"zero config is the default mode", func(c *Config) { c.Debezium = DebeziumConfig{}; c.SnapshotMode = SnapshotModeInitial }, ""},
		{"disabled with a setting", func(c *Config) { c.Debezium.Enabled = false }, debezium.CodeConfigInvalid},
		{"snapshot initial", func(c *Config) { c.SnapshotMode = SnapshotModeInitial }, debezium.CodeConfigInvalid},
		{"payload schema extraction", func(c *Config) { c.PayloadEnabled = &on }, debezium.CodeConfigInvalid},
		{"key schema extraction", func(c *Config) { c.KeyEnabled = &on }, debezium.CodeConfigInvalid},
		{"extraction off", func(c *Config) {
			c.KeyEnabled, c.PayloadEnabled = &off, &off
		}, ""},
		{"compat 2.6", func(c *Config) { c.Debezium.Compat = "2.6" }, ""},
		{"compat unknown", func(c *Config) { c.Debezium.Compat = "1.9" }, debezium.CodeConfigInvalid},
		{"drift halt", func(c *Config) { c.Debezium.SchemaDrift = "halt" }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			c := enabled()
			tc.modify(&c)
			err := c.ValidateDebezium()
			if tc.want == "" {
				is.NoErr(err)
				is.NoErr(c.Validate(context.Background()))
				return
			}
			is.Equal(debezium.CodeOf(err), tc.want)
			is.True(c.Validate(context.Background()) != nil)
		})
	}
}

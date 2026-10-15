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

	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/matryer/is"
)

const importSetting = `{"format":"debezium-postgres/v1","connector":"c","database":"inventory",
"systemIdentifier":"7412345678901234567","plugin":"pgoutput","debeziumVersion":"3.2.4.Final","debeziumSlotName":"debezium",
"publicationName":"dbz_publication","tables":["public.orders"],
"identity":{"conduitSlotConsistentPoint":"0/16E0000","debeziumSlotFlushAtWait":"0/16E2000","conduitPublication":"v1:00"},
"offset":{"lsn_commit":24023064,"lsn":24023128,"txId":555,"messageType":"INSERT"}}`

func TestConfig_ValidateImport(t *testing.T) {
	base := func() Config {
		return Config{ //nolint:gosec // credentials of the docker-compose test database
			URL:                         "postgresql://meroxauser:meroxapass@127.0.0.1:5432/meroxadb",
			Tables:                      []string{"orders"},
			CDCMode:                     CDCModeLogrepl,
			SnapshotMode:                SnapshotModeNever,
			LogreplSlotName:             "conduitslot",
			LogreplImportDebeziumOffset: importSetting,
		}
	}
	cases := []struct {
		name   string
		modify func(*Config)
		want   dbzoffset.Code
	}{
		{"valid", func(*Config) {}, ""},
		{"no import", func(c *Config) { c.LogreplImportDebeziumOffset = ""; c.SnapshotMode = SnapshotModeInitial }, ""},
		{"snapshot initial", func(c *Config) { c.SnapshotMode = SnapshotModeInitial }, dbzoffset.CodeSnapshotModeConflict},
		{"cdc auto", func(c *Config) { c.CDCMode = CDCModeAuto }, dbzoffset.CodeCDCModeConflict},
		{"reuse debezium slot", func(c *Config) { c.LogreplSlotName = "debezium" }, dbzoffset.CodeSlotReuseUnsupported},
		{"malformed", func(c *Config) { c.LogreplImportDebeziumOffset = "{" }, dbzoffset.CodeOffsetInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			c := base()
			tc.modify(&c)
			err := c.Validate(context.Background())
			if tc.want == "" {
				is.NoErr(err)
				return
			}
			is.Equal(dbzoffset.CodeOf(err), tc.want)
		})
	}
}

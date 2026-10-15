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

package dbzoffset

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

// designExample is the D2 example, verbatim.
const designExample = `{
  "format": "debezium-postgres/v1",
  "connector": "inventory-connector",
  "partition": {"server": "dbserver1"},
  "database": "inventory",
  "systemIdentifier": "7412345678901234567",
  "plugin": "pgoutput",
  "debeziumVersion": "3.2.4.Final",
  "debeziumSlotName": "debezium",
  "publicationName": "dbz_publication",
  "tables": ["public.orders", "public.customers"],
  "identity": {"conduitSlotConsistentPoint": "0/16E0000",
               "debeziumSlotFlushAtWait": "0/16E2000",
               "conduitPublication": "v1:0123456789abcdef"},
  "offset": {"lsn_proc": 24023128, "lsn_commit": 24023064, "lsn": 24023128,
             "txId": 555, "ts_usec": 1760000000000000, "messageType": "INSERT"}
}`

func TestParseSetting_DesignExample(t *testing.T) {
	is := is.New(t)
	imp, err := ParseSetting(designExample)
	is.NoErr(err)
	is.Equal(imp.Setting.Database, "inventory")
	is.Equal(imp.Setting.Plugin, PluginPgoutput)
	is.Equal(imp.ConsistentPoint, pglogrepl.LSN(0x16E0000))
	is.Equal(imp.DebeziumFlushAtWait, pglogrepl.LSN(0x16E2000))
	is.Equal(imp.Offset.LSNCommit, pglogrepl.LSN(24023064))
	is.Equal(imp.Plan.Case, CaseDML)
	is.Equal(imp.Plan.Partial, &Partial{Xid: 555, CommitTimeUsec: 1760000000000000, MatchCommitTime: true, SkipChangesBelow: 24023128})
}

// Idempotence (DoD): parsing the same setting twice, or parsing what Encode
// wrote, gives the same plan.
func TestParseSetting_Idempotent(t *testing.T) {
	is := is.New(t)
	a, err := ParseSetting(designExample)
	is.NoErr(err)
	enc, err := a.Setting.Encode()
	is.NoErr(err)
	b, err := ParseSetting(enc)
	is.NoErr(err)
	c, err := ParseSetting(designExample)
	is.NoErr(err)
	is.Equal(a.Plan, b.Plan)
	is.Equal(a.Plan, c.Plan)
	is.Equal(a.ConsistentPoint, b.ConsistentPoint)
}

func withField(t *testing.T, key string, value any) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(designExample), &m); err != nil {
		t.Fatal(err)
	}
	if value == nil {
		delete(m, key)
	} else {
		m[key] = value
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestParseSetting_Refusals(t *testing.T) {
	cases := []struct {
		name     string
		setting  func(t *testing.T) string
		wantCode Code
		wantPath string
	}{
		{"not json", func(*testing.T) string { return "{" }, CodeOffsetInvalid, SettingPath},
		{"empty", func(*testing.T) string { return "" }, CodeOffsetInvalid, SettingPath},
		{"unknown field", func(t *testing.T) string { return withField(t, "resume", "slotOnly") }, CodeOffsetInvalid, SettingPath},
		{"trailing data", func(*testing.T) string { return designExample + " {}" }, CodeOffsetInvalid, SettingPath},
		{"trailing brace", func(*testing.T) string { return designExample + "}" }, CodeOffsetInvalid, SettingPath},
		{"trailing bracket", func(*testing.T) string { return designExample + "]" }, CodeOffsetInvalid, SettingPath},
		{"duplicate offset key", func(*testing.T) string {
			return strings.Replace(designExample, `"offset":`,
				`"offset": {"lsn_commit": 999999999999, "lsn": 999999999999, "messageType": "COMMIT"}, "offset":`, 1)
		}, CodeOffsetInvalid, SettingPath},
		{"duplicate tables key", func(*testing.T) string {
			return strings.Replace(designExample, `"tables":`, `"tables": ["public.other"], "tables":`, 1)
		}, CodeOffsetInvalid, SettingPath},
		{"duplicate nested key", func(*testing.T) string {
			return strings.Replace(designExample, `"conduitSlotConsistentPoint": "0/16E0000",`,
				`"conduitSlotConsistentPoint": "0/1", "conduitSlotConsistentPoint": "0/16E0000",`, 1)
		}, CodeOffsetInvalid, SettingPath},
		{"wrong format", func(t *testing.T) string { return withField(t, "format", "debezium-postgres/v2") }, CodeOffsetInvalid, SettingPath + ".format"},
		{"no format", func(t *testing.T) string { return withField(t, "format", nil) }, CodeOffsetInvalid, SettingPath + ".format"},
		{"no database", func(t *testing.T) string { return withField(t, "database", nil) }, CodeOffsetInvalid, SettingPath + ".database"},
		{"no system identifier", func(t *testing.T) string { return withField(t, "systemIdentifier", nil) }, CodeOffsetInvalid, SettingPath + ".systemIdentifier"},
		{"bad system identifier", func(t *testing.T) string { return withField(t, "systemIdentifier", "abc") }, CodeOffsetInvalid, SettingPath + ".systemIdentifier"},
		{"old slot field name", func(t *testing.T) string { return withField(t, "slotName", "debezium") }, CodeOffsetInvalid, SettingPath},
		{"no debezium slot", func(t *testing.T) string { return withField(t, "debeziumSlotName", nil) }, CodeOffsetInvalid, SettingPath + ".debeziumSlotName"},
		{"no publication", func(t *testing.T) string { return withField(t, "publicationName", "") }, CodeOffsetInvalid, SettingPath + ".publicationName"},
		{"no tables", func(t *testing.T) string { return withField(t, "tables", []string{}) }, CodeOffsetInvalid, SettingPath + ".tables"},
		{"blank table", func(t *testing.T) string { return withField(t, "tables", []string{"public.a", " "}) }, CodeOffsetInvalid, SettingPath + ".tables"},
		{"no identity", func(t *testing.T) string { return withField(t, "identity", nil) }, CodeOffsetInvalid, SettingPath + ".identity.conduitSlotConsistentPoint"},
		{"bad consistent point", func(t *testing.T) string {
			return withField(t, "identity", map[string]string{"conduitSlotConsistentPoint": "16E0000", "debeziumSlotFlushAtWait": "0/16E2000"})
		}, CodeOffsetInvalid, SettingPath + ".identity.conduitSlotConsistentPoint"},
		{"zero consistent point", func(t *testing.T) string {
			return withField(t, "identity", map[string]string{"conduitSlotConsistentPoint": "0/0", "debeziumSlotFlushAtWait": "0/16E2000"})
		}, CodeOffsetInvalid, SettingPath + ".identity.conduitSlotConsistentPoint"},
		{"debezium flush below C", func(t *testing.T) string {
			return withField(t, "identity", map[string]string{"conduitSlotConsistentPoint": "0/16E0000", "debeziumSlotFlushAtWait": "0/16D0000"})
		}, CodeOffsetInvalid, SettingPath + ".identity"},
		{"no offset", func(t *testing.T) string { return withField(t, "offset", nil) }, CodeOffsetInvalid, SettingPath + ".offset"},
		{"no debezium version", func(t *testing.T) string { return withField(t, "debeziumVersion", nil) }, CodeOffsetInvalid, SettingPath + ".debeziumVersion"},
		{"bad debezium version", func(t *testing.T) string { return withField(t, "debeziumVersion", "latest") }, CodeOffsetInvalid, SettingPath + ".debeziumVersion"},
		{"future debezium major", func(t *testing.T) string { return withField(t, "debeziumVersion", "4.0.0.Final") }, CodeOffsetUnsupported, SettingPath + ".debeziumVersion"},
		{"no publication identity", func(t *testing.T) string {
			return withField(t, "identity", map[string]string{"conduitSlotConsistentPoint": "0/16E0000", "debeziumSlotFlushAtWait": "0/16E2000"})
		}, CodeOffsetInvalid, SettingPath + ".identity.conduitPublication"},
		{"snapshot offset", func(t *testing.T) string {
			return withField(t, "offset", map[string]any{"lsn": 100, "snapshot": "INITIAL", "last_snapshot_record": false})
		}, CodeSnapshotInProgress, SettingPath + ".offset.snapshot"},
		{"offset without lsn_commit", func(t *testing.T) string {
			return withField(t, "offset", map[string]any{"lsn": 24023128, "txId": 555})
		}, CodeOffsetUnsupported, SettingPath + ".offset.lsn_commit"},
		{"wal2json", func(t *testing.T) string { return withField(t, "plugin", "wal2json") }, CodePluginUnsupported, SettingPath + ".plugin"},
		{"wal2json_streaming", func(t *testing.T) string { return withField(t, "plugin", "wal2json_streaming") }, CodePluginUnsupported, SettingPath + ".plugin"},
		{"no plugin", func(t *testing.T) string { return withField(t, "plugin", nil) }, CodePluginUnsupported, SettingPath + ".plugin"},
		{"unknown plugin", func(t *testing.T) string { return withField(t, "plugin", "decoderbufs2") }, CodePluginUnsupported, SettingPath + ".plugin"},
		// Lc below C: the fresh slot was created after the offset.
		{"lsn_commit below C", func(t *testing.T) string {
			return withField(t, "offset", map[string]any{"lsn_commit": 0x16D0000, "lsn": 0x16D0100, "txId": 1, "messageType": "INSERT"})
		}, CodeSlotAhead, SettingPath + ".offset.lsn_commit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			_, err := ParseSetting(tc.setting(t))
			is.True(err != nil)
			var e *Error
			is.True(asError(err, &e))
			is.Equal(e.Code, tc.wantCode)
			is.Equal(e.Path, tc.wantPath)
			is.True(e.Fix != "")
			is.True(strings.HasPrefix(err.Error(), string(tc.wantCode)+": "))
		})
	}
}

// The S5 order: an offset problem is reported before a plugin problem, so the
// first fix the operator is told is the one the design puts first.
func TestParseSetting_CheckOrder(t *testing.T) {
	is := is.New(t)
	var m map[string]any
	is.NoErr(json.Unmarshal([]byte(designExample), &m))
	m["plugin"] = "wal2json"
	m["offset"] = map[string]any{"lsn": 100, "incremental_snapshot_collections": "x"}
	b, err := json.Marshal(m)
	is.NoErr(err)
	_, err = ParseSetting(string(b))
	is.Equal(CodeOf(err), CodeIncrementalSnapshotInProgress)
}

func TestParseSetting_Decoderbufs(t *testing.T) {
	is := is.New(t)
	imp, err := ParseSetting(withField(t, "plugin", PluginDecoderbufs))
	is.NoErr(err)
	is.True(imp.Plan.Partial != nil)
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error) //nolint:errorlint // ParseSetting returns *Error unwrapped; the test pins that
	if ok {
		*target = e
	}
	return ok
}

func TestParseSetting_DebeziumVersionAllowlist(t *testing.T) {
	for _, tc := range []struct {
		version  string
		override bool
		want     Code // "" means accepted
		warn     bool
	}{
		{version: "3.2.4.Final"},
		{version: "3.2.0"},
		{version: "2.7.4.Final"},
		{version: "3.2.5-SNAPSHOT"},
		{version: "3.2.0.CR1"},
		{version: "3.4.1.Final", want: CodeDebeziumVersionUntested},
		{version: "3.3.0-SNAPSHOT", want: CodeDebeziumVersionUntested},
		{version: "2.5.0.Final", want: CodeDebeziumVersionUntested},
		{version: "1.9.7.Final", want: CodeDebeziumVersionUntested},
		{version: "3.4.1.Final", override: true, warn: true},
		{version: "1.9.7.Final", override: true, warn: true},
		{version: "4.0.0.Final", want: CodeOffsetUnsupported},
		{version: "4.0.0.Final", override: true, want: CodeOffsetUnsupported},
		{version: "0.10.0.Final", want: CodeOffsetUnsupported},
		{version: " 3.2.4.Final ", want: CodeOffsetInvalid},
		{version: "latest", want: CodeOffsetInvalid},
	} {
		t.Run(fmt.Sprintf("%s/override=%v", tc.version, tc.override), func(t *testing.T) {
			is := is.New(t)
			raw := withField(t, "debeziumVersion", tc.version)
			if tc.override {
				var m map[string]any
				is.NoErr(json.Unmarshal([]byte(raw), &m))
				m["allowUntestedDebeziumVersion"] = true
				b, err := json.Marshal(m)
				is.NoErr(err)
				raw = string(b)
			}
			imp, err := ParseSetting(raw)
			is.Equal(CodeOf(err), tc.want)
			if tc.want == "" {
				is.Equal(len(imp.Warnings) > 0, tc.warn)
			}
		})
	}
}

// encoding/json matches struct fields case-insensitively, so keys that
// differ only in case are duplicates (review NIT): the last would win.
func TestParseSetting_CaseVariantDuplicateKeys(t *testing.T) {
	for name, raw := range map[string]string{
		"offset/Offset": strings.Replace(designExample, `"offset":`,
			`"offset": {"lsn_commit": 999999999999, "lsn": 999999999999, "messageType": "COMMIT"}, "Offset":`, 1),
		"tables/TABLES": strings.Replace(designExample, `"tables":`, `"tables": ["public.other"], "TABLES":`, 1),
		"conduitPublication/ConduitPublication": strings.Replace(designExample, `"conduitPublication":`,
			`"conduitPublication": "v1:x", "ConduitPublication":`, 1),
		"lsn_commit/LSN_COMMIT in the offset": strings.Replace(designExample, `"lsn_commit": 24023064`,
			`"lsn_commit": 24023064, "LSN_COMMIT": 1`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			_, err := ParseSetting(raw)
			is.Equal(CodeOf(err), CodeOffsetInvalid)
		})
	}
}

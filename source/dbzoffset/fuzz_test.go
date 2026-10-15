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
	"testing"
)

// canonical re-encodes a parsed offset with the keys the parser reads.
func canonical(o Offset) []byte {
	m := map[string]any{"lsn_commit": uint64(o.LSNCommit)}
	if o.LSN != 0 {
		m["lsn"] = uint64(o.LSN)
	}
	if o.LSNProc != 0 {
		m["lsn_proc"] = uint64(o.LSNProc)
	}
	if o.HasTxID {
		m["txId"] = o.TxID
	}
	if o.HasTsUsec {
		m["ts_usec"] = o.TsUsec
	}
	if o.MessageType != "" {
		m["messageType"] = string(o.MessageType)
	}
	if o.LSNEventsProcessed != 0 {
		m["lsn_events_processed"] = o.LSNEventsProcessed
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

// FuzzParseDebeziumOffset is the parser's boundary test (design: Tests). The
// seed corpus is every captured fixture plus the hand-written refusals.
func FuzzParseDebeziumOffset(f *testing.F) {
	for _, fx := range loadFixtures(f) {
		f.Add([]byte(fx.Offset))
	}
	for _, s := range []string{
		`{}`, `[]`, `null`, `{"lsn_commit":1}`, `{"lsn_commit":1,"lsn_commit":1}`,
		`{"lsn_commit":18446744073709551615,"txId":-2147483648,"messageType":"INSERT","lsn":1}`,
		`{"lsn":5,"snapshot":"INITIAL"}`, `{"lsn":5,"incremental_snapshot_x":1}`,
		`{"lsn_commit":5,"lsn":5,"messageType":"MESSAGE"}`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		o, err := ParseOffset(raw)
		if err != nil {
			if CodeOf(err) == "" {
				t.Fatalf("an uncoded error: %v", err)
			}
			return
		}
		if o.LSNCommit == 0 {
			t.Fatalf("accepted an offset without lsn_commit: %s", raw)
		}
		p := NewPlan(o)
		if _, _, ok := p.ResumeKey(); !ok {
			t.Fatalf("no resume key for an accepted offset: %s", raw)
		}
		if p.Partial != nil && (!o.MessageType.IsDML() || !o.HasTxID || o.LSN == 0) {
			t.Fatalf("partial rule armed without a DML type, xid and lsn: %s", raw)
		}
		// Round trip: what was read is all the rule uses.
		o2, err := ParseOffset(canonical(o))
		if err != nil {
			t.Fatalf("canonical form refused: %v", err)
		}
		if o2 != o {
			t.Fatalf("round trip changed the offset: %+v != %+v", o2, o)
		}
	})
}

// FuzzParseSetting covers the whole setting, the value the connector reads
// from its configuration.
func FuzzParseSetting(f *testing.F) {
	f.Add(designExample)
	f.Add(`{"format":"debezium-postgres/v1"}`)
	f.Add(`{"format":"debezium-postgres/v1","offset":{}}`)
	f.Fuzz(func(t *testing.T, raw string) {
		imp, err := ParseSetting(raw)
		if err != nil {
			if CodeOf(err) == "" {
				t.Fatalf("an uncoded error: %v", err)
			}
			return
		}
		if imp.Offset.LSNCommit < imp.ConsistentPoint {
			t.Fatalf("accepted lsn_commit below the consistent point")
		}
		enc, err := imp.Setting.Encode()
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseSetting(enc)
		if err != nil {
			t.Fatalf("re-encoded setting refused: %v", err)
		}
		if again.Plan != imp.Plan {
			// Plan holds a pointer; compare what it points at.
			if (again.Plan.Partial == nil) != (imp.Plan.Partial == nil) ||
				(again.Plan.Partial != nil && *again.Plan.Partial != *imp.Plan.Partial) ||
				again.Plan.SkipCommitsBelow != imp.Plan.SkipCommitsBelow || again.Plan.Case != imp.Plan.Case {
				t.Fatalf("re-encoding changed the plan")
			}
		}
	})
}

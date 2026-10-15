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
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixture struct {
	Dir    string          `json:"-"`
	Phase  string          `json:"phase"`
	Offset json.RawMessage `json:"offset"`
}

// loadFixtures reads every testdata/*/offsets.jsonl: one JSON object per
// line, {"phase": ..., "offset": {...}}, as capture.sh writes it.
func loadFixtures(tb testing.TB) []fixture {
	tb.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "*", "offsets.jsonl"))
	if err != nil {
		tb.Fatal(err)
	}
	if len(files) == 0 {
		tb.Fatal("no fixtures under testdata/*/offsets.jsonl")
	}
	var out []fixture
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			tb.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var fx fixture
			if err := json.Unmarshal([]byte(line), &fx); err != nil {
				tb.Fatalf("%s: %v", path, err)
			}
			fx.Dir = filepath.Base(filepath.Dir(path))
			out = append(out, fx)
		}
		_ = f.Close()
		if err := sc.Err(); err != nil {
			tb.Fatal(err)
		}
	}
	return out
}

// TestFixtures runs every captured offset through ParseOffset and NewPlan.
// The expectation comes from the design, keyed on what Debezium wrote:
//   - an incremental_snapshot_* key: CodeIncrementalSnapshotInProgress;
//   - a snapshot key (any value but false/null), or a snapshot phase:
//     CodeSnapshotInProgress;
//   - no lsn_commit: CodeOffsetUnsupported;
//   - otherwise accepted, with the case its messageType implies, and a DML
//     offset arms the partial rule for its own txId.
func TestFixtures(t *testing.T) { //nolint:gocyclo // one flat table of expected outcomes
	midTransaction := map[string]bool{}
	for i, fx := range loadFixtures(t) {
		t.Run(fmt.Sprintf("%s/%d-%s", fx.Dir, i, fx.Phase), func(t *testing.T) {
			var m map[string]json.RawMessage
			if err := json.Unmarshal(fx.Offset, &m); err != nil {
				t.Fatal(err)
			}
			present := func(k string) bool { v, ok := m[k]; return ok && string(v) != "null" }
			incremental := false
			for k := range m {
				if strings.HasPrefix(k, "incremental_snapshot_") && present(k) {
					incremental = true
				}
			}
			snapshot := present("snapshot") && string(m["snapshot"]) != "false"

			o, err := ParseOffset(fx.Offset)
			got := CodeOf(err)
			switch {
			case incremental:
				if got != CodeIncrementalSnapshotInProgress {
					t.Fatalf("want %s, got %q (%v): %s", CodeIncrementalSnapshotInProgress, got, err, fx.Offset)
				}
			case snapshot || fx.Phase == "snapshot" || fx.Phase == "snapshot_done":
				if got != CodeSnapshotInProgress {
					t.Fatalf("want %s, got %q (%v): %s", CodeSnapshotInProgress, got, err, fx.Offset)
				}
			case !present("lsn_commit"):
				if got != CodeOffsetUnsupported {
					t.Fatalf("want %s, got %q: %s", CodeOffsetUnsupported, got, fx.Offset)
				}
			default:
				if err != nil {
					t.Fatalf("a streaming offset was refused: %v", err)
				}
				p := NewPlan(o)
				switch o.MessageType {
				case MessageCommit:
					if p.Case != CaseCommit || p.Partial != nil {
						t.Fatalf("COMMIT offset planned as %s", p.Case)
					}
				case MessageBegin:
					if p.Case != CaseBegin {
						t.Fatalf("BEGIN offset planned as %s", p.Case)
					}
				case MessageInsert, MessageUpdate, MessageDelete, MessageTruncate:
					if p.Case != CaseDML || p.Partial == nil || p.Partial.Xid != o.TxID {
						t.Fatalf("DML offset not armed: %+v", p)
					}
					if o.LSN > o.LSNCommit {
						midTransaction[fx.Dir] = true
					}
				case MessageMessage:
					if o.IsNonTransactionalMessage() != (p.Case == CaseCommit) {
						t.Fatalf("MESSAGE offset planned as %s", p.Case)
					}
				}
			}
		})
	}
	// Every real capture has a workload phase that commits offsets in the
	// middle of a large transaction: the partial rule's case.
	for _, fx := range loadFixtures(t) {
		if fx.Dir != "synthetic" && !midTransaction[fx.Dir] {
			t.Errorf("%s: no mid-transaction DML offset captured", fx.Dir)
		}
	}
}

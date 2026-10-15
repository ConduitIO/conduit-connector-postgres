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

// Each capture phase has one expected outcome. The phases are capture.sh's.
func TestFixtures(t *testing.T) { //nolint:gocyclo // one flat table of expected outcomes per capture phase
	for i, fx := range loadFixtures(t) {
		t.Run(fx.Dir+"/"+fx.Phase, func(t *testing.T) {
			o, err := ParseOffset(fx.Offset)
			got := CodeOf(err)
			hasIncremental := strings.Contains(string(fx.Offset), `"incremental_snapshot_`) &&
				!strings.Contains(string(fx.Offset), `"incremental_snapshot_collections":null`)
			switch {
			case fx.Phase == "snapshot" || fx.Phase == "snapshot_done":
				if got != CodeSnapshotInProgress {
					t.Fatalf("fixture %d: want %s, got %q (%v): %s", i, CodeSnapshotInProgress, got, err, fx.Offset)
				}
			case fx.Phase == "legacy_1x":
				if got != CodeOffsetUnsupported {
					t.Fatalf("fixture %d: want %s, got %q", i, CodeOffsetUnsupported, got)
				}
			case hasIncremental:
				if got != CodeIncrementalSnapshotInProgress {
					t.Fatalf("fixture %d: want %s, got %q: %s", i, CodeIncrementalSnapshotInProgress, got, fx.Offset)
				}
			default:
				if err != nil {
					t.Fatalf("fixture %d: a streaming offset was refused: %v", i, err)
				}
				p := NewPlan(o)
				switch o.MessageType {
				case MessageCommit:
					if p.Case != CaseCommit || p.Partial != nil {
						t.Fatalf("fixture %d: COMMIT offset planned as %s", i, p.Case)
					}
				case MessageBegin:
					if p.Case != CaseBegin {
						t.Fatalf("fixture %d: BEGIN offset planned as %s", i, p.Case)
					}
				case MessageInsert, MessageUpdate, MessageDelete, MessageTruncate:
					if p.Case != CaseDML || p.Partial == nil || p.Partial.Xid != o.TxID {
						t.Fatalf("fixture %d: DML offset not armed: %+v", i, p)
					}
				case MessageMessage:
					if o.IsNonTransactionalMessage() != (p.Case == CaseCommit) {
						t.Fatalf("fixture %d: MESSAGE offset planned as %s", i, p.Case)
					}
				}
			}
		})
	}
}

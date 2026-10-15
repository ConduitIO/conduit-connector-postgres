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

//go:build debezium

package debezium

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// rowImage is the part of a row the diff looks at.
type rowImage struct {
	ID  json.Number `json:"id"`
	Ver json.Number `json:"ver"`
}

func (r *rowImage) ints() (pk, ver int64, err error) {
	if r == nil {
		return 0, 0, errors.New("row image is absent")
	}
	if pk, err = r.ID.Int64(); err != nil {
		return 0, 0, err
	}
	ver, err = r.Ver.Int64()
	return pk, ver, err
}

// dbzEnvelope is Debezium's change event with the JSON converter's schemas
// disabled.
type dbzEnvelope struct {
	Before *rowImage `json:"before"`
	After  *rowImage `json:"after"`
	Op     string    `json:"op"`
	Source struct {
		Table string `json:"table"`
	} `json:"source"`
}

// readTopics reads every topic whose name starts with prefix to the end, all
// partitions, with the Kafka console consumer inside the kafka container.
// "To the end" is the consumer's idle timeout: Debezium is stopped by now, so
// nothing is being produced. It returns the raw message values.
func (s *stack) readTopics(ctx context.Context, prefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	// The project and file come from run.sh, not from input.
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", s.project, "-f", s.composeFile, //nolint:gosec // test harness; arguments come from run.sh
		"exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", "localhost:9092",
		"--include", "^"+strings.ReplaceAll(prefix, ".", "\\.")+"\\..*$",
		"--from-beginning", "--timeout-ms", "20000")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	// The console consumer exits non-zero when its idle timeout fires, which
	// is how it ends here. A run that never consumed anything is the failure.
	if err != nil && !strings.Contains(stderr.String(), "Processed a total of") {
		return nil, fmt.Errorf("kafka-console-consumer: %w: %s", err, tail(stderr.String()))
	}
	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			lines = append(lines, l)
		}
	}
	return lines, sc.Err()
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 600 {
		return s[len(s)-600:]
	}
	return s
}

// debeziumChanges turns topic messages into change identities. Snapshot reads
// (op r) are not changes the workload made and are counted apart. A change
// may appear more than once: the count is kept.
func debeziumChanges(lines []string) (counts map[string]int, snapshotReads int, err error) {
	counts = map[string]int{}
	for _, l := range lines {
		var e dbzEnvelope
		dec := json.NewDecoder(strings.NewReader(l))
		dec.UseNumber()
		if err := dec.Decode(&e); err != nil {
			return nil, 0, fmt.Errorf("decode Debezium message %q: %w", l, err)
		}
		var img *rowImage
		var op string
		switch e.Op {
		case "r":
			snapshotReads++
			continue
		case "c", "u":
			img, op = e.After, e.Op
		case "d":
			img, op = e.Before, e.Op
		default:
			return nil, 0, fmt.Errorf("unexpected Debezium op %q in %q", e.Op, l)
		}
		pk, ver, err := img.ints()
		if err != nil {
			return nil, 0, fmt.Errorf("Debezium message %q: %w", l, err)
		}
		counts[change{table: e.Source.Table, pk: pk, ver: ver, op: op}.key()]++
	}
	return counts, snapshotReads, nil
}

// probeTx is one transaction decoded from the probe slot.
type probeTx struct {
	xid        uint32
	commitLSN  pglogrepl.LSN // BEGIN's final LSN (the commit record's start)
	commitTime time.Time
	changes    []probeChange
}

type probeChange struct {
	lsn pglogrepl.LSN
	c   change
}

// probeTransactions decodes everything a pgoutput slot holds, without
// consuming it. The probe slot is created before Conduit's, with Debezium's
// publication, so it holds every transaction Conduit's slot does.
func probeTransactions(ctx context.Context, conn *pgx.Conn, slot, publication string) ([]probeTx, error) {
	rows, err := conn.Query(ctx, `SELECT lsn, data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL,
		'proto_version', '1', 'publication_names', $2)`, slot, publication)
	if err != nil {
		return nil, fmt.Errorf("peek probe slot: %w", err)
	}
	defer rows.Close()

	rels := map[uint32]*pglogrepl.RelationMessage{}
	var txs []probeTx
	var cur *probeTx
	for rows.Next() {
		var lsn pglogrepl.LSN
		var data []byte
		if err := rows.Scan(&lsn, &data); err != nil {
			return nil, err
		}
		msg, err := pglogrepl.Parse(data)
		if err != nil {
			return nil, fmt.Errorf("parse pgoutput message at %s: %w", lsn, err)
		}
		switch m := msg.(type) {
		case *pglogrepl.RelationMessage:
			rels[m.RelationID] = m
		case *pglogrepl.BeginMessage:
			txs = append(txs, probeTx{xid: m.Xid, commitLSN: m.FinalLSN, commitTime: m.CommitTime})
			cur = &txs[len(txs)-1]
		case *pglogrepl.CommitMessage:
			cur = nil
		case *pglogrepl.InsertMessage:
			err = addProbeChange(cur, lsn, rels[m.RelationID], m.Tuple, opCreate)
		case *pglogrepl.UpdateMessage:
			err = addProbeChange(cur, lsn, rels[m.RelationID], m.NewTuple, opUpdate)
		case *pglogrepl.DeleteMessage:
			err = addProbeChange(cur, lsn, rels[m.RelationID], m.OldTuple, opDelete)
		}
		if err != nil {
			return nil, err
		}
	}
	return txs, rows.Err()
}

func addProbeChange(cur *probeTx, lsn pglogrepl.LSN, rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData, op string) error {
	if cur == nil || rel == nil || tup == nil {
		return fmt.Errorf("probe: %s change at %s outside a transaction or without its relation", op, lsn)
	}
	c := change{table: rel.RelationName, op: op}
	for i, col := range tup.Columns {
		if col.DataType != 't' {
			continue
		}
		v, err := strconv.ParseInt(string(col.Data), 10, 64)
		switch rel.Columns[i].Name {
		case "id":
			c.pk = v
		case "ver":
			c.ver = v
		default:
			continue
		}
		if err != nil {
			return fmt.Errorf("probe: column %s at %s: %w", rel.Columns[i].Name, lsn, err)
		}
	}
	cur.changes = append(cur.changes, probeChange{lsn: lsn, c: c})
	return nil
}

// conduitOutput is what the Postgres source delivered.
type conduitOutput struct {
	counts  map[string]int
	records int
	sawLast bool
}

// recordChange maps a source record to a change identity. A delete carries
// its row in Before (the tables use REPLICA IDENTITY FULL).
func recordChange(rec opencdc.Record) (change, error) {
	var op string
	var img opencdc.Data
	switch rec.Operation {
	case opencdc.OperationCreate:
		op, img = opCreate, rec.Payload.After
	case opencdc.OperationUpdate:
		op, img = opUpdate, rec.Payload.After
	case opencdc.OperationDelete:
		op, img = opDelete, rec.Payload.Before
	default:
		return change{}, fmt.Errorf("unexpected operation %s", rec.Operation)
	}
	if img == nil {
		return change{}, fmt.Errorf("%s record has no row image", rec.Operation)
	}
	var row rowImage
	dec := json.NewDecoder(bytes.NewReader(img.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&row); err != nil {
		return change{}, fmt.Errorf("decode row image %q: %w", img.Bytes(), err)
	}
	pk, ver, err := row.ints()
	if err != nil {
		return change{}, err
	}
	return change{table: rec.Metadata[opencdc.MetadataCollection], pk: pk, ver: ver, op: op}, nil
}

// drainConduit reads and acks everything the source delivers until the
// workload's last row has arrived and the source has then been quiet for
// quiet. Every batch is acked after it is read, as the engine does.
func drainConduit(ctx context.Context, t *testing.T, src sdk.Source, last change, quiet, limit time.Duration) conduitOutput {
	t.Helper()
	out := conduitOutput{counts: map[string]int{}}
	deadline := time.Now().Add(limit)
	var quietSince time.Time
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		recs, err := src.ReadN(rctx, 500)
		expired := rctx.Err() != nil
		cancel()
		if err != nil && !expired && !errors.Is(err, sdk.ErrBackoffRetry) {
			t.Fatalf("source ReadN after %d records: %v", out.records, err)
		}
		for _, rec := range recs {
			c, err := recordChange(rec)
			if err != nil {
				t.Fatalf("record %d (position %s): %v", out.records, rec.Position, err)
			}
			out.counts[c.key()]++
			out.records++
			if c.key() == last.key() {
				out.sawLast = true
			}
			if err := src.Ack(ctx, rec.Position); err != nil {
				t.Fatalf("ack record %d: %v", out.records, err)
			}
		}
		switch {
		case len(recs) > 0:
			quietSince = time.Time{}
		case out.sawLast && quietSince.IsZero():
			quietSince = time.Now()
		case out.sawLast && time.Since(quietSince) >= quiet:
			return out
		}
	}
	t.Fatalf("source did not deliver the last row %s within %s (read %d records)", last.key(), limit, out.records)
	return out
}

// composeLogs returns the tail of a service's log.
func (s *stack) composeLogs(ctx context.Context, service string, lines int) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", s.project, "-f", s.composeFile, //nolint:gosec // test harness; arguments come from run.sh
		"logs", "--no-color", "--tail", strconv.Itoa(lines), service)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

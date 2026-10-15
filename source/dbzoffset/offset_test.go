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
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestParseOffset_Streaming(t *testing.T) {
	is := is.New(t)
	o, err := ParseOffset([]byte(`{"lsn_proc":24023128,"lsn_commit":24023064,"lsn":24023128,
		"txId":555,"ts_usec":1760000000000000,"messageType":"INSERT","lsn_events_processed":2,
		"xmin":null,"transaction_id":null}`))
	is.NoErr(err)
	is.Equal(o, Offset{
		LSN:                24023128,
		LSNCommit:          24023064,
		LSNProc:            24023128,
		TxID:               555,
		HasTxID:            true,
		TsUsec:             1760000000000000,
		HasTsUsec:          true,
		MessageType:        MessageInsert,
		LSNEventsProcessed: 2,
	})
}

// LSNs above 2^53 do not survive float64; they must round-trip exactly.
func TestParseOffset_LargeLSNExact(t *testing.T) {
	is := is.New(t)
	o, err := ParseOffset([]byte(`{"lsn_commit":18446744073709551615,"lsn":9007199254740993}`))
	is.NoErr(err)
	is.Equal(o.LSNCommit, pglogrepl.LSN(18446744073709551615))
	is.Equal(o.LSN, pglogrepl.LSN(9007199254740993))
}

func TestParseOffset_TxIDNormalisation(t *testing.T) {
	cases := []struct {
		in      string
		want    uint32
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "555", want: 555},
		{in: "2147483647", want: 2147483647},
		{in: "2147483648", want: 2147483648},
		{in: "4294967295", want: 4294967295},
		// Older Debezium read the decoderbufs xid as a signed int.
		{in: "-1", want: 4294967295},
		{in: "-2147483648", want: 2147483648},
		{in: "4294967296", wantErr: true},
		{in: "-2147483649", wantErr: true},
		{in: "1.5", wantErr: true},
		{in: `"555"`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			is := is.New(t)
			o, err := ParseOffset([]byte(`{"lsn_commit":100,"txId":` + tc.in + `}`))
			if tc.wantErr {
				is.Equal(CodeOf(err), CodeOffsetInvalid)
				return
			}
			is.NoErr(err)
			is.True(o.HasTxID)
			is.Equal(o.TxID, tc.want)
		})
	}
}

func TestParseOffset_Refusals(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    Code
		wantFix string
	}{
		{"not json", `{`, CodeOffsetInvalid, "Re-export"},
		{"not an object", `[1,2]`, CodeOffsetInvalid, "Re-export"},
		{"trailing data", `{"lsn_commit":1} {}`, CodeOffsetInvalid, ""},
		{"duplicate key", `{"lsn_commit":1,"lsn_commit":2}`, CodeOffsetInvalid, ""},
		{"negative lsn", `{"lsn_commit":-1}`, CodeOffsetInvalid, ""},
		{"lsn too large", `{"lsn_commit":18446744073709551616}`, CodeOffsetInvalid, ""},
		{"lsn as float", `{"lsn_commit":1.0}`, CodeOffsetInvalid, ""},
		{"lsn as exponent", `{"lsn_commit":1e3}`, CodeOffsetInvalid, ""},
		{"lsn as string", `{"lsn_commit":"0/16E8F58"}`, CodeOffsetInvalid, ""},
		{"lsn_commit zero", `{"lsn_commit":0}`, CodeOffsetInvalid, ""},
		{"messageType not string", `{"lsn_commit":1,"messageType":1}`, CodeOffsetInvalid, ""},
		{"ts_usec not integer", `{"lsn_commit":1,"ts_usec":true}`, CodeOffsetInvalid, ""},
		{"snapshot flag not bool", `{"lsn_commit":1,"snapshot_completed":"yes"}`, CodeOffsetInvalid, ""},

		{"no lsn_commit", `{"lsn":100,"txId":1,"messageType":"INSERT"}`, CodeOffsetUnsupported, "lsn_commit"},
		{"empty", `{}`, CodeOffsetUnsupported, ""},

		{"snapshot running (old bool)", `{"lsn":100,"snapshot":true,"last_snapshot_record":false}`, CodeSnapshotInProgress, "Let the Debezium snapshot finish"},
		{"snapshot running (type)", `{"lsn":100,"snapshot":"INITIAL","last_snapshot_record":false,"snapshot_completed":false}`, CodeSnapshotInProgress, "Let the Debezium snapshot finish"},
		{"last snapshot record", `{"lsn":100,"snapshot":"INITIAL","last_snapshot_record":true,"snapshot_completed":false}`, CodeSnapshotInProgress, "Let the Debezium snapshot finish"},
		{"snapshot key even with lsn_commit", `{"lsn":100,"lsn_commit":90,"snapshot":"BLOCKING"}`, CodeSnapshotInProgress, ""},
		{"completed, no stream yet", `{"lsn":100,"last_snapshot_record":true}`, CodeSnapshotInProgress, "two separate transactions"},
		{"completed (3.x), no stream yet", `{"lsn":100,"snapshot_completed":true}`, CodeSnapshotInProgress, "heartbeat.action.query"},

		{"incremental snapshot", `{"lsn_commit":100,"incremental_snapshot_collections":"[{\"incremental_snapshot_collections_id\":\"public.t\"}]"}`, CodeIncrementalSnapshotInProgress, "stop-snapshot"},
		// S5: an incremental snapshot gets its own code even without lsn_commit.
		{"incremental snapshot without lsn_commit", `{"lsn":100,"incremental_snapshot_primary_key":"aced0005"}`, CodeIncrementalSnapshotInProgress, ""},
		// ... and before the snapshot check.
		{"incremental and snapshot", `{"lsn":100,"snapshot":true,"incremental_snapshot_correlation_id":"x"}`, CodeIncrementalSnapshotInProgress, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			_, err := ParseOffset([]byte(tc.in))
			is.True(err != nil)
			is.Equal(CodeOf(err), tc.want)
			// The code leads the message, so it survives string-only layers.
			is.True(strings.HasPrefix(err.Error(), string(tc.want)+": "))
			is.True(strings.Contains(err.Error(), SettingPath))
			if tc.wantFix != "" {
				is.True(strings.Contains(err.Error(), tc.wantFix))
			}
		})
	}
}

// A snapshot that completed and was followed by a committed transaction is a
// streaming offset (snapshot_completed stays in some versions' offsets).
func TestParseOffset_SnapshotCompletedWithStream(t *testing.T) {
	is := is.New(t)
	o, err := ParseOffset([]byte(`{"lsn":200,"lsn_commit":180,"snapshot_completed":true,"last_snapshot_record":false,"snapshot":false}`))
	is.NoErr(err)
	is.Equal(o.LSNCommit, pglogrepl.LSN(180))
}

// Null values are the same as absent keys, so a null snapshot key is not a
// snapshot and a null incremental key is not an incremental snapshot.
func TestParseOffset_Nulls(t *testing.T) {
	is := is.New(t)
	o, err := ParseOffset([]byte(`{"lsn_commit":180,"lsn":null,"txId":null,"ts_usec":null,"messageType":null,
		"snapshot":null,"incremental_snapshot_collections":null}`))
	is.NoErr(err)
	is.Equal(o, Offset{LSNCommit: 180})
}

func TestOffset_IsNonTransactionalMessage(t *testing.T) {
	is := is.New(t)
	is.True(Offset{MessageType: MessageMessage, LSN: 10, LSNCommit: 10}.IsNonTransactionalMessage())
	is.True(!Offset{MessageType: MessageMessage, LSN: 12, LSNCommit: 10}.IsNonTransactionalMessage())
	is.True(!Offset{MessageType: MessageCommit, LSN: 10, LSNCommit: 10}.IsNonTransactionalMessage())
}

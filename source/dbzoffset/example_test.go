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

package dbzoffset_test

import (
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
)

// The CLI's dry run: parse an offset read from Kafka Connect and print the
// resume point the connector would use.
func ExampleParseOffset() {
	o, err := dbzoffset.ParseOffset([]byte(`{"lsn_proc":24023128,"lsn_commit":24023064,
		"lsn":24023128,"txId":555,"ts_usec":1760000000000000,"messageType":"INSERT"}`))
	if err != nil {
		fmt.Println(dbzoffset.CodeOf(err))
		return
	}
	p := dbzoffset.NewPlan(o)
	fmt.Println("skip commits below:", p.SkipCommitsBelow)
	fmt.Println("partial: xid", p.Partial.Xid, "skip changes below", p.Partial.SkipChangesBelow)
	fmt.Println("duplicates:", p.ExpectedDuplicates())
	// Output:
	// skip commits below: 0/16E9018
	// partial: xid 555 skip changes below 0/16E9058
	// duplicates: xid 555, changes at LSN 0/16E9058
}

// A snapshot offset is refused with a stable code; the CLI passes it through.
func ExampleCodeOf() {
	_, err := dbzoffset.ParseOffset([]byte(`{"lsn":100,"snapshot":"INITIAL","last_snapshot_record":false}`))
	fmt.Println(dbzoffset.CodeOf(err))
	// Output: postgres.import.snapshot_in_progress
}

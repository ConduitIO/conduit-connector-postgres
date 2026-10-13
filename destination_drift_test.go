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

package postgres

import (
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/matryer/is"
)

// TestIsDriftMarker pins the marker shape. The positive cases are what
// source/logrepl's emitDriftMarker builds (sdk.Util.Source.NewRecordCreate
// with nil key and nil payload) plus empty-value normalizations; every
// negative case carries the drift metadata and must therefore be written.
func TestIsDriftMarker(t *testing.T) {
	drift := map[string]string{logrepl.MetadataSchemaDrift: "true"}

	tests := []struct {
		name string
		rec  opencdc.Record
		want bool
	}{
		{"source marker shape", opencdc.Record{Operation: opencdc.OperationCreate, Metadata: drift}, true},
		{"empty raw key and payload", opencdc.Record{
			Operation: opencdc.OperationCreate, Metadata: drift,
			Key: opencdc.RawData(nil), Payload: opencdc.Change{After: opencdc.RawData{}},
		}, true},
		{"empty structured key and payload", opencdc.Record{
			Operation: opencdc.OperationCreate, Metadata: drift,
			Key: opencdc.StructuredData{}, Payload: opencdc.Change{After: opencdc.StructuredData{}},
		}, true},
		{"no drift metadata", opencdc.Record{Operation: opencdc.OperationCreate}, false},
		{"drift metadata not true", opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  map[string]string{logrepl.MetadataSchemaDrift: "false"},
		}, false},
		{"keyed", opencdc.Record{
			Operation: opencdc.OperationCreate, Metadata: drift,
			Key: opencdc.StructuredData{"id": 1},
		}, false},
		{"raw key", opencdc.Record{
			Operation: opencdc.OperationCreate, Metadata: drift,
			Key: opencdc.RawData("k"),
		}, false},
		{"payload after", opencdc.Record{
			Operation: opencdc.OperationCreate, Metadata: drift,
			Payload: opencdc.Change{After: opencdc.StructuredData{"v": "x"}},
		}, false},
		{"payload before", opencdc.Record{
			Operation: opencdc.OperationCreate, Metadata: drift,
			Payload: opencdc.Change{Before: opencdc.StructuredData{"v": "x"}},
		}, false},
		{"update", opencdc.Record{Operation: opencdc.OperationUpdate, Metadata: drift}, false},
		{"delete", opencdc.Record{Operation: opencdc.OperationDelete, Metadata: drift}, false},
		{"snapshot", opencdc.Record{Operation: opencdc.OperationSnapshot, Metadata: drift}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is.New(t).Equal(isDriftMarker(tc.rec), tc.want)
		})
	}
}

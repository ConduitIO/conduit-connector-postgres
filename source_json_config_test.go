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
	"context"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// json.invalidUTF8 accepts base64 and halt, defaults to base64 (the source
// never halts on such text by default), and reaches the logical replication
// config, which hands the same policy to the snapshot and CDC.
func TestSource_JSONInvalidUTF8_Param(t *testing.T) {
	for _, tc := range []struct {
		value     *string
		wantErr   bool
		wantHalts bool
	}{
		{value: nil},
		{value: ptr("base64")},
		{value: ptr("halt"), wantHalts: true},
		{value: ptr("replace"), wantErr: true},
		{value: ptr("Halt"), wantErr: true},
		{value: ptr("")}, // empty is unset: the default
	} {
		name := "unset"
		if tc.value != nil {
			name = "'" + *tc.value + "'"
		}
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			src := NewSource()
			cfg := map[string]string{"url": "postgres://localhost:5432/db", "tables": "t"}
			if tc.value != nil {
				cfg["json.invalidUTF8"] = *tc.value
			}
			err := sdk.Util.ParseConfig(context.Background(), cfg, src.Config(), Connector.NewSpecification().SourceParams)
			if tc.wantErr {
				is.True(err != nil)
				return
			}
			is.NoErr(err)
			conf := src.Config().(*source.Config)
			if tc.value == nil {
				is.Equal(conf.JSONInvalidUTF8, types.JSONInvalidUTF8Base64)
			}
			s := &Source{config: *conf}
			s.config.BatchSize = ptr(0)
			lc := s.logreplConfig(nil, nil)
			is.True(lc.JSON != nil)
			is.Equal(lc.JSON.Halts(), tc.wantHalts)
			is.True(s.logreplConfig(nil, nil).JSON == lc.JSON) // one policy per source
		})
	}
}

func ptr[T any](v T) *T { return &v }

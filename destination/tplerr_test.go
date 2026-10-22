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

package destination

import (
	"errors"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// A table template runs on every record, and the errors of its functions quote
// the record value they were given.
func TestTableFunction_ErrorDoesNotContainRecordValues(t *testing.T) {
	const secret = "customer-SECRET-4711"
	for name, tc := range map[string]struct {
		table  string
		wantFn string // "" if the error must not name a function
	}{
		"mustToDate":          {`{{ mustToDate "2006-01-02" (index .Payload.After "note") }}`, "mustToDate"},
		"mustDateModify":      {`{{ mustDateModify (index .Payload.After "note") now }}`, "mustDateModify"},
		"mustFromJson":        {`{{ mustFromJson (index .Payload.After "note") }}`, "mustFromJson"},
		"range over a string": {`{{ range (index .Payload.After "note") }}{{ end }}`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			f, err := (&Config{Table: tc.table}).TableFunction()
			is.NoErr(err)
			_, err = f(opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"note": secret}}})
			is.True(err != nil)
			is.True(!strings.Contains(err.Error(), "SECRET"))
			is.True(strings.Contains(err.Error(), "template execution failed at line 1, column"))
			if tc.wantFn == "" {
				is.True(!strings.Contains(err.Error(), "in function"))
			} else {
				is.True(strings.Contains(err.Error(), "in function "+tc.wantFn))
			}
			var te *tplExecError
			is.True(errors.As(err, &te)) // the original error stays in the chain
		})
	}
}

// The value a template error quotes can look like the part of the message the
// function name is read from.
func TestExecError_ValueCannotSupplyTheFunctionName(t *testing.T) {
	for _, value := range []string{
		"q error calling SECRET_TOKEN: x",
		`x" at <.s>: error calling SECRET_TOKEN: x`,
	} {
		is := is.New(t)
		f, err := (&Config{Table: `{{ range (index .Payload.After "note") }}{{ end }}`}).TableFunction()
		is.NoErr(err)
		_, err = f(opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"note": value}}})
		is.True(err != nil)
		is.True(!strings.Contains(err.Error(), "SECRET"))
		is.True(!strings.Contains(err.Error(), "in function"))
	}
	is.New(t).Equal(execError(nil), nil)
}

// Copyright © 2023 Meroxa, Inc.
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
	"fmt"
	"regexp"
)

var (
	tplPosition = regexp.MustCompile(`^template: [^:]*:(\d+):(\d+): `)
	// tplFunction is anchored right after the `executing "..." at <expr>: `
	// prefix text/template writes. What follows can quote a record value
	// (range can't iterate over <value>), and the value can contain "error
	// calling X:" too, so the name is not searched for in the whole text. The
	// expression is template source and has no '>' to confuse the prefix; one
	// that has is reported without a function.
	tplFunction = regexp.MustCompile(`^template: [^:]*:\d+:\d+: executing "[^"]*" at <[^>]*>: error calling ([A-Za-z0-9_]+):`)
)

// tplExecError is a template execution error without the values the template
// functions quote. The original error stays in the chain.
type tplExecError struct {
	msg string
	err error
}

func (e *tplExecError) Error() string { return e.msg }
func (e *tplExecError) Unwrap() error { return e.err }

// execError turns err, the error of Template.Execute for the table template,
// into "template execution failed at line L, column C in function F" with the
// details withheld. Template functions such as sprig's mustToDate and
// mustFromJson return the error of a parser that quotes its input, a field of
// the record, and text/template puts that text into the error it returns. The
// error reaches the DLQ, the API and CLI output, and a record value must not
// be in it (the same rule as the conduit engine's tplerr package).
func execError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	msg := "template execution failed"
	if m := tplPosition.FindStringSubmatch(text); m != nil {
		msg += fmt.Sprintf(" at line %s, column %s", m[1], m[2])
	}
	if m := tplFunction.FindStringSubmatch(text); m != nil {
		msg += " in function " + m[1]
	}
	return &tplExecError{msg: msg + " (details not shown: they can quote record values)", err: err}
}

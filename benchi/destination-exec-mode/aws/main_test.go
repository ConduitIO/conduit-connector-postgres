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

package main

import (
	"strings"
	"testing"
	"text/template"
)

// The rendered user data schedules the hard halt before anything else, fills
// every value, and never carries a credential.
func TestUserData(t *testing.T) {
	var b strings.Builder
	err := template.Must(template.New("ud").Option("missingkey=error").Parse(userDataTmpl)).Execute(&b, map[string]any{
		"MaxMinutes": 150, "Bucket": "conduit-pg367-1-x", "Prefix": "runs/x", "Region": "us-west-1",
		"BaseSHA": strings.Repeat("a", 40), "FixSHA": strings.Repeat("b", 40), "OptoutSHA": strings.Repeat("c", 40),
		"GuardSHA": strings.Repeat("d", 40),
		"GoVersion": "1.26.9", "GoSHA256": strings.Repeat("0", 64), "Rounds": 4, "BenchiVersion": benchiVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	ud := b.String()

	var firstCmd string
	for _, line := range strings.Split(ud, "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			firstCmd = line
			break
		}
	}
	if firstCmd != "shutdown -h +150" {
		t.Fatalf("first command is %q, want the hard halt", firstCmd)
	}
	for _, s := range []string{"<no value>", "AWS_ACCESS_KEY", "AWS_SECRET", "aws_secret", "AWS_SESSION_TOKEN"} {
		if strings.Contains(ud, s) {
			t.Fatalf("user data contains %q", s)
		}
	}
	for _, s := range []string{"BASE_SHA=" + strings.Repeat("a", 40), "OPTOUT_SHA=" + strings.Repeat("c", 40),
		"GUARD_SHA=" + strings.Repeat("d", 40), "shutdown -h now"} {
		if !strings.Contains(ud, s) {
			t.Fatalf("user data lacks %q", s)
		}
	}
}

func TestTagValue(t *testing.T) {
	for v, ok := range map[string]bool{"pg367-20261015": true, "Bad": false, "a b": false, "": false} {
		if tagValueRe.MatchString(v) != ok {
			t.Errorf("%q: got %v, want %v", v, !ok, ok)
		}
	}
}

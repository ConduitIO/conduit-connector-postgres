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

package debezium

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestSettings_DisabledNeedsDefaults(t *testing.T) {
	is := is.New(t)
	is.NoErr(DefaultSettings().Validate())

	s := DefaultSettings()
	s.TopicPrefix = "dbz" // set while the mode is off
	is.Equal(CodeOf(s.Validate()), CodeConfigInvalid)
}

func TestSettings_Enabled(t *testing.T) {
	is := is.New(t)
	ok := testSettings()
	is.NoErr(ok.Validate())

	for name, mutate := range map[string]func(*Settings){
		"no topic prefix":       func(s *Settings) { s.TopicPrefix = "" },
		"bad topic prefix":      func(s *Settings) { s.TopicPrefix = "a b" },
		"empty delimiter":       func(s *Settings) { s.TopicDelimiter = "" },
		"unknown compat":        func(s *Settings) { s.Compat = "3.0" },
		"format version 2":      func(s *Settings) { s.FormatVersion = 2 },
		"decimal string":        func(s *Settings) { s.DecimalHandlingMode = "string" },
		"time connect":          func(s *Settings) { s.TimePrecisionMode = "connect" },
		"binary base64":         func(s *Settings) { s.BinaryHandlingMode = "base64" },
		"interval string":       func(s *Settings) { s.IntervalHandlingMode = "string" },
		"hstore map":            func(s *Settings) { s.HstoreHandlingMode = "map" },
		"unknown datatypes":     func(s *Settings) { s.IncludeUnknownDatatypes = true },
		"column include list":   func(s *Settings) { s.ColumnIncludeList = "public.t.a" },
		"message key columns":   func(s *Settings) { s.MessageKeyColumns = "public.t:a" },
		"truncates not skipped": func(s *Settings) { s.SkippedOperations = "none" },
		"creates skipped":       func(s *Settings) { s.SkippedOperations = "c,t" },
		"empty placeholder":     func(s *Settings) { s.UnavailableValuePlaceholder = "" },
		"negative money digits": func(s *Settings) { s.MoneyFractionDigits = -1 },
		"drift dlq":             func(s *Settings) { s.SchemaDrift = "dlq" },
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			s := testSettings()
			mutate(&s)
			err := s.Validate()
			is.Equal(CodeOf(err), CodeConfigInvalid)
			is.True(strings.HasPrefix(err.Error(), "debezium.config_invalid: "))
		})
	}
}

// Decision 18: a standalone build refuses the mode, because int64 and bytes
// do not survive the plugin boundary's protobuf Struct.
func TestSettings_StandaloneRefused(t *testing.T) {
	is := is.New(t)
	t.Cleanup(func() { standalone.Store(false) })
	MarkStandalone()
	is.True(IsStandalone())
	err := testSettings().Validate()
	is.Equal(CodeOf(err), CodeConfigInvalid)
	is.True(strings.Contains(err.Error(), "builtin:postgres"))
	is.NoErr(DefaultSettings().Validate()) // the default mode still works standalone
}

// Decision 18 without the entry point's cooperation: every standalone plugin
// Conduit launches gets the go-plugin handshake variable.
func TestSettings_StandaloneByPluginCookie(t *testing.T) {
	is := is.New(t)
	is.True(!IsStandalone())
	t.Setenv(pluginCookieEnv, "x")
	is.True(IsStandalone())
	is.Equal(CodeOf(testSettings().Validate()), CodeConfigInvalid)
	t.Setenv(pluginCookieEnv, "")
	is.True(!IsStandalone()) // an empty value is not a handshake
	is.NoErr(testSettings().Validate())
}

func TestSettings_SourceVersion(t *testing.T) {
	is := is.New(t)
	s := testSettings()
	is.Equal(s.SourceVersion(), "3.7.0.Final")
	s.Compat = Compat26
	is.Equal(s.SourceVersion(), "2.7.4.Final")
	s.Version = "2.6.2.Final"
	is.Equal(s.SourceVersion(), "2.6.2.Final")
}

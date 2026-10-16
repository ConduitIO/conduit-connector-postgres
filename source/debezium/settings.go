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
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
)

// Compat values (design: Debezium versions targeted). This version supports
// the two boundaries the goldens and the encoder cover.
const (
	Compat26 = "2.6"
	Compat35 = "3.5"
)

// defaultSourceVersion is the source.version written for a compat level
// when debezium.version is not set (design D3: the configured emulated
// version). They are the versions the goldens were captured from.
var defaultSourceVersion = map[string]string{
	Compat26: "2.7.4.Final",
	Compat35: "3.7.0.Final",
}

// Settings are the source's debezium.* settings that this version
// implements. The source config parses them; Validate checks them.
type Settings struct {
	Enabled                     bool
	Compat                      string
	Version                     string
	FormatVersion               int
	TopicPrefix                 string
	TopicDelimiter              string
	ProvideTransactionMetadata  bool
	SkippedOperations           string
	UnavailableValuePlaceholder string
	MoneyFractionDigits         int
	DecimalHandlingMode         string
	TimePrecisionMode           string
	BinaryHandlingMode          string
	IntervalHandlingMode        string
	HstoreHandlingMode          string
	IncludeUnknownDatatypes     bool
	SchemaNameAdjustmentMode    string
	FieldNameAdjustmentMode     string
	MessageKeyColumns           string
	ColumnIncludeList           string
	ColumnExcludeList           string
	MessagePrefixIncludeList    string
	MessagePrefixExcludeList    string
	SchemaDrift                 string
}

// standalone is set by MarkStandalone in a standalone build of the
// connector (cmd/connector). Decision 18: the mode requires the built-in
// connector, because the plugin boundary's protobuf Struct turns int64
// into float64 and bytes into base64.
var standalone atomic.Bool

// MarkStandalone records that this process is a standalone connector
// plugin. cmd/connector calls it before serving; a built-in connector never
// does.
func MarkStandalone() { standalone.Store(true) }

// pluginCookieEnv is the go-plugin handshake variable Conduit sets for every
// standalone plugin process it launches
// (conduit-connector-protocol/pconnector.HandshakeConfig). A built-in
// connector runs in the Conduit process, which never has it.
const pluginCookieEnv = "CONDUIT_PLUGIN_MAGIC_COOKIE"

// IsStandalone reports whether this process is a standalone plugin: either
// MarkStandalone was called (cmd/connector), or the plugin handshake
// variable is set. The second covers standalone builds that do not go
// through cmd/connector, so the decision-18 guard does not depend on an
// entry point remembering to opt in. Only the variable's presence is read.
func IsStandalone() bool {
	if standalone.Load() {
		return true
	}
	v, ok := os.LookupEnv(pluginCookieEnv)
	return ok && v != ""
}

// The handling modes this version supports (Debezium's defaults).
const (
	modePrecise  = "precise"
	modeAdaptive = "adaptive"
	modeBytes    = "bytes"
	modeNumeric  = "numeric"
	modeJSON     = "json"
	modeNone     = "none"
	driftEvolve  = "evolve"
)

var topicPrefixRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// Validate checks the settings. It returns nil when the mode is disabled and
// every setting is at its default.
//
//nolint:gocyclo // one check per setting
func (s Settings) Validate() error {
	var problems []string
	if !s.Enabled {
		if !s.isDefault() {
			return configErr("debezium.* settings are set but debezium.enabled is false",
				"Set debezium.enabled: true, or remove the debezium.* settings.")
		}
		return nil
	}
	if IsStandalone() {
		return configErr("debezium.enabled is true in a standalone build of the connector; the Debezium mode requires "+
			"the built-in connector (builtin:postgres), because records crossing the plugin boundary lose int64 "+
			"precision and bytes (decision 18)",
			"Use the built-in Postgres connector (plugin: builtin:postgres).")
	}
	if s.TopicPrefix == "" {
		problems = append(problems, "debezium.topic.prefix is required")
	} else if !topicPrefixRE.MatchString(s.TopicPrefix) {
		problems = append(problems, fmt.Sprintf("debezium.topic.prefix %q may contain only letters, digits, '.', '_' and '-'", s.TopicPrefix))
	}
	if s.TopicDelimiter == "" {
		problems = append(problems, "debezium.topic.delimiter must not be empty")
	}
	if s.Compat != Compat26 && s.Compat != Compat35 {
		problems = append(problems, fmt.Sprintf("debezium.compat %q is not supported (supported: %s, %s)", s.Compat, Compat26, Compat35))
	}
	if s.FormatVersion != 1 {
		problems = append(problems, fmt.Sprintf("debezium.format.version %d is not supported (supported: 1)", s.FormatVersion))
	}
	for _, m := range []struct{ name, got, want string }{
		{"debezium.decimal.handling.mode", s.DecimalHandlingMode, modePrecise},
		{"debezium.time.precision.mode", s.TimePrecisionMode, modeAdaptive},
		{"debezium.binary.handling.mode", s.BinaryHandlingMode, modeBytes},
		{"debezium.interval.handling.mode", s.IntervalHandlingMode, modeNumeric},
		{"debezium.hstore.handling.mode", s.HstoreHandlingMode, modeJSON},
		{"debezium.schema.name.adjustment.mode", s.SchemaNameAdjustmentMode, modeNone},
		{"debezium.field.name.adjustment.mode", s.FieldNameAdjustmentMode, modeNone},
	} {
		if m.got != m.want {
			problems = append(problems, fmt.Sprintf("%s %q is not supported in this version (supported: %s)", m.name, m.got, m.want))
		}
	}
	for _, l := range []struct{ name, val string }{
		{"debezium.message.key.columns", s.MessageKeyColumns},
		{"debezium.column.include.list", s.ColumnIncludeList},
		{"debezium.column.exclude.list", s.ColumnExcludeList},
		{"debezium.message.prefix.include.list", s.MessagePrefixIncludeList},
		{"debezium.message.prefix.exclude.list", s.MessagePrefixExcludeList},
	} {
		if l.val != "" {
			problems = append(problems, l.name+" is not supported in this version")
		}
	}
	if s.IncludeUnknownDatatypes {
		problems = append(problems, "debezium.include.unknown.datatypes=true is not supported in this version")
	}
	if s.MoneyFractionDigits < 0 || s.MoneyFractionDigits > 100 {
		problems = append(problems, "debezium.money.fraction.digits must be between 0 and 100")
	}
	if s.UnavailableValuePlaceholder == "" {
		problems = append(problems, "debezium.unavailable.value.placeholder must not be empty")
	}
	ops := map[string]bool{}
	for _, o := range strings.Split(s.SkippedOperations, ",") {
		if o = strings.TrimSpace(o); o != "" {
			ops[o] = true
		}
	}
	// Truncate events and skipping c/u/d are not implemented in this
	// version: truncates must stay skipped (Debezium's default), and no
	// other operation may be.
	if len(ops) != 1 || !ops["t"] {
		problems = append(problems, fmt.Sprintf("debezium.skipped.operations %q is not supported in this version (supported: t)", s.SkippedOperations))
	}
	// halt is refused until debezium.encode passes drift markers through: a
	// marker carries no debezium.* metadata, so the encoder fails it and the
	// approving restart can never get past it. evolve still halts on a
	// narrowing change, with the same limitation, documented.
	if s.SchemaDrift != driftEvolve {
		problems = append(problems, fmt.Sprintf("debezium.schema.drift %q is not supported in this version (supported: evolve; halt arrives once the debezium.encode processor passes drift markers through)", s.SchemaDrift))
	}
	if len(problems) > 0 {
		return configErr(strings.Join(problems, "; "), "Change the listed settings; the Debezium mode documentation lists what this version supports.")
	}
	return nil
}

// SourceVersion is the source.version value: debezium.version, or the
// default for the compat level.
func (s Settings) SourceVersion() string {
	if s.Version != "" {
		return s.Version
	}
	return defaultSourceVersion[s.Compat]
}

func (s Settings) isDefault() bool {
	d := DefaultSettings()
	return s == d
}

// DefaultSettings are the defaults the connector specification declares.
func DefaultSettings() Settings {
	return Settings{
		Compat:                      Compat35,
		FormatVersion:               1,
		TopicDelimiter:              ".",
		SkippedOperations:           "t",
		UnavailableValuePlaceholder: "__debezium_unavailable_value",
		MoneyFractionDigits:         2,
		DecimalHandlingMode:         modePrecise,
		TimePrecisionMode:           modeAdaptive,
		BinaryHandlingMode:          modeBytes,
		IntervalHandlingMode:        modeNumeric,
		HstoreHandlingMode:          modeJSON,
		SchemaNameAdjustmentMode:    modeNone,
		FieldNameAdjustmentMode:     modeNone,
		SchemaDrift:                 driftEvolve,
	}
}

func configErr(msg, fix string) error {
	return &Error{Code: CodeConfigInvalid, Message: msg, Fix: fix}
}

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
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
)

// FormatV1 is the only setting format this build reads. A setting with new
// semantics must use a new format string, so an older build refuses it
// instead of ignoring what it does not understand.
const FormatV1 = "debezium-postgres/v1"

// Debezium's decoding plugins (`plugin.name`), S4.
const (
	PluginPgoutput    = "pgoutput"
	PluginDecoderbufs = "decoderbufs"
)

// Setting is the value of `logrepl.import.debeziumOffset` (D2, S3, S4, S13).
// `conduit migrate kafka-connect` builds it from the Kafka Connect config,
// the committed offset and the cutover it ran; people do not normally write
// it by hand.
type Setting struct {
	// Format must be FormatV1.
	Format string `json:"format"`
	// Connector is the Kafka Connect connector name the offset came from.
	// Informational.
	Connector string `json:"connector"`
	// Partition is the offset's Kafka Connect source partition, for example
	// {"server": "dbserver1"}. Informational.
	Partition json.RawMessage `json:"partition,omitempty"`
	// Database is Debezium's `database.dbname`. At Open it must equal
	// current_database() (S3).
	Database string `json:"database"`
	// SystemIdentifier is the server's system identifier from
	// IDENTIFY_SYSTEM, as a decimal string.
	SystemIdentifier string `json:"systemIdentifier"`
	// Plugin is Debezium's `plugin.name`; the CLI writes decoderbufs when the
	// config leaves it unset. Only pgoutput and decoderbufs are importable
	// (S4).
	Plugin string `json:"plugin"`
	// DebeziumVersion is the Debezium Postgres connector version that wrote
	// the offset, as Kafka Connect reports it (`GET /connector-plugins`, for
	// example "3.2.4.Final"). Offsets of the lines in VerifiedDebeziumLines
	// were captured and checked against Postgres; other 1.x to 3.x lines are
	// accepted with a warning (Import.Warnings), anything else is refused.
	DebeziumVersion string `json:"debeziumVersion"`
	// DebeziumSlotName names Debezium's replication slot, which the connector
	// checks at Open (S13).
	DebeziumSlotName string `json:"debeziumSlotName"`
	// PublicationName is Debezium's publication. The connector does not read
	// it; it is kept for the dry run and for the CLI's lossy-config checks
	// (S2). Conduit's own publication is logrepl.publicationName.
	PublicationName string `json:"publicationName"`
	// Tables is Debezium's capture list, with its include/exclude regexes
	// resolved against the catalog, as schema-qualified names.
	Tables []string `json:"tables"`
	// Identity is the S13 proof the CLI observed during the cutover.
	Identity Identity `json:"identity"`
	// Offset is Debezium's offset map, unchanged.
	Offset json.RawMessage `json:"offset"`
}

// Identity is the S13 proof that the offset belongs to this server.
type Identity struct {
	// ConduitSlotConsistentPoint is C: the consistent point of the slot the
	// CLI created for Conduit, in Postgres's "X/Y" notation.
	ConduitSlotConsistentPoint string `json:"conduitSlotConsistentPoint"`
	// DebeziumSlotFlushAtWait is the confirmed_flush_lsn the CLI saw on
	// Debezium's slot on this server once it had reached C. It is at least C.
	DebeziumSlotFlushAtWait string `json:"debeziumSlotFlushAtWait"`
	// ConduitPublication is the identity of Conduit's publication when the
	// CLI created Conduit's slot (PublicationIdentity). pgoutput reads
	// publication membership through the historic catalog, so a table added
	// to the publication after the slot's start silently loses its earlier
	// WAL (B3). The connector refuses to import if the identity changed.
	ConduitPublication string `json:"conduitPublication"`
}

// Import is a parsed and validated setting.
type Import struct {
	Setting Setting
	// Offset is the parsed Setting.Offset.
	Offset Offset
	// ConsistentPoint is the parsed Identity.ConduitSlotConsistentPoint (C).
	ConsistentPoint pglogrepl.LSN
	// DebeziumFlushAtWait is the parsed Identity.DebeziumSlotFlushAtWait.
	DebeziumFlushAtWait pglogrepl.LSN
	// Plan is the resume point computed from Offset.
	Plan Plan
	// Warnings are conditions that do not block the import but that the
	// operator should see; the connector logs them at Open and the CLI
	// prints them.
	Warnings []string
}

// VerifiedDebeziumLines are the Debezium major.minor lines whose offsets are
// in testdata (captured from a running connector and checked against
// Postgres).
var VerifiedDebeziumLines = []string{"2.7", "3.2"}

// ParseSetting parses and validates the value of
// `logrepl.import.debeziumOffset`, then computes the resume point. It is the
// one entry point the connector (at configuration and at Open) and the CLI
// (before it writes a pipeline) share.
//
// The checks are offline and run in the S5 order: the setting's shape and
// the offset parse (CodeOffsetInvalid), the offset's snapshot state and
// lsn_commit (see ParseOffset), then the decoding plugin
// (CodePluginUnsupported). Every check that needs Postgres runs later, at
// Open.
//
// Unknown fields are refused: Format is versioned, and a field this build
// does not understand could carry a rule it would silently skip.
func ParseSetting(raw string) (Import, error) {
	// encoding/json keeps the last of duplicate keys and Decode ignores a
	// trailing '}' or ']' left in the stream; a setting with either cannot
	// be interpreted, so it is refused before decoding.
	if err := checkStrictJSON([]byte(raw)); err != nil {
		return Import{}, newError(CodeOffsetInvalid, SettingPath, fixReexport,
			"the setting is not valid JSON for format %q: %v", FormatV1, err)
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var s Setting
	if err := dec.Decode(&s); err != nil {
		return Import{}, newError(CodeOffsetInvalid, SettingPath, fixReexport,
			"the setting is not valid JSON for format %q: %v", FormatV1, err)
	}
	if dec.More() {
		return Import{}, newError(CodeOffsetInvalid, SettingPath, fixReexport,
			"unexpected data after the setting's JSON object")
	}

	if s.Format != FormatV1 {
		return Import{}, newError(CodeOffsetInvalid, SettingPath+".format", fixReexport,
			"format is %q; this connector build reads only %q", s.Format, FormatV1)
	}
	if err := checkRequiredFields(s); err != nil {
		return Import{}, err
	}

	c, err := parseIdentityLSN(s.Identity.ConduitSlotConsistentPoint, "conduitSlotConsistentPoint")
	if err != nil {
		return Import{}, err
	}
	fw, err := parseIdentityLSN(s.Identity.DebeziumSlotFlushAtWait, "debeziumSlotFlushAtWait")
	if err != nil {
		return Import{}, err
	}
	if fw < c {
		// The CLI records Debezium's slot only once it has reached C (S13).
		return Import{}, newError(CodeOffsetInvalid, SettingPath+".identity", fixReexport,
			"identity.debeziumSlotFlushAtWait %s is below identity.conduitSlotConsistentPoint %s", fw, c)
	}

	if strings.TrimSpace(s.Identity.ConduitPublication) == "" {
		return Import{}, newError(CodeOffsetInvalid, SettingPath+".identity.conduitPublication", fixReexport,
			"identity.conduitPublication is required")
	}
	warning, err := checkDebeziumVersion(s.DebeziumVersion)
	if err != nil {
		return Import{}, err
	}
	if len(bytes.TrimSpace(s.Offset)) == 0 {
		return Import{}, newError(CodeOffsetInvalid, SettingPath+".offset", fixReexport, "offset is required")
	}
	o, err := parseOffset(s.Offset, SettingPath+".offset")
	if err != nil {
		return Import{}, err
	}

	// S4: the partial rule assumes each change carries its own LSN.
	// wal2json stamps one LSN for the whole transaction.
	switch s.Plugin {
	case PluginPgoutput, PluginDecoderbufs:
	default:
		return Import{}, newError(CodePluginUnsupported, SettingPath+".plugin", fixPluginUnsupported,
			"Debezium's decoding plugin %q cannot be imported; only %q and %q can", s.Plugin, PluginPgoutput, PluginDecoderbufs)
	}

	// D3: the fresh slot decodes in full every transaction committing at or
	// after C, and Conduit must deliver everything committing at or after
	// Lc. So Lc >= C is required; with Lc < C the transactions committing in
	// [Lc, C) are in neither Debezium's output nor the slot. The slot check
	// at Open (F <= Lc) catches this too; refusing here says so before any
	// connection is made.
	if o.LSNCommit < c {
		return Import{}, newError(CodeSlotAhead, SettingPath+".offset.lsn_commit", fixSlotAheadOffline,
			"Debezium's lsn_commit %s is below the consistent point %s of Conduit's slot, so changes "+
				"committed in between are covered by neither", o.LSNCommit, c)
	}

	imp := Import{
		Setting:             s,
		Offset:              o,
		ConsistentPoint:     c,
		DebeziumFlushAtWait: fw,
		Plan:                NewPlan(o),
	}
	if warning != "" {
		imp.Warnings = append(imp.Warnings, warning)
	}
	return imp, nil
}

var debeziumVersionRE = regexp.MustCompile(`^([0-9]+)\.([0-9]+)\.([0-9]+)(\..+)?$`)

// checkDebeziumVersion returns a warning for a Debezium line whose offsets
// were not captured, and refuses versions this build knows nothing about.
func checkDebeziumVersion(v string) (string, error) {
	path := SettingPath + ".debeziumVersion"
	m := debeziumVersionRE.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return "", newError(CodeOffsetInvalid, path, fixReexport,
			"debeziumVersion %q is not a Debezium version such as \"3.2.4.Final\"", v)
	}
	major, _ := strconv.Atoi(m[1])
	if major < 1 || major > 3 {
		return "", newError(CodeOffsetUnsupported, path,
			"This connector build reads offsets of Debezium 1.x to 3.x. Upgrade the connector to a build that lists your Debezium version, or re-snapshot.",
			"Debezium %s is not a version this connector build knows the offset format of", v)
	}
	line := m[1] + "." + m[2]
	for _, l := range VerifiedDebeziumLines {
		if l == line {
			return "", nil
		}
	}
	return fmt.Sprintf("Debezium %s: offsets of the %s line were not captured and verified for this connector "+
		"(verified: %s); the offset passed every check, but review the dry run's resume point before cutting over",
		v, line, strings.Join(VerifiedDebeziumLines, ", ")), nil
}

// Encode returns the setting as the JSON string the connector reads. The CLI
// calls ParseSetting on the result before it writes a pipeline, so it never
// writes a setting the connector would refuse at configuration.
func (s Setting) Encode() (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseIdentityLSN(v, name string) (pglogrepl.LSN, error) {
	path := SettingPath + ".identity." + name
	if v == "" {
		return 0, newError(CodeOffsetInvalid, path, fixReexport, "identity.%s is required", name)
	}
	lsn, err := pglogrepl.ParseLSN(v)
	if err != nil || lsn == 0 {
		return 0, newError(CodeOffsetInvalid, path, fixReexport,
			"identity.%s %q is not an LSN in X/Y notation", name, v)
	}
	return lsn, nil
}

// checkRequiredFields checks the presence and shape of the setting's plain
// fields.
func checkRequiredFields(s Setting) error {
	for _, f := range []struct{ name, value string }{
		{"database", s.Database},
		{"systemIdentifier", s.SystemIdentifier},
		{"debeziumSlotName", s.DebeziumSlotName},
		{"publicationName", s.PublicationName},
	} {
		if strings.TrimSpace(f.value) == "" {
			return newError(CodeOffsetInvalid, SettingPath+"."+f.name, fixReexport,
				"%q is required", f.name)
		}
	}
	if _, err := strconv.ParseUint(s.SystemIdentifier, 10, 64); err != nil {
		return newError(CodeOffsetInvalid, SettingPath+".systemIdentifier", fixReexport,
			"systemIdentifier %q is not a decimal system identifier", s.SystemIdentifier)
	}
	if len(s.Tables) == 0 {
		return newError(CodeOffsetInvalid, SettingPath+".tables", fixReexport,
			"tables is empty; it must list the tables Debezium captured")
	}
	for i, t := range s.Tables {
		if strings.TrimSpace(t) == "" {
			return newError(CodeOffsetInvalid, SettingPath+".tables", fixReexport,
				"tables[%d] is empty", i)
		}
	}

	return nil
}

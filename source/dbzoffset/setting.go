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
	// DebeziumSlotName and PublicationName name Debezium's replication slot
	// and publication.
	DebeziumSlotName string `json:"debeziumSlotName"`
	PublicationName  string `json:"publicationName"`
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
}

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
	for _, f := range []struct{ name, value string }{
		{"database", s.Database},
		{"systemIdentifier", s.SystemIdentifier},
		{"debeziumSlotName", s.DebeziumSlotName},
		{"publicationName", s.PublicationName},
	} {
		if strings.TrimSpace(f.value) == "" {
			return Import{}, newError(CodeOffsetInvalid, SettingPath+"."+f.name, fixReexport,
				"%q is required", f.name)
		}
	}
	if _, err := strconv.ParseUint(s.SystemIdentifier, 10, 64); err != nil {
		return Import{}, newError(CodeOffsetInvalid, SettingPath+".systemIdentifier", fixReexport,
			"systemIdentifier %q is not a decimal system identifier", s.SystemIdentifier)
	}
	if len(s.Tables) == 0 {
		return Import{}, newError(CodeOffsetInvalid, SettingPath+".tables", fixReexport,
			"tables is empty; it must list the tables Debezium captured")
	}
	for i, t := range s.Tables {
		if strings.TrimSpace(t) == "" {
			return Import{}, newError(CodeOffsetInvalid, SettingPath+".tables", fixReexport,
				"tables[%d] is empty", i)
		}
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

	return Import{
		Setting:             s,
		Offset:              o,
		ConsistentPoint:     c,
		DebeziumFlushAtWait: fw,
		Plan:                NewPlan(o),
	}, nil
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

// Copyright © 2022 Meroxa, Inc.
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

package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/conduitio/conduit-connector-postgres/source/debezium"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

type SnapshotMode string

const (
	// SnapshotModeInitial creates a snapshot in the first run of the pipeline.
	SnapshotModeInitial SnapshotMode = "initial"
	// SnapshotModeNever skips snapshot creation altogether.
	SnapshotModeNever SnapshotMode = "never"
)

type CDCMode string

const (
	// CDCModeAuto tries to set up logical replication and falls back to long
	// polling if that is impossible.
	CDCModeAuto CDCMode = "auto"
	// CDCModeLogrepl uses logical replication to listen to changes.
	CDCModeLogrepl CDCMode = "logrepl"

	// AllTablesWildcard can be used if you'd like to listen to all tables.
	AllTablesWildcard = "*"
)

type Config struct {
	sdk.DefaultSourceMiddleware

	// URL is the connection string for the Postgres database.
	// With TLS, SCRAM authentication does not use channel binding by default
	// (`channel_binding=disable`); add `channel_binding=require` to the URL to
	// require SCRAM-SHA-256-PLUS.
	URL string `json:"url" validate:"required"`

	// Tables is a List of table names to read from, separated by a comma, e.g.:"table1,table2".
	// Use "*" if you'd like to listen to all tables.
	Tables []string `json:"tables" validate:"required"`

	// SnapshotMode is whether the plugin will take a snapshot of the entire table before starting cdc mode.
	SnapshotMode SnapshotMode `json:"snapshotMode" validate:"inclusion=initial|never" default:"initial"`

	// Snapshot fetcher size determines the number of rows to retrieve at a time.
	SnapshotFetchSize int `json:"snapshot.fetchSize" default:"50000"`

	// CDCMode determines how the connector should listen to changes.
	CDCMode CDCMode `json:"cdcMode" validate:"inclusion=auto|logrepl" default:"auto"`

	// LogreplPublicationName determines the publication name in case the
	// connector uses logical replication to listen to changes (see CDCMode).
	LogreplPublicationName string `json:"logrepl.publicationName" default:"conduitpub"`
	// LogreplSlotName determines the replication slot name in case the
	// connector uses logical replication to listen to changes (see CDCMode).
	// Can only contain lower-case letters, numbers, and the underscore character.
	LogreplSlotName string `json:"logrepl.slotName" validate:"regex=^[a-z0-9_]+$" default:"conduitslot"`

	// LogreplAutoCleanup determines if the replication slot and publication should be
	// removed when the connector is deleted.
	LogreplAutoCleanup bool `json:"logrepl.autoCleanup" default:"true"`

	// WithAvroSchema determines whether the connector should attach an avro schema on each
	// record.
	WithAvroSchema bool `json:"logrepl.withAvroSchema" default:"true"`

	// LogreplSchemaDriftPolicy determines how the connector handles schema
	// drift (a DDL that changes a table's shape during CDC; DBZ-3 B1):
	//   - "halt" (default): emit a drift marker record, checkpoint before it,
	//     and stop with a terminal error until the pipeline is restarted to
	//     approve the change.
	//   - "evolve": accept additive changes silently; incompatible (narrowing)
	//     changes still halt.
	// "dlq" is reserved for a future version and rejected at validation.
	LogreplSchemaDriftPolicy logrepl.SchemaDriftPolicy `json:"logrepl.schemaDrift.policy" default:"halt"`

	// LogreplImportDebeziumOffset starts the pipeline from a Debezium Postgres
	// connector's committed offset instead of a snapshot. It is written by
	// `conduit migrate kafka-connect --cutover`, not by hand. It applies only
	// while the pipeline has no position: the first acknowledged record writes
	// an ordinary position and the setting is ignored from then on (remove it
	// once the import is complete). Requires snapshotMode "never" and cdcMode
	// "logrepl", and an existing slot and publication, which import mode
	// never creates. Requires Conduit v0.19.0 or later, which acks a record
	// only after its position is persisted.
	LogreplImportDebeziumOffset string `json:"logrepl.import.debeziumOffset"`

	// Debezium switches the source to Debezium-compatible output: records
	// carry Debezium-typed values and debezium.* metadata for the
	// debezium.encode processor, which writes the bytes a Debezium Postgres
	// connector would. Requires the built-in connector (builtin:postgres).
	Debezium DebeziumConfig `json:"debezium"`
}

// DebeziumConfig holds the debezium.* settings. Every setting other than
// enabled must keep its default while enabled is false, and this version
// accepts only the default handling modes; see debezium.Settings.Validate.
type DebeziumConfig struct {
	// Enabled switches the output mode. Off by default; with it off the
	// connector's output is unchanged.
	Enabled bool `json:"enabled" default:"false"`
	// Compat is the Debezium version boundary the output emulates: "2.6"
	// (Debezium 2.6 to 2.7) or "3.5" (3.5 and later).
	Compat string `json:"compat" default:"3.5"`
	// Version is the source.version value. Empty means the release the
	// output was verified against for the compat level (2.7.4.Final for
	// 2.6, 3.7.0.Final for 3.5). `conduit migrate kafka-connect` sets the
	// version of the connector it replaces.
	Version string `json:"version"`
	// FormatVersion is the revision of Conduit's emulation; only 1 exists.
	FormatVersion int `json:"format.version" default:"1"`
	// TopicPrefix is Debezium's topic.prefix: the first part of every topic
	// name, source.name, and the schema names. Required when enabled.
	TopicPrefix string `json:"topic.prefix"`
	// TopicDelimiter is Debezium's topic.delimiter.
	TopicDelimiter string `json:"topic.delimiter" default:"."`
	// ProvideTransactionMetadata is Debezium's provide.transaction.metadata:
	// fill each event's transaction block. The transaction topic is not
	// produced.
	ProvideTransactionMetadata bool `json:"provide.transaction.metadata" default:"false"`
	// SkippedOperations is Debezium's skipped.operations. Only "t" (skip
	// truncates, Debezium's default) is supported in this version.
	SkippedOperations string `json:"skipped.operations" default:"t"`
	// UnavailableValuePlaceholder is Debezium's
	// unavailable.value.placeholder, written for unchanged TOAST values.
	UnavailableValuePlaceholder string `json:"unavailable.value.placeholder" default:"__debezium_unavailable_value"`
	// MoneyFractionDigits is Debezium's money.fraction.digits, the scale of
	// money columns.
	MoneyFractionDigits int `json:"money.fraction.digits" default:"2"`
	// DecimalHandlingMode is Debezium's decimal.handling.mode. Only
	// "precise" is supported in this version.
	DecimalHandlingMode string `json:"decimal.handling.mode" default:"precise"`
	// TimePrecisionMode is Debezium's time.precision.mode. Only "adaptive"
	// is supported in this version.
	TimePrecisionMode string `json:"time.precision.mode" default:"adaptive"`
	// BinaryHandlingMode is Debezium's binary.handling.mode. Only "bytes" is
	// supported in this version.
	BinaryHandlingMode string `json:"binary.handling.mode" default:"bytes"`
	// IntervalHandlingMode is Debezium's interval.handling.mode. Only
	// "numeric" is supported in this version.
	IntervalHandlingMode string `json:"interval.handling.mode" default:"numeric"`
	// HstoreHandlingMode is Debezium's hstore.handling.mode. Only "json" is
	// supported in this version.
	HstoreHandlingMode string `json:"hstore.handling.mode" default:"json"`
	// SchemaDrift is the schema drift policy in this mode, used instead of
	// logrepl.schemaDrift.policy: "evolve" (default; additive changes pass,
	// narrowing ones halt) or "halt".
	SchemaDrift string `json:"schema.drift" default:"evolve"`
}

// Settings returns the settings in the form the debezium package uses. The
// settings this version does not expose keep their defaults. A zero
// DebeziumConfig (a Config built in code, without the specification's
// defaults) is the default, disabled mode.
func (d DebeziumConfig) Settings() debezium.Settings {
	s := debezium.DefaultSettings()
	if d == (DebeziumConfig{}) {
		return s
	}
	s.Enabled = d.Enabled
	s.Compat = d.Compat
	s.Version = d.Version
	s.FormatVersion = d.FormatVersion
	s.TopicPrefix = d.TopicPrefix
	s.TopicDelimiter = d.TopicDelimiter
	s.ProvideTransactionMetadata = d.ProvideTransactionMetadata
	s.SkippedOperations = d.SkippedOperations
	s.UnavailableValuePlaceholder = d.UnavailableValuePlaceholder
	s.MoneyFractionDigits = d.MoneyFractionDigits
	s.DecimalHandlingMode = d.DecimalHandlingMode
	s.TimePrecisionMode = d.TimePrecisionMode
	s.BinaryHandlingMode = d.BinaryHandlingMode
	s.IntervalHandlingMode = d.IntervalHandlingMode
	s.HstoreHandlingMode = d.HstoreHandlingMode
	s.SchemaDrift = d.SchemaDrift
	return s
}

// ValidateDebezium checks the debezium.* settings and the settings the mode
// constrains. Every failure is coded debezium.config_invalid. Validate
// calls it; Open calls it again.
func (c *Config) ValidateDebezium() error {
	s := c.Debezium.Settings()
	if err := s.Validate(); err != nil {
		return err
	}
	if !s.Enabled {
		return nil
	}
	if c.SnapshotMode != SnapshotModeNever {
		return &debezium.Error{Code: debezium.CodeConfigInvalid,
			Message: fmt.Sprintf("snapshotMode is %q; the Debezium mode does not snapshot in this version", c.SnapshotMode),
			Fix:     "Set `snapshotMode: never` (the snapshot arrives in a later version), or resume from a Debezium offset with logrepl.import.debeziumOffset."}
	}
	if extract := c.SourceWithSchemaExtraction; (extract.PayloadEnabled != nil && *extract.PayloadEnabled) ||
		(extract.KeyEnabled != nil && *extract.KeyEnabled) {
		return &debezium.Error{Code: debezium.CodeConfigInvalid,
			Message: "sdk.schema.extract is on; in the Debezium mode the row schema travels in debezium.descriptor, and extracted schemas would add registry subjects Debezium never creates",
			Fix:     "Set `sdk.schema.extract.payload.enabled: false` and `sdk.schema.extract.key.enabled: false`."}
	}
	return nil
}

// DebeziumImport parses LogreplImportDebeziumOffset. It returns nil and no
// error when the setting is empty.
func (c *Config) DebeziumImport() (*dbzoffset.Import, error) {
	if c.LogreplImportDebeziumOffset == "" {
		return nil, nil
	}
	imp, err := dbzoffset.ParseSetting(c.LogreplImportDebeziumOffset)
	if err != nil {
		return nil, err
	}
	return &imp, nil
}

// ValidateImport runs the offline import checks (D2, S10, Q4). Validate
// calls it; Open calls it again, so an import never runs on a config that
// skipped validation.
func (c *Config) ValidateImport() error {
	imp, err := c.DebeziumImport()
	if err != nil || imp == nil {
		return err
	}
	if c.SnapshotMode != SnapshotModeNever {
		return &dbzoffset.Error{Code: dbzoffset.CodeSnapshotModeConflict, Path: "snapshotMode",
			Message: fmt.Sprintf("snapshotMode is %q, but an import resumes from Debezium's offset with no snapshot", c.SnapshotMode),
			Fix:     "Set `snapshotMode: never`."}
	}
	if c.CDCMode != CDCModeLogrepl {
		return &dbzoffset.Error{Code: dbzoffset.CodeCDCModeConflict, Path: "cdcMode",
			Message: fmt.Sprintf("cdcMode is %q; an import needs logical replication stated explicitly", c.CDCMode),
			Fix:     "Set `cdcMode: logrepl`."}
	}
	if c.LogreplSlotName == imp.Setting.DebeziumSlotName {
		return &dbzoffset.Error{Code: dbzoffset.CodeSlotReuseUnsupported, Path: "logrepl.slotName",
			Message: fmt.Sprintf("logrepl.slotName %q is Debezium's slot; reusing it is not supported in this version", c.LogreplSlotName),
			Fix:     "Use the fresh slot `conduit migrate kafka-connect --cutover` created (`--slot-strategy fresh`). Reusing Debezium's slot arrives in a later release."}
	}
	return nil
}

// Validate validates the provided config values.
func (c *Config) Validate(ctx context.Context) error {
	var errs []error
	if _, err := pgx.ParseConfig(c.URL); err != nil {
		errs = append(errs, fmt.Errorf("invalid url: %w", err))
	}

	err := c.DefaultSourceMiddleware.Validate(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	// "dlq" and unknown values are rejected with a stable coded error
	// (postgres.schema_drift.policy.unsupported); the SDK default fills in
	// "halt" when the param is absent.
	if _, err := logrepl.ParseSchemaDriftPolicy(string(c.LogreplSchemaDriftPolicy)); err != nil {
		errs = append(errs, err)
	}

	if err := c.ValidateImport(); err != nil {
		errs = append(errs, err)
	}

	if err := c.ValidateDebezium(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

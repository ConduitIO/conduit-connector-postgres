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

	// LogreplRequireExistingSlot decides whether Open may create the
	// replication slot and the publication. Unset (the default): refuse when
	// the pipeline already has a position, because a new slot starts at the
	// current WAL and silently skips every change since the old one; create
	// them as before for a new pipeline. "true": always refuse (what
	// `conduit migrate kafka-connect` writes). "false": always allow, and log
	// postgres.logrepl.slot_recreated when a slot is created for a pipeline
	// with a position.
	LogreplRequireExistingSlot *bool `json:"logrepl.requireExistingSlot"`
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

	return errors.Join(errs...)
}

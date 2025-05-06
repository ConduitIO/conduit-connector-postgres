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

	return errors.Join(errs...)
}

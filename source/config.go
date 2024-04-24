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

//go:generate paramgen Config

package source

import (
	"errors"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
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
	// CDCModeLongPolling uses long polling to listen to changes.
	CDCModeLongPolling CDCMode = "long_polling"

	// AllTablesWildcard can be used if you'd like to listen to all tables.
	AllTablesWildcard = "*"
)

type Config struct {
	// URL is the connection string for the Postgres database.
	URL string `json:"url" validate:"required"`
	// Tables is a List of table names to read from, separated by a comma, e.g.:"table1,table2".
	// Use "*" if you'd like to listen to all tables.
	Tables []string `json:"tables"` // TODO: make it required once `Table` is removed.
	// Deprecated: use `tables` instead.
	Table []string `json:"table"`

	// SnapshotMode is whether the plugin will take a snapshot of the entire table before starting cdc mode.
	SnapshotMode SnapshotMode `json:"snapshotMode" validate:"inclusion=initial|never" default:"initial"`
	// CDCMode determines how the connector should listen to changes.
	CDCMode CDCMode `json:"cdcMode" validate:"inclusion=auto|logrepl|long_polling" default:"auto"`

	// LogreplPublicationName determines the publication name in case the
	// connector uses logical replication to listen to changes (see CDCMode).
	LogreplPublicationName string `json:"logrepl.publicationName" default:"conduitpub"`
	// LogreplSlotName determines the replication slot name in case the
	// connector uses logical replication to listen to changes (see CDCMode).
	LogreplSlotName string `json:"logrepl.slotName" default:"conduitslot"`
}

// Validate validates the provided config values.
func (c Config) Validate() error {
	var errs []error

	// try parsing the url
	_, err := pgx.ParseConfig(c.URL)
	if err != nil {
		errs = append(errs, fmt.Errorf("invalid url: %w", err))
	}

	if len(c.Tables) > 0 && len(c.Table) > 0 {
		errs = append(errs, fmt.Errorf(`error validating "tables": cannot provide both "table" and "tables", use "tables" only`))
	}

	if len(c.Tables) == 0 {
		errs = append(errs, fmt.Errorf(`error validating "tables": %w`, config.ErrRequiredParameterMissing))
	}

	// TODO: when cdcMode "auto" is implemented, change this check
	if len(c.Tables) != 1 && c.CDCMode == CDCModeLongPolling {
		errs = append(errs, fmt.Errorf("multi-tables are only supported for logrepl CDCMode, please provide only one table"))
	}
	return errors.Join(errs...)
}

// Init sets the desired value on Tables while Table is being deprecated.
func (c Config) Init() Config {
	if len(c.Table) > 0 && len(c.Tables) == 0 {
		c.Tables = c.Table
		c.Table = nil
	}
	return c
}

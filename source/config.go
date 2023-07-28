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
)

type Config struct {
	// URL is the connection string for the Postgres database.
	URL string `json:"url" validate:"required"`
	// The name of the table in Postgres that the connector should read.
	Table string `json:"table" validate:"required"`
	// Comma separated list of column names that should be included in each Record's payload.
	Columns []string `json:"columns"`
	// Column name that records should use for their `Key` fields.
	Key string `json:"key"`

	// Whether or not the plugin will take a snapshot of the entire table before starting cdc mode.
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

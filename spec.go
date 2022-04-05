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

package pg

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "postgres",
		Summary: "A PostgreSQL source and destination plugin for Conduit.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		DestinationParams: map[string]sdk.Parameter{
			"url": {
				Default:     "",
				Required:    true,
				Description: "Connection string for the Postgres database.",
			},
			"table": {
				Default:     "",
				Required:    false,
				Description: "The name of the table in Postgres that the connector should write to.",
			},
			"key": {
				Default:     "",
				Required:    false,
				Description: "Column name used to detect if the target table already contains the record.",
			},
		},
		SourceParams: map[string]sdk.Parameter{
			"url": {
				Default:     "",
				Required:    true,
				Description: "Connection string for the Postgres database.",
			},
			"table": {
				Default:     "",
				Required:    true,
				Description: "The name of the table in Postgres that the connector should read.",
			},
			"columns": {
				Default:     "",
				Required:    false,
				Description: "Comma separated list of column names that should be included in each Record's payload.",
			},
			"key": {
				Default:     "",
				Required:    false,
				Description: "Column name that records should use for their `Key` fields.",
			},
			"snapshotMode": {
				Default:     "initial",
				Required:    false,
				Description: "Whether or not the plugin will take a snapshot of the entire table before starting cdc mode (allowed values: `initial` or `never`).",
			},
			"cdcMode": {
				Default:     "auto",
				Required:    false,
				Description: "Determines the CDC mode (allowed values: `auto`, `logrepl` or `long_polling`).",
			},
			"logrepl.publicationName": {
				Default:     "conduitpub",
				Required:    false,
				Description: "Name of the publication to listen for WAL events.",
			},
			"logrepl.slotName": {
				Default:     "conduitslot",
				Required:    false,
				Description: "Name of the slot opened for replication events.",
			},
		},
	}
}

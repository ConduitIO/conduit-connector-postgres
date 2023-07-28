// Copyright © 2023 Meroxa, Inc.
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

package destination

type Config struct {
	// URL is the connection string for the Postgres database.
	URL string `json:"url" validate:"required"`
	// Table is used as the target table into which records are inserted.
	Table string `json:"table"`
	// Key represents the column name for the key used to identify and update existing rows.
	Key string `json:"key"`
}

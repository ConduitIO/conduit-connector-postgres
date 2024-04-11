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

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type TableFn func(sdk.Record) (string, error)

type Config struct {
	// URL is the connection string for the Postgres database.
	URL string `json:"url" validate:"required"`
	// Table is used as the target table into which records are inserted.
	Table string `json:"table"`
	// Key represents the column name for the key used to identify and update existing rows.
	Key string `json:"key"`
}

// TableFunction returns a function that determines the table for each record individually.
// The function might be returning a static table name.
// If the table is neither static nor a template, an error is returned.
func (c Config) TableFunction() (f TableFn, err error) {
	// Not a template, i.e. it's a static table name
	if !strings.HasPrefix(c.Table, "{{") && !strings.HasSuffix(c.Table, "}}") {
		return func(record sdk.Record) (string, error) {
			return c.Table, nil
		}, nil
	}

	// Try to parse the table
	t, err := template.New("table").Funcs(sprig.FuncMap()).Parse(c.Table)
	if err != nil {
		// The table is not a valid Go template.
		return nil, fmt.Errorf("table is neither a valid static table nor a valid Go template: %w", err)
	}

	// The table is a valid template, return TableFn.
	var buf bytes.Buffer
	return func(r sdk.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute table template: %w", err)
		}
		return buf.String(), nil
	}, nil
}

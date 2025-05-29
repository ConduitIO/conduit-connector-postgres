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

package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreatePublicationOptions contains additional options for creating a publication.
// If AllTables and Tables are both true and not empty at the same time,
// publication creation will fail.
type CreatePublicationOptions struct {
	Tables            []string
	PublicationParams []string
}

// CreatePublication creates a publication.
func CreatePublication(ctx context.Context, conn *pgxpool.Pool, name string, opts CreatePublicationOptions) error {
	if len(opts.Tables) == 0 {
		return fmt.Errorf("publication %q requires at least one table", name)
	}

	wrappedTablesNames := make([]string, 0, len(opts.Tables))
	for _, t := range opts.Tables {
		wrappedTablesNames = append(wrappedTablesNames, internal.WrapSQLIdent(t))
	}

	forTableString := fmt.Sprintf("FOR TABLE %s", strings.Join(wrappedTablesNames, ", "))

	var publicationParams string
	if len(opts.PublicationParams) > 0 {
		publicationParams = fmt.Sprintf("WITH (%s)", strings.Join(opts.PublicationParams, ", "))
	}

	if _, err := conn.Exec(
		ctx,
		fmt.Sprintf("CREATE PUBLICATION %q %s %s", name, forTableString, publicationParams),
	); err != nil {
		return fmt.Errorf("failed to create publication %q: %w", name, err)
	}

	return nil
}

// DropPublicationOptions contains additional options for dropping a publication.
type DropPublicationOptions struct {
	IfExists bool
}

// DropPublication drops a publication.
func DropPublication(ctx context.Context, conn *pgxpool.Pool, name string, opts DropPublicationOptions) error {
	var ifExistsString string
	if opts.IfExists {
		ifExistsString = "IF EXISTS"
	}

	if _, err := conn.Exec(
		ctx,
		fmt.Sprintf("DROP PUBLICATION %s %q", ifExistsString, name),
	); err != nil {
		return fmt.Errorf("failed to drop publication %q: %w", name, err)
	}

	return nil
}

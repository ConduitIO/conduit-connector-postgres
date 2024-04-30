// Copyright © 2024 Meroxa, Inc.
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

package logrepl

import (
	"context"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type CleanupConfig struct {
	URL             string
	SlotName        string
	PublicationName string
}

// Cleanup drops the provided replication slot and publication.
// It will terminate any backends consuming the replication slot before deletion.
func Cleanup(ctx context.Context, c CleanupConfig) error {
	pgconfig, err := pgconn.ParseConfig(c.URL)
	if err != nil {
		return fmt.Errorf("failed to parse config URL: %w", err)
	}

	if pgconfig.RuntimeParams == nil {
		pgconfig.RuntimeParams = make(map[string]string)
	}
	pgconfig.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, pgconfig)
	if err != nil {
		return fmt.Errorf("could not establish replication connection: %w", err)
	}
	defer conn.Close(ctx)

	var errs []error

	if c.SlotName != "" {
		// Terminate any outstanding backends which are consuming the slot before deleting it.
		mrr := conn.Exec(ctx, fmt.Sprintf(
			"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='%s AND active=true'", c.SlotName,
		))
		if err := mrr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to terminate active backends on slot: %w", err))
		}

		if err := pglogrepl.DropReplicationSlot(
			ctx,
			conn,
			c.SlotName,
			pglogrepl.DropReplicationSlotOptions{},
		); err != nil {
			errs = append(errs, fmt.Errorf("failed to clean up replication slot %q: %w", c.SlotName, err))
		}
	} else {
		sdk.Logger(ctx).Warn().
			Msg("cleanup: skipping replication slot cleanup, name is empty")
	}

	if c.PublicationName != "" {
		if err := internal.DropPublication(
			ctx,
			conn,
			c.PublicationName,
			internal.DropPublicationOptions{IfExists: true},
		); err != nil {
			errs = append(errs, fmt.Errorf("failed to clean up publication %q: %w", c.PublicationName, err))
		}
	} else {
		sdk.Logger(ctx).Warn().
			Msg("cleanup: skipping publication cleanup, name is empty")
	}

	return errors.Join(errs...)
}
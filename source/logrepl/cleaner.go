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
	"github.com/jackc/pgx/v5/pgxpool"
)

type CleanupConfig struct {
	URL             string
	SlotName        string
	PublicationName string
}

// Cleanup drops the provided replication slot and publication.
// It will terminate any backends consuming the replication slot before deletion.
func Cleanup(ctx context.Context, c CleanupConfig) error {
	logger := sdk.Logger(ctx)
	logger.Debug().Msg("Cleanup() called")

	pool, err := pgxpool.New(ctx, c.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	var errs []error

	logger.Debug().
		Str("slot", c.SlotName).
		Str("publication", c.PublicationName).
		Msg("removing replication slot and publication")

	if c.SlotName != "" {
		sdk.Logger(ctx).Info().Msgf("attempting to terminate outstanding backends consuming replication slot: %s", c.SlotName)

		// Terminate any outstanding backends which are consuming the slot before deleting it.
		if _, err := pool.Exec(
			ctx,
			"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1 AND active=true", c.SlotName,
		); err != nil {
			errs = append(errs, fmt.Errorf("failed to terminate active backends on slot: %w", err))
		}
		sdk.Logger(ctx).Info().Msgf("terminated outstanding backends consuming replication slot: %s", c.SlotName)
		
		sdk.Logger(ctx).Info().Msgf("attempting to remove replication slot: %s", c.SlotName)

		if _, err := pool.Exec(
			ctx,
			"SELECT pg_drop_replication_slot($1)", c.SlotName,
		); err != nil {
			errs = append(errs, fmt.Errorf("failed to clean up replication slot %q: %w", c.SlotName, err))
		}
		
		sdk.Logger(ctx).Info().Msgf("removed replication slot: %s", c.SlotName)
	} else {
		logger.Warn().Msg("cleanup: skipping replication slot cleanup, name is empty")
	}

	if c.PublicationName != "" {
		if err := internal.DropPublication(
			ctx,
			pool,
			c.PublicationName,
			internal.DropPublicationOptions{IfExists: true},
		); err != nil {
			errs = append(errs, fmt.Errorf("failed to clean up publication %q: %w", c.PublicationName, err))
		}
	} else {
		logger.Warn().Msg("cleanup: skipping publication cleanup, name is empty")
	}

	return errors.Join(errs...)
}

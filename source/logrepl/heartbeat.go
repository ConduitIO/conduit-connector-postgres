// Copyright © 2026 Meroxa, Inc.
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
	"slices"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-connector-postgres/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Heartbeat defaults (DBZ-3 B2 design doc, Decision 2). The source config
// declares the same values as its parameter defaults.
const (
	DefaultHeartbeatSchema   = "public"
	DefaultHeartbeatTable    = "_conduit_heartbeat"
	DefaultHeartbeatInterval = 30 * time.Second

	// heartbeatMaxWriteTimeout caps how long one write may take, so a hung
	// write never overlaps the next tick by more than this.
	heartbeatMaxWriteTimeout = 10 * time.Second

	// heartbeatStaleFactor: the stream counts as stale once no heartbeat has
	// been observed for this many intervals.
	heartbeatStaleFactor = 3

	// maxIdentifierBytes is Postgres's identifier length limit
	// (NAMEDATALEN - 1 in a default build).
	maxIdentifierBytes = 63
)

// Stable, machine-actionable heartbeat error and log codes, embedded at the
// start of the message the same way as the schema-drift codes.
const (
	// ErrorCodeHeartbeatSetupFailed prefixes the Open error when heartbeats
	// are enabled and the table or its publication membership cannot be set
	// up.
	ErrorCodeHeartbeatSetupFailed = "postgres.heartbeat.setup_failed"
	// ErrorCodeHeartbeatTableConflict prefixes the config error when the
	// heartbeat table is also listed as a source table.
	ErrorCodeHeartbeatTableConflict = "postgres.heartbeat.table_conflict"
	// ErrorCodeHeartbeatInvalidConfig prefixes config errors for an empty
	// schema or table name or a non-positive interval.
	ErrorCodeHeartbeatInvalidConfig = "postgres.heartbeat.invalid_config"
	// LogCodeHeartbeatWriteFailed tags the Warn logged when one heartbeat
	// write fails. The write is not retried.
	LogCodeHeartbeatWriteFailed = "postgres.heartbeat.write_failed"
	// LogCodeHeartbeatStale tags the Warn logged once per episode when no
	// heartbeat has come back through the replication stream for
	// heartbeatStaleFactor intervals.
	LogCodeHeartbeatStale = "postgres.heartbeat.stale"
)

// HeartbeatConfig configures the DBZ-3 B2 heartbeat. The zero value is
// disabled.
type HeartbeatConfig struct {
	Enabled  bool
	Interval time.Duration
	Schema   string
	Table    string
}

// Validate checks an enabled heartbeat config against the source tables.
// tables are the configured source table names (unqualified, in the public
// schema); the heartbeat table must not be one of them, because heartbeat
// changes are never emitted as records. A disabled config is always valid.
func (c HeartbeatConfig) Validate(tables []string) error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	if c.Schema == "" {
		errs = append(errs, fmt.Errorf("%s: logrepl.heartbeat.schema must not be empty", ErrorCodeHeartbeatInvalidConfig))
	}
	if c.Table == "" {
		errs = append(errs, fmt.Errorf("%s: logrepl.heartbeat.table must not be empty", ErrorCodeHeartbeatInvalidConfig))
	}
	// Postgres silently truncates longer identifiers, so the table would be
	// created under a name the handler never recognizes and its changes would
	// flow as records.
	if len(c.Schema) > maxIdentifierBytes {
		errs = append(errs, fmt.Errorf("%s: logrepl.heartbeat.schema %q is longer than Postgres's %d-byte identifier limit",
			ErrorCodeHeartbeatInvalidConfig, c.Schema, maxIdentifierBytes))
	}
	if len(c.Table) > maxIdentifierBytes {
		errs = append(errs, fmt.Errorf("%s: logrepl.heartbeat.table %q is longer than Postgres's %d-byte identifier limit",
			ErrorCodeHeartbeatInvalidConfig, c.Table, maxIdentifierBytes))
	}
	if c.Interval <= 0 {
		errs = append(errs, fmt.Errorf("%s: logrepl.heartbeat.interval must be positive, got %s", ErrorCodeHeartbeatInvalidConfig, c.Interval))
	}
	if c.Schema == DefaultHeartbeatSchema && slices.Contains(tables, c.Table) {
		errs = append(errs, fmt.Errorf(
			"%s: table %q is the heartbeat table (logrepl.heartbeat.table) and cannot also be a source table;"+
				" remove it from tables or set logrepl.heartbeat.table to another name",
			ErrorCodeHeartbeatTableConflict, c.Table))
	}
	return errors.Join(errs...)
}

// IsHeartbeatTable reports whether the unqualified public-schema table name
// is this config's heartbeat table. The source uses it to keep the heartbeat
// table out of a `tables: "*"` expansion.
func (c HeartbeatConfig) IsHeartbeatTable(table string) bool {
	return c.Enabled && c.Schema == DefaultHeartbeatSchema && c.Table == table
}

// qualifiedName returns the quoted "schema"."table" identifier.
func (c HeartbeatConfig) qualifiedName() string {
	return internal.WrapSQLIdent(c.Schema) + "." + internal.WrapSQLIdent(c.Table)
}

// setupHeartbeat creates the heartbeat table if it is missing and makes sure
// the publication contains it (design doc, Decision 3). It runs on Open, before
// the subscription exists, and fails Open on any error: the operator turned
// heartbeats on, so running without them would hide the misconfiguration.
func setupHeartbeat(ctx context.Context, pool *pgxpool.Pool, c HeartbeatConfig, publication string) error {
	createQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		slot_name text PRIMARY KEY,
		beat      bigint      NOT NULL,
		beat_at   timestamptz NOT NULL
	)`, c.qualifiedName())
	if _, err := pool.Exec(ctx, createQuery); err != nil {
		return fmt.Errorf(
			"%s: create heartbeat table %s: %w (grant CREATE on schema %q to the connector's user,"+
				" or create the table beforehand)",
			ErrorCodeHeartbeatSetupFailed, c.qualifiedName(), err, c.Schema)
	}

	var inPublication bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)`,
		publication, c.Schema, c.Table,
	).Scan(&inPublication)
	if err != nil {
		return fmt.Errorf("%s: check whether publication %q contains %s: %w",
			ErrorCodeHeartbeatSetupFailed, publication, c.qualifiedName(), err)
	}
	if inPublication {
		return nil
	}

	alterQuery := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", internal.WrapSQLIdent(publication), c.qualifiedName())
	if _, err := pool.Exec(ctx, alterQuery); err != nil {
		return fmt.Errorf(
			"%s: add heartbeat table %s to publication %q: %w (the connector's user must own both"+
				" the publication and the table, or run %q yourself)",
			ErrorCodeHeartbeatSetupFailed, c.qualifiedName(), publication, err, alterQuery)
	}
	sdk.Logger(ctx).Info().
		Str("publication", publication).
		Str("heartbeat_table", c.qualifiedName()).
		Msg("added heartbeat table to publication")
	return nil
}

// heartbeatWriter upserts this slot's heartbeat row on a timer and tracks
// write and delivery staleness (design doc, Decisions 1 and 6).
//
// Position safety does not depend on anything here. The writer never touches
// the subscription's walWritten/walFlushed or any emitted position. A write
// that fails only means no new heartbeat comes back through the stream, so
// the reported flush position cannot advance from heartbeats.
type heartbeatWriter struct {
	pool     *pgxpool.Pool
	query    string
	slotName string
	interval time.Duration

	// lastObserved returns when the handler last saw a heartbeat change come
	// back through the replication stream (zero if never).
	lastObserved func() time.Time

	lastWriteOK         atomic.Int64 // unix nanos of the last successful write, 0 if none
	consecutiveFailures atomic.Uint64

	// started is when run began. It is the staleness baseline until the first
	// heartbeat is observed. Only the run goroutine uses it.
	started time.Time
	// staleLogged keeps the stale Warn to one per episode. Only the run
	// goroutine uses it.
	staleLogged bool
}

func newHeartbeatWriter(pool *pgxpool.Pool, c HeartbeatConfig, slotName string, lastObserved func() time.Time) *heartbeatWriter {
	return &heartbeatWriter{
		pool: pool,
		query: fmt.Sprintf(
			`INSERT INTO %s AS t (slot_name, beat, beat_at) VALUES ($1, 1, now())
			ON CONFLICT (slot_name) DO UPDATE SET beat = t.beat + 1, beat_at = now()`,
			c.qualifiedName()),
		slotName:     slotName,
		interval:     c.Interval,
		lastObserved: lastObserved,
	}
}

// run writes one heartbeat immediately, then one per interval, until ctx is
// done or done is closed (the subscription ended). Writes are never retried
// or queued: a failed or skipped tick is simply the next tick's job.
func (w *heartbeatWriter) run(ctx context.Context, done <-chan struct{}) {
	w.started = time.Now()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		w.beat(ctx)
		w.checkStale(ctx)

		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

// beat performs one heartbeat write.
func (w *heartbeatWriter) beat(ctx context.Context) {
	wctx, cancel := context.WithTimeout(ctx, min(w.interval, heartbeatMaxWriteTimeout))
	defer cancel()

	if _, err := w.pool.Exec(wctx, w.query, w.slotName); err != nil {
		if ctx.Err() != nil {
			return // shutting down, not a heartbeat failure
		}
		failures := w.consecutiveFailures.Add(1)
		sdk.Logger(ctx).Warn().
			Err(err).
			Str("code", LogCodeHeartbeatWriteFailed).
			Uint64("consecutive_failures", failures).
			Msg(LogCodeHeartbeatWriteFailed + ": heartbeat write failed; not retried, the next interval tries again")
		return
	}
	w.consecutiveFailures.Store(0)
	w.lastWriteOK.Store(time.Now().UnixNano())
}

// checkStale logs LogCodeHeartbeatStale once per episode when no heartbeat
// has been observed for heartbeatStaleFactor intervals. It says whether the
// writes themselves are succeeding, so the operator can tell a connector->DB
// failure from a DB->connector one.
func (w *heartbeatWriter) checkStale(ctx context.Context) {
	threshold := heartbeatStaleFactor * w.interval
	since := w.lastObserved()
	if since.IsZero() {
		since = w.started
	}
	age := time.Since(since)
	if age <= threshold {
		w.staleLogged = false
		return
	}
	if w.staleLogged {
		return
	}
	w.staleLogged = true

	writesOK := w.consecutiveFailures.Load() == 0 && w.lastWriteOK.Load() != 0
	side := "connector->database: heartbeat writes are failing"
	if writesOK {
		side = "database->connector: heartbeat writes succeed but do not come back through the replication stream" +
			" (is the heartbeat table still in the publication?)"
	}
	sdk.Logger(ctx).Warn().
		Str("code", LogCodeHeartbeatStale).
		Dur("since_last_observed", age).
		Bool("writes_succeeding", writesOK).
		Msg(LogCodeHeartbeatStale + ": no heartbeat observed for " + age.Round(time.Second).String() + "; " + side)
}

// HeartbeatStatus is a point-in-time view of the heartbeat. The two
// timestamps are the two staleness numbers from the parent design's
// observability section: a failing write (connector->DB) and a heartbeat that
// is written but never delivered (DB->connector) are different failures.
type HeartbeatStatus struct {
	// Enabled is false when heartbeats are off; the other fields are then zero.
	Enabled bool
	// LastWriteOK is when the last heartbeat write succeeded (zero if none).
	LastWriteOK time.Time
	// ConsecutiveWriteFailures counts failed writes since the last success.
	ConsecutiveWriteFailures uint64
	// LastObserved is when a heartbeat change last came back through the
	// replication stream (zero if none).
	LastObserved time.Time
	// LastObservedLSN is that change's LSN (0 if none). The flush position
	// reported to Postgres may advance to it only while no emitted record is
	// unacked.
	LastObservedLSN string
}

func unixNanoTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

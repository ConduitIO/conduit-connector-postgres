#!/usr/bin/env bash
# Runs once, at the primary's initdb (docker-entrypoint-initdb.d).
set -euo pipefail
# Replication connections from the standby and from the connector.
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U postgres -c "SELECT pg_create_physical_replication_slot('standby1')"
if [ -n "${FAILOVER_SYNC_STANDBY_SLOTS:-}" ]; then
  # Read at the restart that follows initdb. The container is removed with
  # `down -v`, so nothing outlives the run.
  psql -v ON_ERROR_STOP=1 -U postgres -c "ALTER SYSTEM SET synchronized_standby_slots = '${FAILOVER_SYNC_STANDBY_SLOTS}'"
fi

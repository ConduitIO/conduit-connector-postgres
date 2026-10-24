#!/usr/bin/env bash
# The standby's entrypoint: a base backup of the primary on first start,
# then a hot standby that synchronizes the primary's failover slots.
set -euo pipefail
export PGDATA=/tmp/pgdata
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_basebackup -D "$PGDATA" -R -S standby1 -X stream \
      -d "host=primary user=postgres password=postgres dbname=postgres"; do
    rm -rf "$PGDATA"
    sleep 1
  done
fi
chmod 700 "$PGDATA"
# The base backup copied the primary's synchronized_standby_slots. It names
# the primary's physical slot for this standby, which does not exist here: on
# this server once promoted, decoding a failover slot would wait for it
# forever. A promoted standby needs its own list (empty in this test).
exec postgres -D "$PGDATA" \
  -c listen_addresses='*' -c synchronized_standby_slots='' \
  -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20 \
  -c hot_standby=on -c hot_standby_feedback=on -c sync_replication_slots=on

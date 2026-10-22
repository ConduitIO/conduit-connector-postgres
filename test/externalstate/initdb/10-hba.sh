#!/bin/sh
# The official image only lets replication connections in over loopback.
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"

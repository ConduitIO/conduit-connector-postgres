#!/usr/bin/env bash
# Copyright © 2026 Meroxa, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Captures the offsets a real Debezium Postgres connector commits, for the
# dbzoffset fixtures (design: Tests, "fixture capture", and pre-merge gate
# item 8). It runs Postgres, Kafka (KRaft) and Kafka Connect with Debezium,
# drives a workload in phases, and records every distinct committed offset
# that `GET /connectors/{name}/offsets` returns, tagged with the phase.
#
# It also checks the design's assumptions (a) and (b) against Postgres
# itself: a second pgoutput slot ("probe"), created before Debezium starts,
# is peeked after the workload, and every captured streaming offset is
# matched against it:
#   (a) lsn_commit is the END LSN of a commit record;
#   (b) for a change offset, lsn is the LSN of a change of transaction txId,
#       and ts_usec is that transaction's commit time.
# The verdicts go to verify.tsv next to the offsets.
#
# Usage: capture.sh <out-dir>
#   DBZ_VERSION   Debezium Connect image tag (default 3.2)
#   PG_IMAGE      Postgres image (default quay.io/debezium/postgres:16)
#   DBZ_PLUGIN    pgoutput (default) or decoderbufs
#
# Every wait is bounded, the whole run is bounded by the caller's timeout,
# and the compose project is removed on exit.
set -euo pipefail

out=${1:?usage: capture.sh <out-dir>}
mkdir -p "$out"
here=$(cd "$(dirname "$0")" && pwd)
plugin=${DBZ_PLUGIN:-pgoutput}
project=dbzoffset-capture-$$
dc() { docker compose -p "$project" -f "$here/docker-compose.yml" "$@"; }
psql() { dc exec -T pg psql -v ON_ERROR_STOP=1 -U postgres -d inventory -qAt "$@"; }

poller_pid=
cleanup() {
  if [ -n "$poller_pid" ]; then kill "$poller_pid" 2>/dev/null || true; fi
  dc down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

dc up -d --wait pg >/dev/null
dc up -d kafka connect >/dev/null
port=$(dc port connect 8083 | sed 's/.*://')
url=http://127.0.0.1:$port
for _ in $(seq 1 180); do
  curl -fs "$url/connectors" >/dev/null 2>&1 && break
  sleep 1
done
curl -fs "$url/connectors" >/dev/null

psql <<'SQL'
CREATE TABLE orders (id bigint PRIMARY KEY, v text);
CREATE TABLE customers (id bigint PRIMARY KEY, v text);
CREATE TABLE big (id bigint PRIMARY KEY, v text);
CREATE TABLE debezium_signal (id varchar(64) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048));
INSERT INTO big SELECT g, md5(g::text) FROM generate_series(1, 200000) g;
CREATE PUBLICATION dbz_publication FOR TABLE orders, customers, big, debezium_signal;
SELECT 1 FROM pg_create_logical_replication_slot('probe', 'pgoutput');
-- test_decoding sees every commit, including transactions with no change
-- to a published table, which pgoutput (PG 15+) does not send but
-- decoderbufs does: Debezium's lsn_commit can be the end of one of those.
SELECT 1 FROM pg_create_logical_replication_slot('probe_all', 'test_decoding');
SQL

phase_file=$out/.phase
echo setup >"$phase_file"
offsets=$out/offsets.jsonl
: >"$offsets"
(
  # Bounded by its own deadline, not only by the trap: it must not outlive
  # this script.
  deadline=$((SECONDS + 900))
  last=
  while [ $SECONDS -lt $deadline ]; do
    o=$(curl -fs "$url/connectors/inventory-connector/offsets" 2>/dev/null | jq -c '.offsets[0].offset // empty' 2>/dev/null || true)
    if [ -n "$o" ] && [ "$o" != "$last" ]; then
      printf '{"phase":"%s","offset":%s}\n' "$(cat "$phase_file")" "$o" >>"$offsets"
      last=$o
    fi
    sleep 0.05
  done
) &
poller_pid=$!

connector_config() {
  local extra=${1:-'{}'}
  jq -n --arg plugin "$plugin" --argjson extra "$extra" '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "pg", "database.port": "5432",
    "database.user": "postgres", "database.password": "postgres",
    "database.dbname": "inventory", "topic.prefix": "dbserver1",
    "plugin.name": $plugin, "slot.name": "debezium",
    "publication.name": "dbz_publication", "publication.autocreate.mode": "disabled",
    "table.include.list": "public.orders,public.customers,public.big,public.debezium_signal",
    "signal.data.collection": "public.debezium_signal",
    "snapshot.mode": "initial", "snapshot.fetch.size": "500", "max.batch.size": "256",
    "skipped.operations": "none", "tombstones.on.delete": "false"
  } + $extra'
}
put_config() {
  connector_config "${1:-}" | curl -fs -X PUT -H 'Content-Type: application/json' \
    --data @- "$url/connectors/inventory-connector/config" >/dev/null
}
wait_for() { # wait_for <seconds> <jq predicate on the offset>
  for _ in $(seq 1 $(($1 * 10))); do
    if curl -fs "$url/connectors/inventory-connector/offsets" | jq -e ".offsets[0].offset | $2" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for: $2" >&2
  return 1
}

echo snapshot >"$phase_file"
put_config
wait_for 300 '(has("snapshot") | not) or .last_snapshot_record == true'
sleep 2
echo snapshot_done >"$phase_file"
sleep 2

echo single >"$phase_file"
for i in 1 2 3; do
  psql -c "INSERT INTO orders VALUES ($i, 'single')"
  sleep 1
done
wait_for 30 'has("lsn_commit")'

echo large >"$phase_file"
psql -c "INSERT INTO orders SELECT g, 'large' FROM generate_series(1000, 61000) g"
sleep 3

echo copy >"$phase_file"
seq 100000 105000 | awk '{print $1 "\tcopy"}' | dc exec -T pg psql -U postgres -d inventory -c "COPY orders FROM STDIN"
sleep 3

echo update_delete >"$phase_file"
psql -c "UPDATE customers SET v = 'u' WHERE id < 0" -c "INSERT INTO customers VALUES (1, 'c')"
psql -c "BEGIN" -c "UPDATE customers SET v = 'u2' WHERE id = 1" -c "DELETE FROM orders WHERE id = 2" -c "COMMIT"
sleep 2

echo truncate >"$phase_file"
psql -c "TRUNCATE orders, customers"
sleep 2

echo message >"$phase_file"
psql -c "SELECT pg_logical_emit_message(false, 'conduit.migrate.cutover', 'x')"
psql -c "INSERT INTO customers VALUES (2, 'after-message')"
sleep 2

echo incremental >"$phase_file"
psql -c "INSERT INTO debezium_signal VALUES ('sig-1', 'execute-snapshot', '{\"data-collections\": [\"public.big\"], \"type\": \"incremental\"}')"
wait_for 120 'keys | map(startswith("incremental_snapshot_")) | any' || true
wait_for 300 'keys | map(startswith("incremental_snapshot_")) | any | not' || true
sleep 2

echo blocking >"$phase_file"
# A blocking snapshot (Debezium 2.5+): streaming pauses while it runs, and
# the offsets committed meanwhile carry snapshot = "BLOCKING" (D7 refuses them).
psql -c "INSERT INTO debezium_signal VALUES ('sig-2', 'execute-snapshot', '{\"data-collections\": [\"public.big\"], \"type\": \"blocking\"}')"
wait_for 120 '.snapshot == "BLOCKING" or .snapshot == true' || true
wait_for 300 'has("snapshot") | not' || true
sleep 2

echo heartbeat >"$phase_file"
put_config '{"heartbeat.interval.ms": "200"}'
sleep 5
psql -c "INSERT INTO customers VALUES (3, 'before-heartbeat')"
sleep 3

echo txmetadata >"$phase_file"
put_config '{"heartbeat.interval.ms": "0", "provide.transaction.metadata": "true"}'
sleep 5
psql -c "BEGIN" -c "INSERT INTO customers VALUES (4, 'tx')" -c "INSERT INTO customers VALUES (5, 'tx')" -c "COMMIT"
sleep 3

echo stopped >"$phase_file"
curl -fs -X PUT "$url/connectors/inventory-connector/stop" >/dev/null
sleep 3
kill "$poller_pid" 2>/dev/null || true
wait "$poller_pid" 2>/dev/null || true
poller_pid=
rm -f "$phase_file"

curl -fs "$url/connectors/inventory-connector/offsets" >"$out/final-rest.json"
dc exec -T connect bash -c 'ls /kafka/connect/debezium-connector-postgres/ | grep -o "debezium-connector-postgres-.*\.jar"' >"$out/version.txt"
psql -c "SELECT version()" >>"$out/version.txt"
echo "plugin.name=$plugin" >>"$out/version.txt"

# Assumptions (a) and (b), checked against the probe slot.
jq -c '.offset' "$offsets" | psql -c "CREATE TABLE captured (o jsonb)" -c "COPY captured FROM STDIN WITH (FORMAT csv, QUOTE e'\x01', DELIMITER e'\x02')"
psql >"$out/verify.tsv" <<'SQL'
CREATE FUNCTION be8(b bytea, off int) RETURNS numeric LANGUAGE sql IMMUTABLE AS
  $$ SELECT ('x' || encode(substr(b, off, 8), 'hex'))::bit(64)::bigint::numeric
            + CASE WHEN get_byte(b, off - 1) >= 128 THEN 18446744073709551616 ELSE 0 END $$;
CREATE TABLE probe AS
  SELECT lsn, xid::text::bigint AS xid, chr(get_byte(data, 0)) AS t, data
  FROM pg_logical_slot_peek_binary_changes('probe', NULL, NULL,
    'proto_version', '1', 'publication_names', 'dbz_publication', 'messages', 'true');
CREATE TABLE commits AS
  SELECT xid, be8(data, 3) AS commit_lsn, be8(data, 11) AS end_lsn,
         be8(data, 19) + 946684800000000 AS ts_usec
  FROM probe WHERE t = 'C';
-- Non-transactional logical messages (flags byte 0). Debezium sets
-- lsn_commit to such a message's LSN (S1).
CREATE TABLE ntx_messages AS
  SELECT (lsn - '0/0'::pg_lsn) AS lsn FROM probe WHERE t = 'M' AND get_byte(data, 1) = 0;
-- Every commit's end, from test_decoding (its COMMIT row's lsn is the
-- commit record's end).
CREATE TABLE all_commit_ends AS
  SELECT (lsn - '0/0'::pg_lsn) AS end_lsn
  FROM pg_logical_slot_peek_changes('probe_all', NULL, NULL, 'skip-empty-xacts', '0')
  WHERE data LIKE 'COMMIT%';
CREATE TABLE changes AS
  SELECT xid, (lsn - '0/0'::pg_lsn) AS lsn FROM probe WHERE t IN ('I', 'U', 'D', 'T');
-- Importable offsets: streaming, no snapshot and no incremental-snapshot
-- state (the parser refuses the others before any LSN rule applies).
CREATE VIEW importable AS SELECT o FROM captured
  WHERE o ? 'lsn_commit' AND NOT o ? 'snapshot'
    AND NOT EXISTS (SELECT 1 FROM jsonb_object_keys(o) k WHERE k LIKE 'incremental_snapshot_%');
SELECT 'offsets', count(*) FROM captured;
SELECT 'importable_offsets', count(*) FROM importable;
-- (a) every lsn_commit is the end of a commit record (of any transaction,
-- including ones with no published change), or the LSN of a
-- non-transactional message (S1).
SELECT 'a_lsn_commit_is_commit_end_or_message', count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM all_commit_ends c WHERE c.end_lsn = (o->>'lsn_commit')::numeric) OR EXISTS (
         SELECT 1 FROM ntx_messages m WHERE m.lsn = (o->>'lsn_commit')::numeric)),
       count(*)
FROM importable;
-- (b) a change offset's lsn is a change of txId, and ts_usec its commit time.
SELECT 'b_lsn_is_change_of_txid', count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM changes ch WHERE ch.xid = (o->>'txId')::bigint AND ch.lsn = (o->>'lsn')::numeric)),
       count(*)
FROM importable WHERE o->>'messageType' IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE');
SELECT 'b_ts_usec_is_commit_time', count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM commits c WHERE c.xid = (o->>'txId')::bigint AND c.ts_usec = (o->>'ts_usec')::numeric)),
       count(*)
FROM importable WHERE o->>'messageType' IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE');
SELECT 'a_unmatched_lsn_commit', o->>'lsn_commit', o->>'messageType', o->>'txId'
FROM importable WHERE NOT EXISTS (
  SELECT 1 FROM all_commit_ends c WHERE c.end_lsn = (o->>'lsn_commit')::numeric) AND NOT EXISTS (
  SELECT 1 FROM ntx_messages m WHERE m.lsn = (o->>'lsn_commit')::numeric);
SELECT 'probe_first_commit_end', min(end_lsn) FROM commits;
SELECT 'b_ts_mismatch', o->>'txId', o->>'messageType', o->>'ts_usec', c.ts_usec, (o->>'ts_usec')::numeric - c.ts_usec
FROM importable JOIN commits c ON c.xid = (o->>'txId')::bigint
WHERE o->>'messageType' IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')
  AND c.ts_usec <> (o->>'ts_usec')::numeric
LIMIT 8;
-- COMMIT offsets: lsn == lsn_commit == the commit's end.
SELECT 'commit_offset_lsn_is_end', count(*) FILTER (WHERE o->>'lsn' = o->>'lsn_commit' AND EXISTS (
         SELECT 1 FROM all_commit_ends c WHERE c.end_lsn = (o->>'lsn')::numeric)),
       count(*)
FROM importable WHERE o->>'messageType' = 'COMMIT';
SQL
cat "$out/verify.tsv"

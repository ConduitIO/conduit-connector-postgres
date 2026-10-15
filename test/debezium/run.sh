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

# Runs the Debezium offset import end-to-end test (test/debezium) against a
# throwaway stack: Postgres, Kafka (KRaft) and Kafka Connect with Debezium, in
# a compose project of its own. Nothing is published on a fixed host port, and
# the project is removed with `down -v` on exit, pass or fail.
#
# Environment (all optional):
#   DBZ_VERSION   Debezium Connect image tag (default 3.2)
#   PG_IMAGE      Postgres image (default quay.io/debezium/postgres:16)
#   DBZ_PLUGIN    Debezium decoding plugin: pgoutput (default) or decoderbufs
#   GOTEST_FLAGS  extra `go test` flags (default -v)
#   DBZ_PROJECT   compose project name (default: unique per run)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
project=${DBZ_PROJECT:-conduit-pg-dbz-$$-$(date +%s)}
compose_file=$here/docker-compose.yml
dc() { docker compose -p "$project" -f "$compose_file" "$@"; }

cleanup() { dc down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

dc up --quiet-pull -d --wait --wait-timeout 300 pg kafka connect

pg_port=$(dc port pg 5432 | sed 's/.*://')
connect_port=$(dc port connect 8083 | sed 's/.*://')

export DBZ_PG_ADDR=127.0.0.1:$pg_port
export DBZ_CONNECT_URL=http://127.0.0.1:$connect_port
export DBZ_COMPOSE_PROJECT=$project
export DBZ_COMPOSE_FILE=$compose_file
export DBZ_VERSION=${DBZ_VERSION:-3.2}
export DBZ_PLUGIN=${DBZ_PLUGIN:-pgoutput}

cd "$root"
# shellcheck disable=SC2086
go test -tags debezium -count=1 -timeout 40m ${GOTEST_FLAGS:--v} ./test/debezium/...

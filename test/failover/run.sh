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

# Runs the slot identity failover test (test/failover, #442) against a
# throwaway primary and standby in a compose project of its own. Nothing is
# published on a fixed host port, and the project is removed with `down -v`
# on exit, pass or fail.
#
# Environment (all optional):
#   PG_IMAGE                     Postgres image, 17 or later (default postgres:17)
#   FAILOVER_SYNC_STANDBY_SLOTS  synchronized_standby_slots on the primary
#                                (default standby1; empty leaves it unset)
#   GOTEST_FLAGS                 extra `go test` flags (default -v)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
project=${FAILOVER_PROJECT:-conduit-pg-failover-$$-$(date +%s)}
compose_file=$here/docker-compose.yml
dc() { docker compose -p "$project" -f "$compose_file" "$@"; }

cleanup() { dc down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

dc up --quiet-pull -d --wait --wait-timeout 300 primary standby

primary_port=$(dc port primary 5432 | sed 's/.*://')
standby_port=$(dc port standby 5432 | sed 's/.*://')
export FAILOVER_PRIMARY_URL="postgres://postgres:postgres@127.0.0.1:$primary_port/postgres?sslmode=disable"
export FAILOVER_STANDBY_URL="postgres://postgres:postgres@127.0.0.1:$standby_port/postgres?sslmode=disable"
export FAILOVER_SYNC_STANDBY_SLOTS=${FAILOVER_SYNC_STANDBY_SLOTS-standby1}

cd "$root"
# shellcheck disable=SC2086
go test -tags failover -count=1 -timeout 15m ${GOTEST_FLAGS:--v} ./test/failover/...

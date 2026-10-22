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

# Runs the same-timeline restore tests (test/restore, #447 re-review S2 and
# round-4 B1/B2). Each scenario gets a stack of its own:
#   A: a pipeline delivers and saves P0;           the data volume is snapshotted;
#   B: it resumes from P0, delivers more, saves P1;  the snapshot is restored;
#   C: new rows, then WAL past P1's bound; the resume from P1 must be refused
#      (before #447 it resumed "covered" and the new rows were lost).
# Scenarios (RESTORE_SCENARIOS, default all three):
#   basic      P1 is a record's position.
#   walinspect pg_walinspect is installed, and the restored history commits
#              a transaction with P1's xid right after P1's commit LSN, with
#              a record that straddles it (round-4 B1). Postgres 15+.
#   drift      P1 is a schema-drift marker's position (round-4 B2).
# Nothing is published on a fixed host port; `down -v` on exit, pass or fail.
#
# Environment (all optional):
#   PG_IMAGE           Postgres image (default postgres:17)
#   RESTORE_SCENARIOS  scenarios to run (default "basic walinspect drift")
#   GOTEST_FLAGS       extra `go test` flags (default -v)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
image=${PG_IMAGE:-postgres:17}
compose_file=$here/docker-compose.yml
base=${RESTORE_PROJECT:-conduit-pg-restore-$$-$(date +%s)}
project=
work=

dc() { docker compose -p "$project" -f "$compose_file" "$@"; }

cleanup() {
  if [ -n "$project" ]; then
    dc down --volumes --remove-orphans >/dev/null 2>&1 || true
    docker volume rm -f "${project}_snapshot" >/dev/null 2>&1 || true
  fi
  [ -n "$work" ] && rm -rf "$work"
  project= work=
}
trap cleanup EXIT

copyvol() { # copyvol <from> <to>: a stopped server's data, byte for byte
  docker run --rm -v "$1":/from -v "$2":/to --entrypoint bash "$image" -c 'rm -rf /to/* && cp -a /from/. /to/'
}

phase() { # phase <scenario> <phase>
  local port
  port=$(dc port pg 5432 | sed 's/.*://')
  (cd "$root" && RESTORE_SCENARIO=$1 RESTORE_PHASE=$2 RESTORE_DIR=$work RESTORE_TABLE=restore_t \
    RESTORE_URL="postgres://postgres:postgres@127.0.0.1:$port/postgres?sslmode=disable" \
    go test -tags restore -count=1 -timeout 5m ${GOTEST_FLAGS:--v} -run TestRestore ./test/restore/...)
}

for scenario in ${RESTORE_SCENARIOS:-basic walinspect drift}; do
  echo "=== scenario $scenario ($image)"
  project=$base-$scenario
  work=$(mktemp -d)
  vol=${project}_pgdata
  snap=${project}_snapshot
  dc up --quiet-pull -d --wait pg
  phase "$scenario" A
  dc stop pg
  docker volume create "$snap" >/dev/null
  copyvol "$vol" "$snap"
  dc start pg && dc up -d --wait pg
  phase "$scenario" B
  dc stop pg
  copyvol "$snap" "$vol"
  dc start pg && dc up -d --wait pg
  phase "$scenario" C
  cleanup
done

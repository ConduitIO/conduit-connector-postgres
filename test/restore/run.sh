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

# Runs the same-timeline restore test (test/restore, #447 re-review S2):
#   A: a pipeline delivers r1 and saves P0;      the data volume is snapshotted;
#   B: it resumes from P0, delivers r2 r3, saves P1; the snapshot is restored;
#   C: new rows n1..n3, then WAL past P1's bound; the resume from P1 must be
#      refused (before #447 it resumed "covered" and n1..n3 were lost).
# Nothing is published on a fixed host port; `down -v` on exit, pass or fail.
#
# Environment (all optional):
#   PG_IMAGE      Postgres image (default postgres:17)
#   GOTEST_FLAGS  extra `go test` flags (default -v)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
project=${RESTORE_PROJECT:-conduit-pg-restore-$$-$(date +%s)}
compose_file=$here/docker-compose.yml
dc() { docker compose -p "$project" -f "$compose_file" "$@"; }
image=${PG_IMAGE:-postgres:17}
vol=${project}_pgdata
snap=${project}_snapshot
work=$(mktemp -d)

cleanup() {
  dc down --volumes --remove-orphans >/dev/null 2>&1 || true
  docker volume rm -f "$snap" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

copyvol() { # copyvol <from> <to>: a stopped server's data, byte for byte
  docker run --rm -v "$1":/from -v "$2":/to --entrypoint bash "$image" -c 'rm -rf /to/* && cp -a /from/. /to/'
}

phase() {
  local port
  port=$(dc port pg 5432 | sed 's/.*://')
  (cd "$root" && RESTORE_PHASE=$1 RESTORE_DIR=$work RESTORE_TABLE=restore_t \
    RESTORE_URL="postgres://postgres:postgres@127.0.0.1:$port/postgres?sslmode=disable" \
    go test -tags restore -count=1 -timeout 5m ${GOTEST_FLAGS:--v} -run TestRestore ./test/restore/...)
}

dc up --quiet-pull -d --wait pg
phase A
dc stop pg
docker volume create "$snap" >/dev/null
copyvol "$vol" "$snap"
dc start pg && dc up -d --wait pg
phase B
dc stop pg
copyvol "$snap" "$vol"
dc start pg && dc up -d --wait pg
phase C

#!/usr/bin/env bash
# Runs the #367 exec-mode benchmark: alternating single benchi runs of the
# cached-statement build (main), the describe_exec build (fix) and an A/A
# control (a second main build), for each batch size. See README.md.
#
# Usage: benchi/destination-exec-mode/run.sh [ROUNDS]   (default 4)
# Env:   BASE_REF (default origin/main before #367: bd44a41), FIX_REF (default HEAD),
#        BENCHI (path to benchi v0.4.1), OUT (results dir).
set -euo pipefail
cd "$(dirname "$0")"
ROUNDS=${1:-4}
BASE_REF=${BASE_REF:-bd44a41}
FIX_REF=${FIX_REF:-HEAD}
BENCHI=${BENCHI:-benchi}
OUT=${OUT:-results/$(date +%Y%m%dT%H%M%S)}
mkdir -p "$OUT"

# benchi creates and always removes a docker network named "benchi"; never
# run while someone else's benchi holds it.
if [ -n "$(docker network ls --filter name='^benchi$' -q)" ]; then
  echo "a docker network named benchi exists (another benchi run?); not starting" >&2
  exit 1
fi

build() { # ref dir
  local wt; wt=$(mktemp -d)
  git -C ../.. worktree add -q --detach "$wt" "$1"
  (cd "$wt" && CGO_ENABLED=0 GOOS=linux GOARCH="$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/')" \
    go build -trimpath -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=$1'" \
    -o "$OLDPWD/bin/$2/conduit-connector-postgres" ./cmd/connector)
  git -C ../.. worktree remove --force "$wt"
}
if [ ! -x bin/main/conduit-connector-postgres ]; then
  build "$BASE_REF" main; build "$BASE_REF" main2; build "$FIX_REF" describe
fi
shasum -a 256 bin/*/conduit-connector-postgres > "$OUT/binaries.sha256"

run() { # batch tool label
  local dir="$OUT/batch$1/$3"
  mkdir -p "$dir"
  uptime > "$dir/uptime-before.txt"
  # benchi is a TUI; script gives it a pseudo-terminal. Every run is bounded.
  BENCH_PIPELINE="pipeline-batch$1.yml" timeout 600 script -q /dev/null \
    "$BENCHI" -config bench.yml -tool "$2" -out "$dir" > /dev/null 2>&1 || echo "run $dir exited $?" >&2
  uptime > "$dir/uptime-after.txt"
  local before after
  before=$(grep -o 'msg="ROWS_BEFORE=[0-9]*"' "$dir/benchi.log" | grep -o '[0-9]*' || true)
  after=$(grep -o 'msg="ROWS_AFTER=[0-9]*"' "$dir/benchi.log" | grep -o '[0-9]*' || true)
  echo "$1,$3,$2,${before:-NA},${after:-NA}" >> "$OUT/runs.csv"
}

echo "batch,run,tool,rows_before,rows_after" > "$OUT/runs.csv"
for batch in 1 1000; do
  run "$batch" pg-cache "warmup"
  for r in $(seq 1 "$ROUNDS"); do
    case $(( r % 3 )) in
      1) order="pg-cache pg-describe pg-cache-aa" ;;
      2) order="pg-describe pg-cache-aa pg-cache" ;;
      0) order="pg-cache-aa pg-cache pg-describe" ;;
    esac
    for tool in $order; do run "$batch" "$tool" "r$r-$tool"; done
  done
done
echo "results in $OUT/runs.csv"

#!/usr/bin/env bash
# Runs the #367 exec-mode benchmark: alternating single benchi runs of four
# arms for each batch size, in a rotated (Latin square) order:
#   pg-cache     BASE_REF, pgx's cached statements (main before #367)
#   pg-cache-aa  BASE_REF, built a second time: the A/A control
#   pg-describe  FIX_REF, describe_exec (option 1)
#   pg-optout    OPTOUT_REF, option 2 with an explicit cache_statement URL
#   pg-guarded   GUARD_REF, the default URL: cache_statement with the schema
#                guard (#433), the default since v0.17.0
# See README.md.
#
# Usage: benchi/destination-exec-mode/run.sh [ROUNDS]   (default 5)
# Env:   BASE_REF, FIX_REF, OPTOUT_REF, GUARD_REF (commits), BENCHI (benchi v0.4.1 binary),
#        OUT (results dir), BATCHES (default "1 1000"), BENCH_CONFIG (default
#        bench.yml; a shorter copy is useful to check the setup).
set -euo pipefail
cd "$(dirname "$0")"
ROUNDS=${1:-5}
BASE_REF=${BASE_REF:-bd44a41}
FIX_REF=${FIX_REF:-HEAD}
OPTOUT_REF=${OPTOUT_REF:-$FIX_REF}
GUARD_REF=${GUARD_REF:-HEAD}
BENCHI=${BENCHI:-benchi}
BATCHES=${BATCHES:-1 1000}
CONFIG=${BENCH_CONFIG:-bench.yml}
OUT=${OUT:-results/$(date +%Y%m%dT%H%M%S)}
mkdir -p "$OUT"

# benchi creates and always removes a docker network named "benchi"; never
# run while someone else's benchi holds it.
if [ -n "$(docker network ls --filter name='^benchi$' -q)" ]; then
  echo "a docker network named benchi exists (another benchi run?); not starting" >&2
  exit 1
fi

sha256() { if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }
# benchi is a TUI and needs a terminal; script(1) provides one. Its arguments
# differ between util-linux and BSD.
with_tty() {
  if script --version >/dev/null 2>&1; then
    script -q -e -c "$(printf '%q ' "$@")" /dev/null
  else
    script -q /dev/null "$@"
  fi
}

ARCH=$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/')
build() { # ref dir
  local wt; wt=$(mktemp -d)
  git -C ../.. worktree add -q --detach "$wt" "$1"
  (cd "$wt" && CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" go build -trimpath \
    -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=$1'" \
    -o "$OLDPWD/bin/$2/conduit-connector-postgres" ./cmd/connector)
  git -C ../.. worktree remove --force "$wt"
}
if [ ! -x bin/main/conduit-connector-postgres ]; then
  build "$BASE_REF" main; build "$BASE_REF" main2; build "$FIX_REF" describe; build "$OPTOUT_REF" optout
  build "$GUARD_REF" guarded
fi
sha256 bin/*/conduit-connector-postgres > "$OUT/binaries.sha256"
{ echo "base $(git -C ../.. rev-parse "$BASE_REF")"; echo "fix $(git -C ../.. rev-parse "$FIX_REF")"; echo "optout $(git -C ../.. rev-parse "$OPTOUT_REF")"; echo "guarded $(git -C ../.. rev-parse "$GUARD_REF")"; } > "$OUT/commits.txt"

run() { # batch tool label
  local dir="$OUT/batch$1/$3"
  mkdir -p "$dir"
  cat /proc/loadavg > "$dir/load-before.txt" 2>/dev/null || uptime > "$dir/load-before.txt"
  # Every run is bounded; a stuck run cannot hold the machine.
  BENCH_PIPELINE="pipeline-batch$1.yml" BENCH_PIPELINE_OPTOUT="pipeline-batch$1-optout.yml" \
    with_tty timeout 600 "$BENCHI" -config "$CONFIG" -tool "$2" -out "$dir" < /dev/null > /dev/null 2>&1 ||
    echo "run $dir exited $?" >&2
  cat /proc/loadavg > "$dir/load-after.txt" 2>/dev/null || uptime > "$dir/load-after.txt"
  local before after
  before=$(grep -o 'msg="ROWS_BEFORE=[0-9]*"' "$dir/benchi.log" 2>/dev/null | grep -o '[0-9]*' || true)
  after=$(grep -o 'msg="ROWS_AFTER=[0-9]*"' "$dir/benchi.log" 2>/dev/null | grep -o '[0-9]*' || true)
  echo "$1,$3,$2,${before:-NA},${after:-NA}" >> "$OUT/runs.csv"
}

ARMS=(pg-cache pg-describe pg-cache-aa pg-optout pg-guarded)
echo "batch,run,tool,rows_before,rows_after" > "$OUT/runs.csv"
for batch in $BATCHES; do
  run "$batch" pg-cache "warmup"
  for ((r = 1; r <= ROUNDS; r++)); do # not seq: BSD seq 1 0 counts down
    # Latin square: round r starts at arm (r-1) mod 5, so every arm runs in
    # every position once over five rounds.
    for k in 0 1 2 3 4; do
      tool=${ARMS[$(( (r - 1 + k) % 5 ))]}
      run "$batch" "$tool" "r$r-$tool"
    done
  done
done
echo "results in $OUT/runs.csv"

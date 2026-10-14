#!/usr/bin/env bash
# Runs the SCRAM channel-binding integration tests against real TLS servers
# (`make test-tls`; the `tls` workflow).
#
# The tests (cpool.TestPool_SCRAMChannelBinding and
# TestDestination_Open_SCRAMChannelBinding) skip unless PGX_TLS_ED25519_URL and
# PGX_TLS_PROXY_URL are set, and `make test` sets neither. This script starts
# what they need and fails if any of them skips:
#
#   - Postgres 17 with ssl=on and an Ed25519 server certificate, SCRAM-SHA-256
#     only. pgx 5.9's default channel_binding=prefer cannot authenticate to it.
#   - Postgres 17 with an ECDSA certificate, reachable only through
#     test/tls/proxy.go, a TLS re-terminating proxy that passes the SASL
#     exchange through. The server offers SCRAM-SHA-256-PLUS but sees another
#     TLS channel than the client, so channel binding fails and plain SCRAM works.
#
# Certificates are generated here with openssl into a temp dir and deleted on
# exit; no key is committed. Containers get random names and ephemeral host
# ports on 127.0.0.1, so concurrent runs do not collide.
set -euo pipefail

PG_IMAGE="${PG_IMAGE:-postgres:17.9}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
SUFFIX="$$-$RANDOM"
CONTAINERS=()
PROXY_PID=""

cleanup() {
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
  for c in "${CONTAINERS[@]:-}"; do
    [ -n "$c" ] && docker rm -f -v "$c" >/dev/null 2>&1 || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

# gen_cert <dir> <openssl genpkey algorithm args...>
gen_cert() {
  local dir="$1"; shift
  mkdir -p "$dir"
  openssl genpkey "$@" -out "$dir/server.key" 2>/dev/null
  openssl req -new -x509 -key "$dir/server.key" -out "$dir/server.crt" -days 1 \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null
  # the postgres user (uid 999) in the container must read the key
  chmod 644 "$dir/server.key"
}

# start_pg <name> <cert dir>; prints the mapped host port
start_pg() {
  local name="tls-$1-$SUFFIX" dir="$2"
  CONTAINERS+=("$name")
  docker run -d --name "$name" \
    -e POSTGRES_PASSWORD=s3cret \
    -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
    -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
    -v "$dir:/certs:ro" \
    -p 127.0.0.1::5432 \
    --entrypoint sh "$PG_IMAGE" -c '
      cp /certs/server.* /tmp/ && chown postgres /tmp/server.* && chmod 600 /tmp/server.key &&
      exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.crt \
        -c ssl_key_file=/tmp/server.key -c password_encryption=scram-sha-256' >/dev/null
  # The image restarts postgres once after initdb, so poll a real TCP login
  # instead of trusting the first pg_isready. libpq has the same Ed25519
  # channel-binding failure as pgx 5.9 ("could not find digest for NID UNDEF"),
  # hence PGCHANNELBINDING=disable for the probe.
  local i
  for i in $(seq 1 60); do
    if docker exec -e PGPASSWORD=s3cret -e PGCHANNELBINDING=disable "$name" psql -U postgres -h 127.0.0.1 -tAc 'select 1' >/dev/null 2>&1; then
      docker port "$name" 5432/tcp | head -n1 | sed 's/.*://'
      return 0
    fi
    sleep 1
  done
  echo "postgres container $name did not become ready" >&2
  docker logs "$name" >&2 || true
  return 1
}

gen_cert "$WORK/ed25519" -algorithm ed25519
gen_cert "$WORK/ec" -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1
gen_cert "$WORK/proxy" -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1

ED_PORT="$(start_pg ed25519 "$WORK/ed25519")"
EC_PORT="$(start_pg ec "$WORK/ec")"

go run "$ROOT/test/tls/proxy.go" -cert "$WORK/proxy/server.crt" -key "$WORK/proxy/server.key" \
  -up "127.0.0.1:$EC_PORT" >"$WORK/proxy.out" 2>"$WORK/proxy.err" &
PROXY_PID=$!
for _ in $(seq 1 60); do
  grep -q '^listening ' "$WORK/proxy.out" && break
  kill -0 "$PROXY_PID" 2>/dev/null || { cat "$WORK/proxy.err" >&2; echo "proxy exited" >&2; exit 1; }
  sleep 1
done
PROXY_ADDR="$(sed -n 's/^listening //p' "$WORK/proxy.out")"
[ -n "$PROXY_ADDR" ] || { echo "proxy did not start" >&2; exit 1; }

export PGX_TLS_ED25519_URL="postgres://postgres:s3cret@127.0.0.1:$ED_PORT/postgres?sslmode=require"
export PGX_TLS_PROXY_URL="postgres://postgres:s3cret@$PROXY_ADDR/postgres?sslmode=require"

# Exactly the two server-backed tests, in the two packages that hold them. A
# broader -run would also pick up TestDestination_Open_AppliesSCRAMCompat, which
# needs the `make test` database; ./... would race-compile every package for nothing.
OUT="$WORK/go-test.out"
set +e
(cd "$ROOT" && go test -count=1 -race -v . ./source/cpool \
  -run '^(TestPool_SCRAMChannelBinding|TestDestination_Open_SCRAMChannelBinding)$' 2>&1 | tee "$OUT")
RC=${PIPESTATUS[0]}
set -e
[ "$RC" -eq 0 ] || exit "$RC"

# A green `go test` is not evidence the tests ran: they skip when the URLs are
# unset or renamed. Require every subtest by name, and no SKIP at all.
if grep -q -- '--- SKIP' "$OUT"; then
  echo "::error::a channel-binding test skipped; the TLS servers were not used" >&2
  exit 1
fi
for want in \
  'TestPool_SCRAMChannelBinding/PGX_TLS_ED25519_URL' \
  'TestPool_SCRAMChannelBinding/PGX_TLS_PROXY_URL' \
  'TestDestination_Open_SCRAMChannelBinding/PGX_TLS_ED25519_URL' \
  'TestDestination_Open_SCRAMChannelBinding/PGX_TLS_PROXY_URL'; do
  if ! grep -qF -- "--- PASS: $want" "$OUT"; then
    echo "::error::$want did not run and pass" >&2
    exit 1
  fi
done
echo "channel-binding TLS tests ran and passed"

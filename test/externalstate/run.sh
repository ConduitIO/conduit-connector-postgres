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

# Runs the external-state suite described by cases.txt. Fails closed:
#   - a required case fails if a test fails, is skipped, or fewer than `min`
#     tests ran (a typo in a regex must not look like a pass);
#   - a pending case is reported as PENDING and fails the run once the test
#     that was blocking it exists, so it has to be flipped to required;
#   - a known-gap case must fail; it fails the run if it passes.
#   - a known-gap case must fail WITH SILENT-loss evidence (a "SILENT" line in
#     the test output); a failure for any other reason (infrastructure, a
#     panic, a wrong error) is a FAIL of the run, not a known gap.
# Both stacks are their own compose projects on their own host ports, and only
# those projects are removed (`down -v -p <project>`) on exit, pass or fail.
# Nothing here starts, reuses or removes the default `make test` stack
# (project "test", port 5433) or the chaos stack (5434).
#
# Environment (all optional):
#   EXTSTATE_PG_PORT / EXTSTATE_PROXY_PORT / EXTSTATE_PROXY_API_PORT
#                         host ports of the ext stack (56150-56152)
#   EXTSTATE_BASE_PORT / EXTSTATE_BASE_BOUNCER_PORT
#                         host ports of the base stack (56153, 56154)
#   EXTSTATE_BASE_COMPOSE compose file of the base stack (default
#                         test/docker-compose.yml, run as project pgextbase)
#   EXTSTATE_CASES        regexp of case ids to run (default: all)
#   GOTEST_FLAGS          extra `go test` flags
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
cd "$root" || exit 1

command -v jq >/dev/null || { echo "INFRA: jq is required" >&2; exit 1; }

export EXTSTATE_PROJECT=pgextstate
export EXTSTATE_COMPOSE_FILE="$here/docker-compose.yml"
export EXTSTATE_PG_PORT=${EXTSTATE_PG_PORT:-56150}
export EXTSTATE_PROXY_PORT=${EXTSTATE_PROXY_PORT:-56151}
export EXTSTATE_PROXY_API_PORT=${EXTSTATE_PROXY_API_PORT:-56152}
base_compose=${EXTSTATE_BASE_COMPOSE:-"$root/test/docker-compose.yml"}
# The base stack is test/docker-compose.yml under its own project and ports, so
# it can never collide with, reuse or `down -v` a developer's `make test` stack.
base_project=pgextbase
export CONDUIT_PG_TEST_PORT=${EXTSTATE_BASE_PORT:-56153}
export CONDUIT_PG_TEST_BOUNCER_PORT=${EXTSTATE_BASE_BOUNCER_PORT:-56154}
case "$CONDUIT_PG_TEST_PORT $CONDUIT_PG_TEST_BOUNCER_PORT" in
*5433* | *5434* | *6432*) echo "INFRA: refusing ports 5433, 5434 and 6432: they belong to make test and make test-chaos" >&2; exit 1 ;;
esac

up_base=0 up_ext=0
cleanup() {
	if [ "$up_ext" = 1 ]; then
		docker compose -p "$EXTSTATE_PROJECT" -f "$EXTSTATE_COMPOSE_FILE" down -v >/dev/null 2>&1
	fi
	docker volume rm -f "${EXTSTATE_PROJECT}_pgsnap" >/dev/null 2>&1
	if [ "$up_base" = 1 ]; then
		docker compose -p "$base_project" -f "$base_compose" down -v >/dev/null 2>&1
	fi
}
trap cleanup EXIT

ensure_stack() {
	case "$1" in
	base)
		if [ "$up_base" = 0 ]; then
			up_base=1
			docker compose -p "$base_project" -f "$base_compose" up --force-recreate --quiet-pull -d --wait || return 1
		fi ;;
	ext)
		if [ "$up_ext" = 0 ]; then
			up_ext=1
			docker compose -p "$EXTSTATE_PROJECT" -f "$EXTSTATE_COMPOSE_FILE" up --force-recreate --quiet-pull -d --wait || return 1
		fi ;;
	esac
}

exists_when() {
	local spec=$1
	case "$spec" in
	file:*) [ -e "$root/${spec#file:}" ] ;;
	func:*)
		local rest=${spec#func:}
		local dir=${rest%%:*} re=${rest#*:}
		grep -rEq --include='*_test.go' -- "$re" "$root/$dir" ;;
	*) return 1 ;;
	esac
}

results=() failed=0
record() { results+=("$1|$2|$3"); }

while IFS='|' read -r id status stack kind target regex min envs when <&3; do
	case "$id" in '' | '#'*) continue ;; esac
	if [ -n "${EXTSTATE_CASES:-}" ] && ! [[ $id =~ $EXTSTATE_CASES ]]; then continue; fi
	echo "=== case $id ($status)"

	if [ "$status" = pending ]; then
		if exists_when "$when"; then
			record "$id" FAIL "pending, but the blocker now exists ($when): flip it to required in cases.txt"
			failed=1
		else
			record "$id" PENDING "blocked: $when absent"
		fi
		continue
	fi

	ensure_stack "$stack" || { record "$id" FAIL "stack $stack did not start"; failed=1; continue; }

	if [ "$kind" = script ]; then
		if bash "$root/$target"; then record "$id" PASS "$target"; else record "$id" FAIL "$target exited non-zero"; failed=1; fi
		continue
	fi

	tags=""
	case "$target" in ./test/externalstate*) tags="-tags externalstate" ;; esac
	envargs=()
	if [ "$envs" != "-" ]; then IFS=',' read -ra envargs <<<"$envs"; fi
	out=$(mktemp)
	# shellcheck disable=SC2086
	env ${envargs[@]+"${envargs[@]}"} go test -json -count=1 -p 1 -race $tags ${GOTEST_FLAGS:-} -run "$regex" "$target" >"$out" 2>&1
	code=$?
	counts=$(jq -Rrs '[split("\n")[] | fromjson? | select(.Test != null and (.Test | contains("/") | not))
		| select(.Action == "pass" or .Action == "fail" or .Action == "skip") | .Action]
		| "\(map(select(. == "pass")) | length) \(map(select(. == "fail")) | length) \(map(select(. == "skip")) | length)"' "$out")
	read -r npass nfail nskip <<<"$counts"
	evidence=$(jq -Rrs '[split("\n")[] | fromjson? | select(.Action == "output") | .Output]
		| join("") | split("\n") | map(select(test("SILENT|INFRA|--- FAIL"))) | .[0:3] | join(" // ")' "$out")

	case "$status" in
	required)
		if [ "$code" = 0 ] && [ "$nfail" = 0 ] && [ "$nskip" = 0 ] && [ "$npass" -ge "$min" ]; then
			record "$id" PASS "$npass passed"
		else
			record "$id" FAIL "exit=$code pass=$npass fail=$nfail skip=$nskip want>=$min. $evidence"
			failed=1
			grep -h '"Action":"output"' "$out" | jq -r .Output 2>/dev/null | tail -40
		fi ;;
	known-gap)
		silent=$(jq -Rrs '[split("\n")[] | fromjson? | select(.Action == "output") | .Output]
			| join("") | split("\n") | map(select(test("SILENT (LOSS|STALL)"))) | length' "$out")
		if [ "$nfail" -ge 1 ] && [ "$npass" = 0 ] && [ "$silent" -ge 1 ]; then
			record "$id" KNOWN-GAP "fails with silent loss, as expected: $evidence"
		elif [ "$nfail" -ge 1 ] && [ "$npass" = 0 ]; then
			record "$id" FAIL "known-gap test failed, but not with SILENT evidence (infrastructure or another error?): $evidence"
			failed=1
		elif [ "$npass" -ge 1 ]; then
			record "$id" FAIL "known-gap test passed: the gap is closed, flip it to required in cases.txt"
			failed=1
		else
			record "$id" FAIL "known-gap test did not run to a failure (exit=$code): $evidence"
			failed=1
		fi ;;
	*) record "$id" FAIL "unknown status $status"; failed=1 ;;
	esac
	rm -f "$out"
done 3<"$here/cases.txt"

echo
echo "external-state suite"
printf '%-34s %-10s %s\n' CASE RESULT DETAIL
for r in "${results[@]}"; do
	IFS='|' read -r a b c <<<"$r"
	printf '%-34s %-10s %s\n' "$a" "$b" "$c"
done
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	{
		echo "### External-state suite"
		echo "| case | result | detail |"
		echo "| --- | --- | --- |"
		for r in "${results[@]}"; do
			IFS='|' read -r a b c <<<"$r"
			echo "| $a | $b | ${c//|/\\|} |"
		done
	} >>"$GITHUB_STEP_SUMMARY"
fi
exit "$failed"

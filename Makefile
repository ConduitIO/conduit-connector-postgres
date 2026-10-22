VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=${VERSION}'" -o conduit-connector-postgres cmd/connector/main.go

.PHONY: test
test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --force-recreate --quiet-pull -d --wait
	env -u CONDUIT_PG_SLOT_INVALIDATION go test -count=1 $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		if [ $$ret -eq 0 ]; then \
			echo "slot invalidation test: alone, after the suite (it sets max_slot_wal_keep_size cluster-wide)"; \
			CONDUIT_PG_SLOT_INVALIDATION=1 go test -count=1 $(GOTEST_FLAGS) -race -p 1 -run '^TestCombinedIterator_InvalidatedSlot_FailsLoudly$$' ./source/logrepl/; ret=$$?; \
		fi; \
		docker compose -f test/docker-compose.yml down --volumes; \
		exit $$ret

# Guard mutation gate (tools/guardmut, docs/guard-markers.md). Own compose
# project (pgguardmut) on its own host ports, so it never touches, reuses or
# removes a developer's `make test` stack (project "test", port 5433); only the
# pgguardmut project is torn down (`down -v`).
GUARDMUT_PG_PORT ?= 56170
GUARDMUT_BOUNCER_PORT ?= 56171
GUARDMUT_RUN = env -u CONDUIT_PG_SLOT_INVALIDATION CONDUIT_PG_TEST_PORT=$(GUARDMUT_PG_PORT) \
	CONDUIT_PG_TEST_BOUNCER_PORT=$(GUARDMUT_BOUNCER_PORT) go run ./guardmut -root .. -dirs source/logrepl \
	-baseline ../.github/guardmut-baseline.txt -guards ../.github/guardmut-guards.txt -timeout 15m $(GUARDMUT_FLAGS)
.PHONY: guardmut
guardmut:
	# Mutants run one at a time because they share the database. Guards whose
	# test needs CONDUIT_PG_SLOT_INVALIDATION=1 run ALONE and last, as in
	# `make test`: that mode sets max_slot_wal_keep_size cluster-wide.
	@case "$(GUARDMUT_PG_PORT) $(GUARDMUT_BOUNCER_PORT)" in *5433*|*5434*|*6432*) echo "refusing ports 5433, 5434 and 6432: they belong to make test and make test-chaos"; exit 1;; esac
	CONDUIT_PG_TEST_PORT=$(GUARDMUT_PG_PORT) CONDUIT_PG_TEST_BOUNCER_PORT=$(GUARDMUT_BOUNCER_PORT) \
		docker compose -p pgguardmut -f test/docker-compose.yml up --force-recreate --quiet-pull -d --wait pg-0
	cd tools && $(GUARDMUT_RUN) -without-env CONDUIT_PG_SLOT_INVALIDATION -budget 30m; ret=$$?; \
		if [ $$ret -eq 0 ]; then \
			echo "guards that need slot invalidation: alone, after the others"; \
			$(GUARDMUT_RUN) -with-env CONDUIT_PG_SLOT_INVALIDATION -budget 20m; ret=$$?; \
		fi; \
		CONDUIT_PG_TEST_PORT=$(GUARDMUT_PG_PORT) CONDUIT_PG_TEST_BOUNCER_PORT=$(GUARDMUT_BOUNCER_PORT) \
			docker compose -p pgguardmut -f ../test/docker-compose.yml down --volumes; \
		exit $$ret

.PHONY: test-tls
test-tls:
	# SCRAM channel-binding tests against TLS servers (Ed25519 certificate, TLS
	# re-terminating proxy). `make test` skips them because it sets no
	# PGX_TLS_*_URL; this target starts the servers and fails if they skip.
	# Needs docker and openssl. Not part of `make test`.
	test/tls/run.sh

.PHONY: test-timezones
test-timezones:
	# Values must not depend on the host time zone (#302). `make test` runs in
	# whatever zone the machine is in (UTC on CI), where the pre-fix code is
	# right, so replay the value tests in zones with a non-zero offset, a half
	# hour offset and DST gaps.
	docker compose -f test/docker-compose.yml up --force-recreate --quiet-pull -d --wait
	ret=0; for tz in America/Los_Angeles Asia/Kolkata Pacific/Auckland; do \
		echo "TZ=$$tz"; \
		TZ=$$tz go test -count=1 $(GOTEST_FLAGS) -race -run 'TestGolden|TestRelationSet|Test_FetcherRun_TemporalTypes' ./source/... || ret=1; \
	done; \
	docker compose -f test/docker-compose.yml down --volumes; \
	exit $$ret

.PHONY: test-chaos
test-chaos:
	# DBZ-3 process-kill chaos harness (test/chaos). Separate compose
	# project/port(5434)/volume from `make test` - see
	# test/docker-compose.chaos.yml - so the two suites can never collide
	# or silently share a misconfigured stack. Fails closed: no
	# continue-on-error here or in chaos.yml, no skip on missing docker -
	# a missing/misconfigured stack is an INFRA: t.Fatal from inside the
	# suite itself (test/chaos/pgstate.go's requireChaosStack), not a
	# quietly green run.
	docker compose -f test/docker-compose.chaos.yml up --force-recreate --quiet-pull -d --wait
	go test -tags conduitchaos -count=1 $(GOTEST_FLAGS) -race ./test/chaos/...; ret=$$?; \
		docker compose -f test/docker-compose.chaos.yml down --volumes; \
		exit $$ret

.PHONY: test-external-state
test-external-state:
	# External-state chaos suite (test/externalstate): the slot, the privileges,
	# the network or the database volume change under a source, which must then
	# deliver every row or fail with a code. cases.txt lists the cases; run.sh
	# fails closed (a required case that runs no test fails) and removes its own
	# compose projects with `down -v`. Needs docker and jq. Runs its own compose projects on ports 56150-56154;
	# it never touches the stacks of `make test` (5433) or `make test-chaos` (5434).
	test/externalstate/run.sh

.PHONY: test-debezium
test-debezium:
	# End-to-end test of the Debezium offset import against real Debezium
	# (test/debezium). run.sh brings up a throwaway Postgres + Kafka + Kafka
	# Connect stack in its own compose project on ephemeral ports and removes it
	# with `down -v` on exit. Matrix: DBZ_VERSION (3.2, 2.7), PG_IMAGE,
	# DBZ_PLUGIN (pgoutput, decoderbufs). Fails closed: a missing stack is an
	# INFRA: failure from inside the test, never a skip.
	test/debezium/run.sh

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...
	conn-sdk-cli readmegen -w

.PHONY: fmt
fmt:
	gofumpt -l -w .
	gci write --skip-generated  .

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

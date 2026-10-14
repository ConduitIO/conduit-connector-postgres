VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=${VERSION}'" -o conduit-connector-postgres cmd/connector/main.go

.PHONY: test
test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --force-recreate --quiet-pull -d --wait
	go test -count=1 $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down --volumes; \
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

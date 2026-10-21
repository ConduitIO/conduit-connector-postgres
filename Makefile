VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=${VERSION}'" -o conduit-connector-postgres cmd/connector/main.go

.PHONY: test
test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --force-recreate --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		if [ $$ret -eq 0 ]; then \
			echo "slot invalidation test: alone, after the suite (it sets max_slot_wal_keep_size cluster-wide)"; \
			CONDUIT_PG_SLOT_INVALIDATION=1 go test -count=1 $(GOTEST_FLAGS) -race -p 1 -run '^TestCombinedIterator_InvalidatedSlot_FailsLoudly$$' ./source/logrepl/; ret=$$?; \
		fi; \
		docker compose -f test/docker-compose.yml down --volumes; \
		exit $$ret

.PHONY: test-tls
test-tls:
	# SCRAM channel-binding tests against TLS servers (Ed25519 certificate, TLS
	# re-terminating proxy). `make test` skips them because it sets no
	# PGX_TLS_*_URL; this target starts the servers and fails if they skip.
	# Needs docker and openssl. Not part of `make test`.
	test/tls/run.sh

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

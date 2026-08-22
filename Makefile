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

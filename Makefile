VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=${VERSION}'" -o conduit-connector-postgres cmd/connector/main.go

.PHONY: test
test: start_test_service tests stop_test_service 

.PHONY: start_test_service
start_test_service:
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait --force-recreate

.PHONY: stop_test_service
stop_test_service:
	docker compose -f test/docker-compose.yml down --volumes

.PHONY: tests
tests: 
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: generate
generate:
	go generate ./...

.PHONY: fmt
fmt:
	gofumpt -l -w .
	gci write --skip-generated  .

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-postgres.version=${VERSION}'" -o conduit-connector-postgres cmd/connector/main.go

test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down; \
		exit $$ret

lint:
	golangci-lint run

generate:
	go generate ./...

.PHONY: build test generate lint

build:
	go build -o conduit-connector-postgres cmd/pg/main.go

test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose-postgres.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose-postgres.yml down; \
		exit $$ret

generate:
	go generate ./...

lint:
	golangci-lint run

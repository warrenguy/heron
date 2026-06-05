.PHONY: check test coverage clean test-integration test-zero-allocs aeronmd lint gofumpt

ROOT := $(CURDIR)
export PATH := $(ROOT)/bin:$(PATH)

COVERAGE_PROFILE := coverage.out

check: lint test

lint:
	go tool golangci-lint run ./...

gofumpt:
	go tool gofumpt -extra -w $$(grep -rl --include='*.go' -L 'DO NOT EDIT' .)

test:
	go test -race -coverprofile=$(COVERAGE_PROFILE) ./...
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1

coverage: test
	go tool cover -html=$(COVERAGE_PROFILE) -o coverage.html

clean:
	rm -f $(COVERAGE_PROFILE) coverage.html
	rm -f bin/aeronmd

test-integration: bin/aeronmd
	go test -race ./test/integration/...

test-zero-allocs:
	go test -run 'ZeroAllocs' ./...

bin/aeronmd: specs/versions.yaml docker/aeronmd/Dockerfile scripts/build-aeronmd.sh
	./scripts/build-aeronmd.sh

aeronmd: bin/aeronmd
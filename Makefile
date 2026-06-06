.PHONY: bench bench-baseline bench-regression check test test-stress coverage clean test-integration aeronmd lint gofumpt

ROOT := $(CURDIR)
export PATH := $(ROOT)/bin:$(PATH)
SHELL := /bin/bash

# Little-endian 64-bit targets only (matches Aeron wire layout on host).
SUPPORTED_GOARCHES := amd64 arm64
NATIVE_GOARCH := $(shell env -u GOARCH -u GOOS go env GOARCH)
GOARCH ?= $(NATIVE_GOARCH)
ifeq ($(filter $(GOARCH),$(SUPPORTED_GOARCHES)),)
$(error unsupported GOARCH=$(GOARCH); want $(SUPPORTED_GOARCHES))
endif
ifneq ($(GOARCH),$(NATIVE_GOARCH))
ifneq ($(ALLOW_CROSS),1)
$(error GOARCH=$(GOARCH) != native $(NATIVE_GOARCH); check/test use host arch (ALLOW_CROSS=1 to cross-build))
endif
endif

COVERAGE_PROFILE := coverage.out

check: lint test test-stress

lint:
	go tool golangci-lint run ./...

gofumpt:
	go tool gofumpt -extra -w $$(grep -rl --include='*.go' -L 'DO NOT EDIT' .)

test:
	go test -race -coverprofile=$(COVERAGE_PROFILE) ./...
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1

test-stress:
	go test -tags=stress -count=1 -run Stress ./aeron/ringbuffer/ ./aeron/broadcast/

bench:
	go test -bench=. -benchmem ./...

BENCH_DIR := .bench
BENCH_BASELINE := $(BENCH_DIR)/baseline.txt
BENCH_COUNT ?= 10
BENCH_PKGS := ./aeron/broadcast/ ./aeron/ringbuffer/
export BENCH_PKGS BENCH_COUNT BENCH_BASELINE GOARCH

bench-baseline:
	@mkdir -p $(BENCH_DIR) && set -o pipefail && \
		go test -bench=. -benchmem -count=$(BENCH_COUNT) $(BENCH_PKGS) \
		| tee $(BENCH_BASELINE)

bench-regression:
	@scripts/bench-regression.sh

coverage: test
	go tool cover -html=$(COVERAGE_PROFILE) -o coverage.html

clean:
	rm -f $(COVERAGE_PROFILE) coverage.html
	rm -f bin/aeronmd

test-integration: bin/aeronmd
	go test -race ./test/integration/...

bin/aeronmd: specs/versions.yaml docker/aeronmd/Dockerfile scripts/build-aeronmd.sh
	./scripts/build-aeronmd.sh

aeronmd: bin/aeronmd
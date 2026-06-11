.PHONY: bench bench-baseline bench-regression check check-inner test test-stress coverage clean test-integration aeronmd lint gofumpt goimports

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
GO_MODULE := github.com/warrenguy/heron
GO_SOURCES := $$(grep -rl --include='*.go' -L 'DO NOT EDIT' .)
# Conductor cross-package test hooks (test_hooks.go, hook_access.go).
UNIT_TEST_TAGS := testhooks
export UNIT_TEST_TAGS
# Per-package go test deadline (replaces go default 10m on hung tests).
TEST_TIMEOUT ?= 2m
# Wall-clock cap for full check (lint + test + test-stress).
CHECK_TIMEOUT ?= 5m

# Without GNU timeout/gtimeout on PATH, CHECK_TIMEOUT is not enforced; TEST_TIMEOUT still applies.
check:
	@T=$$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null); \
	if [ -n "$$T" ]; then \
		$$T $(CHECK_TIMEOUT) $(MAKE) --no-print-directory check-inner; \
	else \
		$(MAKE) --no-print-directory check-inner; \
	fi

check-inner: lint test test-stress

lint:
	go tool golangci-lint run ./...
	go tool golangci-lint run --build-tags=integration -D unused ./test/integration/...

goimports:
	go tool goimports -local $(GO_MODULE) -w $(GO_SOURCES)

gofumpt: goimports
	go tool gofumpt -extra -w $(GO_SOURCES)

# Unit tests need -tags=testhooks (conductor cross-package hooks); use make test.
test:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=$(UNIT_TEST_TAGS) -coverprofile=$(COVERAGE_PROFILE) ./...
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1

BENCH_DIR := .bench
BENCH_BASELINE := $(BENCH_DIR)/baseline.txt
BENCH_COUNT ?= 10
BENCH_PKGS := ./...
export BENCH_PKGS BENCH_COUNT BENCH_BASELINE GOARCH

test-stress:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=stress,$(UNIT_TEST_TAGS) -count=1 -run Stress ./...

bench:
	go test -run '^$$' -tags=$(UNIT_TEST_TAGS) -bench=. -benchmem ./...

bench-baseline:
	@mkdir -p $(BENCH_DIR) && set -o pipefail && \
		go test -run '^$$' -tags=$(UNIT_TEST_TAGS) -bench=. -benchmem -count=$(BENCH_COUNT) $(BENCH_PKGS) \
		| tee $(BENCH_BASELINE)

bench-regression:
	@scripts/bench-regression.sh

coverage: test
	go tool cover -html=$(COVERAGE_PROFILE) -o coverage.html

clean:
	rm -f $(COVERAGE_PROFILE) coverage.html
	rm -f bin/aeronmd
	find . -type d -path '*/testdata/rapid' -prune -exec rm -rf {} +

test-integration: bin/aeronmd
	go test -timeout=$(TEST_TIMEOUT) -race -count=1 -tags=integration ./test/integration/...

bin/aeronmd: specs/versions.yaml docker/aeronmd/Dockerfile scripts/build-aeronmd.sh
	./scripts/build-aeronmd.sh

aeronmd: bin/aeronmd

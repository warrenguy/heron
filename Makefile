.PHONY: bench bench-baseline bench-regression check check-aeron-versions check-inner \
	fuzz test test-rapid test-stress coverage clean test-integration \
	test-java-interop test-java-interop-unit test-java-interop-oracle \
	test-java-interop-contract aeronmd lint gofumpt goimports

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
$(error GOARCH=$(GOARCH) != native $(NATIVE_GOARCH); \
	use ALLOW_CROSS=1 to cross-build for check/test)
endif
endif

COVERAGE_PROFILE := coverage.out
GO_MODULE := github.com/warrenguy/heron
GO_TEST_PKGS := ./...
GO_SOURCES := $$(grep -rl --include='*.go' -L 'DO NOT EDIT' \
	--exclude-dir=docker .)
SHELL_SCRIPTS := $$(find . -name '*.sh' -type f ! -path './.git/*')
# Conductor cross-package test hooks (test_hooks.go, hook_access.go).
UNIT_TEST_TAGS := testhooks
export UNIT_TEST_TAGS
# Per-package go test deadline (replaces go default 10m on hung tests).
TEST_TIMEOUT ?= 2m
FUZZ_TIME ?= 30s
# Per-package go test deadline for make fuzz (baseline + FUZZ_TIME + minimization).
FUZZ_TIMEOUT ?= 5m
# Wall-clock cap for full check (lint + test + test-stress).
CHECK_TIMEOUT ?= 5m

# Without GNU timeout/gtimeout on PATH, CHECK_TIMEOUT is not enforced; TEST_TIMEOUT still applies.
check:
	@T=$$(command -v timeout 2>/dev/null \
		|| command -v gtimeout 2>/dev/null); \
	if [ -n "$$T" ]; then \
		$$T $(CHECK_TIMEOUT) $(MAKE) --no-print-directory check-inner; \
	else \
		$(MAKE) --no-print-directory check-inner; \
	fi

check-inner: lint check-aeron-versions
	@$(MAKE) --no-print-directory -j2 test test-stress

check-aeron-versions:
	@scripts/check-aeron-versions.sh

lint:
	go tool golangci-lint run $(GO_TEST_PKGS)
	go tool golangci-lint run --build-tags=integration,java_interop -D unused ./test/integration/...
	@command -v shellcheck >/dev/null || { \
		echo "heron: shellcheck required (e.g. apt install shellcheck, pacman -S shellcheck)" >&2; \
		exit 1; \
	}
	@awk 'length > 100 { print FILENAME ":" NR; bad = 1 } END { exit bad + 0 }' \
		$(SHELL_SCRIPTS) || { \
		echo "heron: shell scripts must stay within 100 columns" >&2; \
		exit 1; \
	}
	shellcheck $(SHELL_SCRIPTS)

goimports:
	go tool goimports -local $(GO_MODULE) -w $(GO_SOURCES)

gofumpt: goimports
	go tool gofumpt -extra -w $(GO_SOURCES)

# -fuzz=Fuzz requires at most one matching Fuzz* per package (Go errors otherwise).
fuzz:
	@set -euo pipefail; \
	fuzz_pkgs=$$(grep -rl '^func Fuzz' --include='*_fuzz_test.go' . \
		| sed 's|/[^/]*$$||' | sort -u) || [ $$? -eq 1 ]; \
	if [ -z "$$fuzz_pkgs" ]; then \
		echo "no fuzz tests"; exit 1; \
	fi; \
	for pkg in $$fuzz_pkgs; do \
		go test -timeout=$(FUZZ_TIMEOUT) -tags=$(UNIT_TEST_TAGS) -run '^$$' \
			-fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $$pkg; \
	done

# Unit tests need -tags=testhooks (conductor cross-package hooks); use make test.
test:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=$(UNIT_TEST_TAGS) \
		-coverprofile=$(COVERAGE_PROFILE) $(GO_TEST_PKGS)
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1

BENCH_DIR := .bench
BENCH_BASELINE := $(BENCH_DIR)/baseline.txt
BENCH_COUNT ?= 10
export BENCH_PKGS := $(GO_TEST_PKGS)
export BENCH_COUNT BENCH_BASELINE GOARCH

test-rapid:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=rapid,$(UNIT_TEST_TAGS) \
		-count=1 -run rapid $(GO_TEST_PKGS)

test-stress:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=stress,$(UNIT_TEST_TAGS) \
		-count=1 -run Stress $(GO_TEST_PKGS)

bench:
	go test -run '^$$' -tags=$(UNIT_TEST_TAGS) -bench=. -benchmem $(GO_TEST_PKGS)

bench-baseline:
	@mkdir -p $(BENCH_DIR) && set -o pipefail && \
		go test -run '^$$' -tags=$(UNIT_TEST_TAGS) -bench=. -benchmem \
			-count=$(BENCH_COUNT) $(GO_TEST_PKGS) \
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

JAVA_INTEROP_POM := test/integration/java_interop/pom.xml
JAVA_INTEROP_ORACLE_TIMEOUT ?= 30s

test-java-interop:
	./scripts/java-interop-smoke.sh

test-java-interop-unit:
	@command -v mvn >/dev/null || { \
		echo "heron: mvn required (JDK 26 per $(JAVA_INTEROP_POM))" >&2; \
		exit 1; \
	}
	mvn -q -f $(JAVA_INTEROP_POM) test

test-java-interop-oracle:
	go test -timeout=$(JAVA_INTEROP_ORACLE_TIMEOUT) -race \
		-tags=testhooks,integration,java_interop -count=1 \
		./test/integration/java_interop/javainterop/

test-java-interop-contract: test-java-interop-unit test-java-interop-oracle

bin/aeronmd: specs/versions.yaml docker/aeronmd/Dockerfile scripts/build-aeronmd.sh
	./scripts/build-aeronmd.sh

aeronmd: bin/aeronmd

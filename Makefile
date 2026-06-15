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

COVERAGE_DIR := .coverage
COVERAGE_ROOT := $(ROOT)/$(COVERAGE_DIR)
COVERAGE_PROFILE := coverage.out
COVERAGE_MODE := atomic
COVERAGE_SLICES := $(COVERAGE_ROOT)/unit,$(COVERAGE_ROOT)/rapid,$(COVERAGE_ROOT)/stress
COVERAGE_PREPARED := $(COVERAGE_ROOT)/.prepared
# Set by coverage-* targets; names slice dir under COVERAGE_ROOT.
COVERAGE_SLICE ?=
COVERAGE_TEST_ARGS = $(if $(COVERAGE_SLICE),-cover -covermode=$(COVERAGE_MODE) \
	-args -test.gocoverdir=$(COVERAGE_ROOT)/$(COVERAGE_SLICE),)
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
.PHONY: check check-inner check-aeron-versions
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

.PHONY: lint goimports gofumpt
lint:
	go tool golangci-lint run $(GO_TEST_PKGS)
	go tool golangci-lint run --build-tags=integration,java_interop ./test/integration/...
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

.PHONY: fuzz
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

.PHONY: test test-rapid test-stress
# Unit tests need -tags=testhooks,bench; use make test.
test:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=bench,$(UNIT_TEST_TAGS) \
		-count=1 $(GO_TEST_PKGS) $(COVERAGE_TEST_ARGS)

test-rapid:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=rapid,$(UNIT_TEST_TAGS) \
		-count=1 -run rapid $(GO_TEST_PKGS) $(COVERAGE_TEST_ARGS)

test-stress:
	go test -timeout=$(TEST_TIMEOUT) -race -tags=stress,$(UNIT_TEST_TAGS) \
		-count=1 -run Stress $(GO_TEST_PKGS) $(COVERAGE_TEST_ARGS)

BENCH_DIR := .bench
BENCH_BASELINE := $(BENCH_DIR)/baseline.txt
BENCH_COUNT ?= 10
BENCH_PKGS ?= ./test/bench/...
BENCH_TAGS := bench
export BENCH_PKGS
export BENCH_COUNT BENCH_BASELINE GOARCH

.PHONY: bench bench-micro bench-baseline bench-regression
bench:
	go test -tags=$(BENCH_TAGS) -run '^$$' -bench=. -benchmem \
		-benchtime=1s $(BENCH_PKGS)

bench-micro:
	go test -run '^$$' -tags=testhooks -bench=. -benchmem \
		./aeron/ ./aeron/ringbuffer/ ./aeron/broadcast/ ./aeron/conductor/ ./aeron/cnc/

bench-baseline:
	@mkdir -p $(BENCH_DIR) && set -o pipefail && \
		{ printf '%s\n' '# produced by: make bench-baseline (tags=bench)'; \
		go test -run '^$$' -tags=$(BENCH_TAGS) -bench=. -benchmem \
			-count=$(BENCH_COUNT) $(BENCH_PKGS); \
		} | tee $(BENCH_BASELINE)

bench-regression:
	@UNIT_TEST_TAGS=$(BENCH_TAGS) scripts/bench-regression.sh

$(COVERAGE_PREPARED):
	@rm -rf $(COVERAGE_DIR)
	@mkdir -p $(COVERAGE_ROOT)/unit $(COVERAGE_ROOT)/rapid \
		$(COVERAGE_ROOT)/stress $(COVERAGE_ROOT)/merged
	@touch $@

.PHONY: coverage coverage-unit coverage-rapid coverage-stress
coverage-unit: $(COVERAGE_PREPARED)
	$(MAKE) --no-print-directory COVERAGE_SLICE=unit test

coverage-rapid: $(COVERAGE_PREPARED)
	$(MAKE) --no-print-directory COVERAGE_SLICE=rapid test-rapid

coverage-stress: $(COVERAGE_PREPARED)
	$(MAKE) --no-print-directory COVERAGE_SLICE=stress test-stress

$(COVERAGE_PROFILE): coverage-unit coverage-rapid coverage-stress
	go tool covdata merge -i=$(COVERAGE_SLICES) -o=$(COVERAGE_ROOT)/merged
	go tool covdata textfmt -i=$(COVERAGE_ROOT)/merged -o=$(COVERAGE_PROFILE)

coverage.html: $(COVERAGE_PROFILE)
	go tool cover -html=$(COVERAGE_PROFILE) -o coverage.html

coverage: coverage.html
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1

.PHONY: clean
clean:
	rm -rf $(COVERAGE_DIR)
	rm -f $(COVERAGE_PROFILE) coverage.html
	rm -f bin/aeronmd
	find . -type d -path '*/testdata/rapid' -prune -exec rm -rf {} +

.PHONY: test-integration aeronmd
test-integration: bin/aeronmd
	go test -timeout=$(TEST_TIMEOUT) -race -count=1 -tags=integration ./test/integration/...

JAVA_INTEROP_POM := test/integration/java_interop/pom.xml
JAVA_INTEROP_ORACLE_TIMEOUT ?= 30s

.PHONY: test-java-interop test-java-interop-unit test-java-interop-oracle test-java-interop-contract
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

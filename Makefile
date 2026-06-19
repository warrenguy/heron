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
export UNIT_TEST_TAGS := testhooks
# Per-package go test deadline (replaces go default 10m on hung tests).
TEST_TIMEOUT ?= 2m
FUZZ_TIME ?= 30s
# Per-package go test deadline for make fuzz (baseline + FUZZ_TIME + minimization).
FUZZ_TIMEOUT ?= 5m
# Wall-clock cap for full check (check-static + unit + stress).
CHECK_TIMEOUT ?= 5m

# Without GNU timeout/gtimeout on PATH, CHECK_TIMEOUT is not enforced; TEST_TIMEOUT still applies.
.PHONY: check check-inner check-static check-test check-aeron-versions \
	check-sbe-stale check-sbe-stale-optional \
	cluster-sbe-catalog-regen check-catalog-stale check-catalog-stale-optional \
	check-catalog-stale-selftest \
	contract-oracles-optional
check:
	@T=$$(command -v timeout 2>/dev/null \
		|| command -v gtimeout 2>/dev/null); \
	if [ -n "$$T" ]; then \
		$$T --foreground $(CHECK_TIMEOUT) $(MAKE) --no-print-directory check-inner; \
	else \
		$(MAKE) --no-print-directory check-inner; \
	fi

check-static: lint
	+@$(MAKE) --no-print-directory -j4 check-aeron-versions check-sbe-stale-optional \
		check-catalog-stale-optional \
		contract-oracles-optional

check-test:
	+@$(MAKE) --no-print-directory -j2 test test-stress

check-inner: check-static check-test

check-aeron-versions:
	@scripts/check-aeron-versions.sh

# Run $(1) when $(2) succeeds; otherwise print skip message $(3).
define optional-target
$(1)-optional:
	@if $(2); then \
		$(MAKE) --no-print-directory $(1); \
	else \
		echo "heron: skipping $(1) ($(3))"; \
	fi
endef

# Cluster SBE codegen: catalog.json stale check in check-static.
CLUSTER_SBE_CATALOG_JSON := test/integration/cluster_sbe/clustersbe/fixtures/catalog.json
CLUSTER_SBE_CATALOG_REGEN := go run ./tools/cluster-sbe-catalog-gen \
	-catalog $(CLUSTER_SBE_CATALOG_JSON) -out $(CLUSTER_SBE_CATALOG_JSON)

cluster-sbe-catalog-regen:
	@$(CLUSTER_SBE_CATALOG_REGEN)

check-catalog-stale:
	@set -euo pipefail; tmp=$$(mktemp); cp $(CLUSTER_SBE_CATALOG_JSON) "$$tmp"; \
	trap 'cp "$$tmp" $(CLUSTER_SBE_CATALOG_JSON); rm -f "$$tmp"' EXIT; \
	$(CLUSTER_SBE_CATALOG_REGEN); diff -u "$$tmp" $(CLUSTER_SBE_CATALOG_JSON)

$(eval $(call optional-target,check-catalog-stale,command -v go >/dev/null,go not installed))

check-catalog-stale-selftest:
	@CLUSTER_SBE_CATALOG_JSON=$(CLUSTER_SBE_CATALOG_JSON) ./scripts/check-catalog-stale-selftest.sh

check-sbe-stale: sbe-generate
	@git diff --exit-code aeron/sbe/generated/

$(eval $(call optional-target,check-sbe-stale,command -v java >/dev/null,java not installed))

INTEGRATION_LINT_TAGS := integration,java_interop,cluster_sbe

.PHONY: lint goimports gofumpt
lint:
	go tool golangci-lint run $(GO_TEST_PKGS)
	go tool golangci-lint run --build-tags=$(INTEGRATION_LINT_TAGS) \
		./test/integration/...
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
	fuzz_pkgs=$$(grep -rl '^func Fuzz' --include='*_fuzz_test.go' . 2>/dev/null \
		| sed 's|/[^/]*$$||' | sort -u || true); \
	[ -n "$$fuzz_pkgs" ] || { echo "no fuzz tests"; exit 1; }; \
	for pkg in $$fuzz_pkgs; do \
		go test -timeout=$(FUZZ_TIMEOUT) -tags=$(UNIT_TEST_TAGS) -run '^$$' \
			-fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $$pkg; \
	done

# Unit tests need -tags=testhooks,bench; use make test.
define go-test-target
.PHONY: test$(1)
test$(1):
	go test -timeout=$(TEST_TIMEOUT) -race -tags=$(2),$(UNIT_TEST_TAGS) \
		-count=1 $(3) $(GO_TEST_PKGS) $(COVERAGE_TEST_ARGS)
endef

$(eval $(call go-test-target,,bench,))
$(eval $(call go-test-target,-rapid,rapid,-run rapid))
$(eval $(call go-test-target,-stress,stress,-run Stress))

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
	go test -run '^$$' -tags=testhooks -bench=. -benchmem ./aeron/...

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

define coverage-slice-target
.PHONY: coverage-$(1)
coverage-$(1): $(COVERAGE_PREPARED)
	$(MAKE) --no-print-directory COVERAGE_SLICE=$(1) test$(2)
endef

$(eval $(call coverage-slice-target,unit,))
$(eval $(call coverage-slice-target,rapid,-rapid))
$(eval $(call coverage-slice-target,stress,-stress))

.PHONY: coverage

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
CLUSTER_SBE_POM := test/integration/cluster_sbe/pom.xml
JAVA_INTEROP_ORACLE_TIMEOUT ?= 30s
CLUSTER_SBE_ORACLE_TIMEOUT ?= $(JAVA_INTEROP_ORACLE_TIMEOUT)
JAVA_INTEROP_ORACLE_TAGS := testhooks,integration,java_interop
CLUSTER_SBE_ORACLE_TAGS := integration,cluster_sbe

# Run mvn test for an integration POM; $(1)=logical name, $(2)=pom path.
define mvn-integration-unit
test-$(1)-unit:
	@command -v mvn >/dev/null || { \
		echo "heron: mvn required (JDK 26 per $(2))" >&2; \
		exit 1; \
	}
	mvn -q -f $(2) test
endef

# Run go oracle test; $(1)=suite, $(2)=timeout, $(3)=tags var (by name; value has commas), $(4)=package.
define integration-oracle
test-$(1)-oracle:
	go test -timeout=$(2) -race \
		-tags=$($(3)) -count=1 \
		$(4)
endef

# Aggregate unit + oracle; $(1)=suite.
define integration-contract
test-$(1)-contract:
	@$(MAKE) --no-print-directory -j2 test-$(1)-unit test-$(1)-oracle
endef

.PHONY: test-java-interop test-java-interop-unit test-java-interop-oracle test-java-interop-contract \
	test-cluster-sbe-unit test-cluster-sbe-oracle test-cluster-sbe-contract \
	contract-oracles contract-oracles-optional
test-java-interop:
	./scripts/java-interop-smoke.sh

$(eval $(call mvn-integration-unit,java-interop,$(JAVA_INTEROP_POM)))
$(eval $(call integration-oracle,java-interop,$(JAVA_INTEROP_ORACLE_TIMEOUT),JAVA_INTEROP_ORACLE_TAGS,./test/integration/java_interop/javainterop/))
$(eval $(call integration-contract,java-interop))

$(eval $(call mvn-integration-unit,cluster-sbe,$(CLUSTER_SBE_POM)))
$(eval $(call integration-oracle,cluster-sbe,$(CLUSTER_SBE_ORACLE_TIMEOUT),CLUSTER_SBE_ORACLE_TAGS,./test/integration/cluster_sbe/clustersbe/))
$(eval $(call integration-contract,cluster-sbe))

contract-oracles:
	@$(MAKE) --no-print-directory -j2 test-java-interop-contract test-cluster-sbe-contract

$(eval $(call optional-target,contract-oracles,command -v mvn >/dev/null && command -v java >/dev/null,mvn/java not installed))

bin/aeronmd: specs/versions.yaml docker/aeronmd/Dockerfile scripts/build-aeronmd.sh
	./scripts/build-aeronmd.sh

aeronmd: bin/aeronmd

# Java SBE codegen.
.PHONY: sbe-generate
sbe-generate:
	./scripts/generate-sbe.sh

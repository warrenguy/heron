ROOT := $(CURDIR)
export PATH := $(ROOT)/bin:$(PATH)
SHELL := /bin/bash

# Submake -j: use jobserver from +$(MAKE) when present; else fixed local -jN.
SUBMAKE_J2 = $(if $(filter --jobserver%,$(MAKEFLAGS)),,-j2)
SUBMAKE_J3 = $(if $(filter --jobserver%,$(MAKEFLAGS)),,-j3)

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
# Conductor cross-package test hooks (test_hooks.go, hook_access.go).
export UNIT_TEST_TAGS := testhooks
# Stress pass: packages that gain test files when the stress tag is enabled.
STRESS_TEST_PKGS := $(shell paste \
	<(go list -tags=$(UNIT_TEST_TAGS) -test \
		-f '{{if not .ForTest}}{{.ImportPath}} {{len .TestGoFiles}} {{len .XTestGoFiles}}{{end}}' ./...) \
	<(go list -tags=stress,$(UNIT_TEST_TAGS) -test \
		-f '{{if not .ForTest}}{{.ImportPath}} {{len .TestGoFiles}} {{len .XTestGoFiles}}{{end}}' ./...) \
	| awk 'NF && $$2+$$3 < $$5+$$6 {print $$1}')
# Unit pass uses bench tag; omit test/bench root (benchmark-only; see make bench).
UNIT_TEST_PKGS := $(shell go list -tags=bench,$(UNIT_TEST_TAGS) -f \
	'{{if and (or .TestGoFiles .XTestGoFiles) (ne .ImportPath "$(GO_MODULE)/test/bench")}}{{\
.ImportPath}}{{end}}' ./...)
# Rapid pass uses rapid tag only; bench-tagged packages (e.g. test/bench/harness) are omitted.
RAPID_TEST_PKGS := $(shell go list -tags=rapid,$(UNIT_TEST_TAGS) -f \
	'{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)
GO_SOURCES := $$(grep -rl --include='*.go' -L 'DO NOT EDIT' \
	--exclude-dir=docker --exclude-dir=vendor .)
SHELL_SCRIPTS := $$(find . -name '*.sh' -type f ! -path './.git/*' ! -path './vendor/*')
# Per-package go test deadline (replaces go default 10m on hung tests).
TEST_TIMEOUT ?= 2m
# Per-package fuzz budget; 45s avoids deadline flake on heavy packages at 30s.
FUZZ_TIME ?= 45s
# Per-package go test deadline for make fuzz (baseline + FUZZ_TIME + minimization).
FUZZ_TIMEOUT ?= 5m
# Wall-clock cap for full check (check-static + unit + stress).
CHECK_TIMEOUT ?= 5m

.PHONY: help
help: ## List Makefile targets
	@echo "Targets:"
	@{ grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-28s %s\n", $$1, $$2}'; \
		printf '  %-28s %s\n' test 'Unit tests'; \
		printf '  %-28s %s\n' test-rapid 'Property tests'; \
		printf '  %-28s %s\n' test-stress 'Stress tests'; \
	} | sort
	@echo ""
	@echo "Contract oracles per suite: test-<name>-{unit,oracle,contract}"
	@echo "  names: java-interop cluster-sbe cluster-java"

# Without GNU timeout/gtimeout on PATH, CHECK_TIMEOUT is not enforced; TEST_TIMEOUT still applies.
.PHONY: check check-inner check-static check-static-extra check-test check-aeron-versions \
	check-sbe-stale check-sbe-stale-optional \
	check-sbe-stale-archive check-sbe-stale-archive-optional \
	check-sbe-stale-nodestate check-sbe-stale-nodestate-optional \
	cluster-sbe-catalog-regen check-catalog-stale check-catalog-stale-optional \
	check-catalog-stale-selftest recordinglog-fixtures-regen \
	recordinglog-fixtures-regen-optional \
	contract-oracles-optional
check: ## Lint pins unit stress examples (contract-oracles opt)
	@T=$$(command -v timeout 2>/dev/null \
		|| command -v gtimeout 2>/dev/null); \
	if [ -n "$$T" ]; then \
		$$T --foreground $(CHECK_TIMEOUT) $(MAKE) --no-print-directory check-inner; \
	else \
		$(MAKE) --no-print-directory check-inner; \
	fi

check-static: lint check-static-extra

check-static-extra:
	+@$(MAKE) --no-print-directory $(SUBMAKE_J2) \
		check-sbe-stale-optional \
		check-sbe-stale-archive-optional \
		check-sbe-stale-nodestate-optional
	+@$(MAKE) --no-print-directory $(SUBMAKE_J3) check-aeron-versions \
		check-catalog-stale-optional contract-oracles-optional

check-test:
	+@$(MAKE) --no-print-directory $(SUBMAKE_J2) test test-stress build-examples

check-inner: check-static check-test

.PHONY: test-all
test-all: ## Full gate: check rapid integration Docker smokes
	+@$(MAKE) --no-print-directory check test-rapid test-integration aeronmd-compose-image \
		test-java-interop test-cluster-java test-cluster-java-failover test-archive-java

check-aeron-versions:
	@scripts/check-aeron-versions.sh

# Run $(1) when $(2) succeeds; otherwise print skip message $(3).
# Leading + forwards the jobserver to the recursive $(MAKE) under a parallel build.
define optional-target
$(1)-optional:
	+@if $(2); then \
		$(MAKE) --no-print-directory $(1); \
	else \
		echo "heron: skipping $(1) ($(3))"; \
	fi
endef

# $(1)=target suffix (empty, -archive, or -nodestate), $(2)=help label, $(3)=generate-sbe.sh arg
define sbe-generate-target
.PHONY: sbe-generate$(1)
sbe-generate$(1): ## Regenerate $(2) SBE flyweights
	./scripts/generate-sbe.sh $(3)
endef

# $(1)=target suffix (empty, -archive, or -nodestate), $(2)=parent package dir, $(3)=generated subdir
define sbe-stale-check
$(if $(3),,$(error sbe-stale-check: missing generated subdir arg for $(2)))
check-sbe-stale$(1): sbe-generate$(1)
	@git diff --exit-code $(2)/$(3)/
$(eval $(call optional-target,check-sbe-stale$(1),command -v java >/dev/null,java not installed))
endef

# Cluster SBE codegen: catalog.json stale check in check-static.
CLUSTER_SBE_CATALOG_JSON := test/integration/cluster_sbe/clustersbe/fixtures/catalog.json
CLUSTER_SBE_CATALOG_GEN := bin/cluster-sbe-catalog-gen
CLUSTER_SBE_CATALOG_REGEN := $(CLUSTER_SBE_CATALOG_GEN) \
	-catalog $(CLUSTER_SBE_CATALOG_JSON) -out $(CLUSTER_SBE_CATALOG_JSON)

bin/cluster-sbe-catalog-gen: go.mod go.sum $(wildcard tools/cluster-sbe-catalog-gen/*.go)
	go build -o $@ ./tools/cluster-sbe-catalog-gen

cluster-sbe-catalog-regen: bin/cluster-sbe-catalog-gen
	@$(CLUSTER_SBE_CATALOG_REGEN)

RECORDINGLOG_FIXTURES_POM := test/integration/recordinglog_fixtures/pom.xml

recordinglog-fixtures-regen: ## Regenerate cluster/recordinglog testdata from Java
	@command -v mvn >/dev/null || { \
		echo "heron: mvn required (JDK 26 per $(RECORDINGLOG_FIXTURES_POM))" >&2; \
		exit 1; \
	}
	MAVEN_OPTS='--add-opens java.base/jdk.internal.misc=ALL-UNNAMED' \
		mvn -q -f $(RECORDINGLOG_FIXTURES_POM) compile exec:java

$(eval $(call optional-target,recordinglog-fixtures-regen,command -v mvn >/dev/null && command -v java >/dev/null,mvn/java not installed))

check-catalog-stale: bin/cluster-sbe-catalog-gen
	@set -euo pipefail; tmp=$$(mktemp); \
	trap 'rm -f "$$tmp"' EXIT; \
	$(CLUSTER_SBE_CATALOG_GEN) -catalog $(CLUSTER_SBE_CATALOG_JSON) -out "$$tmp"; \
	diff -u $(CLUSTER_SBE_CATALOG_JSON) "$$tmp"

$(eval $(call optional-target,check-catalog-stale,command -v go >/dev/null,go not installed))

check-catalog-stale-selftest:
	@CLUSTER_SBE_CATALOG_JSON=$(CLUSTER_SBE_CATALOG_JSON) ./scripts/check-catalog-stale-selftest.sh

$(eval $(call sbe-stale-check,,cluster/codecs,clusterwire))
$(eval $(call sbe-stale-check,-archive,archive/codecs,archivewire))
$(eval $(call sbe-stale-check,-nodestate,cluster/election,nodewire))

INTEGRATION_LINT_TAGS := integration,java_interop,cluster_sbe,cluster_java,archive_java

.PHONY: lint lint-golangci lint-shell goimports gofumpt
lint: ## golangci-lint and shellcheck
	+@$(MAKE) --no-print-directory $(SUBMAKE_J2) lint-golangci lint-shell

lint-golangci:
	go tool golangci-lint run $(GO_TEST_PKGS)
	go tool golangci-lint run --build-tags=$(INTEGRATION_LINT_TAGS) \
		./test/integration/...

lint-shell:
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

goimports: ## goimports on non-generated Go sources
	go tool goimports -local $(GO_MODULE) -w $(GO_SOURCES)

gofumpt: goimports ## gofumpt after goimports
	go tool gofumpt -extra -w $(GO_SOURCES)

.PHONY: fuzz
# -fuzz=Fuzz requires at most one matching Fuzz* per package (Go errors otherwise).
fuzz: ## Run all Fuzz* tests
	@set -euo pipefail; \
	fuzz_pkgs=$$(grep -rl '^func Fuzz' --include='*_fuzz_test.go' \
		--exclude-dir=vendor . 2>/dev/null \
		| sed 's|/[^/]*$$||' | sort -u || true); \
	[ -n "$$fuzz_pkgs" ] || { echo "no fuzz tests"; exit 1; }; \
	for pkg in $$fuzz_pkgs; do \
		go test -timeout=$(FUZZ_TIMEOUT) -tags=$(UNIT_TEST_TAGS) -run '^$$' \
			-fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $$pkg; \
	done

# Unit tests need -tags=testhooks,bench; use make test.
define go-test-target
.PHONY: test$(1)
test$(1): ## $(4)
	go test -timeout=$(TEST_TIMEOUT) -race -tags=$(2),$(UNIT_TEST_TAGS) \
		-count=1 $(3) $(if $(5),$(5),$(GO_TEST_PKGS)) $(COVERAGE_TEST_ARGS)
endef

$(eval $(call go-test-target,,bench,,Unit tests,$(UNIT_TEST_PKGS)))
$(eval $(call go-test-target,-rapid,rapid,-run rapid,Property tests,$(RAPID_TEST_PKGS)))
$(eval $(call go-test-target,-stress,stress,-run Stress,Stress tests,$(STRESS_TEST_PKGS)))

BENCH_DIR := .bench
BENCH_BASELINE := $(BENCH_DIR)/baseline.txt
BENCH_COUNT ?= 10
BENCH_PKGS ?= ./test/bench/...
BENCH_MICRO_PKGS ?= ./atomicbuffer/... ./mmap/... ./record/... ./ringbuffer/... \
	./idlestrategy/... ./broadcast/... ./aeron/...
BENCH_TAGS := bench
export BENCH_PKGS
export BENCH_COUNT BENCH_BASELINE GOARCH

.PHONY: bench bench-micro bench-baseline bench-regression
bench: ## Quick benchmarks (test/bench)
	go test -tags=$(BENCH_TAGS) -run '^$$' -bench=. -benchmem \
		-benchtime=1s $(BENCH_PKGS)

bench-micro: ## Micro-benchmarks (hoisted layout primitives + aeron)
	go test -run '^$$' -tags=testhooks -bench=. -benchmem \
		$(BENCH_MICRO_PKGS)

bench-baseline: ## Record benchmark baseline to .bench/
	@mkdir -p $(BENCH_DIR) && set -o pipefail && \
		{ printf '%s\n' '# produced by: make bench-baseline (tags=bench)'; \
		go test -run '^$$' -tags=$(BENCH_TAGS) -bench=. -benchmem \
			-count=$(BENCH_COUNT) $(BENCH_PKGS); \
		} | tee $(BENCH_BASELINE)

bench-regression: ## Compare benchmarks to baseline
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

coverage: coverage.html ## Coverage HTML and summary
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1

.PHONY: clean
clean: ## Remove coverage bench artifacts and bin build outputs
	rm -rf $(COVERAGE_DIR)
	rm -f $(COVERAGE_PROFILE) coverage.html
	rm -rf bin
	find . -type d -path '*/testdata/rapid' -prune -exec rm -rf {} +

.PHONY: test-integration aeronmd
test-integration: bin/aeronmd ## Live-driver integration tests
	go test -timeout=$(TEST_TIMEOUT) -race -count=1 -tags=integration ./test/integration/...

JAVA_INTEROP_POM := test/integration/java_interop/pom.xml
CLUSTER_SBE_POM := test/integration/cluster_sbe/pom.xml
CLUSTER_JAVA_POM := test/integration/cluster_java/pom.xml
ARCHIVE_JAVA_POM := test/integration/archive_java/pom.xml
CONTRACT_ORACLE_TIMEOUT ?= 30s
JAVA_INTEROP_ORACLE_TIMEOUT ?= $(CONTRACT_ORACLE_TIMEOUT)
CLUSTER_SBE_ORACLE_TIMEOUT ?= $(CONTRACT_ORACLE_TIMEOUT)
CLUSTER_JAVA_ORACLE_TIMEOUT ?= $(CONTRACT_ORACLE_TIMEOUT)
ARCHIVE_JAVA_ORACLE_TIMEOUT ?= $(CONTRACT_ORACLE_TIMEOUT)
JAVA_INTEROP_ORACLE_TAGS := testhooks,integration,java_interop
CLUSTER_SBE_ORACLE_TAGS := integration,cluster_sbe
CLUSTER_JAVA_ORACLE_TAGS := integration,cluster_java
ARCHIVE_JAVA_ORACLE_TAGS := integration,archive_java
CONTRACT_ORACLE_SUITES := java-interop cluster-sbe cluster-java archive-java

# Run mvn test for an integration POM; $(1)=logical name, $(2)=pom path.
define mvn-integration-unit
test-$(1)-unit:
	@command -v mvn >/dev/null || { \
		echo "heron: mvn required (JDK 26 per $(2))" >&2; \
		exit 1; \
	}; \
	mvn_ver=`mvn -v 2>/dev/null | awk '/^Apache Maven / {print $$$$3; exit}'`; \
	vsort=$$$$(command -v gsort 2>/dev/null || command -v sort 2>/dev/null); \
	if [ -z "$$$$mvn_ver" ] || [ -z "$$$$vsort" ] || ! printf '%s\n%s\n' "3.8.2" "$$$$mvn_ver" | $$$$vsort -V -C 2>/dev/null; then \
		echo "heron: Maven 3.8.2+ required for parallel contract-oracles" >&2; \
		exit 1; \
	fi
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
	+@$(MAKE) --no-print-directory $(SUBMAKE_J2) test-$(1)-unit test-$(1)-oracle
endef

# $(1)=logical name, $(2)=pom, $(3)=timeout var name, $(4)=tags var name, $(5)=go package
define contract-oracle-module
$(eval $(call mvn-integration-unit,$(1),$(2)))
$(eval $(call integration-oracle,$(1),$($(3)),$(4),$(5)))
$(eval $(call integration-contract,$(1)))
endef

.PHONY: test-java-interop test-cluster-java test-cluster-java-failover test-archive-java \
	$(foreach s,$(CONTRACT_ORACLE_SUITES),test-$(s)-unit test-$(s)-oracle test-$(s)-contract) \
	contract-oracles contract-oracles-optional
test-java-interop: aeronmd-compose-image ## Docker smoke: Java/Go IPC duplex
	./scripts/java-interop-smoke.sh

test-archive-java: aeronmd-compose-image ## Docker smoke: record, recording.log, PlanRecovery, replay (TestArchiveRecordingLog)
	./scripts/archive-java-smoke.sh

test-cluster-java: ## Docker smoke: AsyncConnect + echo roundtrip smoke
	./scripts/cluster-java-smoke.sh

test-cluster-java-failover: aeronmd-compose-image ## Docker smoke: 3-node leader failover
	./scripts/cluster-java-failover-smoke.sh

$(eval $(call contract-oracle-module,java-interop,$(JAVA_INTEROP_POM),JAVA_INTEROP_ORACLE_TIMEOUT,JAVA_INTEROP_ORACLE_TAGS,./test/integration/java_interop/javainterop/))
$(eval $(call contract-oracle-module,cluster-sbe,$(CLUSTER_SBE_POM),CLUSTER_SBE_ORACLE_TIMEOUT,CLUSTER_SBE_ORACLE_TAGS,./test/integration/cluster_sbe/clustersbe/))
$(eval $(call contract-oracle-module,cluster-java,$(CLUSTER_JAVA_POM),CLUSTER_JAVA_ORACLE_TIMEOUT,CLUSTER_JAVA_ORACLE_TAGS,./test/integration/cluster_java/clusterjava/))
$(eval $(call contract-oracle-module,archive-java,$(ARCHIVE_JAVA_POM),ARCHIVE_JAVA_ORACLE_TIMEOUT,ARCHIVE_JAVA_ORACLE_TAGS,./test/integration/archive_java/archivejava/))

contract-oracles: ## All four contract oracle suites (mvn + go)
	+@$(MAKE) --no-print-directory $(SUBMAKE_J3) \
		$(foreach s,$(CONTRACT_ORACLE_SUITES),test-$(s)-contract)

$(eval $(call optional-target,contract-oracles,command -v mvn >/dev/null && command -v java >/dev/null,mvn/java not installed))

bin/aeronmd: specs/versions.yaml docker/aeronmd/Dockerfile scripts/build-aeronmd.sh
	./scripts/build-aeronmd.sh

.PHONY: aeronmd-compose-image
aeronmd-compose-image: bin/aeronmd docker/aeronmd/Dockerfile ## Package bin/aeronmd for compose smokes
	docker build -f docker/aeronmd/Dockerfile --target runtime-bin -t java-interop-aeronmd .

aeronmd: bin/aeronmd ## Build C media driver binary

EXAMPLE_BINS := $(patsubst examples/%/main.go,%,$(wildcard examples/*/main.go))

define build-example-bin
bin/$(1): go.mod go.sum $(wildcard examples/$(1)/*.go) $(wildcard examples/$(1)/*/*.go)
	go build -tags=example -o bin/$(1) ./examples/$(1)
endef

$(foreach e,$(EXAMPLE_BINS),$(eval $(call build-example-bin,$(e))))

.PHONY: build-examples
build-examples: $(addprefix bin/,$(EXAMPLE_BINS)) ## Compile example binaries (requires example build tag)

# Java SBE codegen.
$(eval $(call sbe-generate-target,,cluster,))
$(eval $(call sbe-generate-target,-archive,archive,archive))
$(eval $(call sbe-generate-target,-nodestate,nodestate,nodestate))

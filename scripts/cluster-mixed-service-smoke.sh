#!/usr/bin/env bash
# Mixed Java consensus module + Go service container smoke.
# Compose up -d (cluster + go), then wait for go exit 0.
# Cluster runs embedded ClusteredMediaDriver with the Java service
# container withheld. Go runs TestClusterMixedServiceSmoke.
#
# Per-run compose project (-p heron-cluster-mixed-service-$$) so concurrent
# smokes do not share stacks. force-recreate avoids reusing a leftover
# one-shot go under the same project if a prior trap failed. EXIT/INT/TERM
# run compose down for this project only.
#
# Outer timeout 190s: 180s test budget plus compose teardown.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_BASE="${ROOT}/test/integration/cluster_java/docker/compose.yaml"
COMPOSE_OVERLAY="${ROOT}/test/integration/cluster_java/docker/compose.mixed-service.yaml"
COMPOSE_PROJECT="heron-cluster-mixed-service-$$"
# SMOKE_NAME must match the compose.mixed-service.yaml go command -test.run.
SMOKE_NAME="TestClusterMixedServiceSmoke"
COMPOSE_CMD=(
	docker compose
	-p "${COMPOSE_PROJECT}"
	-f "${COMPOSE_BASE}"
	-f "${COMPOSE_OVERLAY}"
)

compose() {
	"${COMPOSE_CMD[@]}" "$@"
}

compose_cleanup() {
	compose down -v >/dev/null 2>&1 || true
}

if ! command -v docker >/dev/null 2>&1; then
	echo "heron: docker required" >&2
	exit 1
fi

GO_VERSION="$(awk '/^go / {print $2; exit}' "${ROOT}/go.mod")"
export DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 GO_VERSION
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID

trap compose_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# up -d + wait go only: cluster is a long-running daemon (ShutdownSignalBarrier);
# compose wait cluster would block forever. Go one-shot test exits when
# TestClusterMixedServiceSmoke passes.
compose build cluster go
compose up -d --force-recreate --no-build

smoke_logs() {
	compose logs --no-color cluster go
}

# Outer timeout 190s: 180s test budget plus compose teardown. Build/up not wrapped.

if command -v timeout >/dev/null 2>&1; then
	timeout 190 "${COMPOSE_CMD[@]}" wait go
else
	compose wait go
fi || {
	status=$?
	echo "heron: cluster mixed service smoke failed" >&2
	smoke_logs >&2 || true
	exit "${status}"
}

# One log fetch feeds require_smoke_ran and the final dump.
SMOKE_LOGS="$(smoke_logs)" || {
	echo "heron: could not read compose logs for smoke check" >&2
	exit 1
}

# Fail closed when compose exits zero without --- PASS for SMOKE_NAME. Compose
# wait alone accepts a skip or an empty -test.run match. Only the Go test
# binary emits go test markers, so the all-service log capture cannot match a
# foreign PASS.
require_smoke_ran() {
	if ! grep -qF -- "--- PASS: ${SMOKE_NAME}" <<<"${SMOKE_LOGS}"; then
		echo "heron: mixed service smoke missing --- PASS: ${SMOKE_NAME}" >&2
		return 1
	fi
}

require_smoke_ran || {
	echo "${SMOKE_LOGS}" >&2
	exit 1
}

echo "heron: cluster mixed service smoke passed"
echo "${SMOKE_LOGS}"

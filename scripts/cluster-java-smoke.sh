#!/usr/bin/env bash
# Cluster Java/Go IPC smoke: compose up -d (cluster + go), then wait for go exit 0.
# Go runs TestClusterConnectSmoke (AsyncConnect IPC handshake) and TestClusterEchoSmoke
# (echo offer/egress round-trip plus admin SNAPSHOT on the same session); cluster runs
# embedded ClusteredMediaDriver.
#
# Go test invocation: docker/cluster_java/go/Dockerfile CMD (-test.run
# TestCluster(Connect|Echo)Smoke, -test.timeout=300s).
#
# Outer timeout 310s: 300s test budget plus compose teardown (see wait block below).
#
# No versions.sh: no aeronmd; Aeron pinned in Dockerfiles/POM at build, not runtime clone.
# See scripts/java-interop-smoke.sh for sibling compose smoke pattern.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="${ROOT}/docker/cluster_java/compose.yaml"
GO_MOD="${ROOT}/go.mod"
COMPOSE_CMD=(
	docker compose
	-f "${COMPOSE}"
)

compose() {
	"${COMPOSE_CMD[@]}" "$@"
}

if ! command -v docker >/dev/null 2>&1; then
	echo "heron: docker required" >&2
	exit 1
fi

if [[ ! -f "${GO_MOD}" ]]; then
	echo "heron: ${GO_MOD} not found" >&2
	exit 1
fi

GO_VERSION="$(awk '/^go / {print $2; exit}' "${GO_MOD}")"
if [[ -z "${GO_VERSION}" ]]; then
	echo "heron: go version required in ${GO_MOD}" >&2
	exit 1
fi

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID
export GO_VERSION

trap 'compose down -v' EXIT

# up -d + wait go only: cluster is a long-running daemon (ShutdownSignalBarrier);
# compose wait cluster would block forever. Go one-shot test exits when both
# TestClusterConnectSmoke and TestClusterEchoSmoke pass.
# HERON_COMPOSE_SKIP_BUILD=1 reuses a prebuilt image; default rebuilds in one up pass.
if [[ "${HERON_COMPOSE_SKIP_BUILD:-}" == "1" ]]; then
	compose up -d --no-build
else
	compose up -d --build
fi

smoke_logs() {
	compose logs --no-color cluster go
}

# Outer timeout 310s: 300s test budget plus compose teardown. Build/up not wrapped.

if command -v timeout >/dev/null 2>&1; then
	timeout 310 "${COMPOSE_CMD[@]}" wait go
else
	compose wait go
fi || {
	status=$?
	echo "heron: cluster java smoke failed" >&2
	smoke_logs >&2
	exit "${status}"
}

echo "heron: cluster java smoke passed"
smoke_logs

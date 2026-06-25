#!/usr/bin/env bash
# Cluster Java/Go IPC smoke: compose up -d (cluster + go), then wait for go exit 0.
# Go runs TestClusterConnectSmoke (AsyncConnect IPC handshake) and TestClusterEchoSmoke
# (offer/egress round-trip); cluster runs embedded ClusteredMediaDriver.
# Go test invocation: docker/cluster_java/go/Dockerfile CMD (-test.run
# TestCluster(Connect|Echo)Smoke, -test.timeout=200s). Outer timeout 210 below.
# No versions.sh: no aeronmd; Aeron pinned in Dockerfiles/POM at build, not runtime clone.
# See scripts/java-interop-smoke.sh for sibling compose smoke pattern.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="${ROOT}/docker/cluster_java/compose.yaml"
GO_MOD="${ROOT}/go.mod"

compose() {
	docker compose -f "${COMPOSE}" "$@"
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

# Health/build during compose up -d above is outside the outer timeout wrapper.
# Outer wall clock 210s: go-container runtime only; inner worst-case 160s (60+60+10+30)
# + 40s slack = 200s (-test.timeout) per docker/cluster_java/go/Dockerfile CMD;
# outer 210s = 200s + ~10s compose wait/teardown.
if command -v timeout >/dev/null 2>&1; then
	timeout 210 docker compose -f "${COMPOSE}" wait go
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

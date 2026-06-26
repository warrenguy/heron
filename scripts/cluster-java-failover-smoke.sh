#!/usr/bin/env bash
# Failover compose smoke: 3 cluster nodes + aeronmd + go; wait for go exit 0.
# Go runs TestClusterFailoverSmoke (leader kill). Needs docker.sock; DOCKER_GID from
# getent group docker when present (override if socket GID differs).
# Outer timeout 430s; see docker/cluster_java/go/Dockerfile failover-runtime CMD.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_BASE="${ROOT}/docker/cluster_java/compose.yaml"
COMPOSE_FAILOVER="${ROOT}/docker/cluster_java/compose.failover.yaml"
GO_MOD="${ROOT}/go.mod"
COMPOSE_CMD=(
	docker compose
	-f "${COMPOSE_BASE}"
	-f "${COMPOSE_FAILOVER}"
	--profile failover
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

if [[ -S /var/run/docker.sock ]]; then
	DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
	export DOCKER_GID
elif command -v getent >/dev/null 2>&1 && getent group docker >/dev/null 2>&1; then
	DOCKER_GID="$(getent group docker | cut -d: -f3)"
	export DOCKER_GID
fi

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID
export GO_VERSION

trap 'compose down -v || true' EXIT

# wait go only: cluster daemons never exit. HERON_COMPOSE_SKIP_BUILD=1 skips rebuild.
if [[ "${HERON_COMPOSE_SKIP_BUILD:-}" == "1" ]]; then
	compose up -d --no-build
else
	compose up -d --build
fi

smoke_logs() {
	compose logs --no-color cluster-0 cluster-1 cluster-2 aeronmd go
}

# Outer timeout 430s (400s -test.timeout + slack). Build/up not wrapped.
if command -v timeout >/dev/null 2>&1; then
	timeout 430 "${COMPOSE_CMD[@]}" wait go
else
	compose wait go
fi || {
	status=$?
	echo "heron: cluster java failover smoke failed" >&2
	smoke_logs >&2 || true
	exit "${status}"
}

echo "heron: cluster java failover smoke passed"
smoke_logs || echo "heron: warning: could not fetch logs (smoke passed)" >&2

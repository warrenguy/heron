#!/usr/bin/env bash
# Failover compose smoke: 3 cluster nodes + aeronmd + go; wait for go exit 0.
# Requires java-interop-aeronmd image (make test-cluster-java-failover runs
# aeronmd-compose-image). Go runs TestClusterFailoverSmoke (leader kill).
# Needs docker.sock; DOCKER_GID from getent group docker when present (override if
# socket GID differs).
#
# Per-run compose project (HERON_COMPOSE_PROJECT=heron-cluster-java-failover-$$
# + -p). Go leader-kill reads COMPOSE_PROJECT_NAME from the same value
# (compose.failover.yaml). force-recreate + EXIT/INT/TERM traps clean this
# project only so concurrent smokes stay isolated.
#
# Outer timeout 430s: 400s test budget plus compose teardown; see Dockerfile
# failover-runtime CMD and wait block below.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_BASE="${ROOT}/docker/cluster_java/compose.yaml"
COMPOSE_FAILOVER="${ROOT}/docker/cluster_java/compose.failover.yaml"
GO_MOD="${ROOT}/go.mod"
# Unique project per process; go kill path must match -p and compose name.
export HERON_COMPOSE_PROJECT="heron-cluster-java-failover-$$"
COMPOSE_CMD=(
	docker compose
	-p "${HERON_COMPOSE_PROJECT}"
	-f "${COMPOSE_BASE}"
	-f "${COMPOSE_FAILOVER}"
	--profile failover
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

if [[ ! -f "${GO_MOD}" ]]; then
	echo "heron: ${GO_MOD} not found" >&2
	exit 1
fi

GO_VERSION="$(awk '/^go / {print $2; exit}' "${GO_MOD}")"
if [[ -z "${GO_VERSION}" ]]; then
	echo "heron: go version required in ${GO_MOD}" >&2
	exit 1
fi

if [[ -z "${DOCKER_GID:-}" ]]; then
	if command -v getent >/dev/null 2>&1 && getent group docker >/dev/null 2>&1; then
		DOCKER_GID="$(getent group docker | cut -d: -f3)"
	elif [[ -S /var/run/docker.sock ]]; then
		DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
	fi
	export DOCKER_GID
fi

export DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 GO_VERSION
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID

if ! docker image inspect java-interop-aeronmd >/dev/null 2>&1; then
	echo "heron: missing image java-interop-aeronmd; run: make aeronmd-compose-image" >&2
	exit 1
fi

trap compose_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# wait go only: cluster daemons never exit. Build cluster-0 and go only;
# cluster-1/2 reuse cluster-0 image. aeronmd uses pre-tagged java-interop-aeronmd.
compose build cluster-0 go
compose up -d --force-recreate --no-build

smoke_logs() {
	compose logs --no-color cluster-0 cluster-1 cluster-2 aeronmd go
}

# Outer timeout 430s: 400s test budget plus compose teardown. Build/up not wrapped.

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

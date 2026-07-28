#!/usr/bin/env bash
# Mixed Go/Java cluster compose smoke. Exactly one overlay per run.
# Requires the java-interop-aeronmd image. make test-cluster-mixed runs
# aeronmd-compose-image first.
#
# Per-run compose project: HERON_COMPOSE_PROJECT=heron-cluster-mixed-$$ with -p.
# force-recreate plus EXIT/INT/TERM traps clean this project only.
#
# Usage: cluster-mixed-smoke.sh <overlay>
#   overlay: java-leader
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${ROOT}/docker/cluster_mixed"
COMPOSE_BASE="${COMPOSE_DIR}/compose.yaml"
GO_MOD="${ROOT}/go.mod"

if [[ "$#" -ne 1 || "$1" != "java-leader" ]]; then
	echo "usage: $0 java-leader" >&2
	exit 1
fi

OVERLAY_ARG="$1"
OVERLAY_FILE="${COMPOSE_DIR}/compose.java-leader.yaml"
GO_SERVICE="go-java-leader"
OUTER_TIMEOUT_S=130

export HERON_COMPOSE_PROJECT="heron-cluster-mixed-$$"
COMPOSE_CMD=(
	docker compose
	-p "${HERON_COMPOSE_PROJECT}"
	-f "${COMPOSE_BASE}"
	-f "${OVERLAY_FILE}"
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

# Wait on the Go service only. Cluster daemons never exit. Build the Java image
# via cluster-0 and the Go service. cluster-1 reuses the cluster-0 image.
# aeronmd uses the pre-tagged image. Build stays outside the outer timeout
# (image build wall clock varies).
compose build cluster-0 "${GO_SERVICE}"

smoke_logs() {
	compose logs --no-color aeronmd cluster-0 cluster-1 "${GO_SERVICE}"
}

# compose up waits for depends_on healthchecks. go-java-leader readiness file is the check.
# Outer timeout bounds that compose wait. Go binary uses its own -test.timeout.
run_smoke() {
	compose up -d --force-recreate --no-build || return
	if command -v timeout >/dev/null 2>&1; then
		timeout "${OUTER_TIMEOUT_S}" "${COMPOSE_CMD[@]}" wait "${GO_SERVICE}"
	else
		compose wait "${GO_SERVICE}"
	fi
}

run_smoke || {
	status=$?
	echo "heron: cluster mixed smoke failed (overlay=${OVERLAY_ARG})" >&2
	smoke_logs >&2 || true
	exit "${status}"
}

echo "heron: cluster mixed smoke passed (overlay=${OVERLAY_ARG})"
smoke_logs || echo "heron: warning: could not fetch logs (smoke passed)" >&2

#!/usr/bin/env bash
# Bidirectional Java/Go IPC smoke: compose up -d (aeronmd + go + java), then
# wait for go and java exit 0. Go runs TestJavaInteropDuplex; Java runs JavaDuplex.
# Requires java-interop-aeronmd image (make smoke-java-interop runs aeronmd-compose-image).
#
# Per-run compose project (-p luas-java-interop-$$) so concurrent smokes do not
# share stacks. force-recreate avoids reusing leftover one-shot go/java under the
# same project if a prior trap failed. EXIT/INT/TERM run compose down for this
# project only.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="${ROOT}/test/integration/java_interop/docker/compose.yaml"
# Unique project per process: concurrent smokes; no shared containers/volumes.
COMPOSE_PROJECT="luas-java-interop-$$"
COMPOSE_CMD=(
	docker compose
	-p "${COMPOSE_PROJECT}"
	-f "${COMPOSE}"
)

compose() {
	"${COMPOSE_CMD[@]}" "$@"
}

compose_cleanup() {
	compose down -v >/dev/null 2>&1 || true
}

if ! command -v docker >/dev/null 2>&1; then
	echo "luas: docker required" >&2
	exit 1
fi

GO_VERSION="$(awk '/^go / {print $2; exit}' "${ROOT}/go.mod")"
export DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 GO_VERSION
LUAS_COMPOSE_UID="$(id -u)"
export LUAS_COMPOSE_UID
LUAS_COMPOSE_GID="$(id -g)"
export LUAS_COMPOSE_GID

if ! docker image inspect java-interop-aeronmd >/dev/null 2>&1; then
	echo "luas: missing image java-interop-aeronmd; run: make aeronmd-compose-image" >&2
	exit 1
fi

trap compose_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Build app services only; aeronmd uses pre-tagged java-interop-aeronmd.
compose build go java
compose up -d --force-recreate --no-build

smoke_logs() {
	compose logs --no-color go java
}

compose wait go java || {
	status=$?
	echo "luas: java interop smoke failed" >&2
	smoke_logs >&2
	exit "${status}"
}

echo "luas: java interop smoke passed"
smoke_logs

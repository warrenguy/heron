#!/usr/bin/env bash
# Bidirectional Java/Go IPC smoke: compose up -d (aeronmd + go + java), then
# wait for go and java exit 0. Go runs TestJavaInteropDuplex; Java runs JavaDuplex.
# Requires java-interop-aeronmd image (make test-java-interop runs aeronmd-compose-image).
#
# Per-run compose project (-p heron-java-interop-$$) so concurrent smokes do not
# share stacks. force-recreate avoids reusing leftover one-shot go/java under the
# same project if a prior trap failed. EXIT/INT/TERM run compose down for this
# project only.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="${ROOT}/docker/java-interop/compose.yaml"
GO_MOD="${ROOT}/go.mod"
# Unique project per process: concurrent smokes; no shared containers/volumes.
COMPOSE_PROJECT="heron-java-interop-$$"
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

# Build app services only; aeronmd uses pre-tagged java-interop-aeronmd.
compose build go java
compose up -d --force-recreate --no-build

smoke_logs() {
	compose logs --no-color go java
}

compose wait go java || {
	status=$?
	echo "heron: java interop smoke failed" >&2
	smoke_logs >&2
	exit "${status}"
}

echo "heron: java interop smoke passed"
smoke_logs

#!/usr/bin/env bash
# Bidirectional Java/Go IPC smoke: compose up -d (aeronmd + go + java), then
# wait for go and java exit 0. Go runs TestJavaInteropDuplex; Java runs JavaDuplex.
# See scripts/cluster-java-smoke.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=SCRIPTDIR/lib/versions.sh
source "${ROOT}/scripts/lib/versions.sh"
COMPOSE="${ROOT}/docker/java-interop/compose.yaml"
VERSIONS="${ROOT}/specs/versions.yaml"
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

require_aeron_versions "${VERSIONS}" || exit 1
export AERON_REPOSITORY AERON_REF AERON_DRIVER_ARTIFACT

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID
export GO_VERSION

trap 'compose down -v' EXIT

compose build --parallel

# up -d + wait go: --exit-code-from implies --abort-on-container-exit, which
# stops the Go duplex test when the Java one-shot duplex main exits.
compose up -d --no-build

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

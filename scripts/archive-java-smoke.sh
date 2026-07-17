#!/usr/bin/env bash
# Archive Java/Go IPC smoke: compose up -d (aeronmd + archive + go), then wait for go
# exit 0. Go runs TestArchiveRecordingLog (record, recording.log, PlanRecovery,
# replay); archive runs standalone Archive.launch.
#
# Go test invocation: docker/archive_java/go/Dockerfile CMD (-test.run
# TestArchiveRecordingLog, -test.timeout=120s).
#
# Per-run compose project (-p heron-archive-java-$$) so concurrent smokes do not
# share stacks. force-recreate avoids reusing a leftover one-shot go under the
# same project if a prior trap failed. EXIT/INT/TERM run compose down for this
# project only.
#
# Outer timeout 130s: 120s test budget plus compose teardown (see wait block below).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="${ROOT}/docker/archive_java/compose.yaml"
GO_MOD="${ROOT}/go.mod"
# Unique project per process: concurrent smokes; no shared containers/volumes.
COMPOSE_PROJECT="heron-archive-java-$$"
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

# up -d + wait go only: archive is a long-running daemon (ShutdownSignalBarrier);
# compose wait archive would block forever. Go one-shot test exits when
# TestArchiveRecordingLog passes.
compose build archive go
compose up -d --force-recreate --no-build

smoke_logs() {
	compose logs --no-color archive go
}

# Outer timeout 130s: 120s test budget plus compose teardown. Build/up not wrapped.

if command -v timeout >/dev/null 2>&1; then
	timeout 130 "${COMPOSE_CMD[@]}" wait go
else
	compose wait go
fi || {
	status=$?
	echo "heron: archive java smoke failed" >&2
	smoke_logs >&2
	exit "${status}"
}

echo "heron: archive java smoke passed"
smoke_logs

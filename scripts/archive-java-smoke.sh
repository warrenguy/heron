#!/usr/bin/env bash
# Archive Java/Go IPC smoke: compose up -d (aeronmd + archive + go), then wait for go
# exit 0. Go runs TestArchiveRecordingLog (record, recording.log, PlanRecovery,
# replay) and TestArchiveTruncatePath (truncate, live refusals, catalog stop);
# archive runs standalone Archive.launch.
#
# Per-run compose project (-p luas-archive-java-$$) so concurrent smokes do not
# share stacks. force-recreate avoids reusing a leftover one-shot go under the
# same project if a prior trap failed. EXIT/INT/TERM run compose down for this
# project only.
#
# Outer timeout 130s: 120s test budget plus compose teardown (see wait block below).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="${ROOT}/test/integration/archive_java/docker/compose.yaml"
# Unique project per process: concurrent smokes; no shared containers/volumes.
COMPOSE_PROJECT="luas-archive-java-$$"
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

# up -d + wait go only: archive is a long-running daemon (ShutdownSignalBarrier);
# compose wait archive would block forever. Go one-shot test exits when
# TestArchiveRecordingLog and TestArchiveTruncatePath pass.
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
	echo "luas: archive java smoke failed" >&2
	smoke_logs >&2
	exit "${status}"
}

echo "luas: archive java smoke passed"
smoke_logs

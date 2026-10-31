#!/usr/bin/env bash
# Mixed Go/Java cluster compose smoke.
#
# Isolation per run:
# - Compose project HERON_COMPOSE_PROJECT=heron-cluster-mixed-$$ with -p
# - Per-run /24 under 172.30.0.0/16 (third octet from shell PID and attempt)
#
# Each overlay uses a disjoint third-octet range. Parallel mixed smokes under
# make smokes therefore start on different /24 pools.
#
# The PID walks the range. Docker IPAM is daemon-global, so another process or
# a foreign 172.30.x.0/24 may still hold the pick. compose up then fails on pool
# overlap. run_smoke retries the next octet in range.
#
# force-recreate plus EXIT/INT/TERM traps clean this project only.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${ROOT}/test/integration/cluster_mixed/docker"
COMPOSE_BASE="${COMPOSE_DIR}/compose.yaml"

usage() {
	echo "usage: $0 java-leader|go-leader|ex-leader-rejoin|java-leader-snapshot|go-leader-snapshot" >&2
	exit 1
}

# Outer wall-clock cap for the compose run. The 130s default covers a 120s Go
# -test.timeout plus teardown. 310s covers a 300s -test.timeout plus teardown.
OUTER_TIMEOUT_S=130

# Max subnet picks before compose up gives up. Each attempt walks one octet.
SUBNET_ATTEMPTS=5

[[ "$#" -eq 1 ]] || usage

# Ranges skip octet 0. A smoke must not use 172.30.0.0/24.
OCTET_SPAN=50
OVERLAY_ARG="$1"
OVERLAY_FILES=()
case "${OVERLAY_ARG}" in
java-leader)
	OVERLAY_FILES=("${COMPOSE_DIR}/compose.java-leader.yaml")
	SMOKE_NAME="TestClusterMixedJavaLeader"
	OCTET_BASE=1
	;;
go-leader)
	OVERLAY_FILES=("${COMPOSE_DIR}/compose.go-leader.yaml")
	SMOKE_NAME="TestClusterMixedGoLeader"
	OCTET_BASE=51
	;;
ex-leader-rejoin)
	OVERLAY_FILES=("${COMPOSE_DIR}/compose.ex-leader-rejoin.yaml")
	SMOKE_NAME="TestClusterMixedGoExLeaderRejoin"
	OCTET_BASE=101
	OUTER_TIMEOUT_S=310
	;;
java-leader-snapshot)
	OVERLAY_FILES=(
		"${COMPOSE_DIR}/compose.java-leader.yaml"
		"${COMPOSE_DIR}/compose.java-leader-snapshot.yaml"
	)
	SMOKE_NAME="TestClusterMixedJavaLeaderSnapshotRestart"
	OCTET_BASE=151
	OUTER_TIMEOUT_S=310
	;;
go-leader-snapshot)
	OVERLAY_FILES=(
		"${COMPOSE_DIR}/compose.go-leader.yaml"
		"${COMPOSE_DIR}/compose.go-leader-snapshot.yaml"
	)
	SMOKE_NAME="TestClusterMixedGoLeaderSnapshotRestart"
	OCTET_BASE=201
	OUTER_TIMEOUT_S=310
	;;
*)
	usage
	;;
esac

export HERON_COMPOSE_PROJECT="heron-cluster-mixed-$$"

# Export one /24 for this attempt. Hosts stay .10/.11/.12 inside that subnet.
# Multicast log interface=172.30.0.0/16 covers every third octet.
pick_subnet() {
	local attempt="$1"
	local octet=$((($$ + attempt) % OCTET_SPAN + OCTET_BASE))

	export HERON_CLUSTER_MIXED_SUBNET="172.30.${octet}.0/24"
	export HERON_CLUSTER_MIXED_IP0="172.30.${octet}.10"
	export HERON_CLUSTER_MIXED_IP1="172.30.${octet}.11"
	export HERON_CLUSTER_MIXED_IP2="172.30.${octet}.12"
	export HERON_CLUSTER_HOSTNAMES="${HERON_CLUSTER_MIXED_IP0},\
${HERON_CLUSTER_MIXED_IP1},${HERON_CLUSTER_MIXED_IP2}"
}

COMPOSE_CMD=(
	docker compose
	-p "${HERON_COMPOSE_PROJECT}"
	-f "${COMPOSE_BASE}"
)
for overlay in "${OVERLAY_FILES[@]}"; do
	COMPOSE_CMD+=(-f "${overlay}")
done

compose() {
	"${COMPOSE_CMD[@]}" "$@"
}

compose_cleanup() {
	compose down -v >/dev/null 2>&1 || true
	[[ -n "${UP_LOG:-}" ]] && rm -f "${UP_LOG}"
}

if ! command -v docker >/dev/null 2>&1; then
	echo "heron: docker required" >&2
	exit 1
fi

GO_VERSION="$(awk '/^go / {print $2; exit}' "${ROOT}/go.mod")"
export DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 GO_VERSION
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID

if ! docker image inspect java-interop-aeronmd >/dev/null 2>&1; then
	echo "heron: missing image java-interop-aeronmd; run: make aeronmd-compose-image" >&2
	exit 1
fi

UP_LOG="$(mktemp)"

trap compose_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Build the Java image via cluster-0, the Go member Archive, and the Go service.
# cluster-1 reuses the cluster-0 image. aeronmd uses the pre-tagged image.
#
# Build stays outside the outer timeout. Image build wall clock varies.
compose build cluster-0 archive-go go

smoke_logs() {
	compose logs --no-color aeronmd archive-go cluster-0 cluster-1 go
}

# True when Docker IPAM refused the subnet because another network holds it.
# Only that failure triggers a retry. Every other compose up failure returns.
is_pool_conflict() {
	grep -qE 'Pool overlaps|predefined address pools' "${UP_LOG}"
}

# compose up waits for depends_on healthchecks. Go readiness file is the check.
#
# Outer timeout bounds that compose wait. Go binary uses its own -test.timeout.
run_smoke() {
	local attempt status=1

	for ((attempt = 0; attempt < SUBNET_ATTEMPTS; attempt++)); do
		pick_subnet "${attempt}"

		# tee prints compose up output. UP_LOG stores the same stream for is_pool_conflict.
		if compose up -d --force-recreate --no-build 2>&1 | tee "${UP_LOG}"; then
			status=0
			break
		fi
		status="${PIPESTATUS[0]}"
		is_pool_conflict || return "${status}"
		echo "heron: subnet ${HERON_CLUSTER_MIXED_SUBNET} taken; retrying" >&2
		compose down -v >/dev/null 2>&1 || true
	done

	if [[ "${status}" -ne 0 ]]; then
		echo "heron: no free /24 after ${SUBNET_ATTEMPTS} picks" >&2
		return "${status}"
	fi

	if command -v timeout >/dev/null 2>&1; then
		timeout "${OUTER_TIMEOUT_S}" "${COMPOSE_CMD[@]}" wait go
	else
		compose wait go
	fi
}

run_smoke || {
	status=$?
	echo "heron: cluster mixed smoke failed (overlay=${OVERLAY_ARG})" >&2
	smoke_logs >&2 || true
	exit "${status}"
}

# One log fetch feeds require_smoke_ran and the final dump.
SMOKE_LOGS="$(smoke_logs)" || {
	echo "heron: could not read compose logs for smoke check" >&2
	exit 1
}

# Fail closed when compose exits zero without --- PASS for SMOKE_NAME. Compose
# wait alone accepts a skip or an empty -test.run match. Only the Go test
# binary emits go test markers, so the all-service log capture cannot match a
# foreign PASS. Named PASS already rejects top-level skip. TestMain rejects a
# green exit when the overlay gate was never met.
require_smoke_ran() {
	if ! grep -qF -- "--- PASS: ${SMOKE_NAME}" <<<"${SMOKE_LOGS}"; then
		echo "heron: mixed smoke missing --- PASS: ${SMOKE_NAME}" >&2
		return 1
	fi
}

require_smoke_ran || {
	echo "${SMOKE_LOGS}" >&2
	exit 1
}

echo "heron: cluster mixed smoke passed (overlay=${OVERLAY_ARG})"
echo "${SMOKE_LOGS}"

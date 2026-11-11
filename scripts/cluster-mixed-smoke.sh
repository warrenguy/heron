#!/usr/bin/env bash
# Mixed Go/Java cluster compose smoke.
#
# Isolation per run:
# - Compose project LUAS_COMPOSE_PROJECT=luas-cluster-mixed-$$ with -p
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

# Overlay order fixes each overlay's third-octet slot. Ranges skip octet 0.
# A smoke must not use 172.30.0.0/24. OCTET_SPAN follows the overlay count,
# so every range stays inside 1..254.
OVERLAYS=(
	java-leader
	go-leader
	go-leader-unicast
	ex-leader-rejoin
	java-leader-snapshot
	go-leader-snapshot
)
OCTET_SPAN=$((254 / ${#OVERLAYS[@]}))

usage() {
	local IFS='|'
	echo "usage: $0 ${OVERLAYS[*]}" >&2
	exit 1
}

# Outer wall-clock cap for compose wait. Each cap is the overlay -test.timeout
# plus 10s.
OUTER_TIMEOUT_S=130

# Max subnet picks before compose up gives up. Each attempt walks one octet.
SUBNET_ATTEMPTS=5

[[ "$#" -eq 1 ]] || usage

OVERLAY_ARG="$1"
OCTET_BASE=
for slot in "${!OVERLAYS[@]}"; do
	[[ "${OVERLAYS[slot]}" == "${OVERLAY_ARG}" ]] || continue
	OCTET_BASE=$((1 + slot * OCTET_SPAN))
done
[[ -n "${OCTET_BASE}" ]] || usage

# Layered overlays override below.
OVERLAY_FILES=("${COMPOSE_DIR}/compose.${OVERLAY_ARG}.yaml")
case "${OVERLAY_ARG}" in
java-leader)
	SMOKE_NAME="TestClusterMixedJavaLeader"
	;;
go-leader)
	SMOKE_NAME="TestClusterMixedGoLeader"
	OUTER_TIMEOUT_S=150
	;;
go-leader-unicast)
	OVERLAY_FILES=(
		"${COMPOSE_DIR}/compose.go-leader.yaml"
		"${COMPOSE_DIR}/compose.go-leader-unicast.yaml"
	)
	SMOKE_NAME="TestClusterMixedGoLeaderUnicast"
	OUTER_TIMEOUT_S=150
	;;
ex-leader-rejoin)
	SMOKE_NAME="TestClusterMixedGoExLeaderRejoin"
	OUTER_TIMEOUT_S=310
	;;
java-leader-snapshot)
	OVERLAY_FILES=(
		"${COMPOSE_DIR}/compose.java-leader.yaml"
		"${COMPOSE_DIR}/compose.java-leader-snapshot.yaml"
	)
	SMOKE_NAME="TestClusterMixedJavaLeaderSnapshotRestart"
	OUTER_TIMEOUT_S=310
	;;
go-leader-snapshot)
	OVERLAY_FILES=(
		"${COMPOSE_DIR}/compose.go-leader.yaml"
		"${COMPOSE_DIR}/compose.go-leader-snapshot.yaml"
	)
	SMOKE_NAME="TestClusterMixedGoLeaderSnapshotRestart"
	OUTER_TIMEOUT_S=310
	;;
*)
	usage
	;;
esac

export LUAS_COMPOSE_PROJECT="luas-cluster-mixed-$$"

# Export one /24 for this attempt. Hosts stay .10/.11/.12 inside that subnet.
# Multicast log interface=172.30.0.0/16 covers every third octet.
pick_subnet() {
	local attempt="$1"
	local octet=$((($$ + attempt) % OCTET_SPAN + OCTET_BASE))

	export LUAS_CLUSTER_MIXED_SUBNET="172.30.${octet}.0/24"
	export LUAS_CLUSTER_MIXED_IP0="172.30.${octet}.10"
	export LUAS_CLUSTER_MIXED_IP1="172.30.${octet}.11"
	export LUAS_CLUSTER_MIXED_IP2="172.30.${octet}.12"
	export LUAS_CLUSTER_HOSTNAMES="${LUAS_CLUSTER_MIXED_IP0},\
${LUAS_CLUSTER_MIXED_IP1},${LUAS_CLUSTER_MIXED_IP2}"
}

COMPOSE_CMD=(
	docker compose
	-p "${LUAS_COMPOSE_PROJECT}"
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
		echo "luas: subnet ${LUAS_CLUSTER_MIXED_SUBNET} taken; retrying" >&2
		compose down -v >/dev/null 2>&1 || true
	done

	if [[ "${status}" -ne 0 ]]; then
		echo "luas: no free /24 after ${SUBNET_ATTEMPTS} picks" >&2
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
	echo "luas: cluster mixed smoke failed (overlay=${OVERLAY_ARG})" >&2
	smoke_logs >&2 || true
	exit "${status}"
}

# One log fetch feeds require_smoke_ran and the final dump.
SMOKE_LOGS="$(smoke_logs)" || {
	echo "luas: could not read compose logs for smoke check" >&2
	exit 1
}

# Fail closed when compose exits zero without --- PASS for SMOKE_NAME. Compose
# wait alone accepts a skip or an empty -test.run match. Only the Go test
# binary emits go test markers, so the all-service log capture cannot match a
# foreign PASS. Named PASS already rejects top-level skip. TestMain rejects a
# green exit when the overlay gate was never met.
require_smoke_ran() {
	if ! grep -qE -- "--- PASS: ${SMOKE_NAME} \(" <<<"${SMOKE_LOGS}"; then
		echo "luas: mixed smoke missing --- PASS: ${SMOKE_NAME}" >&2
		return 1
	fi
}

require_smoke_ran || {
	echo "${SMOKE_LOGS}" >&2
	exit 1
}

echo "luas: cluster mixed smoke passed (overlay=${OVERLAY_ARG})"
echo "${SMOKE_LOGS}"
